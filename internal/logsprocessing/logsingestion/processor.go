// Copyright (C) 2025 CardinalHQ, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, version 3.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package logsingestion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"strings"
	"time"

	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
	"github.com/cardinalhq/lakerunner/internal/processing/ingest"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// hourSlotKey uniquely identifies a writer for a specific hour and slot combination
type hourSlotKey struct {
	dateint int32
	hour    int
	slot    int
}

// writerManager manages multiple parquet writers, one per hour/slot combination
type writerManager struct {
	writers       map[hourSlotKey]*parquetwriter.UnifiedWriter
	tmpdir        string
	orgID         string
	ingestDateint int32
	rpfEstimate   int64
	ll            *slog.Logger
}

func newWriterManager(tmpdir, orgID string, ingestDateint int32, rpfEstimate int64, ll *slog.Logger) *writerManager {
	return &writerManager{
		writers:       make(map[hourSlotKey]*parquetwriter.UnifiedWriter),
		tmpdir:        tmpdir,
		orgID:         orgID,
		ingestDateint: ingestDateint,
		rpfEstimate:   rpfEstimate,
		ll:            ll,
	}
}

// processBatch efficiently processes an entire batch, grouping rows by hour/slot
func (wm *writerManager) processBatch(batch *pipeline.Batch) (processedCount, errorCount int64) {
	batchGroups := make(map[hourSlotKey]*pipeline.Batch)

	// First pass: group rows by hour/slot
	for i := 0; i < batch.Len(); i++ {
		row := batch.Get(i)
		if row == nil {
			wm.ll.Error("Row is nil - skipping", slog.Int("rowIndex", i))
			errorCount++
			continue
		}

		// Extract timestamp
		ts, ok := row[wkk.RowKeyCTimestamp].(int64)
		if !ok {
			wm.ll.Error("_cardinalhq.timestamp field is missing or not int64 - skipping row", slog.Int("rowIndex", i))
			errorCount++
			continue
		}

		// Determine hour/slot
		dateint, hour16 := helpers.MSToDateintHour(ts)
		hour := int(hour16)
		slot := 0
		key := hourSlotKey{dateint, hour, slot}

		// Create or get batch for this group
		if batchGroups[key] == nil {
			batchGroups[key] = pipeline.GetBatch()
		}

		// Add row to the appropriate batch group
		newRow := batchGroups[key].AddRow()
		maps.Copy(newRow, row)
	}

	// Second pass: write each grouped batch to its writer
	for key, groupedBatch := range batchGroups {
		writer, err := wm.getWriter(key)
		if err != nil {
			wm.ll.Error("Failed to get writer for batch group",
				slog.Any("key", key),
				slog.String("error", err.Error()))
			errorCount += int64(groupedBatch.Len())
			pipeline.ReturnBatch(groupedBatch)
			continue
		}

		if err := writer.WriteBatch(groupedBatch); err != nil {
			wm.ll.Error("Failed to write batch group",
				slog.Any("key", key),
				slog.Int("batchSize", groupedBatch.Len()),
				slog.String("error", err.Error()))
			errorCount += int64(groupedBatch.Len())
		} else {
			processedCount += int64(groupedBatch.Len())
		}

		pipeline.ReturnBatch(groupedBatch)
	}

	return processedCount, errorCount
}

// getWriter returns the writer for a specific hour/slot, creating it if necessary
func (wm *writerManager) getWriter(key hourSlotKey) (*parquetwriter.UnifiedWriter, error) {
	if writer, exists := wm.writers[key]; exists {
		return writer, nil
	}

	writer, err := factories.NewLogsWriter(wm.tmpdir, wm.rpfEstimate)
	if err != nil {
		return nil, fmt.Errorf("failed to create logs writer: %w", err)
	}

	wm.writers[key] = writer
	wm.ll.Debug("Created new log writer",
		slog.String("orgID", wm.orgID),
		slog.Int("dateint", int(key.dateint)),
		slog.Int("hour", key.hour),
		slog.Int("slot", key.slot))

	return writer, nil
}

// closeAll closes all writers and returns their results
func (wm *writerManager) closeAll(ctx context.Context) ([]parquetwriter.Result, error) {
	var allResults []parquetwriter.Result
	var errs []error

	for key, writer := range wm.writers {
		results, err := writer.Close(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to close writer %v: %w", key, err))
			continue
		}
		allResults = append(allResults, results...)
		wm.ll.Debug("Closed log writer",
			slog.String("orgID", wm.orgID),
			slog.Int("dateint", int(key.dateint)),
			slog.Int("hour", key.hour),
			slog.Int("slot", key.slot),
			slog.Int("fileCount", len(results)))
	}

	if len(errs) > 0 {
		return allResults, errors.Join(errs...)
	}

	return allResults, nil
}

// createLogReader creates the appropriate filereader based on file type
func createLogReader(filename string) (filereader.Reader, error) {
	return filereader.ReaderForFile(filename, filereader.SignalTypeLogs)
}

// queueLogCompactionForSlot queues a log compaction job for a specific slot
func queueLogCompactionForSlot(ctx context.Context, mdb lrdb.StoreFull, item ingest.IngestItem, slotID int, dateint int32, hourAlignedTS int64) error {
	startTime := time.UnixMilli(hourAlignedTS).UTC()
	endTime := startTime.Add(time.Hour)

	return mdb.WorkQueueAdd(ctx, lrdb.WorkQueueAddParams{
		OrgID:      item.OrganizationID,
		Instance:   item.InstanceNum,
		Signal:     lrdb.SignalEnumLogs,
		Action:     lrdb.ActionEnumCompact,
		Dateint:    dateint,
		Frequency:  -1,
		SlotID:     int32(slotID),
		TsRange:    helpers.TimeRange{Start: startTime, End: endTime}.ToPgRange(),
		RunnableAt: time.Now().UTC().Add(5 * time.Minute),
	})
}

// WorkerInterrupted is returned when processing is safely interrupted
type WorkerInterrupted struct {
	message string
}

func (e WorkerInterrupted) Error() string {
	return e.message
}

func NewWorkerInterrupted(message string) WorkerInterrupted {
	return WorkerInterrupted{message: message}
}

// ProcessBatch processes a single ingestion item with atomic transaction including Kafka offset
func ProcessBatch(ctx context.Context, args ingest.ProcessBatchArgs, item ingest.IngestItem) error {

	ll := logctx.FromContext(ctx)
	ll.Debug("Processing log item with Kafka offset",
		slog.String("consumerGroup", args.KafkaOffset.ConsumerGroup),
		slog.String("topic", args.KafkaOffset.Topic),
		slog.Int("partition", int(args.KafkaOffset.Partition)),
		slog.Int64("offset", args.KafkaOffset.Offset))

	// Get storage profile and S3 client
	var profile storageprofile.StorageProfile
	var err error

	if collectorName := helpers.ExtractCollectorName(item.ObjectID); collectorName != "" {
		profile, err = args.StorageProvider.GetStorageProfileForOrganizationAndCollector(ctx, item.OrganizationID, collectorName)
		if err != nil {
			return fmt.Errorf("failed to get storage profile for collector %s: %w", collectorName, err)
		}
	} else {
		profile, err = args.StorageProvider.GetStorageProfileForOrganizationAndInstance(ctx, item.OrganizationID, item.InstanceNum)
		if err != nil {
			return fmt.Errorf("failed to get storage profile: %w", err)
		}
	}

	s3client, err := args.AWSManager.GetS3ForProfile(ctx, profile)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
	}

	// Create writer manager for organizing output by hour/slot
	wm := newWriterManager(args.TmpDir, item.OrganizationID.String(), args.IngestDateint, args.RPFEstimate, ll)

	// Track total rows across all files
	var batchRowsRead, batchRowsProcessed, batchRowsErrored int64

	// Check for context cancellation before processing
	if err := ctx.Err(); err != nil {
		ll.Info("Context cancelled during batch processing - safe interruption point",
			slog.Any("error", err))
		return NewWorkerInterrupted("context cancelled during file processing")
	}

	ll.Debug("Processing batch item with filereader",
		slog.String("objectID", item.ObjectID),
		slog.Int64("fileSize", item.FileSize))

	// Skip database files (processed outputs, not inputs)
	if strings.HasPrefix(item.ObjectID, "db/") {
		ll.Debug("Skipping database file", slog.String("objectID", item.ObjectID))
		// No segments to insert, just update Kafka offset
		if err := args.DB.KafkaJournalUpsert(ctx, lrdb.KafkaJournalUpsertParams{
			ConsumerGroup:       args.KafkaOffset.ConsumerGroup,
			Topic:               args.KafkaOffset.Topic,
			Partition:           args.KafkaOffset.Partition,
			LastProcessedOffset: args.KafkaOffset.Offset,
		}); err != nil {
			return fmt.Errorf("failed to update Kafka offset: %w", err)
		}
		return nil
	}

	// Download file
	itemTmpdir := fmt.Sprintf("%s/item", args.TmpDir)
	if err := os.MkdirAll(itemTmpdir, 0755); err != nil {
		return fmt.Errorf("creating item tmpdir: %w", err)
	}

	tmpfilename, _, is404, err := storageClient.DownloadObject(ctx, itemTmpdir, item.Bucket, item.ObjectID)
	if err != nil {
		return fmt.Errorf("failed to download file %s: %w", item.ObjectID, err)
	}
	if is404 {
		ll.Warn("S3 object not found, skipping", slog.String("objectID", item.ObjectID))
		// No segments to insert, just update Kafka offset
		if err := args.DB.KafkaJournalUpsert(ctx, lrdb.KafkaJournalUpsertParams{
			ConsumerGroup:       args.KafkaOffset.ConsumerGroup,
			Topic:               args.KafkaOffset.Topic,
			Partition:           args.KafkaOffset.Partition,
			LastProcessedOffset: args.KafkaOffset.Offset,
		}); err != nil {
			return fmt.Errorf("failed to update Kafka offset: %w", err)
		}
		return nil
	}

	// Create appropriate reader for the file type
	var reader filereader.Reader

	reader, err = createLogReader(tmpfilename)
	if err == nil {
		// Add general translator for non-protobuf files
		translator := &LogTranslator{
			orgID:    item.OrganizationID.String(),
			bucket:   item.Bucket,
			objectID: item.ObjectID,
		}
		reader, err = filereader.NewTranslatingReader(reader, translator, 1000)
	}

	if err != nil {
		ll.Warn("Unsupported or problematic file type, skipping",
			slog.String("objectID", item.ObjectID),
			slog.String("error", err.Error()))
		// No segments to insert, just update Kafka offset
		if err := args.DB.KafkaJournalUpsert(ctx, lrdb.KafkaJournalUpsertParams{
			ConsumerGroup:       args.KafkaOffset.ConsumerGroup,
			Topic:               args.KafkaOffset.Topic,
			Partition:           args.KafkaOffset.Partition,
			LastProcessedOffset: args.KafkaOffset.Offset,
		}); err != nil {
			return fmt.Errorf("failed to update Kafka offset: %w", err)
		}
		return nil
	}

	// Process all rows from the file
	var processedCount, errorCount int64
	for {
		batch, err := reader.Next(ctx)

		// Process any rows we got, even if EOF
		if batch != nil {
			batchProcessed, batchErrors := wm.processBatch(batch)
			processedCount += batchProcessed
			errorCount += batchErrors
			pipeline.ReturnBatch(batch)

			if batchErrors > 0 {
				ll.Warn("Some rows failed to process in batch",
					slog.String("objectID", item.ObjectID),
					slog.Int64("processedRows", batchProcessed),
					slog.Int64("errorRows", batchErrors))
			}
		}

		// Break after processing if we hit EOF or other errors
		if err == io.EOF {
			break
		}
		if err != nil {
			if closeErr := reader.Close(); closeErr != nil {
				ll.Warn("Failed to close reader after read error", slog.String("objectID", item.ObjectID), slog.Any("error", closeErr))
			}
			return fmt.Errorf("failed to read from file %s: %w", item.ObjectID, err)
		}
	}

	// Get total rows read from the reader
	fileRowsRead := reader.TotalRowsReturned()

	ll.Debug("File processing completed",
		slog.String("objectID", item.ObjectID),
		slog.Int64("rowsRead", fileRowsRead),
		slog.Int64("rowsProcessed", processedCount),
		slog.Int64("rowsErrored", errorCount))

	if errorCount > 0 {
		ll.Warn("Some rows were dropped due to processing errors",
			slog.String("objectID", item.ObjectID),
			slog.Int64("droppedRows", errorCount),
			slog.Float64("dropRate", float64(errorCount)/float64(fileRowsRead)*100))
	}
	if closeErr := reader.Close(); closeErr != nil {
		ll.Warn("Failed to close reader", slog.String("objectID", item.ObjectID), slog.Any("error", closeErr))
	}

	// Update batch totals
	batchRowsRead += fileRowsRead
	batchRowsProcessed += processedCount
	batchRowsErrored += errorCount

	ll.Debug("Completed processing file", slog.String("objectID", item.ObjectID))

	// Close all writers and get results
	ll.Debug("Closing writers", slog.Int("writerCount", len(wm.writers)))
	results, err := wm.closeAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to close writers: %w", err)
	}
	ll.Debug("Closed writers", slog.Int("resultCount", len(results)))

	// Calculate total output records across all result files
	var totalOutputRecords int64
	for _, result := range results {
		totalOutputRecords += result.RecordCount
	}

	ll.Debug("Batch processing summary",
		slog.Int64("inputRowsRead", batchRowsRead),
		slog.Int64("inputRowsProcessed", batchRowsProcessed),
		slog.Int64("inputRowsErrored", batchRowsErrored),
		slog.Int64("outputRecordsWritten", totalOutputRecords),
		slog.Int("outputFiles", len(results)))

	if len(results) == 0 {
		ll.Warn("No output files generated despite reading rows",
			slog.Int64("rowsRead", batchRowsRead),
			slog.Int64("rowsErrored", batchRowsErrored))
		// No segments to insert, just update Kafka offset
		if err := args.DB.KafkaJournalUpsert(ctx, lrdb.KafkaJournalUpsertParams{
			ConsumerGroup:       args.KafkaOffset.ConsumerGroup,
			Topic:               args.KafkaOffset.Topic,
			Partition:           args.KafkaOffset.Partition,
			LastProcessedOffset: args.KafkaOffset.Offset,
		}); err != nil {
			return fmt.Errorf("failed to update Kafka offset: %w", err)
		}
		return nil
	}

	// The critical check: processed rows should equal written records
	if batchRowsProcessed != totalOutputRecords {
		ll.Error("CRITICAL: Row count mismatch between processed and written",
			slog.Int64("rowsProcessed", batchRowsProcessed),
			slog.Int64("recordsWritten", totalOutputRecords),
			slog.Int64("difference", batchRowsProcessed-totalOutputRecords))
		return fmt.Errorf("data loss detected: %d rows processed but only %d written", batchRowsProcessed, totalOutputRecords)
	}

	// Also report if we had expected failures
	if batchRowsErrored > 0 {
		ll.Warn("Some input rows were dropped due to processing errors",
			slog.Int64("totalDropped", batchRowsErrored),
			slog.Float64("dropRate", float64(batchRowsErrored)/float64(batchRowsRead)*100))
	}

	// Final interruption check before critical section (S3 uploads + DB inserts)
	if err := ctx.Err(); err != nil {
		ll.Info("Context cancelled before S3 upload phase - safe interruption point",
			slog.Int("resultCount", len(results)),
			slog.Any("error", err))
		return NewWorkerInterrupted("context cancelled before S3 upload phase")
	}

	// Upload files to S3 and collect segment parameters for batch insertion
	segmentParams, err := createAndUploadLogSegments(ctx, ll, storageClient, results, item, args.IngestDateint)
	if err != nil {
		return fmt.Errorf("failed to create and upload log segments: %w", err)
	}

	ll.Debug("Log ingestion batch summary",
		slog.Int("inputFileCount", 1),
		slog.Int64("totalInputBytes", item.FileSize),
		slog.Int("outputFileCount", len(results)),
		slog.Int64("rowsRead", batchRowsRead),
		slog.Int64("rowsProcessed", batchRowsProcessed),
		slog.Int64("rowsErrored", batchRowsErrored))

	// Execute the atomic transaction: insert all segments + Kafka offset
	batch := lrdb.LogSegmentBatch{
		Segments:    segmentParams,
		KafkaOffset: args.KafkaOffset,
	}

	criticalCtx := context.WithoutCancel(ctx)
	if err := args.DB.InsertLogSegmentBatchWithKafkaOffset(criticalCtx, batch); err != nil {
		return fmt.Errorf("failed to insert log segments with Kafka offset: %w", err)
	}

	// Queue compaction work for each segment
	slotHourTriggers := make(map[hourSlotKey]int64) // Track earliest timestamp per slot/hour for compaction
	for _, segParams := range segmentParams {
		dateint, hour16 := helpers.MSToDateintHour(segParams.StartTs)
		hour := int(hour16)
		slotID := int(segParams.SlotID)
		key := hourSlotKey{dateint: dateint, hour: hour, slot: slotID}
		if existingTS, exists := slotHourTriggers[key]; !exists || segParams.StartTs < existingTS {
			slotHourTriggers[key] = segParams.StartTs
		}
	}

	// Queue compaction work for each slot/hour combination
	for key, earliestTS := range slotHourTriggers {
		ll.Debug("Queueing log compaction for slot+hour",
			slog.Int("slotID", key.slot),
			slog.Int("dateint", int(key.dateint)),
			slog.Int("hour", key.hour),
			slog.Int64("triggerTS", earliestTS))

		hourAlignedTS := helpers.TruncateToHour(helpers.UnixMillisToTime(earliestTS)).UnixMilli()
		if err := queueLogCompactionForSlot(criticalCtx, args.DB, item, key.slot, key.dateint, hourAlignedTS); err != nil {
			return fmt.Errorf("failed to queue log compaction for slot %d: %w", key.slot, err)
		}
	}

	return nil
}

// createAndUploadLogSegments creates log segments from parquet results, uploads them to S3, and returns segment parameters
func createAndUploadLogSegments(
	ctx context.Context,
	ll *slog.Logger,
	storageClient cloudstorage.Client,
	results []parquetwriter.Result,
	item ingest.IngestItem,
	ingestDateint int32,
) ([]lrdb.InsertLogSegmentParams, error) {
	segmentParams := make([]lrdb.InsertLogSegmentParams, 0, len(results))

	for _, result := range results {
		// Safety check: should never get empty results from the writer
		if result.RecordCount == 0 {
			ll.Error("Received empty result from writer - this should not happen",
				slog.String("fileName", result.FileName),
				slog.Int64("recordCount", result.RecordCount))
			return nil, fmt.Errorf("received empty result file with 0 records")
		}

		// Extract metadata from parquetwriter result
		stats, ok := result.Metadata.(factories.LogsFileStats)
		if !ok {
			return nil, fmt.Errorf("expected LogsFileStats metadata, got %T", result.Metadata)
		}

		// Generate segment ID and upload
		segmentID := s3helper.GenerateID()
		dateint, _ := helpers.MSToDateintHour(stats.FirstTS)
		dbObjectID := helpers.MakeDBObjectID(item.OrganizationID, "",
			dateint, s3helper.HourFromMillis(stats.FirstTS), segmentID, "logs")

		if err := storageClient.UploadObject(ctx, item.Bucket, dbObjectID, result.FileName); err != nil {
			return nil, fmt.Errorf("uploading file to S3: %w", err)
		}

		ll.Debug("Log segment stats",
			slog.Int64("segmentID", segmentID),
			slog.Int64("recordCount", result.RecordCount),
			slog.Int64("fileSize", result.FileSize))

		// Create segment parameters for database insertion
		slotID := 0
		params := lrdb.InsertLogSegmentParams{
			OrganizationID: item.OrganizationID,
			Dateint:        dateint,
			IngestDateint:  ingestDateint,
			SegmentID:      segmentID,
			InstanceNum:    item.InstanceNum,
			SlotID:         int32(slotID),
			StartTs:        stats.FirstTS,
			EndTs:          stats.LastTS + 1, // end is exclusive
			RecordCount:    result.RecordCount,
			FileSize:       result.FileSize,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   stats.Fingerprints,
		}

		segmentParams = append(segmentParams, params)

		// Clean up temp file
		_ = os.Remove(result.FileName)
	}

	return segmentParams, nil
}
