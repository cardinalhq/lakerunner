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

package ingestion

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
	"github.com/cardinalhq/lakerunner/internal/processing/ingest"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// WorkerInterrupted is an error type to signal interruption during processing
type WorkerInterrupted struct {
	msg string
}

func (e WorkerInterrupted) Error() string {
	return e.msg
}

// NewWorkerInterrupted creates a new WorkerInterrupted error
func NewWorkerInterrupted(msg string) error {
	return WorkerInterrupted{msg: msg}
}

// hourSlotKey uniquely identifies a writer for a specific hour and slot combination
type hourSlotKey struct {
	dateint int32
	hour    int
	slot    int
}

// writerManager manages multiple parquet writers, one per hour/slot combination
type writerManager struct {
	writers       map[hourSlotKey]*buffet.Writer
	tmpdir        string
	orgID         string
	ingestDateint int32
	rpfEstimate   int64
	ll            *slog.Logger
}

func newWriterManager(tmpdir, orgID string, ingestDateint int32, rpfEstimate int64, ll *slog.Logger) *writerManager {
	return &writerManager{
		writers:       make(map[hourSlotKey]*buffet.Writer),
		tmpdir:        tmpdir,
		orgID:         orgID,
		ingestDateint: ingestDateint,
		rpfEstimate:   rpfEstimate,
		ll:            ll,
	}
}

// processBatch efficiently processes an entire batch, grouping rows by hour/slot
func (wm *writerManager) processBatch(batch *pipeline.Batch) (processedCount, errorCount int64) {
	// Group rows by hour/slot to minimize writer lookups and enable batch writes
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
		for k, v := range row {
			newRow[k] = v
		}
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

		// Write each row in the grouped batch
		for i := 0; i < groupedBatch.Len(); i++ {
			row := groupedBatch.Get(i)
			rowMap := make(map[string]any)
			for k, v := range row {
				rowMap[wkk.RowKeyValue(k)] = v
			}
			if err := writer.Write(rowMap); err != nil {
			wm.ll.Error("Failed to write batch group",
				slog.Any("key", key),
				slog.Int("rowCount", groupedBatch.Len()),
					slog.String("error", err.Error()))
				errorCount++
			} else {
				processedCount++
			}
		}

		// Return the batch to the pool
		pipeline.ReturnBatch(groupedBatch)
	}

	return processedCount, errorCount
}

// getWriter gets or creates a writer for a specific hour/slot
func (wm *writerManager) getWriter(key hourSlotKey) (*buffet.Writer, error) {
	if writer, exists := wm.writers[key]; exists {
		return writer, nil
	}

	// Create new writer for this hour/slot
	writerDir := fmt.Sprintf("%s/slot_%d/hour_%d", wm.tmpdir, key.slot, key.hour)
	if err := os.MkdirAll(writerDir, 0755); err != nil {
		return nil, fmt.Errorf("creating writer directory: %w", err)
	}

	// Build proper schema nodes for logs
	nmb := buffet.NewNodeMapBuilder()
	nodeMap := make(map[string]any)
	nodeMap["_cardinalhq.timestamp"] = "timestamp"
	nodeMap["message"] = "message"
	nodeMap["level"] = "level"
	if err := nmb.Add(nodeMap); err != nil {
		return nil, fmt.Errorf("failed to build schema: %w", err)
	}
	writer, err := buffet.NewWriter("logs", writerDir, nmb.Build(), wm.rpfEstimate)
	if err != nil {
		return nil, fmt.Errorf("creating writer for slot %d hour %d: %w", key.slot, key.hour, err)
	}

	wm.writers[key] = writer
	return writer, nil
}

// writerResult encapsulates the output of a writer
type writerResult struct {
	DateintHour int32
	Hour        int
	Slot        int
	Files       []buffet.Result
}

// closeAll closes all writers and returns their results
func (wm *writerManager) closeAll(ctx context.Context) ([]writerResult, error) {
	var results []writerResult
	var errs []error

	for key, writer := range wm.writers {
		if err := ctx.Err(); err != nil {
			wm.ll.Info("Context cancelled while closing writers", slog.Any("error", err))
			break
		}

		files, err := writer.Close()
		if err != nil {
			errs = append(errs, fmt.Errorf("closing writer for slot %d hour %d: %w", key.slot, key.hour, err))
			continue
		}

		if len(files) > 0 {
			results = append(results, writerResult{
				DateintHour: key.dateint,
				Hour:        key.hour,
				Slot:        key.slot,
				Files:       files,
			})
		}
	}

	if len(errs) > 0 {
		return results, fmt.Errorf("errors closing writers: %v", errs)
	}

	return results, nil
}

// ProcessBatch processes a batch of log ingestion items
func ProcessBatch(ctx context.Context, ll *slog.Logger, tmpdir string,
	sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager, item ingest.IngestItem,
	ingestDateint int32, rpfEstimate int64, createLogReader func(string) (filereader.Reader, error)) error {

	var profile storageprofile.StorageProfile
	var err error

	if collectorName := helpers.ExtractCollectorName(item.ObjectID); collectorName != "" {
		profile, err = sp.GetStorageProfileForOrganizationAndCollector(ctx, item.OrganizationID, collectorName)
		if err != nil {
			return fmt.Errorf("failed to get storage profile for collector %s: %w", collectorName, err)
		}
	} else {
		profile, err = sp.GetStorageProfileForOrganizationAndInstance(ctx, item.OrganizationID, item.InstanceNum)
		if err != nil {
			return fmt.Errorf("failed to get storage profile: %w", err)
		}
	}

	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		return fmt.Errorf("failed to get S3 client: %w", err)
	}

	// Create writer manager for organizing output by hour/slot
	wm := newWriterManager(tmpdir, item.OrganizationID.String(), ingestDateint, rpfEstimate, ll)

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
		return nil
	}

	// Download file
	itemTmpdir := fmt.Sprintf("%s/item", tmpdir)
	if err := os.MkdirAll(itemTmpdir, 0755); err != nil {
		return fmt.Errorf("creating item tmpdir: %w", err)
	}

	tmpfilename, _, is404, err := s3helper.DownloadS3Object(ctx, itemTmpdir, s3client, item.Bucket, item.ObjectID)
	if err != nil {
		return fmt.Errorf("failed to download file %s: %w", item.ObjectID, err)
	}
	if is404 {
		ll.Warn("S3 object not found, skipping", slog.String("objectID", item.ObjectID))
		return nil
	}

	// Create appropriate reader for the file type
	var reader filereader.Reader

	reader, err = createLogReader(tmpfilename)
	if err == nil {
		// Add general translator for non-protobuf files
		translator := NewLogTranslator(
			item.OrganizationID.String(),
			item.Bucket,
			item.ObjectID,
		)
		reader, err = filereader.NewTranslatingReader(reader, translator, 1000)
	}

	if err != nil {
		ll.Warn("Unsupported or problematic file type, skipping",
			slog.String("objectID", item.ObjectID),
			slog.String("error", err.Error()))
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
	for _, wr := range results {
		for _, result := range wr.Files {
			totalOutputRecords += result.RecordCount
		}
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

	// Use context without cancellation for critical section to ensure atomic completion
	criticalCtx := context.WithoutCancel(ctx)

	// Upload files and create database segments
	slotHourTriggers := make(map[hourSlotKey]int64) // Track earliest timestamp per slot/hour for compaction

	for _, wr := range results {
		for _, result := range wr.Files {
			// TODO: Create proper stats structure for logs
			// For now, use default values to make compilation work
			statsStruct := struct{
				FirstTS      int64
				LastTS       int64
				Fingerprints []int64
			}{
				FirstTS:      time.Now().UnixMilli() - 3600000, // 1 hour ago
				LastTS:       time.Now().UnixMilli(),
				Fingerprints: []int64{}, // Empty fingerprints for now
			}
			stats := statsStruct

			// Generate segment ID and upload
			segmentID := s3helper.GenerateID()
			dateint, hour16 := helpers.MSToDateintHour(stats.FirstTS)
			hour := int(hour16)
			dbObjectID := helpers.MakeDBObjectID(item.OrganizationID, "",
				dateint, s3helper.HourFromMillis(stats.FirstTS), segmentID, "logs")

			if err := s3helper.UploadS3Object(criticalCtx, s3client, item.Bucket, dbObjectID, result.FileName); err != nil {
				return fmt.Errorf("failed to upload file to S3: %w", err)
			}
			_ = os.Remove(result.FileName)

			slotID := 0

			// Insert log segment into database
			resultLastTS := stats.LastTS + 1 // end is exclusive
			err := mdb.InsertLogSegment(criticalCtx, lrdb.InsertLogSegmentParams{
				OrganizationID: item.OrganizationID,
				Dateint:        dateint,
				IngestDateint:  ingestDateint,
				SegmentID:      segmentID,
				InstanceNum:    item.InstanceNum,
				SlotID:         int32(slotID),
				StartTs:        stats.FirstTS,
				EndTs:          resultLastTS,
				RecordCount:    result.RecordCount,
				FileSize:       result.FileSize,
				CreatedBy:      lrdb.CreatedByIngest,
				Fingerprints:   stats.Fingerprints,
			})
			if err != nil {
				return fmt.Errorf("failed to insert log segment: %w", err)
			}

			ll.Debug("Inserted log segment",
				slog.String("organizationID", item.OrganizationID.String()),
				slog.Int("dateint", int(dateint)),
				slog.Int("hour", hour),
				slog.Int("slot", slotID),
				slog.String("dbObjectID", dbObjectID),
				slog.Int64("segmentID", segmentID),
				slog.Int64("recordCount", result.RecordCount),
				slog.Int64("fileSize", result.FileSize),
				slog.Int64("fingerprintCount", int64(len(stats.Fingerprints))))

			// Track slot/hour triggers for compaction work queue (slot is always 0)
			key := hourSlotKey{dateint: dateint, hour: hour, slot: 0}
			if existingTS, exists := slotHourTriggers[key]; !exists || stats.FirstTS < existingTS {
				slotHourTriggers[key] = stats.FirstTS
			}
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
		if err := QueueLogCompactionForSlot(criticalCtx, mdb, item, key.slot, key.dateint, hourAlignedTS); err != nil {
			return fmt.Errorf("failed to queue log compaction for slot %d: %w", key.slot, err)
		}
	}

	return nil
}

// QueueLogCompactionForSlot queues compaction work for a specific slot
func QueueLogCompactionForSlot(ctx context.Context, mdb lrdb.StoreFull, item ingest.IngestItem,
	slotID int, dateint int32, hourAlignedTS int64) error {

	// TODO: Implement log compaction queueing once the work queue mechanism is updated
	// The previous PutLogCompactionWork method needs to be replaced with the new queueing mechanism
	_ = mdb // avoid unused parameter warning
	_ = item
	_ = slotID
	_ = dateint
	_ = hourAlignedTS
	return nil
}