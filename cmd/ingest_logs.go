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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/constants"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "ingest-logs",
		Short: "Ingest logs from the inqueue table",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.SetupTempDir()

			servicename := "lakerunner-ingest-logs"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "logs"),
				attribute.String("action", "ingest"),
			)
			doneCtx, doneFx, err := setupTelemetry(servicename, &addlAttrs)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			go diskUsageLoop(doneCtx)

			loop, err := NewIngestLoopContext(doneCtx, "logs", servicename)
			if err != nil {
				return fmt.Errorf("failed to create ingest loop context: %w", err)
			}

			// Check if we should use the old implementation as a safety net
			if os.Getenv("LAKERUNNER_LOGS_INGEST_OLDPATH") != "" {
				return runOldLogIngestion(doneCtx, slog.Default(), loop)
			}

			for {
				select {
				case <-doneCtx.Done():
					slog.Info("Ingest logs command done")
					return nil
				default:
				}

				err := IngestLoopWithBatch(loop, logIngestItem, logIngestBatch)
				if err != nil {
					slog.Error("Error in ingest loop", slog.Any("error", err))
				}
			}
		},
	}

	rootCmd.AddCommand(cmd)
}

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

// processRow adds a row to the appropriate writer based on timestamp (hour), always using slot 0
func (wm *writerManager) processRow(row filereader.Row) (err error) {
	defer func() {
		if r := recover(); r != nil {
			wm.ll.Error("PANIC in processRow", slog.Any("panic", r))
			err = fmt.Errorf("panic in processRow: %v", r)
		}
	}()

	// Extract timestamp - assume it's properly set
	ts, ok := row[wkk.RowKeyCTimestamp].(int64)
	if !ok {
		return fmt.Errorf("_cardinalhq.timestamp field is missing or not int64")
	}

	// Determine hour - always use slot 0 as requested
	dateint, hour16 := helpers.MSToDateintHour(ts)
	hour := int(hour16)
	slot := 0

	// Get or create writer for this hour/slot
	key := hourSlotKey{dateint, hour, slot}
	writer, err := wm.getWriter(key)
	if err != nil {
		return fmt.Errorf("failed to get writer for key %v: %w", key, err)
	}

	// Write row
	err = writer.Write(pipeline.ToStringMap(row))
	return err
}

// getWriter returns the writer for a specific hour/slot, creating it if necessary
func (wm *writerManager) getWriter(key hourSlotKey) (*parquetwriter.UnifiedWriter, error) {
	if writer, exists := wm.writers[key]; exists {
		return writer, nil
	}

	// Create new writer
	baseName := fmt.Sprintf("logs_%s_%d_%d_%d", wm.orgID, key.dateint, key.hour, key.slot)
	writer, err := factories.NewLogsWriter(baseName, wm.tmpdir, constants.WriterTargetSizeBytesLogs, wm.rpfEstimate)
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

func logIngestItem(ctx context.Context, ll *slog.Logger, tmpdir string, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager, inf lrdb.Inqueue, ingest_dateint int32, rpfEstimate int64, loop *IngestLoopContext) error {

	// Convert single item to batch and process
	return logIngestBatch(ctx, ll, tmpdir, sp, mdb, awsmanager, []lrdb.Inqueue{inf}, ingest_dateint, rpfEstimate, loop)
}

// queueLogCompactionForSlot queues a log compaction job for a specific slot
func queueLogCompactionForSlot(ctx context.Context, mdb lrdb.StoreFull, inf lrdb.Inqueue, slotID int, dateint int32, hourAlignedTS int64) error {
	return mdb.WorkQueueAdd(ctx, lrdb.WorkQueueAddParams{
		OrgID:      inf.OrganizationID,
		Instance:   inf.InstanceNum,
		Signal:     lrdb.SignalEnumLogs,
		Action:     lrdb.ActionEnumCompact,
		Dateint:    dateint,
		Frequency:  -1,
		SlotID:     int32(slotID),
		TsRange:    qmcFromInqueue(inf, 3600000, hourAlignedTS).TsRange,
		RunnableAt: time.Now().UTC().Add(5 * time.Minute),
	})
}

// Functions removed - now using filereader and parquetwriter packages directly

func logIngestBatch(ctx context.Context, ll *slog.Logger, tmpdir string, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager, items []lrdb.Inqueue, ingest_dateint int32, rpfEstimate int64, loop *IngestLoopContext) error {

	if len(items) == 0 {
		return fmt.Errorf("empty batch")
	}

	ll.Debug("Processing log batch", slog.Int("batchSize", len(items)))

	// Get storage profile and S3 client
	firstItem := items[0]
	var profile storageprofile.StorageProfile
	var err error

	if collectorName := helpers.ExtractCollectorName(firstItem.ObjectID); collectorName != "" {
		profile, err = sp.GetStorageProfileForOrganizationAndCollector(ctx, firstItem.OrganizationID, collectorName)
		if err != nil {
			return fmt.Errorf("failed to get storage profile for collector %s: %w", collectorName, err)
		}
	} else {
		profile, err = sp.GetStorageProfileForOrganizationAndInstance(ctx, firstItem.OrganizationID, firstItem.InstanceNum)
		if err != nil {
			return fmt.Errorf("failed to get storage profile: %w", err)
		}
	}

	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		return fmt.Errorf("failed to get S3 client: %w", err)
	}

	// Create writer manager for organizing output by hour/slot
	wm := newWriterManager(tmpdir, firstItem.OrganizationID.String(), ingest_dateint, rpfEstimate, ll)

	// Track total rows across all files
	var batchRowsRead, batchRowsProcessed, batchRowsErrored int64

	// Process each file in the batch
	for _, inf := range items {
		ll.Debug("Processing batch item with filereader",
			slog.String("itemID", inf.ID.String()),
			slog.String("objectID", inf.ObjectID),
			slog.Int64("fileSize", inf.FileSize))

		// Skip database files (processed outputs, not inputs)
		if strings.HasPrefix(inf.ObjectID, "db/") {
			ll.Debug("Skipping database file", slog.String("objectID", inf.ObjectID))
			continue
		}

		// Download file
		itemTmpdir := fmt.Sprintf("%s/item_%s", tmpdir, inf.ID.String())
		if err := os.MkdirAll(itemTmpdir, 0755); err != nil {
			return fmt.Errorf("creating item tmpdir: %w", err)
		}

		tmpfilename, _, is404, err := s3helper.DownloadS3Object(ctx, itemTmpdir, s3client, inf.Bucket, inf.ObjectID)
		if err != nil {
			return fmt.Errorf("failed to download file %s: %w", inf.ObjectID, err)
		}
		if is404 {
			ll.Warn("S3 object not found, skipping", slog.String("objectID", inf.ObjectID))
			continue
		}

		// Create appropriate reader for the file type
		var reader filereader.Reader

		reader, err = createLogReader(tmpfilename)
		if err == nil {
			// Add general translator for non-protobuf files
			translator := &LogTranslator{
				orgID:    firstItem.OrganizationID.String(),
				bucket:   inf.Bucket,
				objectID: inf.ObjectID,
			}
			reader, err = filereader.NewTranslatingReader(reader, translator, 1000)
		}

		if err != nil {
			ll.Warn("Unsupported or problematic file type, skipping",
				slog.String("objectID", inf.ObjectID),
				slog.String("error", err.Error()))
			continue
		}

		// Process all rows from the file
		var processedCount, errorCount int64
		for {
			batch, err := reader.Next()

			// Process any rows we got, even if EOF
			if batch != nil {
				for i := 0; i < batch.Len(); i++ {
					row := batch.Get(i)
					if row == nil {
						ll.Error("Row is nil - skipping", slog.Int("rowIndex", i))
						continue
					}
					processErr := wm.processRow(row)
					if processErr != nil {
						errorCount++
						ll.Error("Failed to process row - row will be dropped",
							slog.String("objectID", inf.ObjectID),
							slog.Int64("rowNumber", processedCount+int64(i)+1),
							slog.String("error", processErr.Error()),
							slog.Any("rowData", row))
						// Continue processing other rows instead of failing the entire batch
					} else {
						processedCount++
					}
				}
			}

			// Break after processing if we hit EOF or other errors
			if err == io.EOF {
				break
			}
			if err != nil {
				if closeErr := reader.Close(); closeErr != nil {
					ll.Warn("Failed to close reader after read error", slog.String("objectID", inf.ObjectID), slog.Any("error", closeErr))
				}
				return fmt.Errorf("failed to read from file %s: %w", inf.ObjectID, err)
			}
		}

		// Get total rows read from the reader
		fileRowsRead := reader.TotalRowsReturned()

		ll.Debug("File processing completed",
			slog.String("objectID", inf.ObjectID),
			slog.Int64("rowsRead", fileRowsRead),
			slog.Int64("rowsProcessed", processedCount),
			slog.Int64("rowsErrored", errorCount))

		if errorCount > 0 {
			ll.Warn("Some rows were dropped due to processing errors",
				slog.String("objectID", inf.ObjectID),
				slog.Int64("droppedRows", errorCount),
				slog.Float64("dropRate", float64(errorCount)/float64(fileRowsRead)*100))
		}
		if closeErr := reader.Close(); closeErr != nil {
			ll.Warn("Failed to close reader", slog.String("objectID", inf.ObjectID), slog.Any("error", closeErr))
		}

		// Update batch totals
		batchRowsRead += fileRowsRead
		batchRowsProcessed += processedCount
		batchRowsErrored += errorCount

		ll.Debug("Completed processing file", slog.String("objectID", inf.ObjectID))
	}

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

	// Upload files and create database segments
	slotHourTriggers := make(map[hourSlotKey]int64) // Track earliest timestamp per slot/hour for compaction

	for _, result := range results {
		// Extract metadata from parquetwriter result
		stats, ok := result.Metadata.(factories.LogsFileStats)
		if !ok {
			return fmt.Errorf("expected LogsFileStats metadata, got %T", result.Metadata)
		}

		// Generate segment ID and upload
		segmentID := s3helper.GenerateID()
		dateint, hour16 := helpers.MSToDateintHour(stats.FirstTS)
		hour := int(hour16)
		dbObjectID := helpers.MakeDBObjectID(firstItem.OrganizationID, firstItem.CollectorName,
			dateint, s3helper.HourFromMillis(stats.FirstTS), segmentID, "logs")

		if err := s3helper.UploadS3Object(ctx, s3client, firstItem.Bucket, dbObjectID, result.FileName); err != nil {
			return fmt.Errorf("failed to upload file to S3: %w", err)
		}
		_ = os.Remove(result.FileName)

		slotID := 0

		// Insert log segment into database
		resultLastTS := stats.LastTS + 1 // end is exclusive
		err := mdb.InsertLogSegment(ctx, lrdb.InsertLogSegmentParams{
			OrganizationID: firstItem.OrganizationID,
			Dateint:        dateint,
			IngestDateint:  ingest_dateint,
			SegmentID:      segmentID,
			InstanceNum:    firstItem.InstanceNum,
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
			slog.String("organizationID", firstItem.OrganizationID.String()),
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

	// Queue compaction work for each slot/hour combination
	for key, earliestTS := range slotHourTriggers {
		ll.Debug("Queueing log compaction for slot+hour",
			slog.Int("slotID", key.slot),
			slog.Int("dateint", int(key.dateint)),
			slog.Int("hour", key.hour),
			slog.Int64("triggerTS", earliestTS))

		hourAlignedTS := helpers.TruncateToHour(helpers.UnixMillisToTime(earliestTS)).UnixMilli()
		if err := queueLogCompactionForSlot(ctx, mdb, firstItem, key.slot, key.dateint, hourAlignedTS); err != nil {
			return fmt.Errorf("failed to queue log compaction for slot %d: %w", key.slot, err)
		}
	}

	return nil
}
