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
	"fmt"
	"log/slog"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/debugging"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/healthcheck"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logsprocessing/ingestion"
	"github.com/cardinalhq/lakerunner/internal/processing/ingest"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "ingest-logs",
		Short: "Ingest logs from Kafka",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.SetupTempDir()

			servicename := "lakerunner-ingest-logs"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "logs"),
				attribute.String("action", "ingest"),
			)
			ctx, doneFx, err := setupTelemetry(servicename, &addlAttrs)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			go diskUsageLoop(ctx)

			// Start pprof server
			go debugging.RunPprof(ctx)

			// Start health check server
			healthConfig := healthcheck.GetConfigFromEnv()
			healthServer := healthcheck.NewServer(healthConfig)

			go func() {
				if err := healthServer.Start(ctx); err != nil {
					slog.Error("Health check server stopped", slog.Any("error", err))
				}
			}()

			// Kafka is required for ingestion
			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// Also set log partitions from config
			if cfg.Logs.Partitions > 0 {
				ingestion.SetNumLogPartitions(cfg.Logs.Partitions)
			}

			kafkaFactory := fly.NewFactory(&cfg.Fly)
			if !kafkaFactory.IsEnabled() {
				return fmt.Errorf("Kafka is required for ingestion but is not enabled")
			}

			slog.Info("Starting logs ingestion with Kafka consumer")

			consumer, err := NewKafkaIngestConsumer(kafkaFactory, cfg, "logs", "lakerunner.ingest.logs")
			if err != nil {
				return fmt.Errorf("failed to create Kafka consumer: %w", err)
			}
			defer func() {
				if err := consumer.Close(); err != nil {
					slog.Error("Error closing Kafka consumer", slog.Any("error", err))
				}
			}()

			// Mark as healthy once consumer is created and about to start
			healthServer.SetStatus(healthcheck.StatusHealthy)

			return consumer.Run(ctx)
		},
	}

	rootCmd.AddCommand(cmd)
}

// createLogReader creates the appropriate filereader based on file type
func createLogReader(filename string) (filereader.Reader, error) {
	return filereader.ReaderForFile(filename, filereader.SignalTypeLogs)
}

func logIngestBatch(ctx context.Context, ll *slog.Logger, tmpdir string, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	cloudManagers *cloudstorage.CloudManagers, items []lrdb.Inqueue, ingest_dateint int32, rpfEstimate int64, loop *IngestLoopContext) error {

	ll.Debug("Processing log item")

	// Convert IngestItem to Inqueue for compatibility with existing code
	inqueueItems := ConvertIngestItemsToInqueue([]IngestItem{item})

	// Get storage profile
	firstItem := items[0]
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

	// Create cloud storage client
	storageClient, err := cloudstorage.NewClient(ctx, cloudManagers, profile)
	if err != nil {
		return fmt.Errorf("failed to create storage client for provider %s: %w", profile.CloudProvider, err)
	}

	// Create writer manager for organizing output by hour/slot
	wm := newWriterManager(tmpdir, item.OrganizationID.String(), ingest_dateint, rpfEstimate, ll)

	// Track total rows across all files
	var batchRowsRead, batchRowsProcessed, batchRowsErrored int64

	// Process each file in the batch
	for _, inf := range inqueueItems {
		// Check for context cancellation before processing each file
		if err := ctx.Err(); err != nil {
			ll.Info("Context cancelled during batch processing - safe interruption point",
				slog.String("itemID", inf.ID.String()),
				slog.Any("error", err))
			return NewWorkerInterrupted("context cancelled during file processing")
		}

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

		tmpfilename, _, is404, err := storageClient.DownloadObject(ctx, itemTmpdir, inf.Bucket, inf.ObjectID)
		if err != nil {
			return fmt.Errorf("failed to download file %s from %s: %w", inf.ObjectID, profile.CloudProvider, err)
		}
		if is404 {
			ll.Warn("Object not found in cloud storage, skipping",
				slog.String("cloudProvider", profile.CloudProvider),
				slog.String("objectID", inf.ObjectID))
			continue
		}

		// Create appropriate reader for the file type
		var reader filereader.Reader

		reader, err = createLogReader(tmpfilename)
		if err == nil {
			// Add general translator for non-protobuf files
			translator := &LogTranslator{
				orgID:    item.OrganizationID.String(),
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
			batch, err := reader.Next(ctx)

			// Process any rows we got, even if EOF
			if batch != nil {
				batchProcessed, batchErrors := wm.processBatch(batch)
				processedCount += batchProcessed
				errorCount += batchErrors
				pipeline.ReturnBatch(batch)

				if batchErrors > 0 {
					ll.Warn("Some rows failed to process in batch",
						slog.String("objectID", inf.ObjectID),
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
		dbObjectID := helpers.MakeDBObjectID(item.OrganizationID, "",
			dateint, s3helper.HourFromMillis(stats.FirstTS), segmentID, "logs")

		if err := storageClient.UploadObject(criticalCtx, firstItem.Bucket, dbObjectID, result.FileName); err != nil {
			return fmt.Errorf("failed to upload file to %s: %w", profile.CloudProvider, err)
		}
		_ = os.Remove(result.FileName)

		slotID := 0

		// Insert log segment into database
		resultLastTS := stats.LastTS + 1 // end is exclusive
		err := mdb.InsertLogSegment(criticalCtx, lrdb.InsertLogSegmentParams{
			OrganizationID: item.OrganizationID,
			Dateint:        dateint,
			IngestDateint:  ingest_dateint,
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

	// Queue compaction work for each slot/hour combination
	for key, earliestTS := range slotHourTriggers {
		ll.Debug("Queueing log compaction for slot+hour",
			slog.Int("slotID", key.slot),
			slog.Int("dateint", int(key.dateint)),
			slog.Int("hour", key.hour),
			slog.Int64("triggerTS", earliestTS))

		hourAlignedTS := helpers.TruncateToHour(helpers.UnixMillisToTime(earliestTS)).UnixMilli()
		if err := queueLogCompactionForSlot(criticalCtx, mdb, item, key.slot, key.dateint, hourAlignedTS); err != nil {
			return fmt.Errorf("failed to queue log compaction for slot %d: %w", key.slot, err)
		}
	}

	return nil
}
