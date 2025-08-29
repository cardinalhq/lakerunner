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
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/constants"
	"github.com/cardinalhq/lakerunner/internal/debugging"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lockmgr"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "rollup-metrics",
		Short: "Roll up metrics",
		RunE: func(_ *cobra.Command, _ []string) error {
			if os.Getenv("LAKERUNNER_METRICS_ROLLUP_OLDPATH") != "" {
				return oldRollupMetricsCommand().RunE(nil, []string{})
			}

			helpers.SetupTempDir()

			servicename := "lakerunner-rollup-metrics"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "metrics"),
				attribute.String("action", "rollup"),
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

			loop, err := NewRunqueueLoopContext(ctx, "metrics", "rollup", servicename)
			if err != nil {
				return fmt.Errorf("failed to create runqueue loop context: %w", err)
			}

			return RunqueueLoop(loop, metricRollupItem, nil)
		},
	}

	rootCmd.AddCommand(cmd)
}

func metricRollupItem(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	awsmanager *awsclient.Manager,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	inf lockmgr.Workable,
	rpfEstimate int64,
	_ any,
) error {
	previousFrequency, ok := helpers.RollupSources[inf.FrequencyMs()]
	if !ok {
		ll.Error("Unknown parent frequency, dropping rollup request", slog.Int("frequencyMs", int(inf.FrequencyMs())))
		return nil
	}
	if !helpers.IsWantedFrequency(inf.FrequencyMs()) || !helpers.IsWantedFrequency(previousFrequency) {
		ll.Info("Skipping rollup for unwanted frequency", slog.Int("frequencyMs", int(inf.FrequencyMs())), slog.Int("previousFrequency", int(previousFrequency)))
		return nil
	}

	profile, err := sp.GetStorageProfileForOrganizationAndInstance(ctx, inf.OrganizationID(), inf.InstanceNum())
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return err
	}

	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		ll.Error("Failed to get S3 client", slog.Any("error", err))
		return err
	}

	ll.Info("Starting metric rollup",
		slog.String("organizationID", inf.OrganizationID().String()),
		slog.Int("instanceNum", int(inf.InstanceNum())),
		slog.Int("dateint", int(inf.Dateint())),
		slog.Int("frequencyMs", int(inf.FrequencyMs())),
		slog.Int("previousFrequencyMs", int(previousFrequency)),
		slog.Int64("workQueueID", inf.ID()),
		slog.Int64("estimatedRowsPerFile", rpfEstimate))

	t0 := time.Now()
	err = metricRollupItemDo(ctx, ll, mdb, tmpdir, inf, profile, s3client, previousFrequency, rpfEstimate)

	if err != nil {
		ll.Info("Metric rollup completed",
			slog.String("result", "error"),
			slog.Int64("workQueueID", inf.ID()),
			slog.Duration("elapsed", time.Since(t0)))
		return err
	} else {
		ll.Info("Metric rollup completed",
			slog.String("result", "success"),
			slog.Int64("workQueueID", inf.ID()),
			slog.Duration("elapsed", time.Since(t0)))
		return nil
	}
}

func metricRollupItemDo(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	tmpdir string,
	inf lockmgr.Workable,
	profile storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	previousFrequency int32,
	rpfEstimate int64,
) error {
	st, et, ok := helpers.RangeBounds(inf.TsRange())
	if !ok {
		return fmt.Errorf("invalid time range in work item: %v", inf.TsRange())
	}

	// Get source segments to rollup from (previous frequency)
	sourceRows, err := mdb.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: inf.OrganizationID(),
		Dateint:        inf.Dateint(),
		FrequencyMs:    previousFrequency,
		InstanceNum:    inf.InstanceNum(),
		SlotID:         inf.SlotId(),
		StartTs:        st.Time.UTC().UnixMilli(),
		EndTs:          et.Time.UTC().UnixMilli(),
	})
	if err != nil {
		ll.Error("Failed to get previous metric segments", slog.Any("error", err))
		return err
	}

	if helpers.AllRolledUp(sourceRows) {
		ll.Debug("All source rows already rolled up, skipping")
		return nil
	}

	// Get existing segments at target frequency to replace
	existingRows, err := mdb.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: inf.OrganizationID(),
		Dateint:        inf.Dateint(),
		FrequencyMs:    inf.FrequencyMs(),
		InstanceNum:    inf.InstanceNum(),
		SlotID:         inf.SlotId(),
		StartTs:        st.Time.UTC().UnixMilli(),
		EndTs:          et.Time.UTC().UnixMilli(),
	})
	if err != nil {
		ll.Error("Failed to get existing metric segments", slog.Any("error", err))
		return err
	}

	err = rollupMetricSegments(ctx, ll, mdb, tmpdir, inf, profile, s3client, sourceRows, existingRows, rpfEstimate)
	return err
}

func processRollupMicrobatches(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	tmpdir string,
	inf lockmgr.Workable,
	profile storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	sourceRows []lrdb.MetricSeg,
	existingRows []lrdb.MetricSeg,
	rpfEstimate int64,
) error {
	maxRecordsPerMicrobatch := rpfEstimate * 6
	if maxRecordsPerMicrobatch <= 0 {
		maxRecordsPerMicrobatch = 60_000 // Default fallback
	}

	ll.Debug("Processing rollup in microbatches",
		slog.Int("totalSegments", len(sourceRows)),
		slog.Int64("maxRecordsPerMicrobatch", maxRecordsPerMicrobatch),
		slog.Int64("rpfEstimate", rpfEstimate))

	var currentMicrobatch []lrdb.MetricSeg
	currentRecords := int64(0)

	for i, row := range sourceRows {
		// Check for cancellation during microbatch processing
		if ctx.Err() != nil {
			ll.Info("Context cancelled during microbatch processing, aborting rollup")
			return NewWorkerInterrupted("context cancelled during microbatch processing")
		}

		currentMicrobatch = append(currentMicrobatch, row)
		currentRecords += row.RecordCount

		// Process microbatch if we've hit the limit or reached the end
		isLastRow := i == len(sourceRows)-1
		shouldProcess := currentRecords >= maxRecordsPerMicrobatch || isLastRow

		if shouldProcess {
			// For rollups, always process every file (even single files) because we aggregate
			ll.Debug("Processing rollup microbatch",
				slog.Int("fileCount", len(currentMicrobatch)),
				slog.Int64("recordCount", currentRecords))

			err := rollupMetricInterval(ctx, ll, mdb, tmpdir, inf, profile, s3client, currentMicrobatch, existingRows, rpfEstimate)
			if err != nil {
				return fmt.Errorf("rolling up microbatch: %w", err)
			}

			// Reset for next microbatch
			currentMicrobatch = nil
			currentRecords = 0
		}
	}

	return nil
}

func rollupMetricSegments(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	tmpdir string,
	inf lockmgr.Workable,
	profile storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	sourceRows []lrdb.MetricSeg,
	existingRows []lrdb.MetricSeg,
	rpfEstimate int64,
) error {
	if len(sourceRows) == 0 {
		ll.Debug("No source rows to rollup, skipping")
		return nil
	}

	// Process segments in microbatches limited to rowestimate * 6 records
	err := processRollupMicrobatches(ctx, ll, mdb, tmpdir, inf, profile, s3client, sourceRows, existingRows, rpfEstimate)
	if err != nil {
		return fmt.Errorf("failed to process rollup microbatches: %w", err)
	}

	// Use context without cancellation for critical section to ensure atomic completion
	criticalCtx := context.WithoutCancel(ctx)

	// Mark source rows as rolled up (after all microbatches are complete)
	if err := markSourceRowsAsRolledUp(criticalCtx, ll, mdb, sourceRows); err != nil {
		ll.Error("Failed to mark source rows as rolled up", slog.Any("error", err))
		// This is not a critical failure - the rollup succeeded but we couldn't update the flag
		// The next run will skip these since they've already been processed
	}

	// Schedule cleanup of old files
	metricsprocessing.ScheduleOldFileCleanup(criticalCtx, ll, mdb, existingRows, profile)

	// Queue next level rollup and compaction
	if err := queueMetricCompaction(criticalCtx, mdb, qmcFromWorkable(inf)); err != nil {
		ll.Error("Failed to queue metric compaction", slog.Any("error", err))
	}
	if err := queueMetricRollup(criticalCtx, mdb, qmcFromWorkable(inf)); err != nil {
		ll.Error("Failed to queue metric rollup", slog.Any("error", err))
	}

	return nil
}

func rollupMetricInterval(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	tmpdir string,
	inf lockmgr.Workable,
	profile storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	sourceRows []lrdb.MetricSeg,
	existingRows []lrdb.MetricSeg,
	rpfEstimate int64,
) error {
	st, _, ok := helpers.RangeBounds(inf.TsRange())
	if !ok {
		ll.Error("Invalid time range in work item", slog.Any("tsRange", inf.TsRange()))
		return fmt.Errorf("invalid time range in work item: %v", inf.TsRange())
	}

	// Download all source files and create ParquetRawReaders with sorting support
	config := metricsprocessing.ReaderStackConfig{
		FileSortedCounter: fileSortedCounter,
		CommonAttributes:  commonAttributes,
	}

	readerStack, err := metricsprocessing.CreateReaderStack(
		ctx, ll, tmpdir, s3client, inf.OrganizationID(), profile, st.Time.UTC().UnixMilli(), sourceRows, config, "")
	if err != nil {
		// Check if this is due to context cancellation
		if ctx.Err() != nil {
			ll.Info("Context cancelled during S3 download phase", slog.Any("error", err))
			return NewWorkerInterrupted("context cancelled during S3 download")
		}
		return err
	}

	readers := readerStack.Readers
	downloadedFiles := readerStack.DownloadedFiles
	finalReader := readerStack.FinalReader

	if len(readers) == 0 {
		ll.Debug("No files to rollup, skipping work item")
		return nil
	}

	defer metricsprocessing.CloseReaderStack(ll, readerStack)

	// Wrap with aggregating reader to perform rollup aggregation
	// Use target frequency for aggregation period
	aggregatingReader, err := filereader.NewAggregatingMetricsReader(finalReader, int64(inf.FrequencyMs()), 1000)
	if err != nil {
		ll.Error("Failed to create aggregating metrics reader", slog.Any("error", err))
		return fmt.Errorf("creating aggregating metrics reader: %w", err)
	}
	defer aggregatingReader.Close()

	recordsPerFile := rpfEstimate
	if recordsPerFile <= 0 {
		recordsPerFile = 10_000
	}

	writer, err := factories.NewMetricsWriter(tmpdir, constants.WriterTargetSizeBytesMetrics, recordsPerFile)
	if err != nil {
		ll.Error("Failed to create metrics writer", slog.Any("error", err))
		return fmt.Errorf("creating metrics writer: %w", err)
	}
	defer writer.Abort()

	totalRows := int64(0)

	for {
		// Check for cancellation during batch processing
		if ctx.Err() != nil {
			ll.Info("Context cancelled during batch processing, aborting rollup")
			return NewWorkerInterrupted("context cancelled during batch processing")
		}

		batch, err := aggregatingReader.Next()
		if err != nil && !errors.Is(err, io.EOF) {
			if batch != nil {
				pipeline.ReturnBatch(batch)
			}
			ll.Error("Failed to read from aggregating reader", slog.Any("error", err))
			return fmt.Errorf("reading from aggregating reader: %w", err)
		}

		if batch == nil || batch.Len() == 0 {
			pipeline.ReturnBatch(batch)
			break
		}

		// Create a new batch for normalized rows
		normalizedBatch := pipeline.GetBatch()

		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			// Normalize sketch field for parquet writing (string -> []byte)
			if err := normalizeRowForParquetWrite(row); err != nil {
				ll.Error("Failed to normalize row", slog.Any("error", err))
				pipeline.ReturnBatch(normalizedBatch)
				pipeline.ReturnBatch(batch)
				return fmt.Errorf("normalizing row: %w", err)
			}

			// Copy normalized row to the new batch
			normalizedRow := normalizedBatch.AddRow()
			for k, v := range row {
				normalizedRow[k] = v
			}
			totalRows++
		}

		// Write the entire normalized batch at once
		if normalizedBatch.Len() > 0 {
			if err := writer.WriteBatch(normalizedBatch); err != nil {
				ll.Error("Failed to write batch", slog.Any("error", err))
				pipeline.ReturnBatch(normalizedBatch)
				pipeline.ReturnBatch(batch)
				return fmt.Errorf("writing batch: %w", err)
			}
		}

		pipeline.ReturnBatch(normalizedBatch)
		pipeline.ReturnBatch(batch)

		if errors.Is(err, io.EOF) {
			break
		}
	}

	results, err := writer.Close(ctx)
	if err != nil {
		ll.Error("Failed to finish writing", slog.Any("error", err))
		return fmt.Errorf("finishing writer: %w", err)
	}

	ll.Debug("Rollup microbatch completed",
		slog.Int64("totalRows", totalRows),
		slog.Int("outputFiles", len(results)),
		slog.Int("inputFiles", len(downloadedFiles)),
		slog.Int64("recordsPerFile", recordsPerFile))

	// If no output files were created, skip upload
	if len(results) == 0 {
		ll.Debug("No output files created in rollup microbatch, skipping upload")
		return nil
	}

	// Create rollup upload params for this microbatch
	rollupParams := metricsprocessing.CompactionUploadParams{
		OrganizationID: inf.OrganizationID().String(),
		InstanceNum:    inf.InstanceNum(),
		Dateint:        inf.Dateint(),
		FrequencyMs:    inf.FrequencyMs(),
		SlotID:         inf.SlotId(),
		IngestDateint:  metricsprocessing.GetIngestDateint(sourceRows),
		CollectorName:  profile.CollectorName,
		Bucket:         profile.Bucket,
	}

	// Use context without cancellation for critical section to ensure atomic completion
	criticalCtx := context.WithoutCancel(ctx)

	// Upload rolled-up metrics using the same pattern as ingestion
	err = uploadRolledUpMetrics(criticalCtx, ll, mdb, s3client, results, existingRows, rollupParams)
	if err != nil {
		return fmt.Errorf("failed to upload rolled-up metrics: %w", err)
	}

	return nil
}

// uploadRolledUpMetrics uploads rolled-up metric files using the same atomic pattern as ingestion
func uploadRolledUpMetrics(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	s3client *awsclient.S3Client,
	results []parquetwriter.Result,
	existingRows []lrdb.MetricSeg,
	params metricsprocessing.CompactionUploadParams,
) error {
	orgUUID, err := uuid.Parse(params.OrganizationID)
	if err != nil {
		return fmt.Errorf("invalid organization ID: %w", err)
	}

	// Base parameters for database update
	replaceParams := lrdb.ReplaceMetricSegsParams{
		OrganizationID: orgUUID,
		Dateint:        params.Dateint,
		InstanceNum:    params.InstanceNum,
		SlotID:         params.SlotID,
		IngestDateint:  params.IngestDateint,
		FrequencyMs:    params.FrequencyMs,
		Published:      true,
		Rolledup:       false, // Rolled-up data itself is not considered "rolled up" - it's a target
		CreatedBy:      lrdb.CreateByRollup,
		SortVersion:    lrdb.CurrentMetricSortVersion, // Rollup output uses current sort version
	}

	// Add existing records to be replaced
	for _, row := range existingRows {
		replaceParams.OldRecords = append(replaceParams.OldRecords, lrdb.ReplaceMetricSegsOld{
			TidPartition: row.TidPartition,
			SegmentID:    row.SegmentID,
			SlotID:       row.SlotID,
		})
	}

	// Process each output file atomically (same as ingestion)
	for _, file := range results {
		// Extract timestamps from file metadata
		var fingerprints []int64
		var startTs, endTs int64
		var dateint int32
		var hour int16

		if stats, ok := file.Metadata.(factories.MetricsFileStats); ok {
			fingerprints = stats.Fingerprints
			startTs = stats.FirstTS
			// Database expects start-inclusive, end-exclusive range [start, end)
			endTs = stats.LastTS + 1

			// Validate timestamp range
			if startTs == 0 || stats.LastTS == 0 || startTs > stats.LastTS {
				ll.Error("Invalid timestamp range in metrics file stats",
					slog.Int64("startTs", startTs),
					slog.Int64("lastTs", stats.LastTS),
					slog.Int64("endTs", endTs),
					slog.Int64("recordCount", file.RecordCount))
				return fmt.Errorf("invalid timestamp range: startTs=%d, lastTs=%d", startTs, stats.LastTS)
			}

			// Extract dateint and hour from actual file timestamp data
			t := time.Unix(stats.FirstTS/1000, 0).UTC()
			dateint = int32(t.Year()*10000 + int(t.Month())*100 + t.Day())
			hour = int16(t.Hour())
		} else {
			ll.Error("Failed to extract MetricsFileStats from result metadata",
				slog.String("metadataType", fmt.Sprintf("%T", file.Metadata)))
			return fmt.Errorf("missing or invalid MetricsFileStats in result metadata")
		}

		// Generate operation ID for tracking this atomic operation
		opID := idgen.GenerateShortBase32ID()
		fileLogger := ll.With(slog.String("operationID", opID), slog.String("file", file.FileName))

		fileLogger.Debug("Starting atomic metric rollup upload operation",
			slog.Int64("recordCount", file.RecordCount),
			slog.Int64("fileSize", file.FileSize),
			slog.Int64("startTs", startTs),
			slog.Int64("endTs", endTs))

		segmentID := s3helper.GenerateID()
		newObjectID := helpers.MakeDBObjectID(orgUUID, params.CollectorName, dateint, hour, segmentID, "metrics")

		fileLogger.Debug("Uploading rolled-up metric file to S3 - point of no return approaching",
			slog.String("newObjectID", newObjectID),
			slog.String("bucket", params.Bucket),
			slog.Int64("newSegmentID", segmentID))

		err := s3helper.UploadS3Object(ctx, s3client, params.Bucket, newObjectID, file.FileName)
		if err != nil {
			fileLogger.Error("Atomic operation failed during S3 upload - no changes made",
				slog.Any("error", err),
				slog.String("objectID", newObjectID))
			return fmt.Errorf("uploading new S3 object: %w", err)
		}

		fileLogger.Debug("S3 upload successful, updating database index - CRITICAL SECTION",
			slog.String("uploadedObject", newObjectID))

		// Create params for this single file
		singleParams := lrdb.ReplaceMetricSegsParams{
			OrganizationID: replaceParams.OrganizationID,
			Dateint:        replaceParams.Dateint,
			FrequencyMs:    replaceParams.FrequencyMs,
			InstanceNum:    replaceParams.InstanceNum,
			IngestDateint:  replaceParams.IngestDateint,
			Published:      replaceParams.Published,
			Rolledup:       replaceParams.Rolledup,
			CreatedBy:      replaceParams.CreatedBy,
			SlotID:         replaceParams.SlotID,
			OldRecords:     replaceParams.OldRecords,
			SortVersion:    replaceParams.SortVersion,
			NewRecords: []lrdb.ReplaceMetricSegsNew{
				{
					TidPartition: 0, // Rollup output uses tid_partition 0
					SegmentID:    segmentID,
					StartTs:      startTs,
					EndTs:        endTs,
					RecordCount:  file.RecordCount,
					FileSize:     file.FileSize,
				},
			},
			Fingerprints: fingerprints,
		}

		if err := mdb.ReplaceMetricSegs(ctx, singleParams); err != nil {
			fileLogger.Error("Database update failed after S3 upload - file orphaned in S3",
				slog.Any("error", err),
				slog.String("orphanedObject", newObjectID),
				slog.Int64("orphanedSegmentID", segmentID),
				slog.String("bucket", params.Bucket))

			// Best effort cleanup - try to delete the uploaded file
			if cleanupErr := s3helper.DeleteS3Object(ctx, s3client, params.Bucket, newObjectID); cleanupErr != nil {
				fileLogger.Error("Failed to cleanup orphaned S3 object - manual cleanup required",
					slog.Any("error", cleanupErr),
					slog.String("objectID", newObjectID),
					slog.String("bucket", params.Bucket))
			}
			return fmt.Errorf("replacing metric segments: %w", err)
		}

		fileLogger.Debug("ATOMIC OPERATION COMMITTED SUCCESSFULLY - database updated, segments swapped",
			slog.Int64("newSegmentID", segmentID),
			slog.Int64("newRecordCount", file.RecordCount),
			slog.Int64("newFileSize", file.FileSize),
			slog.String("newObjectID", newObjectID))
	}

	return nil
}

// markSourceRowsAsRolledUp marks the source segments as having been rolled up
func markSourceRowsAsRolledUp(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	sourceRows []lrdb.MetricSeg,
) error {
	newlyRolled := []lrdb.BatchMarkMetricSegsRolledupParams{}
	for _, row := range sourceRows {
		if row.Rolledup {
			continue
		}
		newlyRolled = append(newlyRolled, lrdb.BatchMarkMetricSegsRolledupParams{
			OrganizationID: row.OrganizationID,
			Dateint:        row.Dateint,
			FrequencyMs:    row.FrequencyMs,
			SegmentID:      row.SegmentID,
			InstanceNum:    row.InstanceNum,
			SlotID:         row.SlotID,
			TidPartition:   row.TidPartition,
		})
	}

	if len(newlyRolled) > 0 {
		result := mdb.BatchMarkMetricSegsRolledup(ctx, newlyRolled)
		result.Exec(func(i int, err error) {
			if err != nil {
				ll.Error("Failed to mark metric segments as rolled up", slog.Int("index", i), slog.Any("error", err), slog.Any("record", newlyRolled[i]))
			}
		})
	}

	return nil
}

// boxesForRange returns a list of timebox IDs for the given start and end timestamps and frequency.
func boxesForRange(startTs, endTs int64, frequencyMs int32) []int64 {
	if startTs > endTs || frequencyMs <= 0 {
		return []int64{}
	}
	firstBox := startTs / int64(frequencyMs)
	lastBox := endTs / int64(frequencyMs)
	nBoxes := lastBox - firstBox + 1
	boxes := make([]int64, nBoxes)
	for n := range nBoxes {
		boxes[n] = firstBox + n
	}
	return boxes
}
