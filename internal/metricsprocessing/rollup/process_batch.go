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

package rollup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/constants"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func processBatch(
	ctx context.Context,
	ll *slog.Logger,
	mdb rollupStore,
	sp storageprofile.StorageProfileProvider,
	awsmanager *awsclient.Manager,
	claimedWork []lrdb.ClaimMetricRollupWorkRow,
) error {
	if len(claimedWork) == 0 {
		return nil
	}

	// Safety check: All work items in a batch must have identical grouping fields
	firstItem := claimedWork[0]
	for i, item := range claimedWork {
		if item.OrganizationID != firstItem.OrganizationID ||
			item.Dateint != firstItem.Dateint ||
			item.FrequencyMs != firstItem.FrequencyMs ||
			item.InstanceNum != firstItem.InstanceNum ||
			item.SlotID != firstItem.SlotID ||
			item.SlotCount != firstItem.SlotCount {
			ll.Error("Inconsistent work batch detected - all items must have same org/dateint/frequency/instance/slot",
				slog.Int("itemIndex", i),
				slog.String("expectedOrg", firstItem.OrganizationID.String()),
				slog.String("actualOrg", item.OrganizationID.String()),
				slog.Int("expectedDateint", int(firstItem.Dateint)),
				slog.Int("actualDateint", int(item.Dateint)),
				slog.Int64("expectedFreq", firstItem.FrequencyMs),
				slog.Int64("actualFreq", item.FrequencyMs),
				slog.Int("expectedInstance", int(firstItem.InstanceNum)),
				slog.Int("actualInstance", int(item.InstanceNum)),
				slog.Int("expectedSlotID", int(firstItem.SlotID)),
				slog.Int("actualSlotID", int(item.SlotID)),
				slog.Int("expectedSlotCount", int(firstItem.SlotCount)),
				slog.Int("actualSlotCount", int(item.SlotCount)))
			return fmt.Errorf("inconsistent work batch: item %d has different grouping fields", i)
		}
	}

	previousFrequency, ok := helpers.RollupSources[int32(firstItem.FrequencyMs)]
	if !ok {
		ll.Error("Unknown parent frequency, dropping rollup request", slog.Int64("frequencyMs", firstItem.FrequencyMs))
		return nil
	}

	if !helpers.IsWantedFrequency(int32(firstItem.FrequencyMs)) || !helpers.IsWantedFrequency(previousFrequency) {
		ll.Info("Skipping rollup for unwanted frequency", slog.Int64("frequencyMs", firstItem.FrequencyMs), slog.Int("previousFrequency", int(previousFrequency)))
		return nil
	}

	profile, err := sp.GetStorageProfileForOrganizationAndInstance(ctx, firstItem.OrganizationID, firstItem.InstanceNum)
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return err
	}

	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		ll.Error("Failed to get S3 client", slog.Any("error", err))
		return err
	}

	tmpdir, err := os.MkdirTemp("", "rollup-work-")
	if err != nil {
		ll.Error("Failed to create temporary directory", slog.Any("error", err))
		return fmt.Errorf("failed to create temporary directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			ll.Error("Failed to remove temporary directory", slog.Any("error", err))
		}
	}()

	ll.Info("Starting metric rollup batch",
		slog.String("organizationID", firstItem.OrganizationID.String()),
		slog.Int("instanceNum", int(firstItem.InstanceNum)),
		slog.Int("dateint", int(firstItem.Dateint)),
		slog.Int64("frequencyMs", firstItem.FrequencyMs),
		slog.Int64("previousFrequencyMs", int64(previousFrequency)),
		slog.Int("slotID", int(firstItem.SlotID)),
		slog.Int("slotCount", int(firstItem.SlotCount)),
		slog.Int("batchSize", len(claimedWork)))

	t0 := time.Now()

	// Find the combined time range from all work items
	var startTs, endTs int64
	for i, work := range claimedWork {
		st, et, ok := helpers.RangeBounds(work.TsRange)
		if !ok {
			return fmt.Errorf("invalid time range in work item: %v", work.TsRange)
		}
		workStartTs := st.Time.UTC().UnixMilli()
		workEndTs := et.Time.UTC().UnixMilli()
		if i == 0 {
			startTs = workStartTs
			endTs = workEndTs
		} else {
			if workStartTs < startTs {
				startTs = workStartTs
			}
			if workEndTs > endTs {
				endTs = workEndTs
			}
		}
	}

	// Get source segments to rollup from (previous frequency)
	sourceRows, err := mdb.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: firstItem.OrganizationID,
		Dateint:        firstItem.Dateint,
		FrequencyMs:    previousFrequency,
		InstanceNum:    firstItem.InstanceNum,
		SlotID:         firstItem.SlotID,
		StartTs:        startTs,
		EndTs:          endTs,
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
		OrganizationID: firstItem.OrganizationID,
		Dateint:        firstItem.Dateint,
		FrequencyMs:    int32(firstItem.FrequencyMs),
		InstanceNum:    firstItem.InstanceNum,
		SlotID:         firstItem.SlotID,
		StartTs:        startTs,
		EndTs:          endTs,
	})
	if err != nil {
		ll.Error("Failed to get existing metric segments", slog.Any("error", err))
		return err
	}

	err = rollupMetricSegments(ctx, ll, mdb, tmpdir, firstItem, profile, s3client, sourceRows, existingRows, previousFrequency)

	if err != nil {
		ll.Info("Metric rollup batch completed",
			slog.String("result", "error"),
			slog.Int("batchSize", len(claimedWork)),
			slog.Duration("elapsed", time.Since(t0)))
		return err
	} else {
		ll.Info("Metric rollup batch completed",
			slog.String("result", "success"),
			slog.Int("batchSize", len(claimedWork)),
			slog.Duration("elapsed", time.Since(t0)))
		return nil
	}
}

func rollupMetricSegments(
	ctx context.Context,
	ll *slog.Logger,
	mdb rollupStore,
	tmpdir string,
	firstItem lrdb.ClaimMetricRollupWorkRow,
	profile storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	sourceRows []lrdb.MetricSeg,
	existingRows []lrdb.MetricSeg,
	previousFrequency int32,
) error {
	if len(sourceRows) == 0 {
		ll.Debug("No source rows to rollup, skipping")
		return nil
	}

	// Download all source files and create ParquetRawReaders with sorting support
	config := metricsprocessing.ReaderStackConfig{
		FileSortedCounter: fileSortedCounter,
		CommonAttributes:  commonAttributes,
	}

	startTs := time.Unix(0, 0).UTC().UnixMilli() // Default, will be updated from sourceRows
	if len(sourceRows) > 0 {
		// Find the earliest start time from source segments using TsRange
		firstBound, _, ok := helpers.RangeBounds(sourceRows[0].TsRange)
		if ok {
			minStartTs := firstBound.Int64
			for _, row := range sourceRows {
				if startBound, _, ok := helpers.RangeBounds(row.TsRange); ok {
					rowStartTs := startBound.Int64
					if rowStartTs < minStartTs {
						minStartTs = rowStartTs
					}
				}
			}
			startTs = minStartTs
		}
	}

	readerStack, err := metricsprocessing.CreateReaderStack(
		ctx, ll, tmpdir, s3client, firstItem.OrganizationID, profile, startTs, sourceRows, config)
	if err != nil {
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
	aggregatingReader, err := filereader.NewAggregatingMetricsReader(finalReader, firstItem.FrequencyMs, 1000)
	if err != nil {
		ll.Error("Failed to create aggregating metrics reader", slog.Any("error", err))
		return fmt.Errorf("creating aggregating metrics reader: %w", err)
	}
	defer aggregatingReader.Close()

	recordsPerFile := int64(10_000) // Default, could be made configurable

	writer, err := factories.NewMetricsWriter(tmpdir, constants.WriterTargetSizeBytesMetrics, recordsPerFile)
	if err != nil {
		ll.Error("Failed to create metrics writer", slog.Any("error", err))
		return fmt.Errorf("creating metrics writer: %w", err)
	}
	defer writer.Abort()

	totalRows := int64(0)

	for {
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
			if err := metricsprocessing.NormalizeRowForParquetWrite(row); err != nil {
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

	ll.Debug("Rollup completed",
		slog.Int64("totalRows", totalRows),
		slog.Int("outputFiles", len(results)),
		slog.Int("inputFiles", len(downloadedFiles)),
		slog.Int64("recordsPerFile", recordsPerFile))

	// Create rollup upload params
	rollupParams := metricsprocessing.CompactionUploadParams{
		OrganizationID: firstItem.OrganizationID.String(),
		InstanceNum:    firstItem.InstanceNum,
		Dateint:        firstItem.Dateint,
		FrequencyMs:    int32(firstItem.FrequencyMs),
		SlotID:         firstItem.SlotID,
		SlotCount:      firstItem.SlotCount,
		IngestDateint:  metricsprocessing.GetIngestDateint(sourceRows),
		CollectorName:  profile.CollectorName,
		Bucket:         profile.Bucket,
	}

	// Use context without cancellation for critical section to ensure atomic completion
	criticalCtx := context.WithoutCancel(ctx)

	// Upload rolled-up metrics using custom rollup logic
	err = uploadRolledUpMetrics(criticalCtx, ll, mdb, s3client, results, existingRows, rollupParams)
	if err != nil {
		return fmt.Errorf("failed to upload rolled-up metrics: %w", err)
	}

	// Mark source rows as rolled up
	if err := markSourceRowsAsRolledUp(criticalCtx, ll, mdb, sourceRows); err != nil {
		ll.Error("Failed to mark source rows as rolled up", slog.Any("error", err))
		// This is not a critical failure - the rollup succeeded but we couldn't update the flag
		// The next run will skip these since they've already been processed
	}

	// Schedule cleanup of old files - TODO: implement this by extending the rollupStore interface
	// metricsprocessing.ScheduleOldFileCleanup(criticalCtx, ll, mdb, existingRows, profile)

	// Queue next level rollup and compaction
	if err := queueMetricCompaction(criticalCtx, mdb, rollupParams); err != nil {
		ll.Error("Failed to queue metric compaction", slog.Any("error", err))
	}
	if err := queueMetricRollup(criticalCtx, mdb, rollupParams); err != nil {
		ll.Error("Failed to queue metric rollup", slog.Any("error", err))
	}

	return nil
}

var (
	meter                = otel.Meter("github.com/cardinalhq/lakerunner/internal/metricsprocessing/rollup")
	fileSortedCounter, _ = meter.Int64Counter("lakerunner.metric.rollup.file.sorted")
	commonAttributes     = attribute.NewSet(
		attribute.String("component", "metric-rollup"),
	)
)

// uploadRolledUpMetrics uploads rolled-up metric files using the same atomic pattern as ingestion
func uploadRolledUpMetrics(
	ctx context.Context,
	ll *slog.Logger,
	mdb rollupStore,
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
		SlotCount:      params.SlotCount,
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
			SegmentID: row.SegmentID,
			SlotID:    row.SlotID,
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
			t := time.Unix(startTs/1000, 0).UTC()
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
			SlotCount:      replaceParams.SlotCount,
			OldRecords:     replaceParams.OldRecords,
			SortVersion:    replaceParams.SortVersion,
			NewRecords: []lrdb.ReplaceMetricSegsNew{
				{
					SegmentID:    segmentID,
					StartTs:      startTs,
					EndTs:        endTs,
					RecordCount:  file.RecordCount,
					FileSize:     file.FileSize,
					Fingerprints: fingerprints,
				},
			},
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
	mdb rollupStore,
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

// queueMetricCompaction queues compaction work for the rollup results
func queueMetricCompaction(ctx context.Context, mdb rollupStore, params metricsprocessing.CompactionUploadParams) error {
	// Queue compaction work for the target frequency
	orgUUID, err := uuid.Parse(params.OrganizationID)
	if err != nil {
		return fmt.Errorf("invalid organization ID: %w", err)
	}

	priority := priorityForFrequencyForCompaction(params.FrequencyMs)
	if priority == 0 {
		return nil // Skip compaction for unknown frequencies
	}

	// Create time range for compaction work
	startTime := time.Unix(0, 0).UTC() // This should be derived from the actual data timestamps
	endTime := startTime.Add(time.Duration(params.FrequencyMs) * time.Millisecond)
	tsRange := helpers.TimeRange{Start: startTime, End: endTime}.ToPgRange()

	err = mdb.PutMetricCompactionWork(ctx, lrdb.PutMetricCompactionWorkParams{
		OrganizationID: orgUUID,
		Dateint:        params.Dateint,
		FrequencyMs:    int64(params.FrequencyMs),
		SegmentID:      0, // This should be derived from the actual segment
		InstanceNum:    params.InstanceNum,
		TsRange:        tsRange,
		RecordCount:    0, // This should be derived from the actual record count
		Priority:       priority,
	})

	if err != nil {
		return fmt.Errorf("failed to queue metric compaction work: %w", err)
	}

	return nil
}

// queueMetricRollup queues next level rollup work for the rollup results
func queueMetricRollup(ctx context.Context, mdb rollupStore, params metricsprocessing.CompactionUploadParams) error {
	// Queue next level rollup work if there's a higher frequency
	nextFrequency, hasNext := helpers.RollupNotifications[params.FrequencyMs]
	if !hasNext {
		return nil // No next level to roll up to
	}

	orgUUID, err := uuid.Parse(params.OrganizationID)
	if err != nil {
		return fmt.Errorf("invalid organization ID: %w", err)
	}

	priority := priorityForFrequencyForRollup(nextFrequency)
	if priority == 0 {
		return nil // Skip rollup for unknown frequencies
	}

	// Create time range for rollup work
	startTime := time.Unix(0, 0).UTC() // This should be derived from the actual data timestamps
	endTime := startTime.Add(time.Duration(nextFrequency) * time.Millisecond)
	tsRange := helpers.TimeRange{Start: startTime, End: endTime}.ToPgRange()

	err = mdb.PutMetricRollupWork(ctx, lrdb.PutMetricRollupWorkParams{
		OrganizationID: orgUUID,
		Dateint:        params.Dateint,
		FrequencyMs:    int64(nextFrequency),
		InstanceNum:    params.InstanceNum,
		SlotID:         params.SlotID,
		SlotCount:      params.SlotCount,
		TsRange:        tsRange,
		Priority:       priority,
	})

	if err != nil {
		return fmt.Errorf("failed to queue metric rollup work: %w", err)
	}

	return nil
}

// Priority functions for compaction and rollup work
func priorityForFrequencyForCompaction(f int32) int32 {
	switch f {
	case 10_000:
		return 1001
	case 60_000:
		return 801
	case 300_000:
		return 601
	case 1_200_000:
		return 401
	case 3_600_000:
		return 201
	default:
		return 0
	}
}

func priorityForFrequencyForRollup(f int32) int32 {
	switch f {
	case 10_000:
		return 1000
	case 60_000:
		return 800
	case 300_000:
		return 600
	case 1_200_000:
		return 400
	case 3_600_000:
		return 200
	default:
		return 0
	}
}
