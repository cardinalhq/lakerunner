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

	// Generate batch ID and enhance logger
	batchID := idgen.GenerateShortBase32ID()
	ll = ll.With(slog.String("batchID", batchID))

	// Log work items we're processing once at the top
	workItemIDs := make([]int64, len(claimedWork))
	for i, work := range claimedWork {
		workItemIDs[i] = work.ID
	}
	ll.Debug("Processing rollup batch",
		slog.Int("workItemCount", len(claimedWork)),
		slog.Any("workItemIDs", workItemIDs))

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

	// Find the target frequency for this source frequency
	// We have work at source frequency (e.g., 10_000) and want to aggregate to target (60_000)
	targetFrequency, found := RollupTo[int32(firstItem.FrequencyMs)]
	if !found {
		var validSources []int32
		for freq := range RollupTo {
			validSources = append(validSources, freq)
		}
		ll.Error("Invalid rollup frequency - not in source frequency list. This work item will be marked as completed to avoid reprocessing.",
			slog.Int64("frequencyMs", firstItem.FrequencyMs),
			slog.Any("validSourceFrequencies", validSources))
		// Return nil to indicate successful processing so the work gets marked as completed
		// This prevents these invalid work items from being retried indefinitely
		return nil
	}

	sourceFrequency := int32(firstItem.FrequencyMs) // This is the source frequency we're rolling up from

	// At this point, we know both frequencies are valid since we found them in RollupTo

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
		slog.Int("sourceFrequencyMs", int(sourceFrequency)),
		slog.Int("targetFrequencyMs", int(targetFrequency)),
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

	// Get source segments to rollup from (source frequency)
	sourceRows, err := mdb.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: firstItem.OrganizationID,
		Dateint:        firstItem.Dateint,
		FrequencyMs:    sourceFrequency,
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
		FrequencyMs:    targetFrequency,
		InstanceNum:    firstItem.InstanceNum,
		SlotID:         firstItem.SlotID,
		StartTs:        startTs,
		EndTs:          endTs,
	})
	if err != nil {
		ll.Error("Failed to get existing metric segments", slog.Any("error", err))
		return err
	}

	err = rollupMetricSegments(ctx, ll, mdb, tmpdir, firstItem, profile, s3client, sourceRows, existingRows, sourceFrequency, targetFrequency)

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
	sourceFrequency int32,
	targetFrequency int32,
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

	if len(readerStack.Readers) == 0 {
		ll.Debug("No files to rollup, skipping work item")
		return nil
	}

	defer metricsprocessing.CloseReaderStack(ll, readerStack)

	// Wrap with aggregating reader to perform rollup aggregation
	// Use target frequency for aggregation period
	aggregatingReader, err := filereader.NewAggregatingMetricsReader(readerStack.FinalReader, int64(targetFrequency), 1000)
	if err != nil {
		ll.Error("Failed to create aggregating metrics reader", slog.Any("error", err))
		return fmt.Errorf("creating aggregating metrics reader: %w", err)
	}
	defer aggregatingReader.Close()

	writer, err := factories.NewMetricsWriter(tmpdir, constants.WriterTargetSizeBytesMetrics, metricsprocessing.DefaultRecordsPerFileRollup)
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

			// Copy row to the new batch
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
		slog.Int("inputFiles", len(readerStack.DownloadedFiles)),
		slog.Int64("recordsPerFile", metricsprocessing.DefaultRecordsPerFileRollup))

	// Create rollup upload params
	rollupParams := metricsprocessing.CompactionUploadParams{
		OrganizationID: firstItem.OrganizationID,
		InstanceNum:    firstItem.InstanceNum,
		Dateint:        firstItem.Dateint,
		FrequencyMs:    targetFrequency,
		SlotID:         firstItem.SlotID,
		SlotCount:      firstItem.SlotCount,
		IngestDateint:  metricsprocessing.GetIngestDateint(sourceRows),
		CollectorName:  profile.CollectorName,
		Bucket:         profile.Bucket,
	}

	// Use context without cancellation for critical section to ensure atomic completion
	criticalCtx := context.WithoutCancel(ctx)

	// Upload rolled-up metrics using atomic rollup logic
	err = uploadRolledUpMetricsAtomic(criticalCtx, ll, mdb, s3client, results, sourceRows, existingRows, rollupParams)
	if err != nil {
		return fmt.Errorf("failed to upload rolled-up metrics: %w", err)
	}

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

// uploadRolledUpMetricsAtomic uploads rolled-up metric files using atomic rollup transaction
func uploadRolledUpMetricsAtomic(
	ctx context.Context,
	ll *slog.Logger,
	mdb rollupStore,
	s3client *awsclient.S3Client,
	results []parquetwriter.Result,
	sourceRows []lrdb.MetricSeg,
	existingRows []lrdb.MetricSeg,
	params metricsprocessing.CompactionUploadParams,
) error {
	// Build list of existing target records to replace
	var targetOldRecords []lrdb.ReplaceMetricSegsOld
	for _, row := range existingRows {
		targetOldRecords = append(targetOldRecords, lrdb.ReplaceMetricSegsOld{
			SegmentID: row.SegmentID,
			SlotID:    row.SlotID,
		})
	}

	// Build list of source segment IDs to mark as rolled up
	sourceSegmentIDs := make([]int64, len(sourceRows))
	for i, row := range sourceRows {
		sourceSegmentIDs[i] = row.SegmentID
	}

	// Process each output file atomically
	for _, file := range results {
		filestats, err := metricsprocessing.ExtractFileMetadata(file, ll)
		if err != nil {
			return fmt.Errorf("failed to extract file metadata: %w", err)
		}

		fileLogger := ll.With(slog.String("file", file.FileName))

		fileLogger.Debug("Starting atomic metric rollup upload operation",
			slog.Int64("recordCount", file.RecordCount),
			slog.Int64("fileSize", file.FileSize),
			slog.Int64("startTs", filestats.StartTs),
			slog.Int64("endTs", filestats.EndTs))

		segmentID := s3helper.GenerateID()
		newObjectID := helpers.MakeDBObjectID(params.OrganizationID, params.CollectorName, filestats.Dateint, filestats.Hour, segmentID, "metrics")

		fileLogger.Debug("Uploading rolled-up metric file to S3 - point of no return approaching",
			slog.String("newObjectID", newObjectID),
			slog.String("bucket", params.Bucket),
			slog.Int64("newSegmentID", segmentID))

		err = s3helper.UploadS3Object(ctx, s3client, params.Bucket, newObjectID, file.FileName)
		if err != nil {
			fileLogger.Error("Atomic operation failed during S3 upload - no changes made",
				slog.Any("error", err),
				slog.String("objectID", newObjectID))
			return fmt.Errorf("uploading new S3 object: %w", err)
		}

		fileLogger.Debug("S3 upload successful, updating database with atomic rollup transaction - CRITICAL SECTION",
			slog.String("uploadedObject", newObjectID))

		// Prepare rollup arguments with source frequency info
		sourceFrequency := int32(0)
		if len(sourceRows) > 0 {
			sourceFrequency = sourceRows[0].FrequencyMs
		}

		if err := mdb.RollupMetricSegs(ctx, lrdb.RollupMetricSegsParams{
			SourceSegments: struct {
				OrganizationID uuid.UUID
				Dateint        int32
				FrequencyMs    int32
				InstanceNum    int16
				SlotID         int32
				SegmentIDs     []int64
			}{
				OrganizationID: params.OrganizationID,
				Dateint:        params.Dateint,
				FrequencyMs:    sourceFrequency,
				InstanceNum:    params.InstanceNum,
				SlotID:         params.SlotID,
				SegmentIDs:     sourceSegmentIDs,
			},
			TargetReplacement: lrdb.ReplaceMetricSegsParams{
				OrganizationID: params.OrganizationID,
				Dateint:        params.Dateint,
				FrequencyMs:    params.FrequencyMs,
				InstanceNum:    params.InstanceNum,
				IngestDateint:  params.IngestDateint,
				Published:      true,
				Rolledup:       false, // Rolled-up data itself is not considered "rolled up" - it's a target
				CreatedBy:      lrdb.CreateByRollup,
				SlotID:         params.SlotID,
				SlotCount:      params.SlotCount,
				OldRecords:     targetOldRecords,
				SortVersion:    lrdb.CurrentMetricSortVersion,
				NewRecords: []lrdb.ReplaceMetricSegsNew{
					{
						SegmentID:    segmentID,
						StartTs:      filestats.StartTs,
						EndTs:        filestats.EndTs,
						RecordCount:  file.RecordCount,
						FileSize:     file.FileSize,
						Fingerprints: filestats.Fingerprints,
					},
				},
			},
		}); err != nil {
			fileLogger.Error("Database rollup transaction failed after S3 upload - file orphaned in S3",
				slog.Any("error", err),
				slog.String("orphanedObject", newObjectID),
				slog.Int64("orphanedSegmentID", segmentID),
				slog.String("bucket", params.Bucket))

			// Mark orphaned file for deletion by sweeper
			if scheduleErr := s3helper.ScheduleS3Delete(ctx, mdb, params.OrganizationID, params.InstanceNum, params.Bucket, newObjectID); scheduleErr != nil {
				fileLogger.Error("Failed to schedule orphaned S3 object for deletion",
					slog.Any("error", scheduleErr),
					slog.String("objectID", newObjectID),
					slog.String("bucket", params.Bucket))
			} else {
				fileLogger.Info("Scheduled orphaned S3 object for deletion",
					slog.String("objectID", newObjectID))
			}
			return fmt.Errorf("atomic rollup transaction: %w", err)
		}

		fileLogger.Debug("ATOMIC ROLLUP OPERATION COMMITTED SUCCESSFULLY - source marked as rolled up, target segments replaced",
			slog.Int64("newSegmentID", segmentID),
			slog.Int64("newRecordCount", file.RecordCount),
			slog.Int64("newFileSize", file.FileSize),
			slog.String("newObjectID", newObjectID),
			slog.Int("sourceSegmentCount", len(sourceSegmentIDs)),
			slog.Int("targetReplacedCount", len(targetOldRecords)))
	}

	return nil
}

// queueMetricCompaction queues compaction work for the rollup results
func queueMetricCompaction(ctx context.Context, mdb rollupStore, params metricsprocessing.CompactionUploadParams) error {
	priority := priorityForFrequencyForCompaction(params.FrequencyMs)
	if priority == 0 {
		return nil // Skip compaction for unknown frequencies
	}

	// Create time range for compaction work
	startTime := time.Unix(0, 0).UTC() // This should be derived from the actual data timestamps
	endTime := startTime.Add(time.Duration(params.FrequencyMs) * time.Millisecond)
	tsRange := helpers.TimeRange{Start: startTime, End: endTime}.ToPgRange()

	err := mdb.PutMetricCompactionWork(ctx, lrdb.PutMetricCompactionWorkParams{
		OrganizationID: params.OrganizationID,
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
	nextFrequency, exists := RollupTo[params.FrequencyMs]
	if !exists {
		return nil // No next level to roll up to
	}

	priority := priorityForFrequencyForRollup(nextFrequency)
	if priority == 0 {
		return nil // Skip rollup for unknown frequencies
	}

	// Create time range for rollup work
	startTime := time.Unix(0, 0).UTC() // This should be derived from the actual data timestamps
	endTime := startTime.Add(time.Duration(nextFrequency) * time.Millisecond)
	tsRange := helpers.TimeRange{Start: startTime, End: endTime}.ToPgRange()

	err := mdb.PutMetricRollupWork(ctx, lrdb.PutMetricRollupWorkParams{
		OrganizationID: params.OrganizationID,
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
	return GetFrequencyPriority(f) + 200
}

func priorityForFrequencyForRollup(f int32) int32 {
	return GetFrequencyPriority(f) + 100
}
