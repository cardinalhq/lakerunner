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
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// RollupUploadParams contains parameters for uploading rolled-up metric files.
type RollupUploadParams struct {
	OrganizationID uuid.UUID
	InstanceNum    int16
	Dateint        int32
	FrequencyMs    int32
	SlotID         int32
	SlotCount      int32
	IngestDateint  int32
	CollectorName  string
	Bucket         string
}

func processBatch(
	ctx context.Context,
	mdb rollupStore,
	sp storageprofile.StorageProfileProvider,
	cmgr *cloudstorage.CloudManagers,
	bundle lrdb.RollupBundleResult,
) error {
	if len(bundle.Items) == 0 {
		return nil
	}

	// Generate batch ID and enhance logger
	batchID := idgen.GenerateShortBase32ID()
	ll := logctx.FromContext(ctx).With(slog.String("batchID", batchID))

	// Log work items we're processing once at the top
	workItemIDs := make([]int64, len(bundle.Items))
	for i, item := range bundle.Items {
		workItemIDs[i] = item.ID
	}
	ll.Debug("Processing rollup batch",
		slog.Int("workItemCount", len(bundle.Items)),
		slog.Any("workItemIDs", workItemIDs),
		slog.Int64("estimatedTarget", bundle.EstimatedTarget))

	// Get source segments to rollup from using specific segment IDs
	sourceRows, err := fetchMetricSegsFromCandidates(ctx, mdb, bundle.Items)
	if err != nil {
		ll.Error("Failed to fetch metric segments for rollup", slog.Any("error", err))
		return err
	}

	if len(sourceRows) == 0 {
		ll.Info("No source segments found for claimed work items")
		return nil
	}

	// Extract key information from the first segment
	firstSeg := sourceRows[0]

	// Work items represent source frequencies that roll up to target frequencies
	sourceFrequency := firstSeg.FrequencyMs
	targetFrequency, found := metricsprocessing.RollupTo[sourceFrequency]
	if !found {
		ll.Error("Invalid rollup frequency - not in source frequency list. This work item will be marked as completed to avoid reprocessing.",
			slog.Int64("frequencyMs", int64(sourceFrequency)))
		// Return nil to indicate successful processing so the work gets marked as completed
		// This prevents these invalid work items from being retried indefinitely
		return nil
	}

	profile, err := sp.GetStorageProfileForOrganizationAndInstance(ctx, firstSeg.OrganizationID, firstSeg.InstanceNum)
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return err
	}

	blobclient, err := cloudstorage.NewClient(ctx, cmgr, profile)
	if err != nil {
		ll.Error("Failed to get S3 client", slog.Any("error", err))
		return err
	}

	tmpdir, err := os.MkdirTemp("", "")
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
		slog.String("organizationID", firstSeg.OrganizationID.String()),
		slog.Int("instanceNum", int(firstSeg.InstanceNum)),
		slog.Int("dateint", int(firstSeg.Dateint)),
		slog.Int("sourceFrequencyMs", int(sourceFrequency)),
		slog.Int("targetFrequencyMs", int(targetFrequency)),
		slog.Int("slotID", int(firstSeg.SlotID)),
		slog.Int("slotCount", int(firstSeg.SlotCount)),
		slog.Int("batchSize", len(bundle.Items)))

	t0 := time.Now()

	// Log if we got fewer segments than expected
	if len(sourceRows) < len(bundle.Items) {
		ll.Error("Retrieved fewer segments than work items - some segments may be missing",
			slog.Int("expectedSegments", len(bundle.Items)),
			slog.Int("retrievedSegments", len(sourceRows)))
	} else if len(sourceRows) > len(bundle.Items) {
		ll.Error("Retrieved more segments than work items - this should not happen",
			slog.Int("expectedSegments", len(bundle.Items)),
			slog.Int("retrievedSegments", len(sourceRows)))
	}

	ll.Debug("Retrieved source segments for rollup analysis",
		slog.Int("sourceSegmentCount", len(sourceRows)),
		slog.Int("sourceFrequencyMs", int(sourceFrequency)),
		slog.Int("targetFrequencyMs", int(targetFrequency)))

	if helpers.AllRolledUp(sourceRows) {
		ll.Info("All source segments already rolled up, skipping rollup operation",
			slog.Int("sourceSegmentCount", len(sourceRows)),
			slog.Int("sourceFrequencyMs", int(firstSeg.FrequencyMs)),
			slog.Int("targetFrequencyMs", int(targetFrequency)))
		return nil
	}

	err = rollupMetricSegments(ctx, mdb, tmpdir, firstSeg, profile, blobclient, sourceRows, targetFrequency, bundle.EstimatedTarget)

	elapsed := time.Since(t0)

	if err != nil {
		ll.Info("Metric rollup batch completed",
			slog.String("result", "error"),
			slog.Int("batchSize", len(bundle.Items)),
			slog.Int("sourceSegmentCount", len(sourceRows)),
			slog.Duration("elapsed", elapsed),
			slog.Any("error", err))
		return err
	} else {
		ll.Info("Metric rollup batch completed",
			slog.String("result", "success"),
			slog.Int("batchSize", len(bundle.Items)),
			slog.Int("sourceSegmentCount", len(sourceRows)),
			slog.Int("sourceFrequencyMs", int(firstSeg.FrequencyMs)),
			slog.Int("targetFrequencyMs", int(targetFrequency)),
			slog.Duration("elapsed", elapsed))
		return nil
	}
}

func rollupMetricSegments(
	ctx context.Context,
	mdb rollupStore,
	tmpdir string,
	firstSeg lrdb.MetricSeg,
	profile storageprofile.StorageProfile,
	blobclient cloudstorage.Client,
	sourceRows []lrdb.MetricSeg,
	targetFrequency int32,
	estimatedTargetRecords int64,
) error {
	ll := logctx.FromContext(ctx)

	if len(sourceRows) == 0 {
		ll.Info("No source segments found for rollup, skipping rollup operation",
			slog.Int("sourceFrequencyMs", int(firstSeg.FrequencyMs)),
			slog.Int("targetFrequencyMs", int(targetFrequency)),
			slog.String("organizationID", firstSeg.OrganizationID.String()),
			slog.Int("dateint", int(firstSeg.Dateint)))
		return nil
	}

	// Track download time
	downloadStart := time.Now()
	readerStack, err := metricsprocessing.CreateReaderStack(ctx, tmpdir, blobclient, firstSeg.OrganizationID, profile, sourceRows)
	if err != nil {
		return err
	}
	downloadDuration := time.Since(downloadStart)
	ll.Info("Download phase completed",
		slog.Duration("duration", downloadDuration),
		slog.Int("filesDownloaded", len(sourceRows)))

	if len(readerStack.Readers) == 0 {
		ll.Info("No files available for rollup processing, skipping work item",
			slog.Int("sourceSegmentCount", len(sourceRows)),
			slog.Int("sourceFrequencyMs", int(firstSeg.FrequencyMs)),
			slog.Int("targetFrequencyMs", int(targetFrequency)),
			slog.String("organizationID", firstSeg.OrganizationID.String()),
			slog.Int("dateint", int(firstSeg.Dateint)))
		return nil
	}

	defer metricsprocessing.CloseReaderStack(ctx, readerStack)

	// Calculate input statistics from source segments
	inputBytes := int64(0)
	inputRecords := int64(0)
	for _, row := range sourceRows {
		inputBytes += row.FileSize
		inputRecords += row.RecordCount
	}

	// Input metrics are tracked in the generic processor

	ll.Debug("Starting rollup processing",
		slog.Int("inputFiles", len(readerStack.DownloadedFiles)),
		slog.Int("sourceSegments", len(sourceRows)),
		slog.Int64("inputRecords", inputRecords),
		slog.Int64("inputBytes", inputBytes),
		slog.Int("targetFrequencyMs", int(targetFrequency)),
		slog.Int64("estimatedTargetRecords", estimatedTargetRecords))

	// Use generic processor for rollup
	processingInput := metricsprocessing.ProcessingInput{
		ReaderStack:       readerStack,
		TargetFrequencyMs: targetFrequency, // Target frequency for rollup
		TmpDir:            tmpdir,
		RecordsLimit:      estimatedTargetRecords,
		EstimatedRecords:  estimatedTargetRecords,
		Action:            "rollup",
		InputRecords:      inputRecords,
		InputBytes:        inputBytes,
	}

	// Track aggregation time
	aggregateStart := time.Now()
	processingResult, err := metricsprocessing.AggregateMetrics(ctx, processingInput)
	if err != nil {
		return fmt.Errorf("rollup processing failed: %w", err)
	}
	aggregateDuration := time.Since(aggregateStart)
	ll.Info("Aggregation phase completed",
		slog.Duration("duration", aggregateDuration),
		slog.Int64("inputRecords", inputRecords),
		slog.Int64("outputRecords", processingResult.Stats.OutputRecords))

	// Calculate compression ratio
	compressionRatio := float64(0)
	if inputBytes > 0 {
		compressionRatio = float64(processingResult.Stats.OutputBytes) / float64(inputBytes) * 100
	}

	ll.Debug("Rollup processing completed",
		slog.Int64("inputRecords", inputRecords),
		slog.Int64("outputRecords", processingResult.Stats.OutputRecords),
		slog.Int64("totalRows", processingResult.Stats.TotalRows),
		slog.Int("batchCount", processingResult.Stats.BatchCount),
		slog.Int("outputFiles", processingResult.Stats.OutputSegments),
		slog.Int("inputFiles", len(readerStack.DownloadedFiles)),
		slog.Int64("inputBytes", inputBytes),
		slog.Int64("outputBytes", processingResult.Stats.OutputBytes),
		slog.Float64("compressionRatio", compressionRatio))

	rollupParams := RollupUploadParams{
		OrganizationID: firstSeg.OrganizationID,
		InstanceNum:    firstSeg.InstanceNum,
		Dateint:        firstSeg.Dateint,
		FrequencyMs:    targetFrequency,
		SlotID:         firstSeg.SlotID,
		SlotCount:      firstSeg.SlotCount,
		IngestDateint:  metricsprocessing.GetIngestDateint(sourceRows),
		CollectorName:  profile.CollectorName,
		Bucket:         profile.Bucket,
	}

	criticalCtx := context.WithoutCancel(ctx)

	// Track upload time
	uploadStart := time.Now()
	err = uploadRolledUpMetricsAtomic(criticalCtx, mdb, blobclient, processingResult.RawResults, sourceRows, rollupParams)
	if err != nil {
		return fmt.Errorf("failed to upload rolled-up metrics: %w", err)
	}
	uploadDuration := time.Since(uploadStart)
	ll.Info("Upload phase completed",
		slog.Duration("duration", uploadDuration),
		slog.Int("filesUploaded", len(processingResult.RawResults)),
		slog.Int64("bytesUploaded", processingResult.Stats.OutputBytes))

	return nil
}

// fetchMetricSegsFromCandidates retrieves the MetricSeg records corresponding to the claimed candidates
// by extracting their segment IDs and fetching the segments from the database.
func fetchMetricSegsFromCandidates(ctx context.Context, db rollupStore, candidates []lrdb.MrqFetchCandidatesRow) ([]lrdb.MetricSeg, error) {
	if len(candidates) == 0 {
		return nil, nil
	}

	segmentIDs := make([]int64, len(candidates))
	for i, candidate := range candidates {
		segmentIDs[i] = candidate.SegmentID
	}

	firstItem := candidates[0]

	segments, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: firstItem.OrganizationID,
		Dateint:        firstItem.Dateint,
		FrequencyMs:    firstItem.FrequencyMs,
		InstanceNum:    firstItem.InstanceNum,
		SegmentIds:     segmentIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metric segments: %w", err)
	}

	return segments, nil
}

func uploadRolledUpMetricsAtomic(
	ctx context.Context,
	mdb rollupStore,
	blobclient cloudstorage.Client,
	results []parquetwriter.Result,
	sourceRows []lrdb.MetricSeg,
	params RollupUploadParams,
) error {
	ll := logctx.FromContext(ctx)

	// Create processed segments from results
	segments, err := metricsprocessing.CreateSegmentsFromResults(ctx, results, params.OrganizationID, params.CollectorName)
	if err != nil {
		return fmt.Errorf("failed to create processed segment: %w", err)
	}

	sourceSegmentIDs := make([]int64, len(sourceRows))
	for i, row := range sourceRows {
		sourceSegmentIDs[i] = row.SegmentID
	}

	ll.Debug("Starting atomic rollup operations for rolled-up files",
		slog.Int("fileCount", len(segments)),
		slog.Int("sourceSegmentCount", len(sourceSegmentIDs)))

	// Upload all segments to S3 first
	ll.Debug("Uploading rolled-up metric files to S3 - point of no return approaching",
		slog.String("bucket", params.Bucket),
		slog.Int("segmentCount", len(segments)))

	if _, err := metricsprocessing.UploadSegments(ctx, blobclient, params.Bucket, segments); err != nil {
		ll.Error("Atomic operation failed during S3 upload - no changes made",
			slog.Any("error", err))
		return fmt.Errorf("uploading new S3 objects: %w", err)
	}

	// Build all new records for atomic database update
	newRecords := make([]lrdb.RollupNewRecord, 0, len(segments))
	for _, segment := range segments {
		newRecords = append(newRecords, lrdb.RollupNewRecord{
			SegmentID:    segment.SegmentID,
			StartTs:      segment.StartTs,
			EndTs:        segment.EndTs,
			RecordCount:  segment.Result.RecordCount,
			FileSize:     segment.Result.FileSize,
			Fingerprints: segment.Fingerprints,
		})
	}

	ll.Debug("S3 uploads successful, updating database with atomic rollup transaction - CRITICAL SECTION",
		slog.Int("newSegmentCount", len(newRecords)),
		slog.Int("sourceSegmentCount", len(sourceSegmentIDs)))

	sourceFrequency := sourceRows[0].FrequencyMs

	// Single atomic database transaction for all segments
	if err := mdb.RollupMetricSegs(ctx,
		lrdb.RollupSourceParams{
			OrganizationID: params.OrganizationID,
			Dateint:        params.Dateint,
			FrequencyMs:    sourceFrequency,
			InstanceNum:    params.InstanceNum,
		},
		lrdb.RollupTargetParams{
			OrganizationID: params.OrganizationID,
			Dateint:        params.Dateint,
			FrequencyMs:    params.FrequencyMs,
			InstanceNum:    params.InstanceNum,
			SlotID:         params.SlotID,
			SlotCount:      params.SlotCount,
			IngestDateint:  params.IngestDateint,
			SortVersion:    lrdb.CurrentMetricSortVersion,
		},
		sourceSegmentIDs,
		newRecords,
	); err != nil {
		ll.Error("Database rollup transaction failed after S3 upload - files orphaned in S3",
			slog.Any("error", err),
			slog.Int("orphanedCount", len(segments)),
			slog.String("bucket", params.Bucket))

		// Schedule cleanup for all orphaned segments
		for _, segment := range segments {
			if scheduleErr := segment.ScheduleCleanupIfUploaded(ctx, mdb, params.OrganizationID, params.InstanceNum, params.Bucket); scheduleErr != nil {
				ll.Error("Failed to schedule orphaned S3 object for deletion",
					slog.Any("error", scheduleErr),
					slog.String("objectID", segment.ObjectID),
					slog.String("bucket", params.Bucket))
			} else {
				ll.Info("Scheduled orphaned S3 object for deletion",
					slog.String("objectID", segment.ObjectID))
			}
		}
		return fmt.Errorf("atomic rollup transaction: %w", err)
	}

	ll.Debug("Source marked as rolled up, new target segments inserted - rollup transaction complete",
		slog.Int("newSegmentCount", len(newRecords)),
		slog.Int("sourceSegmentCount", len(sourceSegmentIDs)),
		slog.Int("sourceFrequencyMs", int(sourceFrequency)),
		slog.Int("targetFrequencyMs", int(params.FrequencyMs)))

	// Queue compaction and rollup work for all processed segments
	if err := segments.QueueCompactionWork(ctx, mdb, params.OrganizationID, params.InstanceNum, params.FrequencyMs); err != nil {
		ll.Error("Failed to queue compaction work for rolled-up segments", slog.Any("error", err))
	}

	if err := segments.QueueRollupWork(ctx, mdb, params.OrganizationID, params.InstanceNum, params.FrequencyMs, params.SlotID, params.SlotCount); err != nil {
		ll.Error("Failed to queue rollup work for rolled-up segments", slog.Any("error", err))
	}

	// Calculate final accumulated statistics
	totalOutputBytes := int64(0)
	totalOutputRecords := int64(0)
	for _, segment := range segments {
		totalOutputBytes += segment.Result.FileSize
		totalOutputRecords += segment.Result.RecordCount
	}

	// Calculate total input statistics
	totalInputBytes := int64(0)
	totalInputRecords := int64(0)
	for _, row := range sourceRows {
		totalInputBytes += row.FileSize
		totalInputRecords += row.RecordCount
	}

	ll.Info("Successfully completed atomic rollup operations",
		slog.Int("outputFilesCreated", len(results)),
		slog.Int("sourceSegmentsMarkedRolledUp", len(sourceSegmentIDs)),
		slog.Int64("inputRecords", totalInputRecords),
		slog.Int64("outputRecords", totalOutputRecords),
		slog.Int64("inputBytes", totalInputBytes),
		slog.Int64("outputBytes", totalOutputBytes),
		slog.Int("sourceFrequencyMs", int(sourceRows[0].FrequencyMs)),
		slog.Int("targetFrequencyMs", int(params.FrequencyMs)))

	return nil
}
