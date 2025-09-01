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

package compaction

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// coordinate handles S3 download, compaction, upload, and database update
func coordinate(
	ctx context.Context,
	ll *slog.Logger,
	mdb compactionStore,
	tmpdir string,
	workItem lrdb.ClaimMetricCompactionWorkRow,
	profile storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	rows []lrdb.MetricSeg,
) error {
	if len(rows) == 0 {
		ll.Debug("No segments to compact")
		return nil
	}

	// Handle single row case: just mark as compacted (keep published=true)
	if len(rows) == 1 {
		ll.Debug("Single segment found - marking as compacted instead of full processing",
			slog.Int64("segmentID", rows[0].SegmentID))

		segmentIDs := []int64{rows[0].SegmentID}
		err := mdb.SetMetricSegCompacted(ctx, lrdb.SetMetricSegCompactedParams{
			OrganizationID: workItem.OrganizationID,
			Dateint:        workItem.Dateint,
			FrequencyMs:    int32(workItem.FrequencyMs),
			InstanceNum:    workItem.InstanceNum,
			SegmentIds:     segmentIDs,
		})
		if err != nil {
			ll.Error("Failed to mark single segment as compacted",
				slog.Int64("segmentID", rows[0].SegmentID),
				slog.Any("error", err))
			return fmt.Errorf("failed to mark single segment as compacted: %w", err)
		}

		ll.Info("Successfully marked single segment as compacted",
			slog.Int64("segmentID", rows[0].SegmentID),
			slog.Int("oldSegmentCount", len(rows)),
			slog.Int("newSegmentCount", 1),
			slog.Int64("inputRecords", rows[0].RecordCount),
			slog.Int64("outputRecords", rows[0].RecordCount),
			slog.Int64("inputBytes", rows[0].FileSize),
			slog.Int64("outputBytes", rows[0].FileSize),
			slog.Int64("targetRecords", workItem.UsedTargetRecords),
			slog.String("estimateSource", workItem.EstimateSource))
		return nil
	}

	// Handle multiple rows case: proceed with full compaction
	if !shouldCompactMetrics(rows) {
		ll.Debug("No need to compact metrics in this batch", slog.Int("rowCount", len(rows)))
		return nil
	}

	if ctx.Err() != nil {
		ll.Info("Context cancelled before starting compaction",
			slog.Int("segmentCount", len(rows)),
			slog.Any("error", ctx.Err()))
		return newWorkerInterrupted("context cancelled before compaction")
	}

	meter := otel.Meter("github.com/cardinalhq/lakerunner/internal/metricsprocessing/compaction")
	fileSortedCounter, _ := meter.Int64Counter("lakerunner.metric.compact.file.sorted")
	commonAttributes := attribute.NewSet()

	config := metricsprocessing.ReaderStackConfig{
		FileSortedCounter: fileSortedCounter,
		CommonAttributes:  commonAttributes,
	}

	readerStack, err := metricsprocessing.CreateReaderStack(
		ctx, ll, tmpdir, s3client, workItem.OrganizationID, profile, rows, config)
	if err != nil {
		return err
	}
	defer metricsprocessing.CloseReaderStack(ll, readerStack)

	// Prepare input for pure compaction
	compactionInput := input{
		ReaderStack:  readerStack,
		FrequencyMs:  workItem.FrequencyMs,
		TmpDir:       tmpdir,
		Logger:       ll,
		RecordsLimit: workItem.UsedTargetRecords * 2,
	}

	// Perform pure compaction
	result, err := perform(ctx, compactionInput)
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	// Calculate input bytes and record counts from segment metadata
	inputBytes := int64(0)
	inputRecords := int64(0)
	for _, row := range rows {
		inputBytes += row.FileSize
		inputRecords += row.RecordCount
	}

	// Calculate statistics for logging
	stats := calculateStats(result, len(readerStack.DownloadedFiles), inputBytes)
	ll.Debug("Compaction completed",
		slog.Int64("totalRows", stats.TotalRows),
		slog.Int("outputFiles", stats.OutputFiles),
		slog.Int("inputFiles", stats.InputFiles),
		slog.Int64("inputBytes", stats.InputBytes),
		slog.Int64("outputBytes", stats.OutputBytes),
		slog.Float64("compressionRatio", stats.CompressionRatio))

	// If we produced 0 output files, skip S3 upload and database updates
	if len(result.Results) == 0 {
		ll.Warn("Produced 0 output files from aggregating reader")
		return nil
	}

	// Final interruption check before critical section (S3 uploads + DB updates)
	if ctx.Err() != nil {
		ll.Info("Context cancelled before critical section - safe to abort",
			slog.Int("resultCount", len(result.Results)),
			slog.Any("error", ctx.Err()))
		return newWorkerInterrupted("context cancelled before metrics upload phase")
	}

	// Upload files to S3 with deadline (10 seconds for spot instance compatibility)
	s3Ctx, s3Cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer s3Cancel()

	segments, err := uploadCompactedFiles(s3Ctx, ll, s3client, result.Results, workItem, profile)
	if err != nil {
		// If upload failed partway through, we need to clean up any uploaded files
		if len(segments) > 0 {
			ll.Warn("S3 upload failed partway through, scheduling cleanup",
				slog.Int("uploadedFiles", len(segments)),
				slog.Any("error", err))
			scheduleS3Cleanup(ctx, mdb, segments, workItem, profile)
		}
		return fmt.Errorf("failed to upload compacted files to S3: %w", err)
	}

	// Atomically replace segments in database with deadline (5 seconds)
	dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbCancel()

	err = replaceCompactedSegments(dbCtx, ll, mdb, segments, rows, workItem, inputRecords, inputBytes)
	if err != nil {
		// Database update failed after successful uploads - schedule cleanup
		ll.Error("Database update failed after successful S3 upload, scheduling cleanup",
			slog.Int("uploadedFiles", len(segments)),
			slog.Any("error", err))
		scheduleS3Cleanup(ctx, mdb, segments, workItem, profile)
		return fmt.Errorf("failed to replace compacted segments in database: %w", err)
	}

	return nil
}

// calculateStats computes statistics from compaction results
func calculateStats(result *result, inputFileCount int, inputBytes int64) stats {
	compressionRatio := float64(0)
	if inputBytes > 0 {
		compressionRatio = float64(result.OutputBytes) / float64(inputBytes) * 100
	}

	return stats{
		TotalRows:        result.TotalRows,
		OutputFiles:      len(result.Results),
		InputFiles:       inputFileCount,
		InputBytes:       inputBytes,
		OutputBytes:      result.OutputBytes,
		CompressionRatio: compressionRatio,
	}
}

func uploadCompactedFiles(
	ctx context.Context,
	ll *slog.Logger,
	s3client *awsclient.S3Client,
	results []parquetwriter.Result,
	workItem lrdb.ClaimMetricCompactionWorkRow,
	profile storageprofile.StorageProfile,
) (metricsprocessing.ProcessedSegments, error) {
	// Create processed segments from results
	segments := make(metricsprocessing.ProcessedSegments, 0, len(results))

	for i, result := range results {
		// Check for context cancellation/timeout before each upload
		if ctx.Err() != nil {
			ll.Warn("Upload context cancelled, returning partial results",
				slog.Int("completedUploads", i),
				slog.Int("totalFiles", len(results)),
				slog.Any("error", ctx.Err()))
			return segments, ctx.Err()
		}

		segment, err := metricsprocessing.NewProcessedSegment(result, workItem.OrganizationID, profile.CollectorName, ll)
		if err != nil {
			return segments, fmt.Errorf("failed to create processed segment: %w", err)
		}

		if err := segment.UploadToS3(ctx, s3client, profile.Bucket); err != nil {
			ll.Error("Failed to upload compacted file to S3",
				slog.String("bucket", profile.Bucket),
				slog.String("objectID", segment.ObjectID),
				slog.String("fileName", result.FileName),
				slog.Int("completedUploads", i),
				slog.Any("error", err))
			// Return partial results - the already uploaded files need cleanup
			return segments, fmt.Errorf("uploading file %s: %w", segment.ObjectID, err)
		}

		segments = append(segments, segment)

		ll.Debug("Uploaded compacted file to S3",
			slog.String("objectID", segment.ObjectID),
			slog.Int64("fileSize", result.FileSize),
			slog.Int64("recordCount", result.RecordCount))
	}
	return segments, nil
}

func replaceCompactedSegments(
	ctx context.Context,
	ll *slog.Logger,
	mdb compactionStore,
	segments metricsprocessing.ProcessedSegments,
	oldRows []lrdb.MetricSeg,
	workItem lrdb.ClaimMetricCompactionWorkRow,
	inputRecords int64,
	inputBytes int64,
) error {
	// Prepare old records for CompactMetricSegs
	oldRecords := make([]lrdb.CompactMetricSegsOld, len(oldRows))
	for i, row := range oldRows {
		oldRecords[i] = lrdb.CompactMetricSegsOld{
			SegmentID: row.SegmentID,
			SlotID:    row.SlotID,
		}
	}

	// Prepare new records for CompactMetricSegs
	newRecords := make([]lrdb.CompactMetricSegsNew, len(segments))

	for i, segment := range segments {
		newRecords[i] = lrdb.CompactMetricSegsNew{
			SegmentID:    segment.SegmentID,
			StartTs:      segment.StartTs,
			EndTs:        segment.EndTs,
			RecordCount:  segment.Result.RecordCount,
			FileSize:     segment.Result.FileSize,
			Fingerprints: segment.Fingerprints,
		}
	}

	err := mdb.CompactMetricSegs(ctx, lrdb.CompactMetricSegsParams{
		OrganizationID: workItem.OrganizationID,
		Dateint:        workItem.Dateint,
		InstanceNum:    workItem.InstanceNum,
		SlotID:         0,
		SlotCount:      1,
		IngestDateint:  metricsprocessing.GetIngestDateint(oldRows),
		FrequencyMs:    int32(workItem.FrequencyMs),
		Published:      true,
		Rolledup:       false,
		OldRecords:     oldRecords,
		NewRecords:     newRecords,
		CreatedBy:      lrdb.CreatedByCompact,
		SortVersion:    lrdb.CurrentMetricSortVersion,
	})
	if err != nil {
		ll.Error("Failed to compact metric segments",
			slog.Int("oldSegmentCount", len(oldRecords)),
			slog.Int("newSegmentCount", len(newRecords)),
			slog.Any("error", err))
		return fmt.Errorf("failed to compact metric segments: %w", err)
	}

	// Calculate output bytes and records
	outputBytes := int64(0)
	outputRecords := int64(0)
	for _, segment := range segments {
		outputBytes += segment.Result.FileSize
		outputRecords += segment.Result.RecordCount
	}

	ll.Info("Successfully replaced compacted metric segments",
		slog.Int("oldSegmentCount", len(oldRecords)),
		slog.Int("newSegmentCount", len(newRecords)),
		slog.Int64("inputRecords", inputRecords),
		slog.Int64("outputRecords", outputRecords),
		slog.Int64("inputBytes", inputBytes),
		slog.Int64("outputBytes", outputBytes),
		slog.Int64("targetRecords", workItem.UsedTargetRecords),
		slog.String("estimateSource", workItem.EstimateSource))

	return nil
}

func shouldCompactMetrics(rows []lrdb.MetricSeg) bool {
	return len(rows) >= 2
}

// scheduleS3Cleanup schedules cleanup work for orphaned S3 objects
// This is called when uploads succeed but database updates fail
func scheduleS3Cleanup(ctx context.Context, mdb compactionStore, segments metricsprocessing.ProcessedSegments, workItem lrdb.ClaimMetricCompactionWorkRow, profile storageprofile.StorageProfile) {
	segments.ScheduleCleanupAll(ctx, mdb, workItem.OrganizationID, workItem.InstanceNum, profile.Bucket)
}
