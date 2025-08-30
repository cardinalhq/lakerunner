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
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/constants"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
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

	st, _, ok := helpers.RangeBounds(workItem.TsRange)
	if !ok {
		return fmt.Errorf("invalid time range in work item: %v", workItem.TsRange)
	}

	meter := otel.Meter("github.com/cardinalhq/lakerunner/internal/metricsprocessing/compaction")
	fileSortedCounter, _ := meter.Int64Counter("lakerunner.metric.compact.file.sorted")
	commonAttributes := attribute.NewSet()

	config := metricsprocessing.ReaderStackConfig{
		FileSortedCounter: fileSortedCounter,
		CommonAttributes:  commonAttributes,
	}

	// Download files from S3
	readerStack, err := metricsprocessing.CreateReaderStack(
		ctx, ll, tmpdir, s3client, workItem.OrganizationID, profile, st.Time.UTC().UnixMilli(), rows, config)
	if err != nil {
		return err
	}
	defer metricsprocessing.CloseReaderStack(ll, readerStack)

	// Prepare input for pure compaction
	compactionInput := input{
		ReaderStack: readerStack,
		FrequencyMs: workItem.FrequencyMs,
		TmpDir:      tmpdir,
		Logger:      ll,
	}

	// Perform pure compaction
	result, err := perform(ctx, compactionInput)
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	// Calculate input bytes from segment metadata
	inputBytes := int64(0)
	for _, row := range rows {
		inputBytes += row.FileSize
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

	segmentIDs, err := uploadCompactedFiles(s3Ctx, ll, s3client, result.Results, workItem, profile)
	if err != nil {
		// If upload failed partway through, we need to clean up any uploaded files
		if len(segmentIDs) > 0 {
			ll.Warn("S3 upload failed partway through, scheduling cleanup",
				slog.Int("uploadedFiles", len(segmentIDs)),
				slog.Any("error", err))
			scheduleS3Cleanup(ctx, mdb, segmentIDs, workItem, profile)
		}
		return fmt.Errorf("failed to upload compacted files to S3: %w", err)
	}

	// Atomically replace segments in database with deadline (5 seconds)
	dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbCancel()

	err = replaceCompactedSegments(dbCtx, ll, mdb, result.Results, rows, workItem, segmentIDs)
	if err != nil {
		// Database update failed after successful uploads - schedule cleanup
		ll.Error("Database update failed after successful S3 upload, scheduling cleanup",
			slog.Int("uploadedFiles", len(segmentIDs)),
			slog.Any("error", err))
		scheduleS3Cleanup(ctx, mdb, segmentIDs, workItem, profile)
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
) ([]int64, error) {
	st, _, ok := helpers.RangeBounds(workItem.TsRange)
	if !ok {
		return nil, fmt.Errorf("invalid time range in work item: %v", workItem.TsRange)
	}

	segmentIDs := make([]int64, len(results))
	var lastErr error

	for i, result := range results {
		// Check for context cancellation/timeout before each upload
		if ctx.Err() != nil {
			ll.Warn("Upload context cancelled, returning partial results",
				slog.Int("completedUploads", i),
				slog.Int("totalFiles", len(results)),
				slog.Any("error", ctx.Err()))
			return segmentIDs[:i], ctx.Err()
		}

		segmentID := idgen.DefaultFlakeGenerator.NextID()
		segmentIDs[i] = segmentID

		dateint, hour := helpers.MSToDateintHour(st.Time.UTC().UnixMilli())
		objectID := helpers.MakeDBObjectID(workItem.OrganizationID, profile.CollectorName, dateint, hour, segmentID, "metrics")

		if err := s3helper.UploadS3Object(ctx, s3client, profile.Bucket, objectID, result.FileName); err != nil {
			ll.Error("Failed to upload compacted file to S3",
				slog.String("bucket", profile.Bucket),
				slog.String("objectID", objectID),
				slog.String("fileName", result.FileName),
				slog.Int("completedUploads", i),
				slog.Any("error", err))
			lastErr = fmt.Errorf("uploading file %s: %w", objectID, err)
			// Return partial results - the already uploaded files need cleanup
			return segmentIDs[:i], lastErr
		}

		ll.Debug("Uploaded compacted file to S3",
			slog.String("objectID", objectID),
			slog.Int64("fileSize", result.FileSize),
			slog.Int64("recordCount", result.RecordCount))
	}
	return segmentIDs, nil
}

func replaceCompactedSegments(
	ctx context.Context,
	ll *slog.Logger,
	mdb compactionStore,
	results []parquetwriter.Result,
	oldRows []lrdb.MetricSeg,
	workItem lrdb.ClaimMetricCompactionWorkRow,
	segmentIDs []int64,
) error {
	// Prepare old records for CompactMetricSegs
	oldRecords := make([]lrdb.ReplaceMetricSegsOld, len(oldRows))
	for i, row := range oldRows {
		oldRecords[i] = lrdb.ReplaceMetricSegsOld{
			SegmentID: row.SegmentID,
			SlotID:    row.SlotID,
		}
	}

	// Prepare new records for CompactMetricSegs
	newRecords := make([]lrdb.ReplaceMetricSegsNew, len(results))
	st, et, ok := helpers.RangeBounds(workItem.TsRange)
	if !ok {
		return fmt.Errorf("invalid time range in work item: %v", workItem.TsRange)
	}

	// Collect fingerprints from all results
	var allFingerprints []int64
	for i, result := range results {
		var fingerprints []int64
		if metadata, ok := result.Metadata.(factories.MetricsFileStats); ok {
			fingerprints = metadata.Fingerprints
		} else {
			ll.Error("Missing metadata for compacted segment - cannot proceed",
				"segment_id", segmentIDs[i],
				"organization_id", workItem.OrganizationID,
				"dateint", workItem.Dateint,
				"frequency_ms", workItem.FrequencyMs,
				"instance_num", workItem.InstanceNum)
			return fmt.Errorf("missing metadata for segment %d", segmentIDs[i])
		}
		allFingerprints = append(allFingerprints, fingerprints...)

		newRecords[i] = lrdb.ReplaceMetricSegsNew{
			SegmentID:   segmentIDs[i],
			StartTs:     st.Time.UTC().UnixMilli(),
			EndTs:       et.Time.UTC().UnixMilli(),
			RecordCount: result.RecordCount,
			FileSize:    result.FileSize,
		}
	}

	// Use the new CompactMetricSegs function
	err := mdb.CompactMetricSegs(ctx, lrdb.ReplaceMetricSegsParams{
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
		CreatedBy:      lrdb.CreatedByIngest,
		Fingerprints:   allFingerprints,
		SortVersion:    lrdb.CurrentMetricSortVersion,
	})
	if err != nil {
		ll.Error("Failed to compact metric segments",
			slog.Int("oldSegmentCount", len(oldRecords)),
			slog.Int("newSegmentCount", len(newRecords)),
			slog.Any("error", err))
		return fmt.Errorf("failed to compact metric segments: %w", err)
	}

	ll.Info("Successfully replaced compacted metric segments",
		slog.Int("oldSegmentCount", len(oldRecords)),
		slog.Int("newSegmentCount", len(newRecords)))

	return nil
}

const targetFileSize = constants.TargetFileSizeBytes

func shouldCompactMetrics(rows []lrdb.MetricSeg) bool {
	if len(rows) < 2 {
		return false
	}

	const smallThreshold = int64(targetFileSize) * 3 / 10

	var totalSize int64
	for _, row := range rows {
		totalSize += row.FileSize
		if row.FileSize > targetFileSize*2 || row.FileSize < smallThreshold {
			return true
		}
	}

	estimatedFileCount := (totalSize + targetFileSize - 1) / targetFileSize
	compact := estimatedFileCount < int64(len(rows))-3
	return compact
}

// scheduleS3Cleanup schedules cleanup work for orphaned S3 objects
// This is called when uploads succeed but database updates fail
func scheduleS3Cleanup(ctx context.Context, mdb compactionStore, segmentIDs []int64, workItem lrdb.ClaimMetricCompactionWorkRow, profile storageprofile.StorageProfile) {
	for _, segmentID := range segmentIDs {
		st, _, ok := helpers.RangeBounds(workItem.TsRange)
		if !ok {
			slog.Warn("Invalid time range for cleanup", slog.Int64("segmentID", segmentID))
			continue
		}
		dateint, hour := helpers.MSToDateintHour(st.Time.UTC().UnixMilli())
		objectID := helpers.MakeDBObjectID(workItem.OrganizationID, profile.CollectorName, dateint, hour, segmentID, "metrics")

		if err := s3helper.ScheduleS3Delete(ctx, mdb, workItem.OrganizationID, workItem.InstanceNum, profile.Bucket, objectID); err != nil {
			slog.Error("Failed to schedule S3 cleanup for orphaned object",
				slog.String("bucket", profile.Bucket),
				slog.String("objectID", objectID),
				slog.Int64("segmentID", segmentID),
				slog.String("organizationID", workItem.OrganizationID.String()),
				slog.Any("error", err))
		} else {
			slog.Info("Scheduled S3 cleanup for orphaned object",
				slog.String("bucket", profile.Bucket),
				slog.String("objectID", objectID),
				slog.Int64("segmentID", segmentID))
		}
	}
}
