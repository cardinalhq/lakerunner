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

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// CompactionWorkMetadata holds the basic metadata needed for compaction work
type CompactionWorkMetadata struct {
	OrganizationID uuid.UUID
	Dateint        int32
	FrequencyMs    int32
	InstanceNum    int16
}

// coordinate handles S3 download, compaction, upload, and database update
func coordinate(
	ctx context.Context,
	ll *slog.Logger,
	mdb compactionStore,
	tmpdir string,
	metadata CompactionWorkMetadata,
	profile storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	rows []lrdb.MetricSeg,
	estimatedTargetRecords int64,
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
			OrganizationID: metadata.OrganizationID,
			Dateint:        metadata.Dateint,
			FrequencyMs:    metadata.FrequencyMs,
			InstanceNum:    metadata.InstanceNum,
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
			slog.Int64("targetRecords", estimatedTargetRecords))
		return nil
	}

	// Handle multiple rows case: proceed with full compaction
	if len(rows) < 2 {
		ll.Debug("No need to compact metrics in this batch", slog.Int("rowCount", len(rows)))
		return nil
	}

	if ctx.Err() != nil {
		ll.Info("Context cancelled before starting compaction",
			slog.Int("segmentCount", len(rows)),
			slog.Any("error", ctx.Err()))
		return newWorkerInterrupted("context cancelled before compaction")
	}

	commonAttributes := attribute.NewSet()

	// Track segments coming into compaction processing
	processingSegmentsIn.Add(ctx, int64(len(rows)), metric.WithAttributes(
		attribute.String("signal", "metrics"),
		attribute.String("action", "compact"),
	))

	config := metricsprocessing.ReaderStackConfig{
		FileSortedCounter: fileSortedCounter,
		CommonAttributes:  commonAttributes,
	}

	readerStack, err := metricsprocessing.CreateReaderStack(
		ctx, ll, tmpdir, s3client, metadata.OrganizationID, profile, rows, config)
	if err != nil {
		return err
	}
	defer metricsprocessing.CloseReaderStack(ll, readerStack)

	// Prepare input for pure compaction
	compactionInput := input{
		ReaderStack:  readerStack,
		FrequencyMs:  metadata.FrequencyMs,
		TmpDir:       tmpdir,
		Logger:       ll,
		RecordsLimit: estimatedTargetRecords * 2,
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

	// Track processing metrics
	processingSegmentsOut.Add(ctx, int64(len(result.Results)), metric.WithAttributes(
		attribute.String("signal", "metrics"),
		attribute.String("action", "compact"),
	))

	processingRecordsIn.Add(ctx, inputRecords, metric.WithAttributes(
		attribute.String("signal", "metrics"),
		attribute.String("action", "compact"),
	))

	processingRecordsOut.Add(ctx, stats.TotalRows, metric.WithAttributes(
		attribute.String("signal", "metrics"),
		attribute.String("action", "compact"),
	))

	processingBytesIn.Add(ctx, inputBytes, metric.WithAttributes(
		attribute.String("signal", "metrics"),
		attribute.String("action", "compact"),
	))

	processingBytesOut.Add(ctx, stats.OutputBytes, metric.WithAttributes(
		attribute.String("signal", "metrics"),
		attribute.String("action", "compact"),
	))

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

	segments, err := uploadCompactedFiles(s3Ctx, ll, s3client, result.Results, metadata, profile)
	if err != nil {
		// If upload failed partway through, we need to clean up any uploaded files
		if len(segments) > 0 {
			ll.Warn("S3 upload failed partway through, scheduling cleanup",
				slog.Int("uploadedFiles", len(segments)),
				slog.Any("error", err))
			segments.ScheduleCleanupAll(ctx, mdb, metadata.OrganizationID, metadata.InstanceNum, profile.Bucket)
		}
		return fmt.Errorf("failed to upload compacted files to S3: %w", err)
	}

	// Atomically replace segments in database with deadline (5 seconds)
	dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbCancel()

	err = replaceCompactedSegments(dbCtx, ll, mdb, segments, rows, metadata, inputRecords, inputBytes, estimatedTargetRecords)
	if err != nil {
		// Database update failed after successful uploads - schedule cleanup
		ll.Error("Database update failed after successful S3 upload, scheduling cleanup",
			slog.Int("uploadedFiles", len(segments)),
			slog.Any("error", err))
		segments.ScheduleCleanupAll(ctx, mdb, metadata.OrganizationID, metadata.InstanceNum, profile.Bucket)
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
	metadata CompactionWorkMetadata,
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

		segment, err := metricsprocessing.NewProcessedSegment(result, metadata.OrganizationID, profile.CollectorName, ll)
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
	metadata CompactionWorkMetadata,
	inputRecords int64,
	inputBytes int64,
	estimatedTargetRecords int64,
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
		OrganizationID: metadata.OrganizationID,
		Dateint:        metadata.Dateint,
		InstanceNum:    metadata.InstanceNum,
		SlotID:         0,
		SlotCount:      1,
		IngestDateint:  metricsprocessing.GetIngestDateint(oldRows),
		FrequencyMs:    metadata.FrequencyMs,
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
		slog.Int64("targetRecords", estimatedTargetRecords))

	return nil
}

// coordinateBundle handles S3 download, compaction, upload, and database update for bundle-based approach
func coordinateBundle(
	ctx context.Context,
	ll *slog.Logger,
	mdb compactionStore,
	tmpdir string,
	bundle lrdb.CompactionBundleResult,
	profile storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	rows []lrdb.MetricSeg,
) error {
	if len(rows) == 0 {
		ll.Debug("No segments to compact")
		return nil
	}
	if len(bundle.Items) == 0 {
		ll.Debug("No bundle items to process")
		return nil
	}

	// Use the first segment to get metadata (all segments in bundle should be consistent)
	firstSeg := rows[0]

	// Create metadata struct from the segment data
	metadata := CompactionWorkMetadata{
		OrganizationID: firstSeg.OrganizationID,
		Dateint:        firstSeg.Dateint,
		FrequencyMs:    firstSeg.FrequencyMs,
		InstanceNum:    firstSeg.InstanceNum,
	}

	ll.Info("Processing compaction bundle with estimation",
		slog.Int64("estimatedTarget", bundle.EstimatedTarget),
		slog.Int("segmentCount", len(rows)),
		slog.Int("bundleItems", len(bundle.Items)))

	return coordinate(ctx, ll, mdb, tmpdir, metadata, profile, s3client, rows, bundle.EstimatedTarget)
}
