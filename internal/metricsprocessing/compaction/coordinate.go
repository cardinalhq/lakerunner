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

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
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
	mdb CompactionStore,
	tmpdir string,
	metadata CompactionWorkMetadata,
	profile storageprofile.StorageProfile,
	blobclient cloudstorage.Client,
	rows []lrdb.MetricSeg,
	estimatedTargetRecords int64,
) error {
	ll := logctx.FromContext(ctx)

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

	readerStack, err := metricsprocessing.CreateReaderStack(ctx, tmpdir, blobclient, metadata.OrganizationID, profile, rows)
	if err != nil {
		return err
	}
	defer metricsprocessing.CloseReaderStack(ctx, readerStack)

	// Calculate input stats for tracking
	inputBytes := int64(0)
	inputRecords := int64(0)
	for _, row := range rows {
		inputBytes += row.FileSize
		inputRecords += row.RecordCount
	}

	// Use generic processor for compaction
	processingInput := metricsprocessing.ProcessingInput{
		ReaderStack:       readerStack,
		TargetFrequencyMs: metadata.FrequencyMs, // Same frequency for compaction
		TmpDir:            tmpdir,
		RecordsLimit:      estimatedTargetRecords * 2,
		EstimatedRecords:  estimatedTargetRecords,
		Action:            "compact",
		InputRecords:      inputRecords,
		InputBytes:        inputBytes,
	}

	processingResult, err := metricsprocessing.AggregateMetrics(ctx, processingInput)
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	// Calculate statistics for logging
	compressionRatio := float64(0)
	if inputBytes > 0 {
		compressionRatio = float64(processingResult.Stats.OutputBytes) / float64(inputBytes) * 100
	}

	ll.Debug("Compaction completed",
		slog.Int64("totalRows", processingResult.Stats.TotalRows),
		slog.Int("outputFiles", processingResult.Stats.OutputSegments),
		slog.Int("inputFiles", len(readerStack.DownloadedFiles)),
		slog.Int64("inputBytes", inputBytes),
		slog.Int64("outputBytes", processingResult.Stats.OutputBytes),
		slog.Float64("compressionRatio", compressionRatio))

	// If we produced 0 output files, skip S3 upload and database updates
	if processingResult.Stats.OutputSegments == 0 {
		ll.Warn("Produced 0 output files from aggregating reader")
		return nil
	}

	// Final interruption check before critical section (S3 uploads + DB updates)
	if ctx.Err() != nil {
		ll.Info("Context cancelled before critical section - safe to abort",
			slog.Int("resultCount", processingResult.Stats.OutputSegments),
			slog.Any("error", ctx.Err()))
		return newWorkerInterrupted("context cancelled before metrics upload phase")
	}

	// Upload files to S3 with deadline (10 seconds for spot instance compatibility)
	s3Ctx, s3Cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer s3Cancel()

	segments, err := metricsprocessing.CreateSegmentsFromResults(ctx, processingResult.RawResults, metadata.OrganizationID, profile.CollectorName)
	if err != nil {
		return fmt.Errorf("failed to create processed segments: %w", err)
	}

	uploadedSegments, err := metricsprocessing.UploadSegments(s3Ctx, blobclient, profile.Bucket, segments)
	if err != nil {
		// If upload failed partway through, we need to clean up any uploaded files
		if len(uploadedSegments) > 0 {
			ll.Warn("S3 upload failed partway through, scheduling cleanup",
				slog.Int("uploadedFiles", len(uploadedSegments)),
				slog.Any("error", err))
			uploadedSegments.ScheduleCleanupAll(ctx, mdb, metadata.OrganizationID, metadata.InstanceNum, profile.Bucket)
		}
		return fmt.Errorf("failed to upload compacted files to S3: %w", err)
	}

	segments = uploadedSegments

	// Atomically replace segments in database with deadline (5 seconds)
	dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbCancel()

	err = replaceCompactedSegments(dbCtx, mdb, segments, rows, metadata, inputRecords, inputBytes, estimatedTargetRecords)
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

func replaceCompactedSegments(
	ctx context.Context,
	mdb CompactionStore,
	segments metricsprocessing.ProcessedSegments,
	oldRows []lrdb.MetricSeg,
	metadata CompactionWorkMetadata,
	inputRecords int64,
	inputBytes int64,
	estimatedTargetRecords int64,
) error {
	ll := logctx.FromContext(ctx)

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

	// Queue rollup work only for 10s (10000ms) frequency compactions
	if metadata.FrequencyMs == 10000 {
		if err := segments.QueueRollupWork(ctx, mdb, metadata.OrganizationID, metadata.InstanceNum, 10000, 0, 1); err != nil {
			ll.Error("Failed to queue rollup work after compaction",
				slog.Int("frequencyMs", int(metadata.FrequencyMs)),
				slog.Any("error", err))
			return fmt.Errorf("failed to queue rollup work: %w", err)
		}
		ll.Debug("Queued rollup work for 10s compaction",
			slog.Int("segmentCount", len(segments)))
	}

	return nil
}
