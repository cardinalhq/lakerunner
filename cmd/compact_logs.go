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
	"log/slog"
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/constants"
	"github.com/cardinalhq/lakerunner/internal/healthcheck"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logcrunch"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lockmgr"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "compact-logs",
		Short: "Compact logs into optimally sized files",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.SetupTempDir()

			servicename := "lakerunner-compact-logs"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "logs"),
				attribute.String("action", "compact"),
			)
			doneCtx, doneFx, err := setupTelemetry(servicename, &addlAttrs)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}
			compactLogsDoneCtx = doneCtx

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			go diskUsageLoop(doneCtx)

			// Start health check server
			healthConfig := healthcheck.GetConfigFromEnv()
			healthServer := healthcheck.NewServer(healthConfig)

			go func() {
				if err := healthServer.Start(doneCtx); err != nil {
					slog.Error("Health check server stopped", slog.Any("error", err))
				}
			}()

			loop, err := NewRunqueueLoopContext(doneCtx, "logs", "compact")
			if err != nil {
				return fmt.Errorf("failed to create runqueue loop context: %w", err)
			}

			// Mark as healthy once loop is created and about to start
			healthServer.SetStatus(healthcheck.StatusHealthy)

			return RunqueueLoop(loop, compactLogsFor, nil)
		},
	}
	rootCmd.AddCommand(cmd)
}

var compactLogsDoneCtx context.Context

// compactionMetricRecorder implements logcrunch.MetricRecorder for recording compaction metrics
type compactionMetricRecorder struct {
	logger *slog.Logger
}

func (r compactionMetricRecorder) RecordFilteredSegments(ctx context.Context, count int64, organizationID, instanceNum, signal, action, reason string) {
	segmentsFilteredCounter.Add(ctx, count,
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributes(
			attribute.String("organizationID", organizationID),
			attribute.String("instanceNum", instanceNum),
			attribute.String("signal", signal),
			attribute.String("action", action),
			attribute.String("reason", reason),
		))

	// Add detailed logging for each filtering reason
	r.logger.Info("Segments filtered during packing",
		slog.Int64("count", count),
		slog.String("reason", reason),
		slog.String("organizationID", organizationID),
		slog.String("instanceNum", instanceNum))
}

func (compactionMetricRecorder) RecordProcessedSegments(ctx context.Context, count int64, organizationID, instanceNum, signal, action string) {
	segmentsProcessedCounter.Add(ctx, count,
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributes(
			attribute.String("organizationID", organizationID),
			attribute.String("instanceNum", instanceNum),
			attribute.String("signal", signal),
			attribute.String("action", action),
		))
}

func compactLogsFor(
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

	ll.Info("Starting log compaction",
		slog.String("organizationID", inf.OrganizationID().String()),
		slog.Int("instanceNum", int(inf.InstanceNum())),
		slog.Int("dateint", int(inf.Dateint())),
		slog.Int64("workQueueID", inf.ID()))

	t0 := time.Now()
	err = logCompactItemDo(ctx, ll, mdb, tmpdir, inf, profile, s3client, rpfEstimate)

	if err != nil {
		ll.Info("Log compaction completed",
			slog.String("result", "error"),
			slog.Int64("workQueueID", inf.ID()),
			slog.Duration("elapsed", time.Since(t0)))
	} else {
		ll.Info("Log compaction completed",
			slog.String("result", "success"),
			slog.Int64("workQueueID", inf.ID()),
			slog.Duration("elapsed", time.Since(t0)))
	}

	return err
}

func logCompactItemDo(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	tmpdir string,
	inf lockmgr.Workable,
	sp storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	rpfEstimate int64,
) error {
	// Extract the time range using our normalized helper functions
	timeRange, ok := helpers.NewTimeRangeFromPgRange(inf.TsRange())
	if !ok {
		return errors.New("error getting range bounds")
	}

	// Validate that the time range falls entirely within one dateint-hour
	if !helpers.IsSameDateintHour(timeRange) {
		startBoundary, endBoundary := helpers.TimeRangeToHourBoundaries(timeRange)
		ll.Warn("Deleting stale work item with multi-hour range - likely from old queueing logic",
			slog.Int("startDateint", int(startBoundary.DateInt)),
			slog.Int("startHour", int(startBoundary.Hour)),
			slog.Time("rangeStart", timeRange.Start),
			slog.Int("endDateint", int(endBoundary.DateInt)),
			slog.Int("endHour", int(endBoundary.Hour)),
			slog.Time("rangeEnd", timeRange.End),
		)
		// This is likely stale data from before hour-aligned compaction was implemented
		// Delete it entirely from the work queue
		if err := inf.Delete(); err != nil {
			ll.Error("Failed to delete stale work item", slog.Any("error", err))
			return err
		}
		return nil
	}

	// Get the dateint and hour boundaries for database queries (both boundaries should be the same now)
	startBoundary, _ := helpers.TimeRangeToHourBoundaries(timeRange)
	stdi := startBoundary.DateInt

	// Calculate hour start and end timestamps in milliseconds
	hourStartTs := timeRange.Start.UnixMilli()
	hourEndTs := timeRange.End.UnixMilli()

	const maxRowsLimit = 1000
	totalBatchesProcessed := 0
	totalSegmentsProcessed := 0
	cursorCreatedAt := time.Time{} // Start from beginning (zero time)
	cursorSegmentID := int64(0)    // Start from beginning (zero ID)

	// Loop until we've processed all available segments
	for {
		// Check if context is cancelled before starting next batch
		if ctx.Err() != nil {
			ll.Info("Context cancelled, interrupting compaction loop gracefully",
				slog.Int("batchCount", totalBatchesProcessed),
				slog.Int("segmentCount", totalSegmentsProcessed),
				slog.Any("error", ctx.Err()))
			return NewWorkerInterrupted("context cancelled during batch processing")
		}

		ll.Info("Querying for log segments to compact",
			slog.Int("batchNumber", totalBatchesProcessed+1),
			slog.Time("cursorCreatedAt", cursorCreatedAt),
			slog.Int64("cursorSegmentID", cursorSegmentID))

		segments, err := mdb.GetLogSegmentsForCompaction(ctx, lrdb.GetLogSegmentsForCompactionParams{
			OrganizationID:  sp.OrganizationID,
			Dateint:         stdi,
			InstanceNum:     inf.InstanceNum(),
			SlotID:          inf.SlotId(),
			MaxFileSize:     constants.TargetFileSizeBytes,
			CursorCreatedAt: cursorCreatedAt,
			CursorSegmentID: cursorSegmentID,
			HourStartTs:     hourStartTs,
			HourEndTs:       hourEndTs,
			Maxrows:         maxRowsLimit,
		})
		if err != nil {
			ll.Error("Error getting log segments for compaction", slog.Any("error", err))
			return err
		}

		// No more segments to process
		if len(segments) == 0 {
			if totalBatchesProcessed == 0 {
				ll.Info("No segments to compact")
			} else {
				ll.Info("Finished processing all compaction batches",
					slog.Int("batchCount", totalBatchesProcessed),
					slog.Int("segmentCount", totalSegmentsProcessed))
			}
			return nil
		}

		ll.Info("Processing compaction batch",
			slog.Int("segmentCount", len(segments)),
			slog.Int("batchNumber", totalBatchesProcessed+1))

		// Update cursor to last (created_at, segment_id) in this batch to ensure forward progress
		if len(segments) > 0 {
			lastSeg := segments[len(segments)-1]
			cursorCreatedAt = lastSeg.CreatedAt
			cursorSegmentID = lastSeg.SegmentID
		}

		originalSegmentCount := len(segments)
		// Allow for 110% of target capacity to account for compression variability
		// This gives us 10% tolerance above the target file size
		adjustedEstimate := rpfEstimate * 11 / 10
		recorder := compactionMetricRecorder{logger: ll}
		packed, err := logcrunch.PackSegments(ctx, segments, adjustedEstimate, recorder,
			inf.OrganizationID().String(), fmt.Sprintf("%d", inf.InstanceNum()), "logs", "compact")
		if err != nil {
			ll.Error("Error packing segments", slog.Any("error", err))
			return err
		}

		// Log if any segments were filtered out during packing
		packedSegmentCount := 0
		for _, group := range packed {
			packedSegmentCount += len(group)
		}
		if packedSegmentCount < originalSegmentCount {
			ll.Info("Some segments were filtered out during packing",
				slog.Int("originalSegmentCount", originalSegmentCount),
				slog.Int("packedSegmentCount", packedSegmentCount),
				slog.Int("filteredSegmentCount", originalSegmentCount-packedSegmentCount))
		}

		lastGroupSmall := false
		if len(packed) > 0 {
			// if the last packed segment is smaller than 30% of our target size, drop it.
			// This is more aggressive than the previous 50% threshold to maximize compaction
			bytecount := int64(0)
			lastGroup := packed[len(packed)-1]
			for _, segment := range lastGroup {
				bytecount += segment.FileSize
			}
			if bytecount < constants.TargetFileSizeBytes*3/10 {
				packed = packed[:len(packed)-1]
				lastGroupSmall = true
			}
		}

		if len(packed) == 0 {
			ll.Info("No segments to compact in this batch")
			// If we didn't hit the limit, we've seen all segments
			if len(segments) < maxRowsLimit {
				if totalBatchesProcessed == 0 {
					ll.Info("No segments need compaction")
				}
				return nil
			}
			// Continue to next batch without processing - cursor already advanced
			totalBatchesProcessed++
			continue
		}

		ll.Info("Packing summary", slog.Int("segmentCount", len(segments)), slog.Int("groupCount", len(packed)), slog.Bool("lastGroupSmall", lastGroupSmall))

		for i, group := range packed {
			// Generate unique operation ID for this atomic group
			opID := generateOperationID()
			ll := ll.With(
				slog.String("operationID", opID),
				slog.Int("groupIndex", i))

			// Check for shutdown BEFORE starting atomic work
			select {
			case <-compactLogsDoneCtx.Done():
				ll.Info("Shutdown requested - interrupting before atomic operation",
					slog.Int("completedGroupCount", i),
					slog.Int("remainingGroupCount", len(packed)-i))
				return NewWorkerInterrupted("shutdown requested before atomic operation")
			default:
			}

			ll.Info("Starting atomic log compaction operation",
				slog.Int("segmentCount", len(group)))

			err = packSegment(ctx, ll, tmpdir, s3client, mdb, group, sp, stdi, inf.InstanceNum())

			if err != nil {
				ll.Error("Atomic operation failed - will retry entire work item",
					slog.Any("error", err),
					slog.Int("failedGroupIndex", i))
				break
			}

			ll.Info("Atomic operation completed successfully",
				slog.Int("segmentCount", len(group)))
		}

		if err != nil {
			return err
		}

		totalBatchesProcessed++
		totalSegmentsProcessed += len(segments)

		ll.Info("Successfully packed segments in batch", slog.Int("groupCount", len(packed)))

		// If we didn't hit the limit, we've processed all available segments
		if len(segments) < maxRowsLimit {
			ll.Info("Completed all compaction batches",
				slog.Int("batchCount", totalBatchesProcessed),
				slog.Int("segmentCount", totalSegmentsProcessed))
			return nil
		}

		// Continue to next batch - cursor already advanced
		ll.Info("Batch completed, checking for more segments",
			slog.Int("segmentCount", len(segments)),
			slog.Time("nextCursorCreatedAt", cursorCreatedAt),
			slog.Int64("nextCursorSegmentID", cursorSegmentID))
	}
}

var dropFieldNames = []string{
	"minute",
	"hour",
	"day",
	"month",
	"year",
}

func firstFromGroup(group []lrdb.GetLogSegmentsForCompactionRow) int64 {
	if len(group) == 0 {
		return 0
	}
	first := group[0].StartTs
	for _, segment := range group {
		first = min(first, segment.StartTs)
	}
	return first
}

func lastFromGroup(group []lrdb.GetLogSegmentsForCompactionRow) int64 {
	last := int64(0)
	for _, segment := range group {
		last = max(last, segment.EndTs)
	}
	return last
}

func segmentIDsFrom(segments []lrdb.GetLogSegmentsForCompactionRow) []int64 {
	ids := make([]int64, len(segments))
	for i, segment := range segments {
		ids[i] = segment.SegmentID
	}
	return ids
}

func ingestDateintFromGroup(group []lrdb.GetLogSegmentsForCompactionRow) int32 {
	if len(group) == 0 {
		return 0
	}
	first := int32(0)
	for _, segment := range group {
		first = max(first, segment.IngestDateint)
	}
	return first
}
