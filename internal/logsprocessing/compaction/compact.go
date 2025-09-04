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
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/constants"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logcrunch"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lockmgr"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// WorkerInterrupted is an error type to signal interruption during processing
type WorkerInterrupted struct {
	msg string
}

func (e WorkerInterrupted) Error() string {
	return e.msg
}

// NewWorkerInterrupted creates a new WorkerInterrupted error
func NewWorkerInterrupted(msg string) error {
	return WorkerInterrupted{msg: msg}
}

// MetricRecorder implements logcrunch.MetricRecorder for recording compaction metrics
type MetricRecorder struct {
	Logger *slog.Logger
	SegmentsFilteredCounter metric.Int64Counter
}

func (r MetricRecorder) RecordFilteredSegments(ctx context.Context, count int64, organizationID, instanceNum, signal, action, reason string) {
	if r.SegmentsFilteredCounter != nil {
		r.SegmentsFilteredCounter.Add(ctx, count,
			metric.WithAttributes(
				attribute.String("signal", signal),
				attribute.String("action", action),
				attribute.String("reason", reason),
			))
	}

	// Add detailed logging for each filtering reason
	r.Logger.Info("Segments filtered during packing",
		slog.Int64("count", count),
		slog.String("reason", reason),
		slog.String("organizationID", organizationID),
		slog.String("instanceNum", instanceNum))
}

// CompactLogsFor is the main entry point for log compaction
func CompactLogsFor(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	awsmanager *awsclient.Manager,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	inf lockmgr.Workable,
	rpfEstimate int64,
	metricsRecorder *MetricRecorder,
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
	err = CompactLogs(ctx, ll, mdb, tmpdir, inf, profile, s3client, rpfEstimate, metricsRecorder)

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

// CompactLogs performs the actual log compaction
func CompactLogs(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	tmpdir string,
	inf lockmgr.Workable,
	sp storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	rpfEstimate int64,
	metricsRecorder *MetricRecorder,
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
			MaxFileSize:     constants.LogCompactionTargetSizeBytes,
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
			break
		}

		ll.Info("Found segments for compaction",
			slog.Int("batchNumber", totalBatchesProcessed+1),
			slog.Int("segmentCount", len(segments)))

		// Pack segments into optimal groups
		packed, err := logcrunch.PackSegments(
			ctx,
			segments,
			rpfEstimate,
			metricsRecorder,
			sp.OrganizationID.String(),
			fmt.Sprintf("%d", inf.InstanceNum()),
			"logs",
			"compact",
		)
		if err != nil {
			ll.Error("Failed to pack segments", slog.Any("error", err))
			return err
		}

		// Process each packed group
		for i, group := range packed {
			ll.Info("Processing packed group",
				slog.Int("groupIndex", i),
				slog.Int("segmentCount", len(group)))
			
			// TODO: Implement actual file compaction logic for each group
			// This would involve downloading files, merging them, and uploading the result
			// For now, we'll just log a placeholder
			_ = group // avoid unused variable error
		}

		if err != nil {
			ll.Error("Aggressive compaction failed", slog.Any("error", err))
			if errors.Is(err, context.Canceled) || errors.Is(err, WorkerInterrupted{}) {
				return err
			}
			// For other errors, return to allow retry
			return err
		}

		// Update counters
		totalBatchesProcessed++
		totalSegmentsProcessed += len(segments)

		// Update cursor to last processed item for pagination
		lastSegment := segments[len(segments)-1]
		cursorCreatedAt = lastSegment.CreatedAt
		cursorSegmentID = lastSegment.SegmentID

		ll.Info("Completed compaction batch",
			slog.Int("batchNumber", totalBatchesProcessed),
			slog.Int("segmentsInBatch", len(segments)),
			slog.Int("totalSegmentsProcessed", totalSegmentsProcessed))
	}

	ll.Info("Log compaction completed successfully",
		slog.Int("batchCount", totalBatchesProcessed),
		slog.Int("segmentCount", totalSegmentsProcessed))

	return nil
}