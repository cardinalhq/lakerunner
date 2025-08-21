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

// Copyright (C) 2025 CardinalHQ, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the GNU Affero General Public License, version 3.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR ANY PURPOSE. See the
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

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/tracecompaction"
	"github.com/cardinalhq/lakerunner/lockmgr"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "compact-traces",
		Short: "Compact traces into optimally sized files by slot",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.SetupTempDir()

			servicename := "lakerunner-compact-traces"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "traces"),
				attribute.String("action", "compact"),
			)
			doneCtx, doneFx, err := setupTelemetry(servicename, &addlAttrs)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}
			compactTracesDoneCtx = doneCtx

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			go diskUsageLoop(doneCtx)

			loop, err := NewRunqueueLoopContext(doneCtx, "traces", "compact", servicename)
			if err != nil {
				return fmt.Errorf("failed to create runqueue loop context: %w", err)
			}

			return RunqueueLoop(loop, compactTracesFor, nil)
		},
	}
	rootCmd.AddCommand(cmd)
}

var compactTracesDoneCtx context.Context

func compactTracesFor(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	awsmanager *awsclient.Manager,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	inf lockmgr.Workable,
	rpfEstimate int64,
	_ any,
) (WorkResult, error) {
	profile, err := sp.GetStorageProfileForOrganizationAndInstance(ctx, inf.OrganizationID(), inf.InstanceNum())
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}

	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		ll.Error("Failed to get S3 client", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}

	ll.Info("Processing trace compression item", slog.Any("workItem", inf.AsMap()))

	// Extract slot_id from work item
	slotID := inf.SlotId()

	// Use the global targetFileSize constant from cmd/root.go
	// targetFileSize = 1_100_000 bytes (≈1.1MB)

	const maxRowsLimit = 1000
	totalBatchesProcessed := 0
	totalSegmentsProcessed := 0
	cursorCreatedAt := time.Time{} // Start from beginning (zero time)
	cursorSegmentID := int64(0)    // Start from beginning (zero ID)

	// Loop until we've processed all available segments
	for {
		// Check if context is cancelled before starting next batch
		if ctx.Err() != nil {
			ll.Info("Context cancelled, stopping compaction loop - will retry to continue",
				slog.Int("processedBatches", totalBatchesProcessed),
				slog.Int("processedSegments", totalSegmentsProcessed),
				slog.Any("error", ctx.Err()))
			return WorkResultTryAgainLater, nil
		}

		ll.Info("Querying for trace segments to compact",
			slog.Int("batch", totalBatchesProcessed+1),
			slog.Int("slotID", int(slotID)),
			slog.Time("cursorCreatedAt", cursorCreatedAt),
			slog.Int64("cursorSegmentID", cursorSegmentID))

		// Query for trace segments in this specific slot
		segments, err := mdb.GetTraceSegmentsForCompaction(ctx, lrdb.GetTraceSegmentsForCompactionParams{
			OrganizationID:  inf.OrganizationID(),
			Dateint:         inf.Dateint(),
			SlotID:          slotID,
			MaxFileSize:     targetFileSize * 9 / 10, // Only include files < 90% of target (larger files are fine as-is)
			CursorCreatedAt: cursorCreatedAt,
			CursorSegmentID: cursorSegmentID,
			Maxrows:         maxRowsLimit,
		})
		if err != nil {
			ll.Error("Error getting trace segments for compaction", slog.String("error", err.Error()))
			return WorkResultTryAgainLater, err
		}

		// No more segments to process
		if len(segments) == 0 {
			if totalBatchesProcessed == 0 {
				ll.Info("No segments to compact")
			} else {
				ll.Info("Finished processing all compaction batches",
					slog.Int("totalBatches", totalBatchesProcessed),
					slog.Int("totalSegments", totalSegmentsProcessed))
			}
			return WorkResultSuccess, nil
		}

		ll.Info("Processing compaction batch",
			slog.Int("segmentCount", len(segments)),
			slog.Int("batch", totalBatchesProcessed+1))

		// Update cursor to last (created_at, segment_id) in this batch to ensure forward progress
		if len(segments) > 0 {
			lastSeg := segments[len(segments)-1]
			cursorCreatedAt = lastSeg.CreatedAt
			cursorSegmentID = lastSeg.SegmentID
		}

		// Pack segments into groups for compaction

		packed, err := tracecompaction.PackTraceSegments(segments, targetFileSize)
		if err != nil {
			ll.Error("Error packing trace segments", slog.String("error", err.Error()))
			return WorkResultTryAgainLater, err
		}

		// Check if the last group is too small (similar to logs compaction)
		lastGroupSmall := false
		if len(packed) > 0 {
			// if the last packed segment is smaller than half our target size, drop it.
			bytecount := int64(0)
			lastGroup := packed[len(packed)-1]
			for _, segment := range lastGroup {
				bytecount += segment.FileSize
			}
			if bytecount < targetFileSize/2 {
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
				return WorkResultSuccess, nil
			}
			// Continue to next batch without processing - cursor already advanced
			totalBatchesProcessed++
			continue
		}

		ll.Info("counts", slog.Int("currentSegments", len(segments)), slog.Int("packGroups", len(packed)), slog.Bool("lastGroupSmall", lastGroupSmall))

		// Process each group for actual compaction

		for i, group := range packed {
			ll := ll.With(slog.Int("groupIndex", i))

			// Call packTraceSegment for each group
			err = packTraceSegment(ctx, ll, tmpdir, s3client, mdb, group, profile, inf.Dateint(), slotID)
			if err != nil {
				ll.Error("packTraceSegment failed",
					slog.Int("groupIndex", i),
					slog.String("error", err.Error()))
				break
			}

			select {
			case <-compactTracesDoneCtx.Done():
				ll.Info("Shutdown requested, stopping group processing")
				return WorkResultTryAgainLater, errors.New("Asked to shut down, will retry work")
			default:
			}
		}

		if err != nil {
			return WorkResultTryAgainLater, err
		}

		totalBatchesProcessed++
		totalSegmentsProcessed += len(segments)

		ll.Info("Successfully packed segments in batch", slog.Int("groupCount", len(packed)))

		// If we didn't hit the limit, we've processed all available segments
		if len(segments) < maxRowsLimit {
			ll.Info("Completed all compaction batches",
				slog.Int("totalBatches", totalBatchesProcessed),
				slog.Int("totalSegments", totalSegmentsProcessed))
			return WorkResultSuccess, nil
		}

		// Continue to next batch - cursor already advanced
		ll.Info("Batch completed, checking for more segments",
			slog.Int("processedSegments", len(segments)),
			slog.Time("nextCursorCreatedAt", cursorCreatedAt),
			slog.Int64("nextCursorSegmentID", cursorSegmentID))
	}
}
