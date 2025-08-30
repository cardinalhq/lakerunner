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
	"fmt"
	"log/slog"
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/internal/cloudprovider"
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
	sessionName string,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	inf lockmgr.Workable,
	rpfEstimate int64, // rows-per-file estimate (from ingest stats), same as logs
	_ any,
) error {
	profile, err := sp.GetStorageProfileForOrganizationAndInstance(ctx, inf.OrganizationID(), inf.InstanceNum())
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return err
	}

	objectStoreClient, err := cloudprovider.GetObjectStoreClientForProfile(ctx, profile)
	if err != nil {
		ll.Error("Failed to get object store client", slog.Any("error", err))
		return err
	}
	s3client := objectStoreClient.GetS3Client()

	ll.Info("Starting trace compaction",
		slog.String("organizationID", inf.OrganizationID().String()),
		slog.Int("instanceNum", int(inf.InstanceNum())),
		slog.Int("dateint", int(inf.Dateint())),
		slog.Int("slotID", int(inf.SlotId())),
		slog.Int64("workQueueID", inf.ID()))

	const maxRowsLimit = 1000
	totalBatchesProcessed := 0
	totalSegmentsProcessed := 0
	cursorCreatedAt := time.Time{} // Start from beginning (zero time)
	cursorSegmentID := int64(0)    // Start from beginning (zero ID)

	for {
		if ctx.Err() != nil {
			ll.Info("Context cancelled, stopping compaction loop - will retry to continue",
				slog.Int("processedBatches", totalBatchesProcessed),
				slog.Int("processedSegments", totalSegmentsProcessed),
				slog.Any("error", ctx.Err()))
			return nil
		}

		ll.Info("Querying for trace segments to compact",
			slog.Int("batch", totalBatchesProcessed+1),
			slog.Int("slotID", int(inf.SlotId())),
			slog.Time("cursorCreatedAt", cursorCreatedAt),
			slog.Int64("cursorSegmentID", cursorSegmentID))

		// Align with logs: include files up to the full target size; let the packer do the grouping by estimate.
		segments, err := mdb.GetTraceSegmentsForCompaction(ctx, lrdb.GetTraceSegmentsForCompactionParams{
			OrganizationID:  inf.OrganizationID(),
			Dateint:         inf.Dateint(),
			InstanceNum:     inf.InstanceNum(),
			SlotID:          inf.SlotId(),
			MaxFileSize:     targetFileSize, // was 90% — align with logs
			CursorCreatedAt: cursorCreatedAt,
			CursorSegmentID: cursorSegmentID,
			Maxrows:         maxRowsLimit,
		})
		if err != nil {
			ll.Error("Error getting trace segments for compaction", slog.Any("error", err))
			return err
		}

		if len(segments) == 0 {
			if totalBatchesProcessed == 0 {
				ll.Info("No segments to compact")
			} else {
				ll.Info("Finished processing all compaction batches",
					slog.Int("totalBatches", totalBatchesProcessed),
					slog.Int("totalSegments", totalSegmentsProcessed))
			}
			return nil
		}

		ll.Info("Processing compaction batch",
			slog.Int("segmentCount", len(segments)),
			slog.Int("batch", totalBatchesProcessed+1))

		// Advance cursor
		lastSeg := segments[len(segments)-1]
		cursorCreatedAt = lastSeg.CreatedAt
		cursorSegmentID = lastSeg.SegmentID

		// ---- Packing (mirrors logs behavior) ----

		// Allow 110% of the target capacity to absorb compression variance like logs.
		// The estimate is rows-per-file; the packer should translate that to bytes using segment stats.
		adjustedEstimate := rpfEstimate * 11 / 10
		originalSegmentCount := len(segments)

		// New API you’ll add in tracecompaction (analogous to logcrunch.PackSegments).
		// It should:
		// - group segments by rowsPerFileEstimate into ~targetFileSize outputs
		// - be conservative near boundaries
		// - optionally filter obviously-bad segments (empty, oversized, etc.)
		recorder := compactionMetricRecorder{logger: ll}
		packed, err := tracecompaction.PackTraceSegmentsWithEstimate(
			ctx,
			segments,
			adjustedEstimate,
			recorder,
			inf.OrganizationID().String(),
			fmt.Sprintf("%d", inf.InstanceNum()),
			"traces",
			"compact",
		)
		if err != nil {
			ll.Error("Error packing trace segments", slog.Any("error", err))
			return err
		}

		// Log if any segments were filtered during packing
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

		// If the last group is too small, drop it (align with logs threshold: 30%).
		lastGroupSmall := false
		if len(packed) > 0 {
			bytecount := int64(0)
			lastGroup := packed[len(packed)-1]
			for _, seg := range lastGroup {
				bytecount += seg.FileSize
			}
			if bytecount < targetFileSize*3/10 {
				packed = packed[:len(packed)-1]
				lastGroupSmall = true
			}
		}

		if len(packed) == 0 {
			ll.Info("No segments to compact in this batch")
			if len(segments) < maxRowsLimit {
				if totalBatchesProcessed == 0 {
					ll.Info("No segments need compaction")
				}
				return nil
			}
			totalBatchesProcessed++
			continue
		}

		ll.Info("Packing summary",
			slog.Int("currentSegments", len(segments)),
			slog.Int("packGroups", len(packed)),
			slog.Bool("lastGroupSmall", lastGroupSmall))

		// ---- Execute atomic compaction per group ----

		for i, group := range packed {
			opID := generateOperationID()
			ll := ll.With(
				slog.String("operationID", opID),
				slog.Int("groupIndex", i),
			)

			select {
			case <-compactTracesDoneCtx.Done():
				ll.Info("Shutdown requested - aborting before atomic operation",
					slog.Int("completedGroups", i),
					slog.Int("remainingGroups", len(packed)-i))
				return nil
			default:
			}

			ll.Info("Starting atomic trace compaction operation",
				slog.Int("segmentCount", len(group)))

			if err := packTraceSegment(ctx, ll, tmpdir, s3client, mdb, group, profile, inf.Dateint(), inf.SlotId(), inf.InstanceNum()); err != nil {
				ll.Error("Atomic operation failed - will retry entire work item",
					slog.Any("error", err),
					slog.Int("failedAtGroup", i))
				return err
			}

			ll.Info("Atomic operation completed successfully",
				slog.Int("segmentsProcessed", len(group)))
		}

		totalBatchesProcessed++
		totalSegmentsProcessed += len(segments)

		ll.Info("Successfully packed segments in batch", slog.Int("groupCount", len(packed)))

		if len(segments) < maxRowsLimit {
			ll.Info("Completed all compaction batches",
				slog.Int("totalBatches", totalBatchesProcessed),
				slog.Int("totalSegments", totalSegmentsProcessed))
			return nil
		}

		ll.Info("Batch completed, checking for more segments",
			slog.Int("processedSegments", len(segments)),
			slog.Time("nextCursorCreatedAt", cursorCreatedAt),
			slog.Int64("nextCursorSegmentID", cursorSegmentID))
	}
}
