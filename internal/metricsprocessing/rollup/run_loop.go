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
	"log/slog"
	"runtime"
	"time"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func runLoop(
	ctx context.Context,
	manager *Manager,
	mdb rollupStore,
	sp storageprofile.StorageProfileProvider,
	awsmanager *awsclient.Manager,
) error {
	ll := slog.Default().With(slog.String("component", "metric-rollup-loop"))

	for {
		select {
		case <-ctx.Done():
			ll.Info("Shutdown signal received, stopping rollup loop")
			return nil
		default:
		}

		// Claim a batch of work items
		claimedWork, err := manager.ClaimWork(ctx)
		if err != nil {
			ll.Error("Failed to claim work", slog.Any("error", err))
			time.Sleep(2 * time.Second)
			continue
		}

		if len(claimedWork) == 0 {
			time.Sleep(2 * time.Second)
			continue
		}

		// Check for cancellation after claiming work but before processing
		select {
		case <-ctx.Done():
			ll.Info("Context cancelled after claiming work, releasing items",
				slog.Int("claimedItems", len(claimedWork)))

			// Extract IDs for release
			ids := make([]int64, len(claimedWork))
			for i, work := range claimedWork {
				ids[i] = work.ID
			}

			// Use protected context and ReleaseWork for cancellation to return items to queue
			if releaseErr := manager.ReleaseWork(context.WithoutCancel(ctx), ids); releaseErr != nil {
				ll.Error("Failed to release work items after cancellation - items will expire", slog.Any("error", releaseErr))
			}
			return ctx.Err()
		default:
		}

		// Process the batch of claimed work items
		processBatchOfItems(ctx, ll, manager, mdb, sp, awsmanager, claimedWork)

		runtime.GC()
	}
}

func processBatchOfItems(
	ctx context.Context,
	ll *slog.Logger,
	manager *Manager,
	mdb rollupStore,
	sp storageprofile.StorageProfileProvider,
	awsmanager *awsclient.Manager,
	claimedWork []lrdb.MrqClaimBatchRow,
) {
	// Track active items that still need heartbeating
	tracker := &activeItemsTracker{
		items: make([]int64, len(claimedWork)),
	}
	for i, work := range claimedWork {
		tracker.items[i] = work.ID
	}

	// Track completed items for logging and cleanup
	completedIDs := make(map[int64]bool)

	// Create a heartbeater with the tracker
	heartbeater := newMRQHeartbeaterWithTracker(mdb, manager.workerID, tracker)
	cancel := heartbeater.Start(ctx)
	defer cancel()

	// Process each work item individually
	for _, workItem := range claimedWork {
		// Check for cancellation before processing each item
		select {
		case <-ctx.Done():
			ll.Info("Context cancelled during batch processing",
				slog.Int("processed", len(completedIDs)),
				slog.Int("total", len(claimedWork)))

			// Release unprocessed items
			unprocessedIDs := make([]int64, 0, len(claimedWork)-len(completedIDs))
			for _, item := range claimedWork {
				if !completedIDs[item.ID] {
					unprocessedIDs = append(unprocessedIDs, item.ID)
				}
			}

			if len(unprocessedIDs) > 0 {
				if releaseErr := manager.ReleaseWork(context.WithoutCancel(ctx), unprocessedIDs); releaseErr != nil {
					ll.Error("Failed to release unprocessed items - items will expire",
						slog.Int("count", len(unprocessedIDs)),
						slog.Any("error", releaseErr))
				}
			}
			return
		default:
		}

		ll.Debug("Processing individual work item",
			slog.Int64("id", workItem.ID),
			slog.Int64("segmentID", workItem.SegmentID),
			slog.String("organizationID", workItem.OrganizationID.String()),
			slog.Int("dateint", int(workItem.Dateint)),
			slog.Int("frequencyMs", int(workItem.FrequencyMs)))

		// Convert single item to bundle format for processBatch
		bundleResult := lrdb.RollupBundleResult{
			Items: []lrdb.MrqFetchCandidatesRow{
				{
					ID:             workItem.ID,
					OrganizationID: workItem.OrganizationID,
					Dateint:        workItem.Dateint,
					FrequencyMs:    workItem.FrequencyMs,
					InstanceNum:    workItem.InstanceNum,
					SlotID:         workItem.SlotID,
					SlotCount:      workItem.SlotCount,
					RollupGroup:    workItem.RollupGroup,
					SegmentID:      workItem.SegmentID,
					RecordCount:    workItem.RecordCount,
					QueueTs:        workItem.QueueTs,
				},
			},
			EstimatedTarget: workItem.RecordCount,
		}

		// Process the single item
		err := processBatch(ctx, ll, mdb, sp, awsmanager, bundleResult)

		if err != nil {
			ll.Error("Failed to process rollup work item",
				slog.Int64("id", workItem.ID),
				slog.Any("error", err))

			// Mark as failed using protected context
			if failErr := manager.FailWork(context.WithoutCancel(ctx), []int64{workItem.ID}); failErr != nil {
				ll.Error("Failed to fail work item - item will expire",
					slog.Int64("id", workItem.ID),
					slog.Any("error", failErr))
			}
		} else {
			// Mark as completed using protected context
			if completeErr := manager.CompleteWork(context.WithoutCancel(ctx), []int64{workItem.ID}); completeErr != nil {
				ll.Error("Failed to complete work item - item will expire",
					slog.Int64("id", workItem.ID),
					slog.Any("error", completeErr))
			} else {
				// Remove from active items and mark as completed
				tracker.Remove(workItem.ID)
				completedIDs[workItem.ID] = true

				ll.Debug("Successfully completed work item",
					slog.Int64("id", workItem.ID))
			}
		}
	}

	ll.Info("Finished processing batch of work items",
		slog.Int("total", len(claimedWork)),
		slog.Int("completed", len(completedIDs)))
}
