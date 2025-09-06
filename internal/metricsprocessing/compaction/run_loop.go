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
	"log/slog"
	"runtime"
	"time"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

func runLoop(
	ctx context.Context,
	manager *Manager,
	mdb compactionStore,
	sp storageprofile.StorageProfileProvider,
	cmgr *cloudstorage.CloudManagers,
) error {
	ll := slog.Default().With(slog.String("component", "metric-compaction-loop"))

	for {
		select {
		case <-ctx.Done():
			ll.Info("Shutdown signal received, stopping compaction loop")
			return nil
		default:
		}

		bundle, err := manager.ClaimWork(ctx)
		if err != nil {
			ll.Error("Failed to claim work", slog.Any("error", err))
			time.Sleep(2 * time.Second)
			continue
		}

		if bundle == nil || len(bundle.Items) == 0 {
			time.Sleep(2 * time.Second)
			continue
		}

		// Check for cancellation after claiming work but before processing
		select {
		case <-ctx.Done():
			ll.Info("Context cancelled after claiming work, releasing items",
				slog.Int("claimedItems", len(bundle.Items)))
			// Use protected context and ReleaseWork for cancellation to return items to queue
			if releaseErr := manager.ReleaseWork(context.WithoutCancel(ctx), bundle.Items); releaseErr != nil {
				ll.Error("Failed to release work items after cancellation - items will expire", slog.Any("error", releaseErr))
			}
			return ctx.Err()
		default:
		}

		// Start heartbeating for claimed items
		itemIDs := make([]int64, len(bundle.Items))
		for i, item := range bundle.Items {
			itemIDs[i] = item.ID
		}
		mcqHeartbeater := newMCQHeartbeater(mdb, manager.workerID, itemIDs)
		cancel := mcqHeartbeater.Start(ctx)

		err = processBatch(ctx, mdb, sp, cmgr, *bundle)

		// Stop heartbeating before handling results
		cancel()

		if err != nil {
			ll.Error("Failed to process compaction batch", slog.Any("error", err))
			// Fail work items using protected context to prevent expiration
			if failErr := manager.FailWork(context.WithoutCancel(ctx), bundle.Items); failErr != nil {
				ll.Error("Failed to fail work items - items will expire", slog.Any("error", failErr))
			}
		} else {
			// Complete work items using protected context to prevent expiration
			if completeErr := manager.CompleteWork(context.WithoutCancel(ctx), bundle.Items); completeErr != nil {
				ll.Error("Failed to complete work items - items will expire", slog.Any("error", completeErr))
			}
		}

		runtime.GC()
	}
}
