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
	"time"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
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

		bundleResult, err := manager.ClaimWork(ctx)
		if err != nil {
			ll.Error("Failed to claim work", slog.Any("error", err))
			time.Sleep(2 * time.Second)
			continue
		}

		if bundleResult == nil || len(bundleResult.Items) == 0 {
			time.Sleep(2 * time.Second)
			continue
		}

		claimedWork := bundleResult.Items

		// Check for cancellation after claiming work but before processing
		select {
		case <-ctx.Done():
			ll.Info("Context cancelled after claiming work, releasing items",
				slog.Int("claimedItems", len(claimedWork)))
			if releaseErr := manager.FailWork(ctx, claimedWork); releaseErr != nil {
				ll.Error("Failed to release work items after cancellation", slog.Any("error", releaseErr))
			}
			return ctx.Err()
		default:
		}

		// Start heartbeating for claimed items
		itemIDs := make([]int64, len(claimedWork))
		for i, work := range claimedWork {
			itemIDs[i] = work.ID
		}
		heartbeater := newMRQHeartbeater(mdb, manager.workerID, itemIDs)
		cancel := heartbeater.Start(ctx)

		err = processBatch(ctx, ll, mdb, sp, awsmanager, *bundleResult)

		// Stop heartbeating before handling results
		cancel()

		if err != nil {
			ll.Error("Failed to process rollup batch", slog.Any("error", err))
			if failErr := manager.FailWork(ctx, claimedWork); failErr != nil {
				ll.Error("Failed to fail work items", slog.Any("error", failErr))
			}
		} else {
			if completeErr := manager.CompleteWork(ctx, claimedWork); completeErr != nil {
				ll.Error("Failed to complete work items", slog.Any("error", completeErr))
			}
		}
	}
}
