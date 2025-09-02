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
	"time"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

func runLoop(
	ctx context.Context,
	manager *Manager,
	mdb compactionStore,
	sp storageprofile.StorageProfileProvider,
	awsmanager *awsclient.Manager,
) error {
	ll := slog.Default().With(slog.String("component", "metric-compaction-loop"))

	for {
		select {
		case <-ctx.Done():
			ll.Info("Shutdown signal received, stopping compaction loop")
			return nil
		default:
		}

		bundle, err := manager.ClaimWorkBundle(ctx)
		if err != nil {
			ll.Error("Failed to claim bundle", slog.Any("error", err))
			time.Sleep(2 * time.Second)
			continue
		}

		if len(bundle.Items) == 0 {
			time.Sleep(2 * time.Second)
			continue
		}

		// Check for cancellation after claiming work but before processing
		select {
		case <-ctx.Done():
			ll.Info("Context cancelled after claiming work, releasing items",
				slog.Int("claimedItems", len(bundle.Items)))
			if releaseErr := manager.FailWork(ctx, bundle.Items); releaseErr != nil {
				ll.Error("Failed to release work items after cancellation", slog.Any("error", releaseErr))
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

		err = processBatch(ctx, ll, mdb, sp, awsmanager, bundle)

		// Stop heartbeating before handling results
		cancel()

		if err != nil {
			ll.Error("Failed to process compaction batch", slog.Any("error", err))
			if failErr := manager.FailWork(ctx, bundle.Items); failErr != nil {
				ll.Error("Failed to fail work items", slog.Any("error", failErr))
			}
		} else {
			if completeErr := manager.CompleteWork(ctx, bundle.Items); completeErr != nil {
				ll.Error("Failed to complete work items", slog.Any("error", completeErr))
			}
		}
	}
}
