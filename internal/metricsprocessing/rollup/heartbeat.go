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
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cardinalhq/lakerunner/internal/heartbeat"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type mrqHeartbeatStore interface {
	MrqHeartbeat(ctx context.Context, arg lrdb.MrqHeartbeatParams) (int64, error)
}

// activeItemsTracker tracks which items are still active and need heartbeating
type activeItemsTracker struct {
	mu    sync.Mutex
	items []int64
}

// Remove removes an item from the active list
func (a *activeItemsTracker) Remove(id int64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for i, item := range a.items {
		if item == id {
			// Remove by swapping with last element and truncating
			a.items[i] = a.items[len(a.items)-1]
			a.items = a.items[:len(a.items)-1]
			return
		}
	}
}

// GetActive returns a copy of the currently active items
func (a *activeItemsTracker) GetActive() []int64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.items) == 0 {
		return nil
	}

	// Return a copy to avoid race conditions
	result := make([]int64, len(a.items))
	copy(result, a.items)
	return result
}

// newMRQHeartbeaterWithTracker creates a heartbeater with an active items tracker
func newMRQHeartbeaterWithTracker(db mrqHeartbeatStore, workerID int64, tracker *activeItemsTracker) *heartbeat.Heartbeater {
	logger := slog.Default().With("component", "mrq_heartbeater", "worker_id", workerID)

	heartbeatFunc := func(ctx context.Context) error {
		activeItems := tracker.GetActive()

		// If no active items left, nothing to heartbeat
		if len(activeItems) == 0 {
			logger.Debug("No active items to heartbeat, all completed")
			return nil
		}

		updatedCount, err := db.MrqHeartbeat(ctx, lrdb.MrqHeartbeatParams{
			WorkerID: workerID,
			Ids:      activeItems,
		})
		if err != nil {
			return err
		}

		expectedCount := int64(len(activeItems))
		if updatedCount != expectedCount {
			logger.Error("Heartbeat did not update all expected rows",
				slog.Int64("expected", expectedCount),
				slog.Int64("updated", updatedCount),
				slog.Int64("missing", expectedCount-updatedCount))
			// Return error to trigger heartbeat failure handling
			return fmt.Errorf("heartbeat updated %d rows, expected %d", updatedCount, expectedCount)
		}

		logger.Debug("Heartbeat successful",
			slog.Int("activeItems", len(activeItems)))

		return nil
	}

	return heartbeat.New(heartbeatFunc, time.Minute, logger)
}

