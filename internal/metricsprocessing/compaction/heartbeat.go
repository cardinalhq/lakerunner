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

	"github.com/cardinalhq/lakerunner/internal/heartbeat"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// mcqHeartbeatStore defines the minimal interface needed for MCQ heartbeat operations
type mcqHeartbeatStore interface {
	TouchMetricCompactionWork(ctx context.Context, params lrdb.TouchMetricCompactionWorkParams) error
}

// newMCQHeartbeater creates a new heartbeater for the given claimed MCQ items
func newMCQHeartbeater(db mcqHeartbeatStore, workerID int64, items []int64) *heartbeat.Heartbeater {
	if len(items) == 0 {
		// Return a no-op heartbeater for empty items
		return heartbeat.New(func(ctx context.Context) error {
			return nil // No-op
		}, time.Minute, slog.Default().With("component", "mcq_heartbeater", "worker_id", workerID, "item_count", 0))
	}

	heartbeatFunc := func(ctx context.Context) error {
		return db.TouchMetricCompactionWork(ctx, lrdb.TouchMetricCompactionWorkParams{
			Ids:       items,
			ClaimedBy: workerID,
		})
	}

	logger := slog.Default().With("component", "mcq_heartbeater", "worker_id", workerID, "item_count", len(items))
	return heartbeat.New(heartbeatFunc, time.Minute, logger)
}
