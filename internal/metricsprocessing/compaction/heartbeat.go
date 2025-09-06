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
	"fmt"
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/internal/heartbeat"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// mcqHeartbeatStore defines the minimal interface needed for MCQ heartbeat operations
type mcqHeartbeatStore interface {
	McqHeartbeat(ctx context.Context, arg lrdb.McqHeartbeatParams) (int64, error)
}

// newMCQHeartbeater creates a new heartbeater for the given claimed MCQ items
func newMCQHeartbeater(db mcqHeartbeatStore, workerID int64, items []int64) *heartbeat.Heartbeater {
	if len(items) == 0 {
		// Return a no-op heartbeater for empty items
		return heartbeat.New(func(ctx context.Context) error {
			return nil // No-op
		}, time.Minute)
	}

	expectedCount := int64(len(items))

	heartbeatFunc := func(ctx context.Context) error {
		ll := logctx.FromContext(ctx)
		updatedCount, err := db.McqHeartbeat(ctx, lrdb.McqHeartbeatParams{
			WorkerID: workerID,
			Ids:      items,
		})
		if err != nil {
			return err
		}

		if updatedCount != expectedCount {
			ll.Error("Heartbeat did not update all expected rows",
				slog.Int64("expected", expectedCount),
				slog.Int64("updated", updatedCount),
				slog.Int64("missing", expectedCount-updatedCount))
			// Return error to trigger heartbeat failure handling
			return fmt.Errorf("heartbeat updated %d rows, expected %d", updatedCount, expectedCount)
		}

		return nil
	}

	return heartbeat.New(heartbeatFunc, time.Minute)
}
