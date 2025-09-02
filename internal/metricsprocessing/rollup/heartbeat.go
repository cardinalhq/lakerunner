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

	"github.com/cardinalhq/lakerunner/internal/heartbeat"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type mrqHeartbeatStore interface {
	MrqHeartbeat(ctx context.Context, arg lrdb.MrqHeartbeatParams) error
}

func newMRQHeartbeater(db mrqHeartbeatStore, workerID int64, items []int64) *heartbeat.Heartbeater {
	if len(items) == 0 {
		return heartbeat.New(func(ctx context.Context) error {
			return nil
		}, time.Minute, slog.Default().With("component", "mrq_heartbeater", "worker_id", workerID, "item_count", 0))
	}

	heartbeatFunc := func(ctx context.Context) error {
		return db.MrqHeartbeat(ctx, lrdb.MrqHeartbeatParams{
			WorkerID: workerID,
			Ids:      items,
		})
	}

	logger := slog.Default().With("component", "mrq_heartbeater", "worker_id", workerID, "item_count", len(items))
	return heartbeat.New(heartbeatFunc, time.Minute, logger)
}
