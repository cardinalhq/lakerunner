// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package sweeper

import (
	"context"
	"log/slog"

	"github.com/cardinalhq/lakerunner/internal/workqueue"
)

// WorkQueueCleanupQuerier defines the interface needed for work queue cleanup
type WorkQueueCleanupQuerier interface {
	workqueue.EnqueueDB
}

func runWorkQueueCleanup(ctx context.Context, querier WorkQueueCleanupQuerier) error {
	err := workqueue.Cleanup(ctx, querier, workQueueHeartbeatTimeout)
	if err != nil {
		slog.Error("Work queue cleanup failed", slog.Any("error", err))
		return err
	}

	slog.Debug("Work queue cleanup completed",
		slog.Duration("heartbeatTimeout", workQueueHeartbeatTimeout))
	return nil
}
