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

package workqueue

import (
	"context"
	"time"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// LRDBWorkQueueAdapter implements WorkQueueStore using lrdb.StoreFull
type LRDBWorkQueueAdapter struct {
	db lrdb.StoreFull
}

func NewLRDBWorkQueueAdapter(db lrdb.StoreFull) *LRDBWorkQueueAdapter {
	return &LRDBWorkQueueAdapter{db: db}
}

func (a *LRDBWorkQueueAdapter) CompleteWork(ctx context.Context, id, workerID int64) error {
	return a.db.WorkQueueComplete(ctx, lrdb.WorkQueueCompleteParams{
		ID:       id,
		WorkerID: workerID,
	})
}

func (a *LRDBWorkQueueAdapter) FailWork(ctx context.Context, id, workerID int64, maxRetries int32, requeueTTL time.Duration) error {
	return a.db.WorkQueueFail(ctx, lrdb.WorkQueueFailParams{
		ID:         id,
		WorkerID:   workerID,
		RequeueTtl: requeueTTL,
		MaxRetries: maxRetries,
	})
}
