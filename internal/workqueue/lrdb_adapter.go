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

	"github.com/google/uuid"

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

// LRDBInqueueAdapter implements InqueueStore using lrdb.StoreFull
type LRDBInqueueAdapter struct {
	db lrdb.StoreFull
}

func NewLRDBInqueueAdapter(db lrdb.StoreFull) *LRDBInqueueAdapter {
	return &LRDBInqueueAdapter{db: db}
}

func (a *LRDBInqueueAdapter) ReleaseWork(ctx context.Context, id uuid.UUID, claimedBy int64) error {
	return a.db.ReleaseInqueueWork(ctx, lrdb.ReleaseInqueueWorkParams{
		ID:        id,
		ClaimedBy: claimedBy,
	})
}

func (a *LRDBInqueueAdapter) DeleteWork(ctx context.Context, id uuid.UUID, claimedBy int64) error {
	return a.db.DeleteInqueueWork(ctx, lrdb.DeleteInqueueWorkParams{
		ID:        id,
		ClaimedBy: claimedBy,
	})
}

func (a *LRDBInqueueAdapter) DeleteJournal(ctx context.Context, orgID uuid.UUID, bucket, objectID string) error {
	return a.db.InqueueJournalDelete(ctx, lrdb.InqueueJournalDeleteParams{
		OrganizationID: orgID,
		Bucket:         bucket,
		ObjectID:       objectID,
	})
}

func (a *LRDBInqueueAdapter) UpsertJournal(ctx context.Context, orgID uuid.UUID, bucket, objectID string) (bool, error) {
	return a.db.InqueueJournalUpsert(ctx, lrdb.InqueueJournalUpsertParams{
		OrganizationID: orgID,
		Bucket:         bucket,
		ObjectID:       objectID,
	})
}
