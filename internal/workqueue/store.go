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

	"github.com/google/uuid"
)

// WorkQueueStore defines operations for work queue management
type WorkQueueStore interface {
	CompleteWork(ctx context.Context, id, workerID int64) error
	FailWork(ctx context.Context, id, workerID int64, maxRetries int32) error
}

// InqueueStore defines operations for inqueue management
type InqueueStore interface {
	ReleaseWork(ctx context.Context, id uuid.UUID, claimedBy int64) error
	DeleteWork(ctx context.Context, id uuid.UUID, claimedBy int64) error
	DeleteJournal(ctx context.Context, orgID uuid.UUID, bucket, objectID string) error
	UpsertJournal(ctx context.Context, orgID uuid.UUID, bucket, objectID string) (bool, error)
}
