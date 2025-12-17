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
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// EnqueueDB defines the database operations needed for enqueuing work.
type EnqueueDB interface {
	WorkQueueAdd(ctx context.Context, arg lrdb.WorkQueueAddParams) (lrdb.WorkQueue, error)
	WorkQueueDepth(ctx context.Context, taskName string) (int64, error)
	WorkQueueCleanup(ctx context.Context, heartbeatTimeout time.Duration) error
}

// DefaultPriority is the default priority for work items (0 = normal).
const DefaultPriority = 0

// LowPriority is used for background/backfill work that should not starve normal work.
const LowPriority = 1000

// Add adds a new work item to the queue with default priority.
func Add(ctx context.Context, db EnqueueDB, taskName string, organizationID uuid.UUID, instanceNum int16, spec map[string]any) (int64, error) {
	return AddWithPriority(ctx, db, taskName, organizationID, instanceNum, spec, DefaultPriority)
}

// AddWithPriority adds a new work item to the queue with the specified priority.
// Lower priority values are processed first (negative = highest priority, 0 = normal).
func AddWithPriority(ctx context.Context, db EnqueueDB, taskName string, organizationID uuid.UUID, instanceNum int16, spec map[string]any, priority int32) (int64, error) {
	specBytes, err := json.Marshal(spec)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal spec: %w", err)
	}
	wq, err := db.WorkQueueAdd(ctx, lrdb.WorkQueueAddParams{
		TaskName:       taskName,
		OrganizationID: organizationID,
		InstanceNum:    instanceNum,
		Spec:           specBytes,
		Priority:       priority,
	})
	if err != nil {
		return 0, err
	}
	return wq.ID, nil
}

// Depth returns the number of unclaimed work items for a given task name.
func Depth(ctx context.Context, db EnqueueDB, taskName string) (int64, error) {
	return db.WorkQueueDepth(ctx, taskName)
}

// Cleanup releases work items claimed by dead workers (based on heartbeat timeout).
func Cleanup(ctx context.Context, db EnqueueDB, heartbeatTimeout time.Duration) error {
	return db.WorkQueueCleanup(ctx, heartbeatTimeout)
}

// AddBundle adds a work item to the queue using a JSON-serialized bundle with default priority.
// The bundleBytes are stored directly as the spec field to preserve numeric precision.
func AddBundle(ctx context.Context, db EnqueueDB, taskName string, organizationID uuid.UUID, instanceNum int16, bundleBytes []byte) (int64, error) {
	return AddBundleWithPriority(ctx, db, taskName, organizationID, instanceNum, bundleBytes, DefaultPriority)
}

// AddBundleWithPriority adds a work item to the queue using a JSON-serialized bundle with the specified priority.
// Lower priority values are processed first (negative = highest priority, 0 = normal).
func AddBundleWithPriority(ctx context.Context, db EnqueueDB, taskName string, organizationID uuid.UUID, instanceNum int16, bundleBytes []byte, priority int32) (int64, error) {
	wq, err := db.WorkQueueAdd(ctx, lrdb.WorkQueueAddParams{
		TaskName:       taskName,
		OrganizationID: organizationID,
		InstanceNum:    instanceNum,
		Spec:           bundleBytes,
		Priority:       priority,
	})
	if err != nil {
		return 0, err
	}
	return wq.ID, nil
}
