// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lockmgr

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/cardinalhq/lakerunner/pkg/lrdb"
)

// WorkItem represents a single row pulled from work_queue.
type WorkItem struct {
	id          int64
	orgId       uuid.UUID
	instanceNum int16
	dateint     int32
	frequencyMs int32
	signal      lrdb.SignalEnum
	tries       int32
	action      lrdb.ActionEnum
	tsRange     pgtype.Range[pgtype.Timestamptz]
	priority    int32
	runnableAt  time.Time

	closed bool
	mgr    *wqManager
}

type Workable interface {
	Complete() error
	Fail() error
	ID() int64
	OrganizationID() uuid.UUID
	InstanceNum() int16
	Dateint() int32
	FrequencyMs() int32
	Signal() lrdb.SignalEnum
	Tries() int32
	Action() lrdb.ActionEnum
	TsRange() pgtype.Range[pgtype.Timestamptz]
	Priority() int32
	AsMap() map[string]any
	RunnableAt() time.Time
}

var _ Workable = (*WorkItem)(nil)

// Complete marks the work item as done, removes it from acquiredIDs, and calls the DB stored proc.
func (w *WorkItem) Complete() error {
	if w.closed {
		return nil
	}
	w.closed = true

	if w.mgr == nil {
		return errors.New("work item manager is nil")
	}

	req := &workCompleteRequest{
		WorkItem: w,
		resp:     make(chan error, 1),
	}
	w.mgr.completeWork <- req
	return <-req.resp
}

// Fail indicates that the work item failed; it removes it from acquiredIDs and calls WorkQueueFail.
func (w *WorkItem) Fail() error {
	if w.closed {
		return nil
	}
	w.closed = true

	if w.mgr == nil {
		return errors.New("work item manager is nil")
	}

	req := &workFailRequest{
		WorkItem: w,
		resp:     make(chan error, 1),
	}
	w.mgr.failWork <- req
	return <-req.resp
}

// ID returns the unique identifier for the work item.
func (w *WorkItem) ID() int64 {
	return w.id
}

// OrganizationID returns the organization ID associated with the work item.
func (w *WorkItem) OrganizationID() uuid.UUID {
	return w.orgId
}

// InstanceNum returns the instance number associated with the work item.
func (w *WorkItem) InstanceNum() int16 {
	return w.instanceNum
}

// Dateint returns the date integer associated with the work item.

func (w *WorkItem) Dateint() int32 {
	return w.dateint
}

// FrequencyMs returns the frequency in milliseconds for the work item.

func (w *WorkItem) FrequencyMs() int32 {
	return w.frequencyMs
}

// Signal returns the signal type for the work item.
func (w *WorkItem) Signal() lrdb.SignalEnum {
	return w.signal
}

// Tries returns the number of tries for the work item.
func (w *WorkItem) Tries() int32 {
	return w.tries
}

// Action returns the action type for the work item.
func (w *WorkItem) Action() lrdb.ActionEnum {
	return w.action
}

// TsRange returns the timestamp range for the work item.
func (w *WorkItem) TsRange() pgtype.Range[pgtype.Timestamptz] {
	return w.tsRange
}

// Priority returns the priority of the work item.
func (w *WorkItem) Priority() int32 {
	return w.priority
}

// RunnableAt returns the time when the work item is runnable.
func (w *WorkItem) RunnableAt() time.Time {
	return w.runnableAt
}

// AsMap converts the work item to a map representation.
func (w *WorkItem) AsMap() map[string]any {
	return map[string]any{
		"id":          w.id,
		"orgId":       w.orgId,
		"instanceNum": w.instanceNum,
		"dateint":     w.dateint,
		"frequencyMs": w.frequencyMs,
		"signal":      w.signal,
		"tries":       w.tries,
		"action":      w.action,
		"tsRange":     w.tsRange,
		"priority":    w.priority,
		"runnableAt":  w.runnableAt,
	}
}
