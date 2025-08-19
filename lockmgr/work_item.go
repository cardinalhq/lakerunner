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

package lockmgr

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/cardinalhq/lakerunner/lrdb"
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
	slotId      int32

	closed bool
	mgr    *wqManager
}

type Workable interface {
	Complete() error
	Fail() error
	Delete() error
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
	SlotId() int32
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

// Delete removes the work item entirely from the queue database.
func (w *WorkItem) Delete() error {
	if w.closed {
		return nil
	}
	w.closed = true

	if w.mgr == nil {
		return errors.New("work item manager is nil")
	}

	req := &workDeleteRequest{
		WorkItem: w,
		resp:     make(chan error, 1),
	}
	w.mgr.deleteWork <- req
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

// SlotId returns the slot ID for the work item.
func (w *WorkItem) SlotId() int32 {
	return w.slotId
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
		"slotId":      w.slotId,
	}
}
