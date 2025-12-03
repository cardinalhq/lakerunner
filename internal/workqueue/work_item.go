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
	"errors"

	"github.com/google/uuid"
)

// WorkItem represents a single row pulled from work_queue.
type WorkItem struct {
	id             int64
	taskName       string
	organizationID uuid.UUID
	instanceNum    int16
	spec           map[string]any
	tries          int32

	closed bool
	mgr    *Manager
}

// Workable defines operations available on a work item.
type Workable interface {
	Complete() error
	Fail(reason *string) error
	ID() int64
	TaskName() string
	OrganizationID() uuid.UUID
	InstanceNum() int16
	Spec() map[string]any
	Tries() int32
}

var _ Workable = (*WorkItem)(nil)

// Complete marks the work item as done and removes it from the queue.
func (w *WorkItem) Complete() error {
	if w.closed {
		return nil
	}
	w.closed = true

	if w.mgr == nil {
		return errors.New("work item manager is nil")
	}

	req := &workCompleteRequest{
		workItem: w,
		resp:     make(chan error, 1),
	}

	select {
	case w.mgr.completeWork <- req:
	case <-w.mgr.done:
		return errors.New("work queue manager is shut down")
	}

	select {
	case err := <-req.resp:
		return err
	case <-w.mgr.done:
		return errors.New("work queue manager is shut down")
	}
}

// Fail marks the work item as failed, releases it back to the queue, and increments the retry counter.
// If reason is provided, the item will be marked as permanently failed and won't be retried.
func (w *WorkItem) Fail(reason *string) error {
	if w.closed {
		return nil
	}
	w.closed = true

	if w.mgr == nil {
		return errors.New("work item manager is nil")
	}

	req := &workFailRequest{
		workItem:     w,
		failedReason: reason,
		resp:         make(chan error, 1),
	}

	select {
	case w.mgr.failWork <- req:
	case <-w.mgr.done:
		return errors.New("work queue manager is shut down")
	}

	select {
	case err := <-req.resp:
		return err
	case <-w.mgr.done:
		return errors.New("work queue manager is shut down")
	}
}

// ID returns the unique identifier for the work item.
func (w *WorkItem) ID() int64 {
	return w.id
}

// TaskName returns the task name for this work item.
func (w *WorkItem) TaskName() string {
	return w.taskName
}

// OrganizationID returns the organization ID associated with the work item.
func (w *WorkItem) OrganizationID() uuid.UUID {
	return w.organizationID
}

// InstanceNum returns the instance number associated with the work item.
func (w *WorkItem) InstanceNum() int16 {
	return w.instanceNum
}

// Spec returns the work specification as a JSONB map.
func (w *WorkItem) Spec() map[string]any {
	return w.spec
}

// Tries returns the number of attempts for this work item.
func (w *WorkItem) Tries() int32 {
	return w.tries
}
