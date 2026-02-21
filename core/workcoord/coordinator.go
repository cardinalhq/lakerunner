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

package workcoord

// IDGenerator produces unique work IDs for reassignment.
type IDGenerator interface {
	NewWorkID() string
}

// ReassignedWork describes a work item that was reassigned to a new worker.
type ReassignedWork struct {
	OldWorkID   string
	NewWorkID   string
	NewWorkerID string
	Item        WorkItem
}

// Coordinator composes the worker registry, work tracker, and assignment
// logic into a single interface for use by API and worker managers.
type Coordinator struct {
	Workers *WorkerRegistry
	Work    *WorkTracker
	IDGen   IDGenerator
}

// NewCoordinator creates a Coordinator with fresh registry and tracker.
func NewCoordinator(idGen IDGenerator) *Coordinator {
	return &Coordinator{
		Workers: NewWorkerRegistry(),
		Work:    NewWorkTracker(),
		IDGen:   idGen,
	}
}

// RegisterWorker adds a worker to the registry.
func (c *Coordinator) RegisterWorker(workerID string) error {
	return c.Workers.Register(workerID)
}

// RemoveWorker removes a worker from the registry and cancels its in-flight work.
// Returns the list of work items that were canceled.
func (c *Coordinator) RemoveWorker(workerID string) ([]WorkItem, error) {
	canceled := c.cancelWorkerWork(workerID)
	if err := c.Workers.Remove(workerID); err != nil {
		return canceled, err
	}
	return canceled, nil
}

// DisconnectWorker marks a worker as disconnected and reassigns its work.
func (c *Coordinator) DisconnectWorker(workerID string) ([]ReassignedWork, error) {
	if err := c.Workers.Disconnect(workerID); err != nil {
		return nil, err
	}
	return c.reassignWorkerWork(workerID)
}

// ReconnectWorker marks a worker as alive again.
func (c *Coordinator) ReconnectWorker(workerID string) error {
	return c.Workers.Reconnect(workerID)
}

// DrainWorker begins draining a worker and reassigns unstarted work.
func (c *Coordinator) DrainWorker(workerID string) ([]ReassignedWork, error) {
	if err := c.Workers.BeginDrain(workerID); err != nil {
		return nil, err
	}
	// Reassign only assigned (not yet accepted) work from draining worker.
	return c.reassignUnstartedWork(workerID)
}

// AssignWork creates a work item and assigns it to the best worker via
// rendezvous hashing.
func (c *Coordinator) AssignWork(queryID, leafID, workID, affinityKey string) (*WorkItem, error) {
	workers := c.Workers.AllWorkers()
	workerID, err := AssignByRendezvous(affinityKey, workers)
	if err != nil {
		return nil, err
	}

	item := &WorkItem{
		QueryID:     queryID,
		LeafID:      leafID,
		WorkID:      workID,
		WorkerID:    workerID,
		State:       WorkStateAssigned,
		AffinityKey: affinityKey,
	}
	if err := c.Work.Add(item); err != nil {
		return nil, err
	}
	return item, nil
}

// AssignWorkToWorker creates a work item assigned to a specific worker.
// Returns ErrWorkerUnavailable if the worker is not alive, not accepting work,
// or draining.
func (c *Coordinator) AssignWorkToWorker(queryID, leafID, workID, affinityKey, workerID string) (*WorkItem, error) {
	w, err := c.Workers.Get(workerID)
	if err != nil {
		return nil, err
	}
	if !w.IsAvailable() {
		return nil, &ErrWorkerUnavailable{WorkerID: workerID}
	}

	item := &WorkItem{
		QueryID:     queryID,
		LeafID:      leafID,
		WorkID:      workID,
		WorkerID:    workerID,
		State:       WorkStateAssigned,
		AffinityKey: affinityKey,
	}
	if err := c.Work.Add(item); err != nil {
		return nil, err
	}
	return item, nil
}

// HandleWorkAccepted transitions a work item to Accepted.
func (c *Coordinator) HandleWorkAccepted(workID string) error {
	return c.Work.Transition(workID, WorkStateAccepted)
}

// HandleWorkRejected marks work as Rejected and reassigns it to the next
// available worker, excluding the worker that rejected it. Returns the
// reassigned work item, or nil if no other workers are available.
// Duplicate rejects for already-rejected work are treated as no-ops.
func (c *Coordinator) HandleWorkRejected(workID string) (*ReassignedWork, error) {
	item, err := c.Work.Get(workID)
	if err != nil {
		return nil, err
	}

	// Duplicate reject on already-rejected work is a no-op.
	if item.State == WorkStateRejected {
		return nil, nil
	}

	if err := c.Work.Transition(workID, WorkStateRejected); err != nil {
		return nil, err
	}

	// Reassign, excluding the rejecting worker to avoid bounce loops.
	allWorkers := c.Workers.AllWorkers()
	newWorkerID, err := AssignByRendezvous(item.AffinityKey, allWorkers, item.WorkerID)
	if err != nil {
		return nil, nil
	}

	newWorkID := c.IDGen.NewWorkID()
	newItem := &WorkItem{
		QueryID:     item.QueryID,
		LeafID:      item.LeafID,
		WorkID:      newWorkID,
		WorkerID:    newWorkerID,
		State:       WorkStateAssigned,
		AffinityKey: item.AffinityKey,
	}
	if err := c.Work.Add(newItem); err != nil {
		return nil, err
	}

	return &ReassignedWork{
		OldWorkID:   workID,
		NewWorkID:   newWorkID,
		NewWorkerID: newWorkerID,
		Item:        *newItem,
	}, nil
}

// HandleWorkReady transitions a work item to Ready with artifact info
// and performs idempotency dedup.
func (c *Coordinator) HandleWorkReady(workID string, artifact ArtifactInfo) error {
	return c.Work.Complete(workID, artifact)
}

// HandleWorkFailed transitions a work item to Failed.
func (c *Coordinator) HandleWorkFailed(workID string) error {
	return c.Work.Transition(workID, WorkStateFailed)
}

// CancelWork transitions a single work item to Canceled.
func (c *Coordinator) CancelWork(workID string) error {
	return c.Work.Transition(workID, WorkStateCanceled)
}

// CancelQueryWork cancels all non-terminal work for a query and removes tracking.
func (c *Coordinator) CancelQueryWork(queryID string) {
	items := c.Work.WorkForQuery(queryID)
	for _, item := range items {
		if !item.State.IsTerminal() {
			_ = c.Work.Transition(item.WorkID, WorkStateCanceled)
		}
	}
	c.Work.RemoveQuery(queryID)
}

// cancelWorkerWork cancels all non-terminal work for a worker.
func (c *Coordinator) cancelWorkerWork(workerID string) []WorkItem {
	items := c.Work.WorkForWorker(workerID)
	for i := range items {
		_ = c.Work.Transition(items[i].WorkID, WorkStateCanceled)
	}
	return items
}

// reassignWorkerWork reassigns all non-terminal work from a worker to other workers.
func (c *Coordinator) reassignWorkerWork(workerID string) ([]ReassignedWork, error) {
	items := c.Work.WorkForWorker(workerID)
	var reassigned []ReassignedWork

	allWorkers := c.Workers.AllWorkers()

	for _, item := range items {
		newWorkerID, err := AssignByRendezvous(item.AffinityKey, allWorkers)
		if err != nil {
			// No available workers; cancel this work item instead.
			_ = c.Work.Transition(item.WorkID, WorkStateCanceled)
			continue
		}
		newWorkID := c.IDGen.NewWorkID()
		newItem, err := c.Work.Reassign(item.WorkID, newWorkID, newWorkerID)
		if err != nil {
			continue
		}
		reassigned = append(reassigned, ReassignedWork{
			OldWorkID:   item.WorkID,
			NewWorkID:   newWorkID,
			NewWorkerID: newWorkerID,
			Item:        *newItem,
		})
	}
	return reassigned, nil
}

// reassignUnstartedWork reassigns only Assigned (not yet Accepted) work from a worker.
func (c *Coordinator) reassignUnstartedWork(workerID string) ([]ReassignedWork, error) {
	items := c.Work.WorkForWorker(workerID)
	var reassigned []ReassignedWork

	allWorkers := c.Workers.AllWorkers()

	for _, item := range items {
		if item.State != WorkStateAssigned {
			continue
		}
		newWorkerID, err := AssignByRendezvous(item.AffinityKey, allWorkers)
		if err != nil {
			_ = c.Work.Transition(item.WorkID, WorkStateCanceled)
			continue
		}
		newWorkID := c.IDGen.NewWorkID()
		newItem, err := c.Work.Reassign(item.WorkID, newWorkID, newWorkerID)
		if err != nil {
			continue
		}
		reassigned = append(reassigned, ReassignedWork{
			OldWorkID:   item.WorkID,
			NewWorkID:   newWorkID,
			NewWorkerID: newWorkerID,
			Item:        *newItem,
		})
	}
	return reassigned, nil
}

// QueryWorkStatus returns a summary of work states for a query.
func (c *Coordinator) QueryWorkStatus(queryID string) map[WorkState]int {
	items := c.Work.WorkForQuery(queryID)
	counts := make(map[WorkState]int)
	for _, item := range items {
		counts[item.State]++
	}
	return counts
}
