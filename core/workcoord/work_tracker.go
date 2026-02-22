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

import (
	"errors"
	"fmt"
	"sync"
)

// ErrWorkNotFound is returned when a work ID is not tracked.
type ErrWorkNotFound struct {
	WorkID string
}

func (e *ErrWorkNotFound) Error() string {
	return fmt.Sprintf("work item not found: %s", e.WorkID)
}

// ErrDuplicateCompletion is returned when a completion has already been recorded.
var ErrDuplicateCompletion = errors.New("duplicate completion: already recorded")

// ErrDuplicateWork is returned when a work item with the same ID already exists.
type ErrDuplicateWork struct {
	WorkID string
}

func (e *ErrDuplicateWork) Error() string {
	return fmt.Sprintf("duplicate work item: %s", e.WorkID)
}

// WorkTracker tracks in-flight work items and provides idempotency dedup.
type WorkTracker struct {
	mu sync.RWMutex

	// work indexed by work_id
	work map[string]*WorkItem

	// queryWork indexes work_ids by query_id for bulk operations
	queryWork map[string]map[string]struct{}

	// workerWork indexes work_ids by worker_id for reassignment
	workerWork map[string]map[string]struct{}

	// completions tracks already-committed completions for dedup
	completions map[CompletionKey]struct{}
}

// NewWorkTracker creates a new empty work tracker.
func NewWorkTracker() *WorkTracker {
	return &WorkTracker{
		work:        make(map[string]*WorkItem),
		queryWork:   make(map[string]map[string]struct{}),
		workerWork:  make(map[string]map[string]struct{}),
		completions: make(map[CompletionKey]struct{}),
	}
}

// Add registers a new work item. Returns error if work_id already exists.
func (t *WorkTracker) Add(item *WorkItem) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.work[item.WorkID]; exists {
		return &ErrDuplicateWork{WorkID: item.WorkID}
	}

	t.work[item.WorkID] = item

	if t.queryWork[item.QueryID] == nil {
		t.queryWork[item.QueryID] = make(map[string]struct{})
	}
	t.queryWork[item.QueryID][item.WorkID] = struct{}{}

	if t.workerWork[item.WorkerID] == nil {
		t.workerWork[item.WorkerID] = make(map[string]struct{})
	}
	t.workerWork[item.WorkerID][item.WorkID] = struct{}{}

	return nil
}

// Get returns a copy of a work item by ID.
func (t *WorkTracker) Get(workID string) (WorkItem, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	item, exists := t.work[workID]
	if !exists {
		return WorkItem{}, &ErrWorkNotFound{WorkID: workID}
	}
	return *item, nil
}

// Transition performs a state transition on a work item.
func (t *WorkTracker) Transition(workID string, to WorkState) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	item, exists := t.work[workID]
	if !exists {
		return &ErrWorkNotFound{WorkID: workID}
	}
	return TransitionWork(item, to)
}

// Complete marks a work item as ready and records the completion for idempotency.
// Returns ErrDuplicateCompletion if this exact completion was already recorded.
func (t *WorkTracker) Complete(workID string, artifact ArtifactInfo) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	item, exists := t.work[workID]
	if !exists {
		return &ErrWorkNotFound{WorkID: workID}
	}

	key := CompletionKey{
		QueryID:          item.QueryID,
		LeafID:           item.LeafID,
		WorkID:           item.WorkID,
		ArtifactChecksum: artifact.ArtifactChecksum,
	}

	if _, dup := t.completions[key]; dup {
		return ErrDuplicateCompletion
	}

	if err := TransitionWork(item, WorkStateReady); err != nil {
		return err
	}
	item.ArtifactInfo = &artifact
	t.completions[key] = struct{}{}
	return nil
}

// Reassign moves a work item to a new worker and resets it to Assigned state.
// This is only valid for items that have not reached a terminal state.
// The work item is given a new work ID to distinguish the retry.
func (t *WorkTracker) Reassign(workID string, newWorkID string, newWorkerID string) (*WorkItem, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	old, exists := t.work[workID]
	if !exists {
		return nil, &ErrWorkNotFound{WorkID: workID}
	}
	if old.State.IsTerminal() {
		return nil, &ErrInvalidTransition{From: old.State, To: WorkStateAssigned}
	}

	// Check for ID collision before any mutation.
	if _, exists := t.work[newWorkID]; exists {
		return nil, &ErrDuplicateWork{WorkID: newWorkID}
	}

	// Mark old item as canceled.
	old.State = WorkStateCanceled

	// Remove old from worker index.
	if ww := t.workerWork[old.WorkerID]; ww != nil {
		delete(ww, workID)
		if len(ww) == 0 {
			delete(t.workerWork, old.WorkerID)
		}
	}

	// Create new work item.
	newItem := &WorkItem{
		QueryID:     old.QueryID,
		LeafID:      old.LeafID,
		WorkID:      newWorkID,
		WorkerID:    newWorkerID,
		State:       WorkStateAssigned,
		AffinityKey: old.AffinityKey,
		Spec:        old.Spec,
	}

	t.work[newWorkID] = newItem

	// Update query index.
	if t.queryWork[newItem.QueryID] == nil {
		t.queryWork[newItem.QueryID] = make(map[string]struct{})
	}
	t.queryWork[newItem.QueryID][newWorkID] = struct{}{}

	// Update worker index.
	if t.workerWork[newWorkerID] == nil {
		t.workerWork[newWorkerID] = make(map[string]struct{})
	}
	t.workerWork[newWorkerID][newWorkID] = struct{}{}

	return newItem, nil
}

// WorkForQuery returns copies of all work items for a given query.
func (t *WorkTracker) WorkForQuery(queryID string) []WorkItem {
	t.mu.RLock()
	defer t.mu.RUnlock()
	ids, exists := t.queryWork[queryID]
	if !exists {
		return nil
	}
	result := make([]WorkItem, 0, len(ids))
	for wid := range ids {
		if item, ok := t.work[wid]; ok {
			result = append(result, *item)
		}
	}
	return result
}

// WorkForWorker returns copies of all non-terminal work items assigned to a worker.
func (t *WorkTracker) WorkForWorker(workerID string) []WorkItem {
	t.mu.RLock()
	defer t.mu.RUnlock()
	ids, exists := t.workerWork[workerID]
	if !exists {
		return nil
	}
	var result []WorkItem
	for wid := range ids {
		if item, ok := t.work[wid]; ok && !item.State.IsTerminal() {
			result = append(result, *item)
		}
	}
	return result
}

// RemoveQuery removes all tracking state for a query, including completions.
func (t *WorkTracker) RemoveQuery(queryID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	ids, exists := t.queryWork[queryID]
	if !exists {
		return
	}

	for wid := range ids {
		item, ok := t.work[wid]
		if !ok {
			continue
		}
		// Remove from worker index.
		if ww := t.workerWork[item.WorkerID]; ww != nil {
			delete(ww, wid)
			if len(ww) == 0 {
				delete(t.workerWork, item.WorkerID)
			}
		}
		// Remove completion entries for this work.
		if item.ArtifactInfo != nil {
			delete(t.completions, CompletionKey{
				QueryID:          item.QueryID,
				LeafID:           item.LeafID,
				WorkID:           item.WorkID,
				ArtifactChecksum: item.ArtifactInfo.ArtifactChecksum,
			})
		}
		delete(t.work, wid)
	}

	delete(t.queryWork, queryID)
}

// PendingCount returns the number of non-terminal work items.
func (t *WorkTracker) PendingCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	count := 0
	for _, item := range t.work {
		if !item.State.IsTerminal() {
			count++
		}
	}
	return count
}

// TotalCount returns the total number of tracked work items.
func (t *WorkTracker) TotalCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.work)
}
