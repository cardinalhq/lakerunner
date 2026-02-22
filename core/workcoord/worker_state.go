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
	"fmt"
	"sync"
)

// ErrWorkerNotFound is returned when a worker ID is not in the registry.
type ErrWorkerNotFound struct {
	WorkerID string
}

func (e *ErrWorkerNotFound) Error() string {
	return fmt.Sprintf("worker not found: %s", e.WorkerID)
}

// ErrWorkerUnavailable is returned when trying to assign work to a worker
// that is not alive, not accepting work, or draining.
type ErrWorkerUnavailable struct {
	WorkerID string
}

func (e *ErrWorkerUnavailable) Error() string {
	return fmt.Sprintf("worker unavailable: %s", e.WorkerID)
}

// ErrWorkerAlreadyRegistered is returned when a worker ID is already in the registry.
type ErrWorkerAlreadyRegistered struct {
	WorkerID string
}

func (e *ErrWorkerAlreadyRegistered) Error() string {
	return fmt.Sprintf("worker already registered: %s", e.WorkerID)
}

// WorkerRegistry tracks worker membership and status.
type WorkerRegistry struct {
	mu      sync.RWMutex
	workers map[string]*WorkerInfo
}

// NewWorkerRegistry creates a new empty worker registry.
func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{
		workers: make(map[string]*WorkerInfo),
	}
}

// Register adds a new worker. The worker starts as alive but not accepting work;
// the worker declares accepting_work via its first status message.
func (r *WorkerRegistry) Register(workerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.workers[workerID]; exists {
		return &ErrWorkerAlreadyRegistered{WorkerID: workerID}
	}
	r.workers[workerID] = &WorkerInfo{
		WorkerID:      workerID,
		Alive:         true,
		AcceptingWork: false,
		Draining:      false,
	}
	return nil
}

// Remove removes a worker from the registry entirely.
func (r *WorkerRegistry) Remove(workerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.workers[workerID]; !exists {
		return &ErrWorkerNotFound{WorkerID: workerID}
	}
	delete(r.workers, workerID)
	return nil
}

// Disconnect marks a worker as not alive and not accepting work.
func (r *WorkerRegistry) Disconnect(workerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, exists := r.workers[workerID]
	if !exists {
		return &ErrWorkerNotFound{WorkerID: workerID}
	}
	w.Alive = false
	w.AcceptingWork = false
	return nil
}

// Reconnect marks a previously disconnected worker as alive and accepting work.
func (r *WorkerRegistry) Reconnect(workerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, exists := r.workers[workerID]
	if !exists {
		return &ErrWorkerNotFound{WorkerID: workerID}
	}
	w.Alive = true
	w.AcceptingWork = true
	w.Draining = false
	return nil
}

// BeginDrain transitions a worker to draining state.
// Sets accepting_work=false and draining=true.
func (r *WorkerRegistry) BeginDrain(workerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, exists := r.workers[workerID]
	if !exists {
		return &ErrWorkerNotFound{WorkerID: workerID}
	}
	w.AcceptingWork = false
	w.Draining = true
	return nil
}

// SetAcceptingWork updates whether a worker is accepting new work.
func (r *WorkerRegistry) SetAcceptingWork(workerID string, accepting bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, exists := r.workers[workerID]
	if !exists {
		return &ErrWorkerNotFound{WorkerID: workerID}
	}
	w.AcceptingWork = accepting
	return nil
}

// Get returns a copy of the worker info, or an error if not found.
func (r *WorkerRegistry) Get(workerID string) (WorkerInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	w, exists := r.workers[workerID]
	if !exists {
		return WorkerInfo{}, &ErrWorkerNotFound{WorkerID: workerID}
	}
	return *w, nil
}

// AvailableWorkers returns a snapshot of all workers currently available for work.
func (r *WorkerRegistry) AvailableWorkers() []WorkerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var result []WorkerInfo
	for _, w := range r.workers {
		if w.IsAvailable() {
			result = append(result, *w)
		}
	}
	return result
}

// AllWorkers returns a snapshot of all registered workers.
func (r *WorkerRegistry) AllWorkers() []WorkerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]WorkerInfo, 0, len(r.workers))
	for _, w := range r.workers {
		result = append(result, *w)
	}
	return result
}

// Count returns the number of registered workers.
func (r *WorkerRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.workers)
}
