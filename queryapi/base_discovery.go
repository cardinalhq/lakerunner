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

package queryapi

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
)

// Worker represents a discovered worker instance
type Worker struct {
	IP   string
	Port int
}

// SegmentWorkerMapping represents the assignment of a segment to a worker
type SegmentWorkerMapping struct {
	SegmentID int64
	Worker    Worker
}

// BaseWorkerDiscovery provides common functionality for worker discovery implementations
type BaseWorkerDiscovery struct {
	mu      sync.RWMutex
	workers []Worker
	running bool
}

// GetWorkersForSegments returns worker assignments for the given segments using consistent hashing
func (b *BaseWorkerDiscovery) GetWorkersForSegments(organizationID uuid.UUID, segmentIDs []int64) ([]SegmentWorkerMapping, error) {
	b.mu.RLock()
	ws := make([]Worker, len(b.workers))
	copy(ws, b.workers)
	b.mu.RUnlock()

	if len(ws) == 0 {
		return nil, fmt.Errorf("no workers available")
	}

	mappings := make([]SegmentWorkerMapping, 0, len(segmentIDs))
	for _, seg := range segmentIDs {
		w := b.assignSegmentToWorker(organizationID, seg, ws)
		mappings = append(mappings, SegmentWorkerMapping{SegmentID: seg, Worker: w})
	}
	return mappings, nil
}

// GetAllWorkers returns a copy of all currently discovered workers
func (b *BaseWorkerDiscovery) GetAllWorkers() ([]Worker, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ws := make([]Worker, len(b.workers))
	copy(ws, b.workers)
	return ws, nil
}

// SetWorkers updates the worker list in a thread-safe manner
func (b *BaseWorkerDiscovery) SetWorkers(workers []Worker) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.workers = workers
}

// GetWorkers returns a copy of the current workers (for internal use)
func (b *BaseWorkerDiscovery) GetWorkers() []Worker {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ws := make([]Worker, len(b.workers))
	copy(ws, b.workers)
	return ws
}

// IsRunning returns whether the discovery is currently running
func (b *BaseWorkerDiscovery) IsRunning() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.running
}

// SetRunning sets the running state
func (b *BaseWorkerDiscovery) SetRunning(running bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.running = running
}

// assignSegmentToWorker uses consistent hashing to assign a segment to a worker
func (b *BaseWorkerDiscovery) assignSegmentToWorker(org uuid.UUID, seg int64, ws []Worker) Worker {
	if len(ws) == 0 {
		return Worker{}
	}
	segKey := fmt.Sprintf("%d:%s", seg, org.String())

	var best Worker
	var bestHash uint64
	for i, w := range ws {
		wk := w.IP + ":" + strconv.Itoa(w.Port)
		hv := xxhash.Sum64String(segKey + wk)
		if i == 0 || hv > bestHash {
			best, bestHash = w, hv
		}
	}
	return best
}
