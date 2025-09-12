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

package sweeper

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// WorkItem represents a cleanup task that can perform work
type WorkItem interface {
	// Perform executes the work and returns the duration until it should run again.
	// Negative values indicate the work item should be dropped/removed.
	Perform(ctx context.Context) time.Duration

	// GetNextRunTime returns when this work should next be executed
	GetNextRunTime() time.Time

	// SetNextRunTime updates when this work should next be executed
	SetNextRunTime(t time.Time)

	// GetKey returns a unique key for this work item (for tracking)
	GetKey() string
}

// WorkItemHeap implements heap.Interface for WorkItem
type WorkItemHeap []WorkItem

func (h WorkItemHeap) Len() int           { return len(h) }
func (h WorkItemHeap) Less(i, j int) bool { return h[i].GetNextRunTime().Before(h[j].GetNextRunTime()) }
func (h WorkItemHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *WorkItemHeap) Push(x any) {
	*h = append(*h, x.(WorkItem))
}

func (h *WorkItemHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// WorkScheduler manages work items in a generic, domain-agnostic way
type WorkScheduler struct {
	heap     WorkItemHeap
	mu       sync.Mutex
	wakeupCh chan struct{} // Signal when new work is added
}

// newWorkScheduler creates a new work scheduler
func newWorkScheduler() *WorkScheduler {
	return &WorkScheduler{
		heap:     make(WorkItemHeap, 0),
		wakeupCh: make(chan struct{}, 1),
	}
}

// popNextWorkItem returns the next work item that's ready to run, or nil if none ready
func (s *WorkScheduler) popNextWorkItem() WorkItem {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.heap.Len() == 0 {
		return nil
	}

	// Check if the top item is ready to run
	item := s.heap[0]
	if time.Now().Before(item.GetNextRunTime()) {
		return nil
	}

	// Pop and return the ready item
	return heap.Pop(&s.heap).(WorkItem)
}

// rescheduleWorkItem reschedules a work item based on the returned duration
func (s *WorkScheduler) rescheduleWorkItem(item WorkItem, rescheduleIn time.Duration) {
	// Drop items with negative durations
	if rescheduleIn < 0 {
		return
	}

	// Set next run time
	item.SetNextRunTime(time.Now().Add(rescheduleIn))

	s.mu.Lock()
	defer s.mu.Unlock()

	// Add back to heap
	heap.Push(&s.heap, item)
	s.signalWakeup()
}

// addWorkItem adds a new work item to the scheduler
func (s *WorkScheduler) addWorkItem(item WorkItem) {
	s.mu.Lock()
	defer s.mu.Unlock()

	heap.Push(&s.heap, item)
	s.signalWakeup()
}

// getNextWakeupTime returns when the scheduler should next check for work
func (s *WorkScheduler) getNextWakeupTime() *time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.heap.Len() == 0 {
		return nil
	}

	nextTime := s.heap[0].GetNextRunTime()
	return &nextTime
}

// signalWakeup signals the scheduler to wake up (non-blocking)
func (s *WorkScheduler) signalWakeup() {
	select {
	case s.wakeupCh <- struct{}{}:
	default:
		// Channel already has signal, no need to add another
	}
}
