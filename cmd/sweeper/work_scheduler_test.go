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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockWorkItem is a test implementation of WorkItem
type MockWorkItem struct {
	mock.Mock
	nextRunTime time.Time
	key         string
}

func (m *MockWorkItem) Perform(ctx context.Context) time.Duration {
	args := m.Called(ctx)
	return args.Get(0).(time.Duration)
}

func (m *MockWorkItem) GetNextRunTime() time.Time {
	return m.nextRunTime
}

func (m *MockWorkItem) SetNextRunTime(t time.Time) {
	m.nextRunTime = t
}

func (m *MockWorkItem) GetKey() string {
	return m.key
}

// Test WorkItemHeap
func TestWorkItemHeap(t *testing.T) {
	t.Run("heap operations", func(t *testing.T) {
		h := &WorkItemHeap{}

		// Create test items with different next run times
		now := time.Now()
		item1 := &MockWorkItem{nextRunTime: now.Add(10 * time.Minute), key: "item1"}
		item2 := &MockWorkItem{nextRunTime: now.Add(5 * time.Minute), key: "item2"}
		item3 := &MockWorkItem{nextRunTime: now.Add(15 * time.Minute), key: "item3"}

		// Test heap.Push
		heap.Push(h, item1)
		heap.Push(h, item2)
		heap.Push(h, item3)

		assert.Equal(t, 3, h.Len())

		// Test that item2 (earliest time) is at the top
		assert.Equal(t, item2, (*h)[0])

		// Test heap.Pop - should return items in chronological order
		first := heap.Pop(h).(WorkItem)
		assert.Equal(t, item2, first)

		second := heap.Pop(h).(WorkItem)
		assert.Equal(t, item1, second)

		third := heap.Pop(h).(WorkItem)
		assert.Equal(t, item3, third)

		assert.Equal(t, 0, h.Len())
	})
}

func TestWorkScheduler_newWorkScheduler(t *testing.T) {
	scheduler := newWorkScheduler()

	assert.NotNil(t, scheduler)
	assert.Equal(t, 0, scheduler.heap.Len())
	assert.NotNil(t, scheduler.wakeupCh)
}

func TestWorkScheduler_popNextWorkItem(t *testing.T) {
	scheduler := newWorkScheduler()

	t.Run("no work items", func(t *testing.T) {
		item := scheduler.popNextWorkItem()
		assert.Nil(t, item)
	})

	t.Run("no ready work items", func(t *testing.T) {
		futureTime := time.Now().Add(time.Hour)
		mockItem := &MockWorkItem{
			nextRunTime: futureTime,
			key:         "test-key",
		}

		scheduler.addWorkItem(mockItem)

		item := scheduler.popNextWorkItem()
		assert.Nil(t, item)

		// Item should still be in heap
		assert.Equal(t, 1, scheduler.heap.Len())
	})

	t.Run("ready work item", func(t *testing.T) {
		scheduler = newWorkScheduler()
		pastTime := time.Now().Add(-time.Minute)
		mockItem := &MockWorkItem{
			nextRunTime: pastTime,
			key:         "test-key",
		}

		scheduler.addWorkItem(mockItem)

		item := scheduler.popNextWorkItem()
		assert.Equal(t, mockItem, item)
		assert.Equal(t, 0, scheduler.heap.Len())
	})

	t.Run("multiple work items - earliest ready item", func(t *testing.T) {
		scheduler = newWorkScheduler()
		now := time.Now()

		item1 := &MockWorkItem{
			nextRunTime: now.Add(-10 * time.Minute),
			key:         "item1",
		}
		item2 := &MockWorkItem{
			nextRunTime: now.Add(-5 * time.Minute),
			key:         "item2",
		}
		item3 := &MockWorkItem{
			nextRunTime: now.Add(-15 * time.Minute), // Earliest
			key:         "item3",
		}

		scheduler.addWorkItem(item1)
		scheduler.addWorkItem(item2)
		scheduler.addWorkItem(item3)

		// Should return item3 (earliest time)
		item := scheduler.popNextWorkItem()
		assert.Equal(t, item3, item)
		assert.Equal(t, 2, scheduler.heap.Len())
	})
}

func TestWorkScheduler_rescheduleWorkItem(t *testing.T) {
	t.Run("reschedule work item", func(t *testing.T) {
		scheduler := newWorkScheduler()
		mockItem := &MockWorkItem{
			nextRunTime: time.Now(),
			key:         "test-key",
		}

		scheduler.rescheduleWorkItem(mockItem, 5*time.Minute)

		assert.Equal(t, 1, scheduler.heap.Len())
		// Verify next run time was updated
		assert.True(t, mockItem.GetNextRunTime().After(time.Now().Add(4*time.Minute)))
	})

	t.Run("drop work item with negative duration", func(t *testing.T) {
		scheduler := newWorkScheduler()
		mockItem := &MockWorkItem{
			nextRunTime: time.Now(),
			key:         "test-key",
		}

		scheduler.rescheduleWorkItem(mockItem, -1*time.Second)

		// Should not be added back to heap
		assert.Equal(t, 0, scheduler.heap.Len())
	})
}

func TestWorkScheduler_getNextWakeupTime(t *testing.T) {
	scheduler := newWorkScheduler()

	t.Run("no work items", func(t *testing.T) {
		nextTime := scheduler.getNextWakeupTime()
		assert.Nil(t, nextTime)
	})

	t.Run("work items in heap", func(t *testing.T) {
		now := time.Now()

		item1 := &MockWorkItem{nextRunTime: now.Add(10 * time.Minute)}
		item2 := &MockWorkItem{nextRunTime: now.Add(5 * time.Minute)}
		item3 := &MockWorkItem{nextRunTime: now.Add(15 * time.Minute)}

		heap.Push(&scheduler.heap, item1)
		heap.Push(&scheduler.heap, item2)
		heap.Push(&scheduler.heap, item3)

		nextTime := scheduler.getNextWakeupTime()
		require.NotNil(t, nextTime)

		// Should return the earliest time (item2)
		assert.Equal(t, item2.nextRunTime, *nextTime)
	})
}

func TestWorkScheduler_signalWakeup(t *testing.T) {
	scheduler := newWorkScheduler()

	// Should not block
	scheduler.signalWakeup()
	scheduler.signalWakeup() // Second call should not block either

	// Channel should have a signal
	select {
	case <-scheduler.wakeupCh:
		// Good, channel had a signal
	default:
		t.Fatal("Expected wakeup signal in channel")
	}

	// Channel should now be empty, second signal should not be there
	select {
	case <-scheduler.wakeupCh:
		t.Fatal("Unexpected extra signal in channel")
	default:
		// Good, channel is empty
	}
}
