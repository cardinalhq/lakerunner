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

package exemplars

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
)

func TestLRUCache_PutAndContains(t *testing.T) {
	publishCalled := false
	cache := NewLRUCache(
		10,
		time.Hour,
		time.Hour,
		100,
		func([]*Entry) { publishCalled = true },
	)
	defer cache.Close()

	// Put an item
	cache.Put(1, pipeline.Row{})

	// Should contain the key
	assert.True(t, cache.Contains(1))

	// Should not contain a different key
	assert.False(t, cache.Contains(2))

	// Publish callback should not be called yet
	assert.False(t, publishCalled)
}

func TestLRUCache_UpdateExisting(t *testing.T) {
	cache := NewLRUCache(
		10,
		time.Hour,
		time.Hour,
		100,
		func([]*Entry) {},
	)
	defer cache.Close()

	// Put initial value
	cache.Put(1, pipeline.Row{})
	assert.True(t, cache.Contains(1))

	// Update with new value
	cache.Put(1, pipeline.Row{})
	assert.True(t, cache.Contains(1))
}

func TestLRUCache_Expiry(t *testing.T) {
	cache := NewLRUCache(
		10,
		50*time.Millisecond, // Short expiry
		time.Hour,
		100,
		func([]*Entry) {},
	)
	defer cache.Close()

	// Put an item
	cache.Put(1, pipeline.Row{})
	assert.True(t, cache.Contains(1))

	// Wait for expiry
	time.Sleep(100 * time.Millisecond)

	// Should no longer be contained (expired)
	assert.False(t, cache.Contains(1))
}

func TestLRUCache_CapacityEviction(t *testing.T) {
	var publishedEntries []*Entry
	var mu sync.Mutex

	cache := NewLRUCache(
		3, // Small capacity
		time.Hour,
		time.Hour,
		100,
		func(entries []*Entry) {
			mu.Lock()
			defer mu.Unlock()
			publishedEntries = append(publishedEntries, entries...)
		},
	)
	defer cache.Close()

	// Fill cache to capacity
	cache.Put(1, pipeline.Row{})
	cache.Put(2, pipeline.Row{})
	cache.Put(3, pipeline.Row{})

	assert.True(t, cache.Contains(1))
	assert.True(t, cache.Contains(2))
	assert.True(t, cache.Contains(3))

	// Add one more - should evict oldest (key 1)
	cache.Put(4, pipeline.Row{})

	// Key 1 should be evicted (it was at the back of the LRU list)
	assert.True(t, cache.Contains(2))
	assert.True(t, cache.Contains(3))
	assert.True(t, cache.Contains(4))

	// Flush pending entries to trigger the publish callback
	cache.FlushPending()

	// Check that entries were published
	mu.Lock()
	defer mu.Unlock()
	// Published entries includes the pending entries that were added
	require.Greater(t, len(publishedEntries), 0, "Expected some entries to be published")
}

func TestLRUCache_LRUOrdering(t *testing.T) {
	cache := NewLRUCache(
		3,
		time.Hour,
		time.Hour,
		100,
		func([]*Entry) {},
	)
	defer cache.Close()

	// Add three items
	cache.Put(1, pipeline.Row{})
	cache.Put(2, pipeline.Row{})
	cache.Put(3, pipeline.Row{})

	// All three should be present
	assert.True(t, cache.Contains(1))
	assert.True(t, cache.Contains(2))
	assert.True(t, cache.Contains(3))

	// Add another item - should evict the oldest (key 1, at the back)
	cache.Put(4, pipeline.Row{})

	// Key 1 should be evicted since it was least recently used
	assert.False(t, cache.Contains(1))
	assert.True(t, cache.Contains(2))
	assert.True(t, cache.Contains(3))
	assert.True(t, cache.Contains(4))
}

func TestLRUCache_PublishCallback(t *testing.T) {
	var publishedEntries []*Entry
	var mu sync.Mutex

	cache := NewLRUCache(
		5,
		50*time.Millisecond,  // Short expiry
		100*time.Millisecond, // Short report interval
		100,
		func(entries []*Entry) {
			mu.Lock()
			defer mu.Unlock()
			publishedEntries = append(publishedEntries, entries...)
		},
	)
	defer cache.Close()

	// Add some entries
	cache.Put(1, pipeline.Row{})
	cache.Put(2, pipeline.Row{})
	cache.Put(3, pipeline.Row{})

	// Wait for cleanup cycle to run
	time.Sleep(200 * time.Millisecond)

	// Check that entries were published
	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, publishedEntries, "Expected some entries to be published")
}

func TestLRUCache_FlushPending(t *testing.T) {
	var publishedEntries []*Entry
	var mu sync.Mutex

	cache := NewLRUCache(
		10,
		time.Hour,
		time.Hour,
		100,
		func(entries []*Entry) {
			mu.Lock()
			defer mu.Unlock()
			publishedEntries = append(publishedEntries, entries...)
		},
	)
	defer cache.Close()

	// Add entries (they become pending immediately upon Put)
	cache.Put(1, pipeline.Row{})
	cache.Put(2, pipeline.Row{})
	cache.Put(3, pipeline.Row{})

	// Flush pending
	cache.FlushPending()

	// Check that entries were published
	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, publishedEntries, "Expected entries to be published after flush")
}

func TestLRUCache_ConcurrentAccess(t *testing.T) {
	cache := NewLRUCache(
		100,
		time.Hour,
		time.Hour,
		100,
		func([]*Entry) {},
	)
	defer cache.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	operationsPerGoroutine := 100

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				key := uint64(offset*operationsPerGoroutine + j)
				cache.Put(key, pipeline.Row{})
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				key := uint64(offset*operationsPerGoroutine + j)
				_ = cache.Contains(key)
			}
		}(i)
	}

	wg.Wait()
	// Test passes if no race conditions or panics occur
}

func TestLRUCache_ReservoirSample(t *testing.T) {
	rng := newTestRand()

	// Test with fewer items than sample size
	items := []*Entry{
		{key: 1, value: pipeline.Row{}},
		{key: 2, value: pipeline.Row{}},
		{key: 3, value: pipeline.Row{}},
	}
	sample := reservoirSample(items, 5, rng)
	assert.Equal(t, 3, len(sample), "Should return all items when n < k")

	// Test with more items than sample size
	manyItems := make([]*Entry, 100)
	for i := range manyItems {
		manyItems[i] = &Entry{key: uint64(i), value: pipeline.Row{}}
	}
	sample = reservoirSample(manyItems, 10, rng)
	assert.Equal(t, 10, len(sample), "Should return exactly k items when n > k")

	// Test with empty slice
	sample = reservoirSample([]*Entry{}, 5, rng)
	assert.Nil(t, sample, "Should return nil for empty input")

	// Test with zero k
	sample = reservoirSample(items, 0, rng)
	assert.Nil(t, sample, "Should return nil for k=0")
}

// newTestRand creates a new rand.Rand for testing
func newTestRand() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

func TestLRUCache_EntryPublishing(t *testing.T) {
	var publishedKeys []uint64
	var mu sync.Mutex

	cache := NewLRUCache(
		5,
		100*time.Millisecond, // Entry expiry
		50*time.Millisecond,  // Cleanup interval
		100,
		func(entries []*Entry) {
			mu.Lock()
			defer mu.Unlock()
			for _, entry := range entries {
				publishedKeys = append(publishedKeys, entry.key)
			}
		},
	)
	defer cache.Close()

	// Add entries
	cache.Put(1, pipeline.Row{})
	cache.Put(2, pipeline.Row{})

	// Wait for entries to expire and be published
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, publishedKeys, "Expected keys to be published after expiry")
}

func TestLRUCache_PutIfAbsent(t *testing.T) {
	cache := NewLRUCache(
		10,
		time.Hour,
		time.Hour,
		100,
		func([]*Entry) {},
	)
	defer cache.Close()

	// First insert should succeed
	added := cache.PutIfAbsent(1, pipeline.Row{})
	assert.True(t, added, "First PutIfAbsent should succeed")
	assert.True(t, cache.Contains(1))

	// Second insert with same key should fail
	added = cache.PutIfAbsent(1, pipeline.Row{})
	assert.False(t, added, "Second PutIfAbsent with same key should fail")

	// Different key should succeed
	added = cache.PutIfAbsent(2, pipeline.Row{})
	assert.True(t, added, "PutIfAbsent with different key should succeed")
	assert.True(t, cache.Contains(2))
}

func TestLRUCache_PutIfAbsentExpired(t *testing.T) {
	cache := NewLRUCache(
		10,
		50*time.Millisecond, // Short expiry
		time.Hour,
		100,
		func([]*Entry) {},
	)
	defer cache.Close()

	// First insert
	added := cache.PutIfAbsent(1, pipeline.Row{})
	assert.True(t, added)
	assert.True(t, cache.Contains(1))

	// Wait for expiry
	time.Sleep(100 * time.Millisecond)

	// After expiry, PutIfAbsent should succeed with same key
	added = cache.PutIfAbsent(1, pipeline.Row{})
	assert.True(t, added, "PutIfAbsent should succeed after entry expires")
	assert.True(t, cache.Contains(1))
}

func TestLRUCache_PutIfAbsentConcurrent(t *testing.T) {
	cache := NewLRUCache(
		100,
		time.Hour,
		time.Hour,
		100,
		func([]*Entry) {},
	)
	defer cache.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	successCount := make([]int, numGoroutines)

	// Multiple goroutines try to insert the same key
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if cache.PutIfAbsent(1, pipeline.Row{}) {
				successCount[idx] = 1
			}
		}(i)
	}

	wg.Wait()

	// Only one goroutine should have succeeded
	totalSuccesses := 0
	for _, count := range successCount {
		totalSuccesses += count
	}
	assert.Equal(t, 1, totalSuccesses, "Only one PutIfAbsent should succeed with concurrent attempts")
	assert.True(t, cache.Contains(1))
}
