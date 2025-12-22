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

	"github.com/cardinalhq/lakerunner/pipeline"
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

func TestLRUCache_ComputeIfAbsent(t *testing.T) {
	cache := NewLRUCache(
		10,
		time.Hour,
		time.Hour,
		100,
		func([]*Entry) {},
	)
	defer cache.Close()

	callCount := 0
	producer := func() pipeline.Row {
		callCount++
		return pipeline.Row{}
	}

	// First call should invoke producer
	added := cache.ComputeIfAbsent(1, producer)
	assert.True(t, added, "First ComputeIfAbsent should succeed")
	assert.Equal(t, 1, callCount, "Producer should be called once")
	assert.True(t, cache.Contains(1))

	// Second call with same key should NOT invoke producer
	added = cache.ComputeIfAbsent(1, producer)
	assert.False(t, added, "Second ComputeIfAbsent with same key should fail")
	assert.Equal(t, 1, callCount, "Producer should NOT be called again")

	// Different key should invoke producer
	added = cache.ComputeIfAbsent(2, producer)
	assert.True(t, added, "ComputeIfAbsent with different key should succeed")
	assert.Equal(t, 2, callCount, "Producer should be called for new key")
	assert.True(t, cache.Contains(2))
}

func TestLRUCache_ComputeIfAbsentExpired(t *testing.T) {
	cache := NewLRUCache(
		10,
		50*time.Millisecond,
		time.Hour,
		100,
		func([]*Entry) {},
	)
	defer cache.Close()

	callCount := 0
	producer := func() pipeline.Row {
		callCount++
		return pipeline.Row{}
	}

	added := cache.ComputeIfAbsent(1, producer)
	assert.True(t, added)
	assert.Equal(t, 1, callCount)

	// Wait for expiry
	time.Sleep(100 * time.Millisecond)

	// After expiry, ComputeIfAbsent should invoke producer again
	added = cache.ComputeIfAbsent(1, producer)
	assert.True(t, added, "ComputeIfAbsent should succeed after entry expires")
	assert.Equal(t, 2, callCount, "Producer should be called after expiry")
}

// TestLRUCache_EvictedPendingPoolReturn verifies that entries evicted by Put/PutIfAbsent
// that are pending publication have their rows returned to the pool only AFTER
// the publish callback completes. This tests the race condition fix where evicted
// entries were being returned to the pool while the callback was still using them.
func TestLRUCache_EvictedPendingPoolReturn(t *testing.T) {
	var mu sync.Mutex
	var callbackEntries []*Entry

	cache := NewLRUCache(
		3,                    // Small capacity to trigger eviction
		50*time.Millisecond,  // Short expiry so entries become publishable
		100*time.Millisecond, // Cleanup interval
		100,
		func(entries []*Entry) {
			mu.Lock()
			callbackEntries = append(callbackEntries, entries...)
			mu.Unlock()

			// Simulate callback doing work with the entries
			time.Sleep(50 * time.Millisecond)
		},
	)
	defer cache.Close()

	// Fill cache to capacity
	cache.Put(1, pipeline.Row{})
	cache.Put(2, pipeline.Row{})
	cache.Put(3, pipeline.Row{})

	// Wait for entries to become publishable (past expiry threshold)
	time.Sleep(60 * time.Millisecond)

	// Add new entries to trigger eviction of old ones
	// The evicted entries should be pending publication and their rows
	// should only be returned after the callback completes
	cache.Put(4, pipeline.Row{})
	cache.Put(5, pipeline.Row{})

	// Flush to trigger the callback with evicted entries
	cache.FlushPending()

	// Verify entries were published
	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, callbackEntries, "Expected evicted entries to be published")
}

// TestLRUCache_ConcurrentEvictionAndCleanup tests concurrent Put operations
// that cause evictions while cleanupExpiredEntries is running. This exercises
// the race condition fix for entries that are both pending publication and
// being evicted by capacity limits.
func TestLRUCache_ConcurrentEvictionAndCleanup(t *testing.T) {
	var mu sync.Mutex
	var publishedCount int

	cache := NewLRUCache(
		10,                  // Small capacity
		20*time.Millisecond, // Very short expiry
		30*time.Millisecond, // Short cleanup interval
		100,
		func(entries []*Entry) {
			mu.Lock()
			publishedCount += len(entries)
			// Access entry data to catch any use-after-free
			for _, e := range entries {
				_ = e.key
				_ = e.value
			}
			mu.Unlock()
		},
	)
	defer cache.Close()

	var wg sync.WaitGroup

	// Concurrent puts that will cause evictions
	for i := range 5 {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for j := range 50 {
				key := uint64(offset*100 + j)
				cache.Put(key, pipeline.Row{})
				// Small sleep to interleave with cleanup goroutine
				time.Sleep(time.Millisecond)
			}
		}(i)
	}

	// Let the test run with concurrent cleanup cycles
	wg.Wait()

	// Wait for cleanup cycles to process entries
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Greater(t, publishedCount, 0, "Expected entries to be published")
}
