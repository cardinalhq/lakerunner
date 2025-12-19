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

package ddcache

import (
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/helpers"
)

func TestCache_ZeroValue(t *testing.T) {
	cache := newCache(1024 * 1024)

	bytes1, err := cache.GetBytesForValue(0)
	require.NoError(t, err)
	require.NotEmpty(t, bytes1)

	bytes2, err := cache.GetBytesForValue(0)
	require.NoError(t, err)

	// Should return the exact same underlying array (not just equal contents)
	assert.Same(t, &bytes1[0], &bytes2[0], "zero value should return cached slice")

	// Decode and verify it contains value 0
	sketch, err := helpers.DecodeSketch(bytes1)
	require.NoError(t, err)
	assert.Equal(t, float64(1), sketch.GetCount())
	min, err := sketch.GetMinValue()
	require.NoError(t, err)
	assert.InDelta(t, 0, min, 0.01)
}

func TestCache_OneValue(t *testing.T) {
	cache := newCache(1024 * 1024)

	bytes1, err := cache.GetBytesForValue(1)
	require.NoError(t, err)
	require.NotEmpty(t, bytes1)

	bytes2, err := cache.GetBytesForValue(1)
	require.NoError(t, err)

	// Should return the exact same underlying array (constant value)
	assert.Same(t, &bytes1[0], &bytes2[0], "one value should return cached slice")

	// Decode and verify
	sketch, err := helpers.DecodeSketch(bytes1)
	require.NoError(t, err)
	assert.Equal(t, float64(1), sketch.GetCount())
	val, err := sketch.GetValueAtQuantile(0.5)
	require.NoError(t, err)
	assert.InDelta(t, 1, val, 0.1)
}

func TestCache_LRUCaching(t *testing.T) {
	cache := newCache(1024 * 1024)

	// Get bytes for non-constant value
	bytes1, err := cache.GetBytesForValue(42.5)
	require.NoError(t, err)
	require.NotEmpty(t, bytes1)

	// Get bytes for same value again - should be cached (same slice)
	bytes2, err := cache.GetBytesForValue(42.5)
	require.NoError(t, err)

	assert.Same(t, &bytes1[0], &bytes2[0], "cached value should return same slice")
	assert.Equal(t, int64(1), cache.ItemCount())

	// Decode and verify
	sketch, err := helpers.DecodeSketch(bytes1)
	require.NoError(t, err)
	assert.Equal(t, float64(1), sketch.GetCount())
}

func TestCache_LRUEviction(t *testing.T) {
	// Each sketch encoding is ~40-50 bytes. Create a small cache.
	// Set max size to hold approximately 3 entries
	cache := newCache(150)

	// Add several unique values to trigger eviction
	for i := range 10 {
		_, err := cache.GetBytesForValue(float64(i + 100)) // Avoid 0, 1 constants
		require.NoError(t, err)
	}

	// Cache should be under the limit
	assert.LessOrEqual(t, cache.BytesSize(), int64(150)+int64(len(cache.zeroBytes)+len(cache.oneBytes)))

	// Should have fewer than 10 items due to eviction
	assert.Less(t, cache.ItemCount(), int64(10))
}

func TestCache_MemoryLimit(t *testing.T) {
	// Create cache with 1KB limit
	maxSize := int64(1024)
	cache := newCache(maxSize)

	// Pre-warm the constants
	_, err := cache.GetBytesForValue(0)
	require.NoError(t, err)
	_, err = cache.GetBytesForValue(1)
	require.NoError(t, err)

	constSize := int64(len(cache.zeroBytes) + len(cache.oneBytes))

	// Add many unique values
	for i := range 100 {
		_, err := cache.GetBytesForValue(float64(i + 1000))
		require.NoError(t, err)
	}

	// LRU portion should be under limit (BytesSize includes constants)
	lruSize := cache.BytesSize() - constSize
	assert.LessOrEqual(t, lruSize, maxSize, "LRU cache should stay under maxSizeBytes")
}

func TestCache_MemoryAccounting(t *testing.T) {
	cache := newCache(1024 * 1024)

	// Before any access, LRU size is 0 (constants not yet initialized)
	assert.Equal(t, int64(0), cache.ItemCount())

	// Access constants and some LRU values
	_, err := cache.GetBytesForValue(0)
	require.NoError(t, err)
	_, err = cache.GetBytesForValue(1)
	require.NoError(t, err)

	constSize := cache.BytesSize()
	assert.Greater(t, constSize, int64(0), "constants should have non-zero size")

	// Add an LRU entry
	_, err = cache.GetBytesForValue(123.456)
	require.NoError(t, err)

	assert.Equal(t, int64(1), cache.ItemCount())
	assert.Greater(t, cache.BytesSize(), constSize, "size should increase with LRU entries")
}

func TestCache_NegativeValue(t *testing.T) {
	cache := newCache(1024 * 1024)

	bytes, err := cache.GetBytesForValue(-100.5)
	require.NoError(t, err)
	require.NotEmpty(t, bytes)

	sketch, err := helpers.DecodeSketch(bytes)
	require.NoError(t, err)
	assert.Equal(t, float64(1), sketch.GetCount())
}

func TestCache_ConcurrentAccess(t *testing.T) {
	cache := newCache(1024 * 1024)

	var wg sync.WaitGroup
	numGoroutines := 100
	numOps := 100

	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range numOps {
				value := float64(j % 50) // Use limited set of values to test caching
				_, err := cache.GetBytesForValue(value)
				assert.NoError(t, err)
			}
		}()
	}

	wg.Wait()

	// Should have at most 50 items (0-49, with 0 and 1 as constants not in LRU)
	assert.LessOrEqual(t, cache.ItemCount(), int64(48))
}

func TestCache_MaxSizeBytes(t *testing.T) {
	cache := newCache(500)
	assert.Equal(t, int64(500), cache.MaxSizeBytes())
}

func TestCache_MemoryOverhead(t *testing.T) {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	// Large cache so nothing gets evicted
	cache := newCache(100 * 1024 * 1024)

	// Warm up constants
	_, err := cache.GetBytesForValue(0)
	require.NoError(t, err)
	_, err = cache.GetBytesForValue(1)
	require.NoError(t, err)

	numItems := 10000

	for i := range numItems {
		value := float64(i + 1000) // Avoid 0, 1 constants
		_, err := cache.GetBytesForValue(value)
		require.NoError(t, err)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	reportedBytes := cache.BytesSize()
	actualAlloc := int64(after.HeapAlloc - before.HeapAlloc)

	overhead := actualAlloc - reportedBytes
	overheadPerItem := float64(overhead) / float64(numItems)
	overheadPct := float64(overhead) / float64(actualAlloc) * 100

	t.Logf("Items: %d", numItems)
	t.Logf("Reported []byte size: %d bytes", reportedBytes)
	t.Logf("Actual heap alloc: %d bytes", actualAlloc)
	t.Logf("Overhead: %d bytes (%.1f%%)", overhead, overheadPct)
	t.Logf("Overhead per item: %.1f bytes", overheadPerItem)

	// Sanity check: overhead should be reasonable (less than 100% of content)
	assert.Less(t, overheadPct, 100.0, "overhead should be less than 100%% of content")
}

func TestCache_LRUOrder(t *testing.T) {
	// Create a tiny cache
	cache := newCache(100)

	// Add values in order
	_, err := cache.GetBytesForValue(10.0)
	require.NoError(t, err)
	_, err = cache.GetBytesForValue(20.0)
	require.NoError(t, err)
	_, err = cache.GetBytesForValue(30.0)
	require.NoError(t, err)

	// Access 10.0 again to make it most recently used
	_, err = cache.GetBytesForValue(10.0)
	require.NoError(t, err)

	// Add more values to trigger eviction
	_, err = cache.GetBytesForValue(40.0)
	require.NoError(t, err)
	_, err = cache.GetBytesForValue(50.0)
	require.NoError(t, err)

	// 10.0 should still be cached (was recently accessed)
	// 20.0 or 30.0 should have been evicted
	bytes1, err := cache.GetBytesForValue(10.0)
	require.NoError(t, err)
	bytes2, err := cache.GetBytesForValue(10.0)
	require.NoError(t, err)
	assert.Same(t, &bytes1[0], &bytes2[0], "recently accessed value should still be cached")
}
