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
	"math"
	"math/rand"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOAMap_BasicOperations(t *testing.T) {
	m := newOAMap(0)

	// Initially empty
	assert.Equal(t, 0, m.Len())
	_, ok := m.Get(1.0)
	assert.False(t, ok)

	// Insert
	prevSize, wasUpdate := m.Put(1.0, []byte("one"))
	assert.Equal(t, 0, prevSize)
	assert.False(t, wasUpdate)
	assert.Equal(t, 1, m.Len())

	// Get
	val, ok := m.Get(1.0)
	assert.True(t, ok)
	assert.Equal(t, []byte("one"), val)

	// Update
	prevSize, wasUpdate = m.Put(1.0, []byte("ONE"))
	assert.Equal(t, 3, prevSize) // len("one")
	assert.True(t, wasUpdate)
	assert.Equal(t, 1, m.Len())

	val, ok = m.Get(1.0)
	assert.True(t, ok)
	assert.Equal(t, []byte("ONE"), val)

	// Delete
	val, ok = m.Delete(1.0)
	assert.True(t, ok)
	assert.Equal(t, []byte("ONE"), val)
	assert.Equal(t, 0, m.Len())

	// Get after delete
	_, ok = m.Get(1.0)
	assert.False(t, ok)

	// Delete non-existent
	_, ok = m.Delete(1.0)
	assert.False(t, ok)
}

func TestOAMap_LRUOrdering(t *testing.T) {
	m := newOAMap(0)

	// Insert in order: 1, 2, 3
	m.Put(1.0, []byte("one"))
	m.Put(2.0, []byte("two"))
	m.Put(3.0, []byte("three"))

	// LRU order should be: 3 (head/MRU) -> 2 -> 1 (tail/LRU)
	assert.Equal(t, 3.0, m.HeadKey())
	assert.Equal(t, 1.0, m.TailKey())
	assert.Equal(t, []float64{3.0, 2.0, 1.0}, m.Keys())

	// Access 1, should move to front
	val, ok := m.GetAndPromote(1.0)
	assert.True(t, ok)
	assert.Equal(t, []byte("one"), val)

	// LRU order should now be: 1 (head/MRU) -> 3 -> 2 (tail/LRU)
	assert.Equal(t, 1.0, m.HeadKey())
	assert.Equal(t, 2.0, m.TailKey())
	assert.Equal(t, []float64{1.0, 3.0, 2.0}, m.Keys())

	// Update 2, should move to front
	m.Put(2.0, []byte("TWO"))
	assert.Equal(t, 2.0, m.HeadKey())
	assert.Equal(t, 3.0, m.TailKey())
	assert.Equal(t, []float64{2.0, 1.0, 3.0}, m.Keys())
}

func TestOAMap_PopTail(t *testing.T) {
	m := newOAMap(0)

	// Pop from empty
	key, val, ok := m.PopTail()
	assert.False(t, ok)
	assert.Equal(t, 0.0, key)
	assert.Nil(t, val)

	// Insert items
	m.Put(1.0, []byte("one"))
	m.Put(2.0, []byte("two"))
	m.Put(3.0, []byte("three"))

	// Pop should return LRU (oldest = 1)
	key, val, ok = m.PopTail()
	assert.True(t, ok)
	assert.Equal(t, 1.0, key)
	assert.Equal(t, []byte("one"), val)
	assert.Equal(t, 2, m.Len())

	// Verify it's gone
	_, ok = m.Get(1.0)
	assert.False(t, ok)

	// Pop again
	key, val, ok = m.PopTail()
	assert.True(t, ok)
	assert.Equal(t, 2.0, key)
	assert.Equal(t, []byte("two"), val)

	// Pop last
	key, val, ok = m.PopTail()
	assert.True(t, ok)
	assert.Equal(t, 3.0, key)
	assert.Equal(t, []byte("three"), val)

	// Empty now
	assert.Equal(t, 0, m.Len())
	assert.Equal(t, nilIndex, m.head)
	assert.Equal(t, nilIndex, m.tail)
}

func TestOAMap_Resize(t *testing.T) {
	m := newOAMap(0)
	assert.Equal(t, initialCapacity, m.Cap()) // Minimum is initialCapacity (16)

	// Insert enough to trigger resize (16 * 0.7 = 11.2, so 12th insert triggers)
	for i := range 11 {
		m.Put(float64(i), []byte{byte(i)})
	}
	assert.Equal(t, 16, m.Cap())

	m.Put(11.0, []byte{11})
	assert.Equal(t, 32, m.Cap()) // Should have resized

	// Verify all data intact
	assert.Equal(t, 12, m.Len())
	for i := range 12 {
		val, ok := m.Get(float64(i))
		assert.True(t, ok, "key %d not found", i)
		assert.Equal(t, []byte{byte(i)}, val)
	}

	// LRU order preserved (most recent first)
	expectedKeys := make([]float64, 12)
	for i := range 12 {
		expectedKeys[i] = float64(11 - i)
	}
	assert.Equal(t, expectedKeys, m.Keys())
}

func TestOAMap_ManyItems(t *testing.T) {
	m := newOAMap(0)
	n := 10000

	// Insert many items
	for i := range n {
		m.Put(float64(i), []byte{byte(i % 256)})
	}

	assert.Equal(t, n, m.Len())

	// Verify all retrievable
	for i := range n {
		val, ok := m.Get(float64(i))
		require.True(t, ok, "key %d not found", i)
		assert.Equal(t, []byte{byte(i % 256)}, val)
	}

	// Verify LRU order (most recent = n-1 at head)
	keys := m.Keys()
	assert.Len(t, keys, n)
	for i, k := range keys {
		assert.Equal(t, float64(n-1-i), k)
	}

	// Delete half
	for i := range n / 2 {
		_, ok := m.Delete(float64(i))
		assert.True(t, ok)
	}
	assert.Equal(t, n/2, m.Len())

	// Verify remaining
	for i := n / 2; i < n; i++ {
		_, ok := m.Get(float64(i))
		assert.True(t, ok)
	}
}

func TestOAMap_FloatEdgeCases(t *testing.T) {
	m := newOAMap(0)

	testCases := []struct {
		name string
		key  float64
	}{
		{"zero", 0.0},
		{"negative_zero", math.Copysign(0, -1)},
		{"one", 1.0},
		{"negative_one", -1.0},
		{"pi", math.Pi},
		{"e", math.E},
		{"max_float", math.MaxFloat64},
		{"smallest_nonzero", math.SmallestNonzeroFloat64},
		{"negative_max", -math.MaxFloat64},
		{"large_negative", -1e100},
		{"small_positive", 1e-100},
	}

	// Insert all
	for _, tc := range testCases {
		m.Put(tc.key, []byte(tc.name))
	}

	// Verify all retrievable
	for _, tc := range testCases {
		val, ok := m.Get(tc.key)
		assert.True(t, ok, "key %s (%v) not found", tc.name, tc.key)
		assert.NotNil(t, val)
	}
}

func TestOAMap_NaNHandling(t *testing.T) {
	m := newOAMap(0)

	nan := math.NaN()

	// NaN != NaN in float comparison, but our hash uses bit representation
	// So NaN should work correctly
	m.Put(nan, []byte("nan"))

	val, ok := m.Get(nan)
	// This should work because we compare bit patterns
	assert.True(t, ok)
	assert.Equal(t, []byte("nan"), val)

	// Delete should also work
	val, ok = m.Delete(nan)
	assert.True(t, ok)
	assert.Equal(t, []byte("nan"), val)
}

func TestOAMap_CollisionHandling(t *testing.T) {
	// Force collisions by using a very small initial capacity
	m := newOAMap(4)

	// Insert more than capacity to test Robin Hood behavior
	for i := range 20 {
		m.Put(float64(i), []byte{byte(i)})
	}

	assert.Equal(t, 20, m.Len())

	// Verify all retrievable
	for i := range 20 {
		val, ok := m.Get(float64(i))
		require.True(t, ok, "key %d not found", i)
		assert.Equal(t, []byte{byte(i)}, val)
	}

	// Delete every other one
	for i := 0; i < 20; i += 2 {
		_, ok := m.Delete(float64(i))
		assert.True(t, ok)
	}

	// Verify remaining
	for i := range 20 {
		_, ok := m.Get(float64(i))
		if i%2 == 0 {
			assert.False(t, ok, "key %d should be deleted", i)
		} else {
			assert.True(t, ok, "key %d should exist", i)
		}
	}
}

func TestOAMap_DeletePreservesProbeChain(t *testing.T) {
	// This tests the backward shift deletion logic
	m := newOAMap(8)

	// Insert items that might share probe chains
	keys := []float64{1.0, 9.0, 17.0, 25.0} // These might collide depending on hash
	for _, k := range keys {
		m.Put(k, []byte{byte(k)})
	}

	// Delete the first one
	m.Delete(keys[0])

	// All others should still be findable
	for _, k := range keys[1:] {
		val, ok := m.Get(k)
		assert.True(t, ok, "key %v not found after deleting %v", k, keys[0])
		assert.Equal(t, []byte{byte(k)}, val)
	}
}

func TestOAMap_SingleElement(t *testing.T) {
	m := newOAMap(0)

	m.Put(42.0, []byte("answer"))

	assert.Equal(t, 1, m.Len())
	assert.Equal(t, 42.0, m.HeadKey())
	assert.Equal(t, 42.0, m.TailKey())
	assert.Equal(t, []float64{42.0}, m.Keys())

	// GetAndPromote on single element
	val, ok := m.GetAndPromote(42.0)
	assert.True(t, ok)
	assert.Equal(t, []byte("answer"), val)
	assert.Equal(t, 42.0, m.HeadKey())
	assert.Equal(t, 42.0, m.TailKey())

	// PopTail
	key, val, ok := m.PopTail()
	assert.True(t, ok)
	assert.Equal(t, 42.0, key)
	assert.Equal(t, []byte("answer"), val)
	assert.Equal(t, 0, m.Len())
	assert.Equal(t, nilIndex, m.head)
	assert.Equal(t, nilIndex, m.tail)
}

func TestOAMap_TwoElements(t *testing.T) {
	m := newOAMap(0)

	m.Put(1.0, []byte("one"))
	m.Put(2.0, []byte("two"))

	// Order: 2 (head) -> 1 (tail)
	assert.Equal(t, 2.0, m.HeadKey())
	assert.Equal(t, 1.0, m.TailKey())

	// Promote 1 to front
	m.GetAndPromote(1.0)
	assert.Equal(t, 1.0, m.HeadKey())
	assert.Equal(t, 2.0, m.TailKey())

	// Delete head
	m.Delete(1.0)
	assert.Equal(t, 1, m.Len())
	assert.Equal(t, 2.0, m.HeadKey())
	assert.Equal(t, 2.0, m.TailKey())

	// Delete last
	m.Delete(2.0)
	assert.Equal(t, 0, m.Len())
	assert.Equal(t, nilIndex, m.head)
	assert.Equal(t, nilIndex, m.tail)
}

func TestOAMap_DeleteHead(t *testing.T) {
	m := newOAMap(0)

	m.Put(1.0, []byte("one"))
	m.Put(2.0, []byte("two"))
	m.Put(3.0, []byte("three"))

	// Delete head (3)
	m.Delete(3.0)
	assert.Equal(t, 2.0, m.HeadKey())
	assert.Equal(t, 1.0, m.TailKey())
	assert.Equal(t, []float64{2.0, 1.0}, m.Keys())
}

func TestOAMap_DeleteMiddle(t *testing.T) {
	m := newOAMap(0)

	m.Put(1.0, []byte("one"))
	m.Put(2.0, []byte("two"))
	m.Put(3.0, []byte("three"))

	// Delete middle (2)
	m.Delete(2.0)
	assert.Equal(t, 3.0, m.HeadKey())
	assert.Equal(t, 1.0, m.TailKey())
	assert.Equal(t, []float64{3.0, 1.0}, m.Keys())
}

func TestOAMap_RandomOperations(t *testing.T) {
	m := newOAMap(0)
	reference := make(map[float64][]byte)
	rng := rand.New(rand.NewSource(42))

	operations := 10000

	for range operations {
		op := rng.Intn(4)
		key := float64(rng.Intn(100))
		value := []byte{byte(rng.Intn(256))}

		switch op {
		case 0: // Put
			m.Put(key, value)
			reference[key] = value
		case 1: // Get
			mVal, mOk := m.Get(key)
			rVal, rOk := reference[key]
			assert.Equal(t, rOk, mOk)
			if rOk {
				assert.Equal(t, rVal, mVal)
			}
		case 2: // Delete
			mVal, mOk := m.Delete(key)
			rVal, rOk := reference[key]
			delete(reference, key)
			assert.Equal(t, rOk, mOk)
			if rOk {
				assert.Equal(t, rVal, mVal)
			}
		case 3: // GetAndPromote
			mVal, mOk := m.GetAndPromote(key)
			rVal, rOk := reference[key]
			assert.Equal(t, rOk, mOk)
			if rOk {
				assert.Equal(t, rVal, mVal)
			}
		}

		assert.Equal(t, len(reference), m.Len())
	}
}

func TestNextPowerOf2(t *testing.T) {
	testCases := []struct {
		input    int
		expected int
	}{
		{0, 1},
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{7, 8},
		{8, 8},
		{9, 16},
		{15, 16},
		{16, 16},
		{17, 32},
		{100, 128},
		{1000, 1024},
	}

	for _, tc := range testCases {
		result := nextPowerOf2(tc.input)
		assert.Equal(t, tc.expected, result, "nextPowerOf2(%d)", tc.input)
	}
}

func TestHash_Consistency(t *testing.T) {
	// Same key should always produce same hash
	keys := []float64{0, 1, -1, math.Pi, math.E, math.MaxFloat64, math.SmallestNonzeroFloat64}

	for _, k := range keys {
		h1 := hash(k)
		h2 := hash(k)
		assert.Equal(t, h1, h2, "hash should be consistent for %v", k)
	}
}

func TestHash_Distribution(t *testing.T) {
	// Test that hashes are reasonably distributed
	buckets := make([]int, 256)
	n := 10000

	for i := range n {
		h := hash(float64(i))
		buckets[h%256]++
	}

	// Check that no bucket is grossly over/under-represented
	expected := n / 256
	for i, count := range buckets {
		ratio := float64(count) / float64(expected)
		assert.Greater(t, ratio, 0.3, "bucket %d has too few items", i)
		assert.Less(t, ratio, 3.0, "bucket %d has too many items", i)
	}
}

func BenchmarkOAMap_Put(b *testing.B) {
	m := newOAMap(b.N)
	value := make([]byte, 20)

	b.ResetTimer()
	for i := range b.N {
		m.Put(float64(i), value)
	}
}

func BenchmarkOAMap_Get(b *testing.B) {
	m := newOAMap(b.N)
	value := make([]byte, 20)

	for i := range b.N {
		m.Put(float64(i), value)
	}

	b.ResetTimer()
	for i := range b.N {
		m.Get(float64(i))
	}
}

func BenchmarkOAMap_GetAndPromote(b *testing.B) {
	m := newOAMap(b.N)
	value := make([]byte, 20)

	for i := range b.N {
		m.Put(float64(i), value)
	}

	b.ResetTimer()
	for i := range b.N {
		m.GetAndPromote(float64(i))
	}
}

func BenchmarkOAMap_Delete(b *testing.B) {
	value := make([]byte, 20)

	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		m := newOAMap(1000)
		for j := range 1000 {
			m.Put(float64(j), value)
		}
		b.StartTimer()

		for j := range 1000 {
			m.Delete(float64(j))
		}
	}
}

func BenchmarkGoMap_Put(b *testing.B) {
	m := make(map[float64][]byte, b.N)
	value := make([]byte, 20)

	b.ResetTimer()
	for i := range b.N {
		m[float64(i)] = value
	}
}

func BenchmarkGoMap_Get(b *testing.B) {
	m := make(map[float64][]byte, b.N)
	value := make([]byte, 20)

	for i := range b.N {
		m[float64(i)] = value
	}

	b.ResetTimer()
	for i := range b.N {
		_ = m[float64(i)]
	}
}

// lruEntryOld is the old LRU entry structure for comparison.
type lruEntryOld struct {
	key   float64
	value []byte
	prev  *lruEntryOld
	next  *lruEntryOld
}

func TestMemoryOverheadComparison(t *testing.T) {
	const numItems = 10000
	valueSize := 50 // Realistic encoded sketch size

	// Pre-create unique values for fair comparison
	// (both approaches will store separate copies)
	values := make([][]byte, numItems)
	for i := range numItems {
		values[i] = make([]byte, valueSize)
		// Fill with unique data
		for j := range valueSize {
			values[i][j] = byte((i + j) % 256)
		}
	}

	// Calculate theoretical overhead for each approach
	// Old: lruEntryOld (48 bytes) + map entry overhead (~40 bytes) = ~88 bytes per item
	// New: entry struct (32 bytes) + load factor overhead (30%) = ~46 bytes per item

	// Old approach: Go's map + pointer-based LRU list
	// Must COPY values to be fair (real caches can't rely on external memory)
	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	goMap := make(map[float64]*lruEntryOld)
	var head, tail *lruEntryOld
	for i := range numItems {
		// Copy value to simulate real cache behavior
		valueCopy := make([]byte, len(values[i]))
		copy(valueCopy, values[i])
		entry := &lruEntryOld{
			key:   float64(i),
			value: valueCopy,
			prev:  nil,
			next:  head,
		}
		if head != nil {
			head.prev = entry
		}
		head = entry
		if tail == nil {
			tail = entry
		}
		goMap[float64(i)] = entry
	}

	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)
	oldApproachBytes := m2.HeapAlloc - m1.HeapAlloc

	// Keep references alive until measurement complete
	runtime.KeepAlive(goMap)
	runtime.KeepAlive(head)
	runtime.KeepAlive(tail)

	// New approach: open-addressed map with intrusive list
	// Force GC to reclaim old approach memory
	runtime.GC()
	runtime.GC()

	var m3 runtime.MemStats
	runtime.ReadMemStats(&m3)

	oam := newOAMap(0)
	for i := range numItems {
		oam.Put(float64(i), values[i]) // Use unique value
	}

	runtime.GC()
	var m4 runtime.MemStats
	runtime.ReadMemStats(&m4)
	newApproachBytes := m4.HeapAlloc - m3.HeapAlloc

	runtime.KeepAlive(oam)

	// Calculate overhead per item
	oldOverheadPerItem := float64(oldApproachBytes) / float64(numItems)
	newOverheadPerItem := float64(newApproachBytes) / float64(numItems)

	// Calculate theoretical minimums
	// Old: lruEntryOld = 48 bytes (8 key + 24 slice header + 8 prev ptr + 8 next ptr) + map overhead
	// New: entry struct = 32 bytes (8 key + 8 page ptr + 4 index + 4 prev + 4 next + 1 state + padding)
	//      + page storage (value bytes + page overhead amortized)
	theoreticalOld := 48.0 + 40.0                        // entry + map overhead
	theoreticalNew := 32.0/0.7 + float64(valueSize)*1.05 // entry/load + value storage with ~5% page overhead

	t.Logf("=== Memory Overhead Comparison (%d items, %d byte values) ===", numItems, valueSize)
	t.Logf("")
	t.Logf("Old approach (Go map + pointer list):")
	t.Logf("  Total: %d bytes", oldApproachBytes)
	t.Logf("  Per item: %.1f bytes (theoretical min: %.0f bytes)", oldOverheadPerItem, theoreticalOld)
	t.Logf("")
	t.Logf("New approach (open-addressed + intrusive list):")
	t.Logf("  Total: %d bytes", newApproachBytes)
	t.Logf("  Per item: %.1f bytes (theoretical min: %.0f bytes)", newOverheadPerItem, theoreticalNew)
	t.Logf("")

	if newApproachBytes < oldApproachBytes {
		savings := (1.0 - float64(newApproachBytes)/float64(oldApproachBytes)) * 100
		t.Logf("Memory savings: %.1f%%", savings)
	} else {
		overhead := (float64(newApproachBytes)/float64(oldApproachBytes) - 1.0) * 100
		t.Logf("Memory overhead: %.1f%% (new approach uses more memory)", overhead)
	}

	// Verify the structures work correctly
	assert.Equal(t, numItems, oam.Len())
	for i := range numItems {
		v, ok := oam.Get(float64(i))
		assert.True(t, ok)
		assert.Equal(t, values[i], v)
	}
}

func TestOAMap_StressOperations(t *testing.T) {
	// Stress test with many mixed operations
	oam := newOAMap(0)
	value := make([]byte, 32)
	const iterations = 100000

	// Track what should be in the map
	expected := make(map[float64]bool)

	for i := range iterations {
		key := float64(i % 1000) // Reuse keys to test updates
		op := i % 4

		switch op {
		case 0, 1: // Insert/update (more common)
			oam.Put(key, value)
			expected[key] = true
		case 2: // Get
			_, found := oam.Get(key)
			if expected[key] {
				assert.True(t, found, "key %v should exist", key)
			}
		case 3: // Delete
			oam.Delete(key)
			delete(expected, key)
		}
	}

	// Verify final state
	assert.Equal(t, len(expected), oam.Len())
	for key := range expected {
		_, found := oam.Get(key)
		assert.True(t, found, "key %v should exist in final state", key)
	}

	// Verify LRU list integrity
	keys := oam.Keys()
	assert.Equal(t, len(expected), len(keys))
}

func TestOAMap_HighLoadFactor(t *testing.T) {
	// Test behavior near maximum load factor
	oam := newOAMap(16) // Start with capacity 16

	// Fill to just under load factor threshold
	for i := range 11 { // 11/16 = 0.6875 < 0.7
		oam.Put(float64(i), []byte{byte(i)})
	}
	assert.Equal(t, 16, oam.Cap(), "should not have resized yet")
	assert.Equal(t, 11, oam.Len())

	// One more should trigger resize
	oam.Put(float64(100), []byte{100})
	assert.Equal(t, 32, oam.Cap(), "should have doubled capacity")
	assert.Equal(t, 12, oam.Len())

	// Verify all entries still accessible
	for i := range 11 {
		v, ok := oam.Get(float64(i))
		assert.True(t, ok)
		assert.Equal(t, []byte{byte(i)}, v)
	}
	v, ok := oam.Get(float64(100))
	assert.True(t, ok)
	assert.Equal(t, []byte{100}, v)
}

func TestOAMap_TombstoneRebuild(t *testing.T) {
	// Test that tombstones trigger rebuild at same capacity
	oam := newOAMap(32)

	// Fill halfway
	for i := range 16 {
		oam.Put(float64(i), []byte{byte(i)})
	}

	// Delete most entries to create tombstones
	for i := range 12 {
		oam.Delete(float64(i))
	}
	assert.Equal(t, 4, oam.Len())
	// Now we have 12 tombstones + 4 occupied = 16 loaded slots

	// Add more entries - should trigger tombstone cleanup
	for i := 100; i < 110; i++ {
		oam.Put(float64(i), []byte{byte(i)})
	}

	// Verify structure integrity
	assert.Equal(t, 14, oam.Len())
	for i := 12; i < 16; i++ {
		_, ok := oam.Get(float64(i))
		assert.True(t, ok, "original key %d should exist", i)
	}
	for i := 100; i < 110; i++ {
		v, ok := oam.Get(float64(i))
		assert.True(t, ok, "new key %d should exist", i)
		assert.Equal(t, []byte{byte(i)}, v)
	}
}
