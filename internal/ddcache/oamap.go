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
)

const (
	// nilIndex represents no entry (sentinel value for prev/next).
	nilIndex = int32(-1)

	// maxLoadFactor is the threshold for resizing the hash table.
	// Includes both occupied and tombstoned slots.
	maxLoadFactor = 0.7

	// tombstoneRatio is the threshold for rebuilding to clear tombstones.
	// When tombstones exceed this fraction of capacity, we rebuild.
	tombstoneRatio = 0.25

	// initialCapacity is the starting size of the hash table.
	initialCapacity = 16
)

// slotState represents the state of a hash table slot.
type slotState uint8

const (
	slotEmpty     slotState = iota // Slot is empty
	slotOccupied                   // Slot contains a valid entry
	slotTombstone                  // Slot was deleted (for probe chain continuity)
)

// entry represents a slot in the open-addressed hash table.
// It also contains intrusive linked list pointers for LRU ordering.
// Uses page-based allocation for values to reduce per-entry overhead.
type entry struct {
	key   float64   // The float64 key (8 bytes)
	page  *page     // Page containing the value (8 bytes)
	index int32     // Index within the page (4 bytes)
	prev  int32     // LRU list: previous entry index (4 bytes)
	next  int32     // LRU list: next entry index (4 bytes)
	state slotState // Slot state: empty, occupied, or tombstone (1 byte)
	// Total: 29 bytes + padding = 32 bytes (vs 48 bytes with []byte)
}

// oamap is an open-addressed hash map with an intrusive LRU linked list.
// Uses linear probing with tombstones for deletion.
// Values are stored in pages to reduce per-entry memory overhead.
type oamap struct {
	entries    []entry // The hash table slots
	size       int     // Number of occupied entries (not counting tombstones)
	tombstones int     // Number of tombstoned slots
	capacity   int     // Length of entries array

	// LRU list head/tail (indices into entries)
	head int32 // Most recently used (front)
	tail int32 // Least recently used (back)

	// Page-based value storage
	pages *pageManager
}

// newOAMap creates a new open-addressed map with the specified initial capacity.
func newOAMap(capacity int) *oamap {
	if capacity < initialCapacity {
		capacity = initialCapacity
	}
	capacity = nextPowerOf2(capacity)

	return &oamap{
		entries:  make([]entry, capacity),
		capacity: capacity,
		head:     nilIndex,
		tail:     nilIndex,
		pages:    newPageManager(),
	}
}

// nextPowerOf2 returns the smallest power of 2 >= n.
func nextPowerOf2(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	return n + 1
}

// hash returns a hash of the float64 key.
func hash(key float64) uint64 {
	bits := math.Float64bits(key)
	// Mixing function (splitmix64)
	bits ^= bits >> 33
	bits *= 0xff51afd7ed558ccd
	bits ^= bits >> 33
	bits *= 0xc4ceb9fe1a85ec53
	bits ^= bits >> 33
	return bits
}

// index returns the table index for a hash value.
func (m *oamap) index(h uint64) int {
	return int(h & uint64(m.capacity-1))
}

// Get looks up a key and returns its value and whether it was found.
func (m *oamap) Get(key float64) ([]byte, bool) {
	idx := m.find(key)
	if idx == nilIndex {
		return nil, false
	}
	e := &m.entries[idx]
	return e.page.get(int(e.index)), true
}

// GetAndPromote looks up a key, returns its value, and promotes it to
// the front of the LRU list if found.
func (m *oamap) GetAndPromote(key float64) ([]byte, bool) {
	idx := m.find(key)
	if idx == nilIndex {
		return nil, false
	}
	m.moveToFront(idx)
	e := &m.entries[idx]
	return e.page.get(int(e.index)), true
}

// keysEqual compares two float64 keys, handling NaN correctly.
// NaN == NaN returns false normally, but we want NaN keys to match.
func keysEqual(a, b float64) bool {
	return math.Float64bits(a) == math.Float64bits(b)
}

// find returns the index of the entry with the given key, or nilIndex if not found.
func (m *oamap) find(key float64) int32 {
	if m.size == 0 {
		return nilIndex
	}

	h := hash(key)
	idx := m.index(h)

	for {
		e := &m.entries[idx]
		switch e.state {
		case slotEmpty:
			return nilIndex
		case slotOccupied:
			if keysEqual(e.key, key) {
				return int32(idx)
			}
		case slotTombstone:
			// Continue probing
		}
		idx = (idx + 1) & (m.capacity - 1)
	}
}

// Put inserts or updates a key-value pair.
// Returns the previous value size (0 if new entry) and whether it was an update.
func (m *oamap) Put(key float64, value []byte) (prevSize int, wasUpdate bool) {
	// Check if we need to resize or rebuild
	loadedSlots := m.size + m.tombstones
	if float64(loadedSlots+1)/float64(m.capacity) > maxLoadFactor {
		if float64(m.tombstones)/float64(m.capacity) > tombstoneRatio {
			// Too many tombstones - rebuild at same capacity
			m.rebuild(m.capacity)
		} else {
			// Genuinely full - double capacity
			m.rebuild(m.capacity * 2)
		}
	}

	h := hash(key)
	idx := m.index(h)
	tombstoneIdx := -1 // First tombstone we encounter (for reuse)

	for {
		e := &m.entries[idx]
		switch e.state {
		case slotEmpty:
			// Use tombstone slot if we found one, otherwise use this empty slot
			insertIdx := idx
			if tombstoneIdx != -1 {
				insertIdx = tombstoneIdx
				m.tombstones--
			}
			// Allocate value in page
			p, pageIdx := m.pages.allocate(value)
			m.entries[insertIdx] = entry{
				key:   key,
				page:  p,
				index: int32(pageIdx),
				prev:  nilIndex,
				next:  nilIndex,
				state: slotOccupied,
			}
			m.size++
			m.addToFront(int32(insertIdx))
			return 0, false

		case slotOccupied:
			if keysEqual(e.key, key) {
				// Get previous value size from page
				prevSize = e.page.stride
				// Allocate new value (old value stays in page - this is a leak for now)
				// TODO: implement page draining to reclaim space
				p, pageIdx := m.pages.allocate(value)
				e.page = p
				e.index = int32(pageIdx)
				m.moveToFront(int32(idx))
				return prevSize, true
			}

		case slotTombstone:
			if tombstoneIdx == -1 {
				tombstoneIdx = idx
			}
		}
		idx = (idx + 1) & (m.capacity - 1)
	}
}

// Delete removes a key from the map.
// Returns the value and true if the key existed.
func (m *oamap) Delete(key float64) ([]byte, bool) {
	idx := m.find(key)
	if idx == nilIndex {
		return nil, false
	}

	e := &m.entries[idx]
	value := e.page.get(int(e.index))
	m.removeFromList(idx)

	// Mark as tombstone (can't just empty, would break probe chains)
	m.entries[idx] = entry{state: slotTombstone}
	m.size--
	m.tombstones++

	return value, true
}

// PopTail removes and returns the least recently used entry.
func (m *oamap) PopTail() (float64, []byte, bool) {
	if m.tail == nilIndex {
		return 0, nil, false
	}

	idx := m.tail
	e := &m.entries[idx]
	key := e.key
	value := e.page.get(int(e.index))

	m.removeFromList(idx)
	m.entries[idx] = entry{state: slotTombstone}
	m.size--
	m.tombstones++

	return key, value, true
}

// Len returns the number of entries in the map.
func (m *oamap) Len() int {
	return m.size
}

// Cap returns the current capacity of the hash table.
func (m *oamap) Cap() int {
	return m.capacity
}

// moveToFront moves an entry to the front of the LRU list.
func (m *oamap) moveToFront(idx int32) {
	if idx == m.head {
		return
	}
	m.removeFromList(idx)
	m.addToFront(idx)
}

// addToFront adds an entry to the front of the LRU list.
func (m *oamap) addToFront(idx int32) {
	e := &m.entries[idx]
	e.prev = nilIndex
	e.next = m.head

	if m.head != nilIndex {
		m.entries[m.head].prev = idx
	}
	m.head = idx

	if m.tail == nilIndex {
		m.tail = idx
	}
}

// removeFromList removes an entry from the LRU list.
func (m *oamap) removeFromList(idx int32) {
	e := &m.entries[idx]

	if e.prev != nilIndex {
		m.entries[e.prev].next = e.next
	} else {
		m.head = e.next
	}

	if e.next != nilIndex {
		m.entries[e.next].prev = e.prev
	} else {
		m.tail = e.prev
	}

	e.prev = nilIndex
	e.next = nilIndex
}

// rebuild rebuilds the hash table with a new capacity, eliminating tombstones.
func (m *oamap) rebuild(newCapacity int) {
	// Collect all entries in LRU order (tail to head)
	// so we can re-insert them and preserve order
	type kv struct {
		key   float64
		value []byte
	}
	entries := make([]kv, 0, m.size)

	idx := m.tail
	for idx != nilIndex {
		e := &m.entries[idx]
		// Copy value from page since we'll create new pages
		value := e.page.get(int(e.index))
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)
		entries = append(entries, kv{e.key, valueCopy})
		idx = e.prev
	}

	// Reset the map with new pages
	m.entries = make([]entry, newCapacity)
	m.capacity = newCapacity
	m.size = 0
	m.tombstones = 0
	m.head = nilIndex
	m.tail = nilIndex
	m.pages = newPageManager() // Create fresh pages

	// Re-insert in reverse LRU order (so first inserted becomes tail)
	for _, kv := range entries {
		m.Put(kv.key, kv.value)
	}
}

// TailKey returns the key of the least recently used entry.
func (m *oamap) TailKey() float64 {
	if m.tail == nilIndex {
		return 0
	}
	return m.entries[m.tail].key
}

// HeadKey returns the key of the most recently used entry.
func (m *oamap) HeadKey() float64 {
	if m.head == nilIndex {
		return 0
	}
	return m.entries[m.head].key
}

// Keys returns all keys in LRU order (most recent first).
func (m *oamap) Keys() []float64 {
	keys := make([]float64, 0, m.size)
	idx := m.head
	for idx != nilIndex {
		keys = append(keys, m.entries[idx].key)
		idx = m.entries[idx].next
	}
	return keys
}
