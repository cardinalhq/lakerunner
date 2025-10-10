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
	"container/list"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/cardinalhq/lakerunner/pipeline"
)

type LRUCache struct {
	sync.RWMutex
	capacity           int
	cache              map[uint64]*list.Element
	list               *list.List
	expiry             time.Duration
	reportInterval     time.Duration
	stopCleanup        chan struct{}
	publishCallBack    func(toPublish []*Entry)
	pending            []*Entry
	maxPublishPerSweep int
	rng                *rand.Rand
}

type Entry struct {
	key             uint64
	value           pipeline.Row
	timestamp       time.Time
	lastPublishTime time.Time
}

func NewLRUCache(
	capacity int,
	expiry time.Duration,
	reportInterval time.Duration,
	maxPublishPerSweep int,
	publishCallBack func(expiredItems []*Entry),
) *LRUCache {
	// 0 means unlimited
	if maxPublishPerSweep == 0 {
		maxPublishPerSweep = math.MaxInt32
	}
	lru := &LRUCache{
		capacity:           capacity,
		cache:              make(map[uint64]*list.Element),
		list:               list.New(),
		reportInterval:     reportInterval,
		expiry:             expiry,
		stopCleanup:        make(chan struct{}),
		pending:            make([]*Entry, 0),
		publishCallBack:    publishCallBack,
		maxPublishPerSweep: maxPublishPerSweep,
		rng:                rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	go lru.startCleanup()
	return lru
}

func (l *LRUCache) startCleanup() {
	ticker := time.NewTicker(l.reportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			l.cleanupExpiredEntries()
		case <-l.stopCleanup:
			return
		}
	}
}

func (l *LRUCache) cleanupExpiredEntries() {
	l.Lock()
	now := time.Now()

	// Mark entries due for publish this sweep.
	for e := l.list.Back(); e != nil; e = e.Prev() {
		entry := e.Value.(*Entry)
		if entry.shouldPublish(l.expiry) {
			l.pending = append(l.pending, entry)
			entry.lastPublishTime = now
		}
	}

	// Evict truly expired entries (TTL-based) from the LRU tail.
	for e := l.list.Back(); e != nil; {
		entry := e.Value.(*Entry)
		if now.Sub(entry.timestamp) > l.expiry {
			prev := e.Prev()
			l.list.Remove(e)
			delete(l.cache, entry.key)
			pipeline.ReturnPooledRow(entry.value)
			e = prev
		} else {
			e = e.Prev()
		}
	}

	// Take a copy of pending for sampling, then reset the shared slice.
	candidates := l.pending
	l.pending = make([]*Entry, 0, 16)
	l.Unlock()

	// Randomly pick up to N exemplars to publish.
	if n := len(candidates); n > 0 {
		toPublish := reservoirSample(candidates, l.maxPublishPerSweep, l.rng)
		slog.Debug("Publishing exemplar batch", "candidates", n, "published", len(toPublish))
		l.publishCallBack(toPublish)
	}
}

// reservoirSample picks up to k items uniformly at random without replacement.
func reservoirSample(in []*Entry, k int, r *rand.Rand) []*Entry {
	n := len(in)
	if k <= 0 || n == 0 {
		return nil
	}
	if n <= k {
		// Return a shallow copy to avoid aliasing callers' slice capacity.
		out := make([]*Entry, n)
		copy(out, in)
		return out
	}
	out := make([]*Entry, k)
	copy(out, in[:k])
	for i := k; i < n; i++ {
		j := r.Intn(i + 1)
		if j < k {
			out[j] = in[i]
		}
	}
	return out
}

// shouldPublish checks if an entry should be published based on expiry
func (e *Entry) shouldPublish(expiry time.Duration) bool {
	return time.Since(e.lastPublishTime) >= expiry
}

// Contains checks if a key exists in the cache and is not expired
func (l *LRUCache) Contains(key uint64) bool {
	l.RLock()
	defer l.RUnlock()

	elem, found := l.cache[key]
	if !found {
		return false
	}

	entry := elem.Value.(*Entry)
	return time.Since(entry.timestamp) <= l.expiry
}

// PutIfAbsent adds a new entry only if the key doesn't exist.
// Returns true if the entry was added, false if it already existed.
func (l *LRUCache) PutIfAbsent(key uint64, row pipeline.Row) bool {
	l.Lock()
	defer l.Unlock()

	// Check if key already exists
	if elem, found := l.cache[key]; found {
		entry := elem.Value.(*Entry)
		// If expired, we'll replace it below
		if time.Since(entry.timestamp) <= l.expiry {
			// Still valid, don't replace
			return false
		}
	}

	now := time.Now()

	if l.list.Len() >= l.capacity {
		back := l.list.Back()
		if back != nil {
			entry := back.Value.(*Entry)
			if entry.shouldPublish(l.expiry) {
				l.pending = append(l.pending, entry)
				entry.lastPublishTime = now
			}
			l.list.Remove(back)
			delete(l.cache, entry.key)
			pipeline.ReturnPooledRow(entry.value)
		}
	}

	newEntry := &Entry{
		key:             key,
		value:           row,
		timestamp:       now,
		lastPublishTime: now,
	}
	elem := l.list.PushFront(newEntry)
	l.cache[key] = elem
	l.pending = append(l.pending, newEntry)
	return true
}

func (l *LRUCache) Put(key uint64, row pipeline.Row) {
	l.Lock()
	defer l.Unlock()

	now := time.Now()

	if elem, found := l.cache[key]; found {
		entry := elem.Value.(*Entry)
		entry.value = row
		entry.timestamp = now
		l.list.MoveToFront(elem)
		return
	}

	if l.list.Len() >= l.capacity {
		back := l.list.Back()
		if back != nil {
			entry := back.Value.(*Entry)
			if entry.shouldPublish(l.expiry) {
				l.pending = append(l.pending, entry)
				entry.lastPublishTime = now
			}
			l.list.Remove(back)
			delete(l.cache, entry.key)
			pipeline.ReturnPooledRow(entry.value)
		}
	}

	newEntry := &Entry{
		key:             key,
		value:           row,
		timestamp:       now,
		lastPublishTime: now,
	}
	elem := l.list.PushFront(newEntry)
	l.cache[key] = elem
	l.pending = append(l.pending, newEntry)
}

func (l *LRUCache) Close() {
	close(l.stopCleanup)
}

// FlushPending forces all pending exemplars to be published via the callback
func (l *LRUCache) FlushPending() {
	l.Lock()
	candidates := l.pending
	l.pending = make([]*Entry, 0, 16)
	l.Unlock()

	if len(candidates) > 0 {
		toPublish := reservoirSample(candidates, l.maxPublishPerSweep, l.rng)
		l.publishCallBack(toPublish)
	}
}
