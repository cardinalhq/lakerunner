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
	"sync"
	"time"
)

type LRUCache[T any] struct {
	capacity        int
	cache           map[int64]*list.Element
	list            *list.List
	mutex           sync.RWMutex
	expiry          time.Duration
	reportInterval  time.Duration
	stopCleanup     chan struct{}
	publishCallBack func(toPublish []*Entry[T])
	pending         []*Entry[T]
}

type Entry[T any] struct {
	key             int64
	attributes      []string
	value           T
	timestamp       time.Time
	lastPublishTime time.Time
}

func NewLRUCache[T any](capacity int, expiry time.Duration, reportInterval time.Duration, publishCallBack func(expiredItems []*Entry[T])) *LRUCache[T] {
	lru := &LRUCache[T]{
		capacity:        capacity,
		cache:           make(map[int64]*list.Element),
		list:            list.New(),
		reportInterval:  reportInterval,
		expiry:          expiry,
		stopCleanup:     make(chan struct{}),
		pending:         make([]*Entry[T], 0),
		publishCallBack: publishCallBack,
	}
	go lru.startCleanup()
	return lru
}

func (l *LRUCache[T]) startCleanup() {
	for {
		select {
		case <-time.NewTicker(l.reportInterval).C:
			l.cleanupExpiredEntries()
		case <-l.stopCleanup:
			return
		}
	}
}

func (l *LRUCache[T]) cleanupExpiredEntries() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	now := time.Now()

	for e := l.list.Back(); e != nil; {
		entry := e.Value.(*Entry[T])
		if entry.shouldPublish(l.expiry) {
			l.pending = append(l.pending, entry)
			entry.lastPublishTime = now
		}
		if now.Sub(entry.timestamp) > l.expiry {
			prev := e.Prev()
			l.list.Remove(e)
			delete(l.cache, entry.key)
			e = prev
		} else {
			e = e.Prev()
		}
	}

	// Add pending items to the list for publishing
	if len(l.pending) > 0 {
		l.publishCallBack(l.pending)
		l.pending = l.pending[:0] // Clear the pending slice
	}
}

// shouldPublish checks if an entry should be published based on expiry
func (e *Entry[T]) shouldPublish(expiry time.Duration) bool {
	now := time.Now()
	sinceLast := now.Sub(e.lastPublishTime)
	return sinceLast >= expiry
}

// Contains checks if a key exists in the cache and is not expired
func (l *LRUCache[T]) Contains(key int64) bool {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	elem, found := l.cache[key]
	if !found {
		return false
	}

	entry := elem.Value.(*Entry[T])
	return time.Since(entry.timestamp) <= l.expiry
}

func (l *LRUCache[T]) Put(key int64, keys []string, exemplar T) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	now := time.Now()

	if elem, found := l.cache[key]; found {
		entry := elem.Value.(*Entry[T])
		entry.value = exemplar
		entry.timestamp = now
		l.list.MoveToFront(elem)
		return
	}

	if l.list.Len() >= l.capacity {
		back := l.list.Back()
		if back != nil {
			entry := back.Value.(*Entry[T])
			// Only publish if it should be published
			if entry.shouldPublish(l.expiry) {
				l.pending = append(l.pending, entry)
				entry.lastPublishTime = now
			}
			l.list.Remove(back)
			delete(l.cache, entry.key)
		}
	}

	newEntry := &Entry[T]{
		key:             key,
		attributes:      keys,
		value:           exemplar,
		timestamp:       now,
		lastPublishTime: now,
	}
	elem := l.list.PushFront(newEntry)
	l.pending = append(l.pending, newEntry)
	l.cache[key] = elem
}

func (l *LRUCache[T]) Close() {
	close(l.stopCleanup)
}

// FlushPending forces all pending exemplars to be published via the callback
func (l *LRUCache[T]) FlushPending() {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// Get all entries from the cache and mark them for publishing
	entries := make([]*Entry[T], 0, l.list.Len())
	for e := l.list.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*Entry[T])
		entries = append(entries, entry)
	}

	if len(entries) > 0 {
		// Call the callback with all entries
		l.publishCallBack(entries)

		// Clear the cache
		l.list.Init()
		l.cache = make(map[int64]*list.Element)
		l.pending = l.pending[:0]
	}
}
