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
	"context"
	"log"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/helpers"
)

// DefaultMaxSizeBytes is the default maximum size for the DDCache.
const DefaultMaxSizeBytes = int64(10 * 1024 * 1024) // 10MB

var (
	globalCache     *Cache
	globalCacheOnce sync.Once

	// OTel metrics
	cacheHits      metric.Int64Counter
	cacheMisses    metric.Int64Counter
	cacheCalls     metric.Int64Counter
	cacheEvictions metric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/internal/ddcache")

	var err error

	cacheHits, err = meter.Int64Counter(
		"lakerunner.ddcache.hits",
		metric.WithDescription("Number of DDCache hits"),
	)
	if err != nil {
		log.Fatalf("failed to create ddcache.hits counter: %v", err)
	}

	cacheMisses, err = meter.Int64Counter(
		"lakerunner.ddcache.misses",
		metric.WithDescription("Number of DDCache misses"),
	)
	if err != nil {
		log.Fatalf("failed to create ddcache.misses counter: %v", err)
	}

	cacheCalls, err = meter.Int64Counter(
		"lakerunner.ddcache.calls",
		metric.WithDescription("Total number of DDCache GetBytesForValue calls"),
	)
	if err != nil {
		log.Fatalf("failed to create ddcache.calls counter: %v", err)
	}

	cacheEvictions, err = meter.Int64Counter(
		"lakerunner.ddcache.evictions",
		metric.WithDescription("Number of DDCache evictions"),
	)
	if err != nil {
		log.Fatalf("failed to create ddcache.evictions counter: %v", err)
	}
}

// Cache provides efficient encoding of single-value DDSketches with LRU caching.
// Uses an open-addressed hash map with intrusive linked list for minimal memory overhead.
// Thread-safe for concurrent access.
type Cache struct {
	mu           sync.RWMutex
	maxSizeBytes int64
	currentSize  int64

	// Pre-computed constant values (outside LRU, never evicted)
	zeroBytes []byte
	oneBytes  []byte
	constOnce sync.Once

	// LRU cache using open-addressed hash map with intrusive list
	cache *oamap
}

// Get returns the global Cache singleton.
// Must call Init first to configure the cache size.
func Get() *Cache {
	globalCacheOnce.Do(func() {
		globalCache = newCache(DefaultMaxSizeBytes)
	})
	return globalCache
}

// Init initializes the global Cache with the specified max size.
// Should be called once at startup before any Get calls.
// Safe to call multiple times; only the first call takes effect.
func Init(maxSizeBytes int64) {
	globalCacheOnce.Do(func() {
		globalCache = newCache(maxSizeBytes)
	})
}

// newCache creates a new Cache with the specified max size in bytes.
func newCache(maxSizeBytes int64) *Cache {
	c := &Cache{
		maxSizeBytes: maxSizeBytes,
		cache:        newOAMap(0),
	}

	// Register observable gauges that read from the cache
	meter := otel.Meter("github.com/cardinalhq/lakerunner/internal/ddcache")

	_, err := meter.Int64ObservableGauge(
		"lakerunner.ddcache.size_bytes",
		metric.WithDescription("Current DDCache size in bytes"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(c.BytesSize())
			return nil
		}),
	)
	if err != nil {
		log.Fatalf("failed to create ddcache.size_bytes gauge: %v", err)
	}

	_, err = meter.Int64ObservableGauge(
		"lakerunner.ddcache.items",
		metric.WithDescription("Current number of items in DDCache"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(c.ItemCount())
			return nil
		}),
	)
	if err != nil {
		log.Fatalf("failed to create ddcache.items gauge: %v", err)
	}

	return c
}

// initConstants initializes the pre-computed constant values.
func (c *Cache) initConstants() error {
	var initErr error
	c.constOnce.Do(func() {
		// Pre-compute zero
		sketch, err := helpers.GetSketch()
		if err != nil {
			initErr = err
			return
		}
		if err := sketch.Add(0); err != nil {
			helpers.PutSketch(sketch)
			initErr = err
			return
		}
		c.zeroBytes = helpers.EncodeSketch(sketch)
		helpers.PutSketch(sketch)

		// Pre-compute one
		sketch, err = helpers.GetSketch()
		if err != nil {
			initErr = err
			return
		}
		if err := sketch.Add(1); err != nil {
			helpers.PutSketch(sketch)
			initErr = err
			return
		}
		c.oneBytes = helpers.EncodeSketch(sketch)
		helpers.PutSketch(sketch)
	})
	return initErr
}

// GetBytesForValue returns the encoded DDSketch bytes for a single value.
// For constants (0, 1), returns pre-computed bytes.
// For other values, uses LRU cache with size-based eviction.
func (c *Cache) GetBytesForValue(value float64) ([]byte, error) {
	ctx := context.Background()
	cacheCalls.Add(ctx, 1)

	// Initialize constants on first access
	if err := c.initConstants(); err != nil {
		return nil, err
	}

	// Fast path: constant values
	if value == 0 {
		cacheHits.Add(ctx, 1)
		return c.zeroBytes, nil
	}
	if value == 1 {
		cacheHits.Add(ctx, 1)
		return c.oneBytes, nil
	}

	// Check cache with read lock
	c.mu.RLock()
	if bytes, ok := c.cache.Get(value); ok {
		c.mu.RUnlock()
		// Promote to front (needs write lock)
		c.mu.Lock()
		c.cache.GetAndPromote(value)
		c.mu.Unlock()
		cacheHits.Add(ctx, 1)
		return bytes, nil
	}
	c.mu.RUnlock()

	// Cache miss: create new sketch bytes
	cacheMisses.Add(ctx, 1)

	sketch, err := helpers.GetSketch()
	if err != nil {
		return nil, err
	}
	if err := sketch.Add(value); err != nil {
		helpers.PutSketch(sketch)
		return nil, err
	}
	bytes := helpers.EncodeAndReturnSketch(sketch)

	// Add to cache with write lock
	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check in case another goroutine added it
	if existing, ok := c.cache.Get(value); ok {
		c.cache.GetAndPromote(value)
		return existing, nil
	}

	// Evict if necessary to make room
	entrySize := int64(len(bytes))
	for c.currentSize+entrySize > c.maxSizeBytes && c.cache.Len() > 0 {
		c.evictLRU(ctx)
	}

	// Add new entry
	c.cache.Put(value, bytes)
	c.currentSize += entrySize

	// Return the cached value (from page) to ensure consistent pointers
	cached, _ := c.cache.Get(value)
	return cached, nil
}

// evictLRU removes the least recently used entry.
// Caller must hold write lock.
func (c *Cache) evictLRU(ctx context.Context) {
	_, value, ok := c.cache.PopTail()
	if !ok {
		return
	}
	c.currentSize -= int64(len(value))
	cacheEvictions.Add(ctx, 1)
}

// BytesSize returns the current memory used by cached bytes (excluding constants).
func (c *Cache) BytesSize() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	// Include constants in reported size
	constSize := int64(len(c.zeroBytes) + len(c.oneBytes))
	return c.currentSize + constSize
}

// ItemCount returns the number of items in the LRU cache (excluding constants).
func (c *Cache) ItemCount() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return int64(c.cache.Len())
}

// MaxSizeBytes returns the configured maximum cache size.
func (c *Cache) MaxSizeBytes() int64 {
	return c.maxSizeBytes
}
