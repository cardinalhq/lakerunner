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

package lrdb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// EstimateKey represents a cache key for metric pack estimates
type EstimateKey struct {
	OrganizationID uuid.UUID
	FrequencyMs    int32
}

// EstimateValue represents a cached estimate value
type EstimateValue struct {
	TargetRecords int64
	UpdatedAt     time.Time
}

// LRUNode represents a node in the LRU cache
type LRUNode struct {
	Key   EstimateKey
	Value EstimateValue
	Prev  *LRUNode
	Next  *LRUNode
}

// MetricPackEstimator provides cached access to metric pack estimates
type MetricPackEstimator struct {
	db       EstimatorStore
	mutex    sync.RWMutex
	cache    map[EstimateKey]*LRUNode
	head     *LRUNode
	tail     *LRUNode
	capacity int
	size     int
}

// MetricEstimator provides access to metric pack estimates
type MetricEstimator interface {
	Get(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64
}

// EstimatorStore defines the interface for database operations
type EstimatorStore interface {
	GetAllMetricPackEstimates(ctx context.Context) ([]MetricPackEstimate, error)
}

// NewMetricPackEstimator creates a new estimator with LRU cache
func NewMetricPackEstimator(db EstimatorStore, capacity int) *MetricPackEstimator {
	if capacity <= 0 {
		capacity = 1000 // Default cache size
	}

	// Create dummy head and tail nodes
	head := &LRUNode{}
	tail := &LRUNode{}
	head.Next = tail
	tail.Prev = head

	return &MetricPackEstimator{
		db:       db,
		cache:    make(map[EstimateKey]*LRUNode),
		head:     head,
		tail:     tail,
		capacity: capacity,
		size:     0,
	}
}

// Get retrieves the target records estimate for the given org and frequency
// Falls back to defaults using UUID zero if no org-specific estimate exists
// This method implements the MetricEstimator interface and handles errors internally
func (e *MetricPackEstimator) Get(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64 {
	estimate, err := e.getEstimate(ctx, orgID, frequencyMs)
	if err != nil {
		// Log error and return a reasonable default
		// In production, this might use a logger
		return e.getHardcodedDefault()
	}
	return estimate
}

// getEstimate is the internal method that can return errors
func (e *MetricPackEstimator) getEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) (int64, error) {
	key := EstimateKey{OrganizationID: orgID, FrequencyMs: frequencyMs}

	// Try cache first
	e.mutex.RLock()
	if node, exists := e.cache[key]; exists {
		e.mutex.RUnlock()

		// Move to front (most recently used)
		e.mutex.Lock()
		e.moveToFront(node)
		e.mutex.Unlock()

		return node.Value.TargetRecords, nil
	}
	e.mutex.RUnlock()

	// Not in cache, try to load from database
	estimate, err := e.loadEstimate(ctx, orgID, frequencyMs)
	if err != nil {
		return 0, err
	}

	// Cache the result
	e.mutex.Lock()
	e.put(key, estimate)
	e.mutex.Unlock()

	return estimate.TargetRecords, nil
}

// getHardcodedDefault returns a hardcoded default for a frequency
func (e *MetricPackEstimator) getHardcodedDefault() int64 {
	return 40000 // Ultimate fallback: 40K records
}

// loadEstimate loads estimate from database with fallback logic
// Optimized to cache all estimates for an organization when first accessed
func (e *MetricPackEstimator) loadEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) (EstimateValue, error) {
	// Load all estimates from database once
	estimates, err := e.db.GetAllMetricPackEstimates(ctx)
	if err != nil {
		return EstimateValue{}, fmt.Errorf("failed to load metric pack estimates: %w", err)
	}

	// Cache all estimates for this organization and defaults at once to avoid repeated DB calls
	zeroUUID := uuid.UUID{}
	orgEstimates := make(map[int32]EstimateValue)
	defaultEstimates := make(map[int32]EstimateValue)

	for _, est := range estimates {
		if est.TargetRecords != nil {
			value := EstimateValue{
				TargetRecords: *est.TargetRecords,
				UpdatedAt:     est.UpdatedAt,
			}

			if est.OrganizationID == orgID {
				orgEstimates[est.FrequencyMs] = value
				// Cache this estimate
				key := EstimateKey{OrganizationID: orgID, FrequencyMs: est.FrequencyMs}
				e.put(key, value)
			} else if est.OrganizationID == zeroUUID {
				defaultEstimates[est.FrequencyMs] = value
				// Cache this default estimate
				key := EstimateKey{OrganizationID: zeroUUID, FrequencyMs: est.FrequencyMs}
				e.put(key, value)
			}
		}
	}

	// Look for exact org + frequency match
	if estimate, exists := orgEstimates[frequencyMs]; exists {
		return estimate, nil
	}

	// Look for UUID zero (default) + frequency match
	if estimate, exists := defaultEstimates[frequencyMs]; exists {
		return estimate, nil
	}

	// Ultimate fallback
	return EstimateValue{
		TargetRecords: 40000, // 40K records
		UpdatedAt:     time.Now(),
	}, nil
}

// put adds or updates a key-value pair in the LRU cache
func (e *MetricPackEstimator) put(key EstimateKey, value EstimateValue) {
	if node, exists := e.cache[key]; exists {
		// Update existing node
		node.Value = value
		e.moveToFront(node)
		return
	}

	// Add new node
	newNode := &LRUNode{
		Key:   key,
		Value: value,
	}

	// Add to front
	e.addToFront(newNode)
	e.cache[key] = newNode
	e.size++

	// Check capacity
	if e.size > e.capacity {
		// Remove least recently used (tail)
		lru := e.tail.Prev
		e.removeNode(lru)
		delete(e.cache, lru.Key)
		e.size--
	}
}

// moveToFront moves a node to the front of the LRU list
func (e *MetricPackEstimator) moveToFront(node *LRUNode) {
	e.removeNode(node)
	e.addToFront(node)
}

// addToFront adds a node right after head
func (e *MetricPackEstimator) addToFront(node *LRUNode) {
	node.Prev = e.head
	node.Next = e.head.Next
	e.head.Next.Prev = node
	e.head.Next = node
}

// removeNode removes a node from the LRU list
func (e *MetricPackEstimator) removeNode(node *LRUNode) {
	node.Prev.Next = node.Next
	node.Next.Prev = node.Prev
}

// CacheStats returns statistics about the cache
func (e *MetricPackEstimator) CacheStats() (size, capacity int) {
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	return e.size, e.capacity
}

// ClearCache clears the entire cache
func (e *MetricPackEstimator) ClearCache() {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	e.cache = make(map[EstimateKey]*LRUNode)
	e.head.Next = e.tail
	e.tail.Prev = e.head
	e.size = 0
}
