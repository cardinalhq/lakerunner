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
	"time"

	"github.com/google/uuid"
	"github.com/jellydator/ttlcache/v3"
)

const (
	defaultEstimateTTL = 5 * time.Minute
)

// EstimateKey represents a cache key for metric pack estimates
type EstimateKey struct {
	OrganizationID uuid.UUID
	FrequencyMs    int32
}

// MetricPackEstimator provides cached access to metric pack estimates
type MetricPackEstimator struct {
	db    EstimatorStore
	cache *ttlcache.Cache[EstimateKey, int64]
}

// MetricEstimator provides access to metric pack estimates
type MetricEstimator interface {
	Get(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64
}

// EstimatorStore defines the interface for database operations
type EstimatorStore interface {
	GetAllMetricPackEstimates(ctx context.Context) ([]MetricPackEstimate, error)
}

// NewMetricPackEstimator creates a new estimator with TTL cache
func NewMetricPackEstimator(db EstimatorStore) *MetricPackEstimator {
	cache := ttlcache.New(
		ttlcache.WithTTL[EstimateKey, int64](defaultEstimateTTL),
	)
	go cache.Start()

	return &MetricPackEstimator{
		db:    db,
		cache: cache,
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
	if item := e.cache.Get(key); item != nil {
		return item.Value(), nil
	}

	// Not in cache, try to load from database
	estimate, err := e.loadEstimate(ctx, orgID, frequencyMs)
	if err != nil {
		return 0, err
	}

	// Cache the result
	e.cache.Set(key, estimate, ttlcache.DefaultTTL)

	return estimate, nil
}

// getHardcodedDefault returns a hardcoded default for a frequency
func (e *MetricPackEstimator) getHardcodedDefault() int64 {
	return 40000 // Ultimate fallback: 40K records
}

// loadEstimate loads estimate from database with fallback logic
// Optimized to cache all estimates for an organization when first accessed
func (e *MetricPackEstimator) loadEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) (int64, error) {
	// Load all estimates from database once
	estimates, err := e.db.GetAllMetricPackEstimates(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to load metric pack estimates: %w", err)
	}

	// Cache all estimates for this organization and defaults at once to avoid repeated DB calls
	zeroUUID := uuid.UUID{}
	orgEstimates := make(map[int32]int64)
	defaultEstimates := make(map[int32]int64)

	for _, est := range estimates {
		if est.TargetRecords != nil {
			value := *est.TargetRecords
			switch est.OrganizationID {
			case orgID:
				orgEstimates[est.FrequencyMs] = value
				// Cache this estimate
				key := EstimateKey{OrganizationID: orgID, FrequencyMs: est.FrequencyMs}
				e.cache.Set(key, value, ttlcache.DefaultTTL)
			case zeroUUID:
				defaultEstimates[est.FrequencyMs] = value
				// Cache this default estimate
				key := EstimateKey{OrganizationID: zeroUUID, FrequencyMs: est.FrequencyMs}
				e.cache.Set(key, value, ttlcache.DefaultTTL)
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
	return 40000, nil
}

// ClearCache clears the entire cache
func (e *MetricPackEstimator) ClearCache() {
	e.cache.DeleteAll()
}

// Stop stops the cache's background goroutine
func (e *MetricPackEstimator) Stop() {
	e.cache.Stop()
}
