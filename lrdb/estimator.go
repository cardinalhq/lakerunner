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

// EstimateKey represents a cache key for pack estimates
type EstimateKey struct {
	OrganizationID uuid.UUID
	FrequencyMs    int32
	Signal         string
}

// PackEstimator provides cached access to pack estimates for all signal types
type PackEstimator struct {
	db    EstimatorStore
	cache *ttlcache.Cache[EstimateKey, int64]
}

// MetricEstimator provides access to metric pack estimates (backward compatibility)
type MetricEstimator interface {
	Get(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64
}

// LogEstimator provides access to log pack estimates
type LogEstimator interface {
	GetLog(ctx context.Context, orgID uuid.UUID) int64
}

// TraceEstimator provides access to trace pack estimates
type TraceEstimator interface {
	GetTrace(ctx context.Context, orgID uuid.UUID) int64
}

// SignalEstimator provides access to pack estimates for any signal type
type SignalEstimator interface {
	GetSignal(ctx context.Context, orgID uuid.UUID, frequencyMs int32, signal string) int64
}

// EstimatorStore defines the interface for database operations
type EstimatorStore interface {
	GetAllPackEstimates(ctx context.Context) ([]GetAllPackEstimatesRow, error)
	GetAllBySignal(ctx context.Context, signal string) ([]GetAllBySignalRow, error)
	GetMetricPackEstimates(ctx context.Context) ([]GetMetricPackEstimatesRow, error) // Backward compatibility
}

// NewPackEstimator creates a new estimator with TTL cache for all signal types
func NewPackEstimator(db EstimatorStore) *PackEstimator {
	cache := ttlcache.New(
		ttlcache.WithTTL[EstimateKey, int64](defaultEstimateTTL),
	)
	go cache.Start()

	return &PackEstimator{
		db:    db,
		cache: cache,
	}
}

// NewMetricPackEstimator creates a new estimator with TTL cache (backward compatibility)
func NewMetricPackEstimator(db EstimatorStore) *PackEstimator {
	return NewPackEstimator(db)
}

// Get retrieves the target records estimate for the given org and frequency for metrics
// Falls back to defaults using UUID zero if no org-specific estimate exists
// This method implements the MetricEstimator interface and handles errors internally
func (e *PackEstimator) Get(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64 {
	return e.GetSignal(ctx, orgID, frequencyMs, "metrics")
}

// GetLog retrieves the target records estimate for logs
func (e *PackEstimator) GetLog(ctx context.Context, orgID uuid.UUID) int64 {
	return e.GetSignal(ctx, orgID, -1, "logs")
}

// GetTrace retrieves the target records estimate for traces
func (e *PackEstimator) GetTrace(ctx context.Context, orgID uuid.UUID) int64 {
	return e.GetSignal(ctx, orgID, -1, "traces")
}

// GetSignal retrieves the target records estimate for any signal type
func (e *PackEstimator) GetSignal(ctx context.Context, orgID uuid.UUID, frequencyMs int32, signal string) int64 {
	estimate, err := e.getEstimate(ctx, orgID, frequencyMs, signal)
	if err != nil {
		// Log error and return a reasonable default
		// In production, this might use a logger
		return e.getHardcodedDefault(signal)
	}
	return estimate
}

// getEstimate is the internal method that can return errors
func (e *PackEstimator) getEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32, signal string) (int64, error) {
	key := EstimateKey{OrganizationID: orgID, FrequencyMs: frequencyMs, Signal: signal}

	// Try cache first
	if item := e.cache.Get(key); item != nil {
		return item.Value(), nil
	}

	// Not in cache, try to load from database
	estimate, err := e.loadEstimate(ctx, orgID, frequencyMs, signal)
	if err != nil {
		return 0, err
	}

	// Cache the result
	e.cache.Set(key, estimate, ttlcache.DefaultTTL)

	return estimate, nil
}

// getHardcodedDefault returns a hardcoded default for a signal type
func (e *PackEstimator) getHardcodedDefault(signal string) int64 {
	switch signal {
	case "metrics":
		return 40000 // Ultimate fallback: 40K records
	case "logs":
		return 40000 // Ultimate fallback: 40K records
	case "traces":
		return 40000 // Ultimate fallback: 40K records
	default:
		return 40000 // Default fallback for unknown signals
	}
}

// loadEstimate loads estimate from database with fallback logic
// Optimized to cache estimates for a specific signal type only (per-signal caching)
func (e *PackEstimator) loadEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32, signal string) (int64, error) {
	// Load estimates for this specific signal type only (much more efficient)
	estimates, err := e.db.GetAllBySignal(ctx, signal)
	if err != nil {
		return 0, fmt.Errorf("failed to load pack estimates for signal %s: %w", signal, err)
	}

	// Cache all estimates for this signal and find the ones we need
	zeroUUID := uuid.UUID{}
	var orgEstimate, defaultEstimate *int64

	for _, est := range estimates {
		if est.TargetRecords != nil {
			value := *est.TargetRecords
			// Cache this estimate
			key := EstimateKey{OrganizationID: est.OrganizationID, FrequencyMs: est.FrequencyMs, Signal: signal}
			e.cache.Set(key, value, ttlcache.DefaultTTL)

			// Track specific values we need for immediate return
			if est.OrganizationID == orgID && est.FrequencyMs == frequencyMs {
				orgEstimate = &value
			} else if est.OrganizationID == zeroUUID && est.FrequencyMs == frequencyMs {
				defaultEstimate = &value
			}
		}
	}

	// Look for exact org + frequency + signal match
	if orgEstimate != nil {
		return *orgEstimate, nil
	}

	// Look for UUID zero (default) + frequency + signal match
	if defaultEstimate != nil {
		return *defaultEstimate, nil
	}

	// Ultimate fallback based on signal type
	return e.getHardcodedDefault(signal), nil
}

// ClearCache clears the entire cache
func (e *PackEstimator) ClearCache() {
	e.cache.DeleteAll()
}

// Stop stops the cache's background goroutine
func (e *PackEstimator) Stop() {
	e.cache.Stop()
}

// Type aliases for backward compatibility and convenience
type MetricPackEstimator = PackEstimator

// Interface implementations
var (
	_ MetricEstimator = (*PackEstimator)(nil)
	_ LogEstimator    = (*PackEstimator)(nil)
	_ TraceEstimator  = (*PackEstimator)(nil)
	_ SignalEstimator = (*PackEstimator)(nil)
)
