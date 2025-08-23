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

package parquetwriter

import (
	"encoding/json"
	"sync"
)

// SizeEstimator estimates the size of rows to help with intelligent file splitting.
type SizeEstimator interface {
	// EstimateRowSize returns the estimated size in bytes for a single row.
	EstimateRowSize(row map[string]any) int64

	// UpdateFromActual updates the estimator with actual file statistics.
	// This helps the estimator improve its accuracy over time.
	UpdateFromActual(rowCount int64, actualFileSize int64)

	// Reset resets the estimator to its initial state.
	Reset()
}

// AdaptiveSizeEstimator learns from actual file sizes to improve estimates over time.
// It uses an exponential moving average to adapt to changing data patterns.
type AdaptiveSizeEstimator struct {
	mu          sync.RWMutex
	avgRowSize  int64   // Current average row size in bytes
	sampleCount int64   // Number of updates received
	decayFactor float64 // How quickly to adapt to new data (0.0-1.0)
}

// NewAdaptiveSizeEstimator creates a new adaptive size estimator with sensible defaults.
func NewAdaptiveSizeEstimator() *AdaptiveSizeEstimator {
	return &AdaptiveSizeEstimator{
		avgRowSize:  250, // Reasonable default for mixed data
		decayFactor: 0.1, // Adapt slowly to avoid overreacting to outliers
	}
}

// NewAdaptiveSizeEstimatorWithConfig creates a new adaptive size estimator with custom settings.
func NewAdaptiveSizeEstimatorWithConfig(initialAvgSize int64, decayFactor float64) *AdaptiveSizeEstimator {
	if initialAvgSize <= 0 {
		initialAvgSize = 250
	}
	if decayFactor <= 0 || decayFactor >= 1 {
		decayFactor = 0.1
	}
	return &AdaptiveSizeEstimator{
		avgRowSize:  initialAvgSize,
		decayFactor: decayFactor,
	}
}

// EstimateRowSize estimates the size of a row using the current average plus
// a rough calculation based on the row's content.
func (e *AdaptiveSizeEstimator) EstimateRowSize(row map[string]any) int64 {
	e.mu.RLock()
	baseEstimate := e.avgRowSize
	e.mu.RUnlock()

	// For early estimates (when we have little data), do a rough calculation
	// based on the actual row content to improve accuracy
	if e.sampleCount < 10 {
		contentSize := e.estimateContentSize(row)
		// Blend the content-based estimate with our running average
		weight := 0.3 // Give some weight to content-based sizing
		if e.sampleCount == 0 {
			weight = 1.0 // Use only content-based sizing initially
		}
		return int64(float64(contentSize)*weight + float64(baseEstimate)*(1.0-weight))
	}

	return baseEstimate
}

// estimateContentSize does a rough calculation of row size based on content.
// This is used when we don't have enough historical data for accurate estimates.
func (e *AdaptiveSizeEstimator) estimateContentSize(row map[string]any) int64 {
	var size int64

	for key, value := range row {
		// Key overhead (column name + some metadata)
		size += int64(len(key)) + 8

		// Value size based on type
		switch v := value.(type) {
		case nil:
			size += 1 // Null marker
		case bool:
			size += 1
		case int, int32, int64:
			size += 8
		case float32:
			size += 4
		case float64:
			size += 8
		case string:
			size += int64(len(v)) + 4 // String length + length prefix
		case []byte:
			size += int64(len(v)) + 4
		default:
			// For complex types, use JSON marshaling as a rough estimate
			if data, err := json.Marshal(v); err == nil {
				size += int64(len(data)) + 4
			} else {
				size += 20 // Fallback for unknown types
			}
		}
	}

	// Add some overhead for Parquet encoding, compression, and metadata
	// This is a rough multiplier based on typical Parquet compression ratios
	return size + (size / 4)
}

// UpdateFromActual updates the running average with actual file statistics.
func (e *AdaptiveSizeEstimator) UpdateFromActual(rowCount int64, actualFileSize int64) {
	if rowCount <= 0 || actualFileSize <= 0 {
		return
	}

	actualAvgRowSize := actualFileSize / rowCount

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.sampleCount == 0 {
		// First sample - use it directly
		e.avgRowSize = actualAvgRowSize
	} else {
		// Exponential moving average: new_avg = old_avg * (1-decay) + new_sample * decay
		e.avgRowSize = int64(float64(e.avgRowSize)*(1.0-e.decayFactor) + float64(actualAvgRowSize)*e.decayFactor)
	}

	e.sampleCount++
}

// Reset resets the estimator to its initial state.
func (e *AdaptiveSizeEstimator) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.avgRowSize = 250
	e.sampleCount = 0
}

// GetCurrentAverage returns the current average row size estimate.
// This is useful for debugging and monitoring.
func (e *AdaptiveSizeEstimator) GetCurrentAverage() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.avgRowSize
}

// FixedSizeEstimator always returns the same size estimate.
// Useful for testing or when you have a good known estimate.
type FixedSizeEstimator struct {
	size int64
}

// NewFixedSizeEstimator creates a size estimator that always returns the same value.
func NewFixedSizeEstimator(size int64) *FixedSizeEstimator {
	return &FixedSizeEstimator{size: size}
}

// EstimateRowSize returns the fixed size.
func (e *FixedSizeEstimator) EstimateRowSize(row map[string]any) int64 {
	return e.size
}

// UpdateFromActual does nothing for fixed estimators.
func (e *FixedSizeEstimator) UpdateFromActual(rowCount int64, actualFileSize int64) {
	// No-op for fixed estimators
}

// Reset does nothing for fixed estimators.
func (e *FixedSizeEstimator) Reset() {
	// No-op for fixed estimators
}
