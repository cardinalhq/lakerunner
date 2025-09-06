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

package filereader

import (
	"context"
	"io"
	"math"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// CookedMetricTranslatingReader wraps another reader and applies metric-specific transformations
// to the data, such as type conversions, validation, and filtering.
// This reader takes ownership of rows it wants to keep using efficient buffer operations.
type CookedMetricTranslatingReader struct {
	wrapped Reader
	closed  bool
}

// NewCookedMetricTranslatingReader creates a new reader that applies metric-specific transformations.
func NewCookedMetricTranslatingReader(wrapped Reader) *CookedMetricTranslatingReader {
	return &CookedMetricTranslatingReader{
		wrapped: wrapped,
	}
}

// Next returns the next batch of transformed metric data.
func (r *CookedMetricTranslatingReader) Next(ctx context.Context) (*Batch, error) {
	if r.closed {
		return nil, io.EOF
	}

	// Get raw batch from wrapped reader
	batch, err := r.wrapped.Next(ctx)
	if err != nil {
		return nil, err
	}

	// Process each row in place, efficiently filtering and transforming
	writeIdx := 0
	for readIdx := 0; readIdx < batch.Len(); readIdx++ {
		row := batch.Get(readIdx)

		// Apply metric-specific transformations and validations
		if r.shouldDropRow(ctx, row) {
			continue
		}

		// Transform the row in place
		r.transformRow(row)

		// Move valid row to write position if needed
		if readIdx != writeIdx {
			batch.SwapRows(readIdx, writeIdx)
		}
		writeIdx++
	}

	// Remove invalid rows from the end
	for i := batch.Len() - 1; i >= writeIdx; i-- {
		batch.DeleteRow(i)
	}

	// Track metrics
	rowsOutCounter.Add(ctx, int64(writeIdx), otelmetric.WithAttributes(
		attribute.String("reader", "CookedMetricTranslatingReader"),
	))

	return batch, nil
}

// shouldDropRow checks if a row should be dropped based on metric-specific criteria.
func (r *CookedMetricTranslatingReader) shouldDropRow(ctx context.Context, row pipeline.Row) bool {
	// Check for NaN or invalid rollup fields
	rollupKeys := []wkk.RowKey{
		wkk.RowKeyRollupSum, wkk.RowKeyRollupCount, wkk.RowKeyRollupAvg,
		wkk.RowKeyRollupMin, wkk.RowKeyRollupMax,
		wkk.RowKeyRollupP25, wkk.RowKeyRollupP50, wkk.RowKeyRollupP75,
		wkk.RowKeyRollupP90, wkk.RowKeyRollupP95, wkk.RowKeyRollupP99,
	}

	for _, key := range rollupKeys {
		if value, exists := row[key]; exists {
			if floatVal, ok := value.(float64); ok {
				if math.IsNaN(floatVal) || math.IsInf(floatVal, 0) {
					rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
						attribute.String("reader", "CookedMetricTranslatingReader"),
						attribute.String("reason", "NaN"),
					))
					return true
				}
			}
		}
	}

	// Check and validate _cardinalhq.tid field
	if tidValue, exists := row[wkk.RowKeyCTID]; exists {
		switch tidValue.(type) {
		case int64:
			// Valid type
		case string:
			// Will be converted in transformRow
		default:
			// Invalid type, drop row
			rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
				attribute.String("reader", "CookedMetricTranslatingReader"),
				attribute.String("reason", "invalid_tid_type"),
			))
			return true
		}
	}

	return false
}

// transformRow applies metric-specific transformations to a row in place.
func (r *CookedMetricTranslatingReader) transformRow(row pipeline.Row) {
	// Convert _cardinalhq.tid from string to int64 if needed
	if tidValue, exists := row[wkk.RowKeyCTID]; exists {
		if tidStr, ok := tidValue.(string); ok {
			if tidInt64, err := strconv.ParseInt(tidStr, 10, 64); err == nil {
				row[wkk.RowKeyCTID] = tidInt64
			} else {
				// Remove invalid tid field rather than keeping bad data
				delete(row, wkk.RowKeyCTID)
			}
		}
	}

	// Convert sketch field from string to []byte if needed
	if sketchValue, exists := row[wkk.RowKeySketch]; exists {
		if sketchStr, ok := sketchValue.(string); ok {
			row[wkk.RowKeySketch] = []byte(sketchStr)
		}
		// If it's already []byte or nil, leave it as-is
	}
}

// Close closes the reader and its wrapped reader.
func (r *CookedMetricTranslatingReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	if r.wrapped != nil {
		return r.wrapped.Close()
	}
	return nil
}

// TotalRowsReturned delegates to the wrapped reader.
func (r *CookedMetricTranslatingReader) TotalRowsReturned() int64 {
	if r.wrapped != nil {
		return r.wrapped.TotalRowsReturned()
	}
	return 0
}
