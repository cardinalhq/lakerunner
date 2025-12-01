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
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// CookedTraceTranslatingReader wraps another reader and applies trace-specific transformations
// to the data, such as type conversions, validation, and filtering.
// This reader takes ownership of rows it wants to keep using efficient buffer operations.
type CookedTraceTranslatingReader struct {
	wrapped Reader
	closed  bool
}

// NewCookedTraceTranslatingReader creates a new reader that applies trace-specific transformations.
func NewCookedTraceTranslatingReader(wrapped Reader) *CookedTraceTranslatingReader {
	return &CookedTraceTranslatingReader{
		wrapped: wrapped,
	}
}

// Next returns the next batch of transformed trace data.
func (r *CookedTraceTranslatingReader) Next(ctx context.Context) (*Batch, error) {
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

		// Apply trace-specific transformations and validations
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
		attribute.String("reader", "CookedTraceTranslatingReader"),
	))

	return batch, nil
}

// shouldDropRow checks if a row should be dropped based on trace-specific criteria.
func (r *CookedTraceTranslatingReader) shouldDropRow(ctx context.Context, row pipeline.Row) bool {
	// Check for required trace fields
	if _, hasTimestamp := row[wkk.RowKeyCTimestamp]; !hasTimestamp {
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "CookedTraceTranslatingReader"),
			attribute.String("reason", "missing_timestamp"),
		))
		return true
	}

	// Check for required trace ID
	if _, hasTraceID := row[wkk.NewRowKey("span_trace_id")]; !hasTraceID {
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "CookedTraceTranslatingReader"),
			attribute.String("reason", "missing_trace_id"),
		))
		return true
	}

	return false
}

// transformRow applies trace-specific transformations to a row in place.
func (r *CookedTraceTranslatingReader) transformRow(row pipeline.Row) {
	// Ensure fingerprint is int64 if present
	if fpValue, exists := row[wkk.RowKeyCFingerprint]; exists {
		switch v := fpValue.(type) {
		case string:
			// Convert string to int64
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				row[wkk.RowKeyCFingerprint] = parsed
			}
			// If parse fails, leave as-is (will be handled downstream)
		case []byte:
			// Convert bytes to int64 via string
			if parsed, err := strconv.ParseInt(string(v), 10, 64); err == nil {
				row[wkk.RowKeyCFingerprint] = parsed
			}
			// If parse fails, leave as-is (will be handled downstream)
		case int64:
			// Already int64, nothing to do
		case int:
			row[wkk.RowKeyCFingerprint] = int64(v)
		case int32:
			row[wkk.RowKeyCFingerprint] = int64(v)
		case uint32:
			row[wkk.RowKeyCFingerprint] = int64(v)
		case uint64:
			row[wkk.RowKeyCFingerprint] = int64(v)
		}
	}

	// Normalize span name field if needed
	if spanNameValue, exists := row[wkk.NewRowKey("span_name")]; exists {
		if spanNameBytes, ok := spanNameValue.([]byte); ok {
			// Convert bytes to string for consistency
			row[wkk.NewRowKey("span_name")] = string(spanNameBytes)
		}
	}

	// Ensure trace ID and span ID are strings
	if traceIDValue, exists := row[wkk.NewRowKey("span_trace_id")]; exists {
		if traceIDBytes, ok := traceIDValue.([]byte); ok {
			row[wkk.NewRowKey("span_trace_id")] = string(traceIDBytes)
		}
	}

	if spanIDValue, exists := row[wkk.NewRowKey("span_id")]; exists {
		if spanIDBytes, ok := spanIDValue.([]byte); ok {
			row[wkk.NewRowKey("span_id")] = string(spanIDBytes)
		}
	}

	// Add tsns field if not present, derived from timestamp
	if _, exists := row[wkk.RowKeyCTsns]; !exists {
		if tsValue, exists := row[wkk.RowKeyCTimestamp]; exists {
			switch v := tsValue.(type) {
			case int64:
				// Timestamp is in milliseconds, convert to nanoseconds
				row[wkk.RowKeyCTsns] = v * 1_000_000
			case float64:
				// Timestamp is in milliseconds, convert to nanoseconds
				row[wkk.RowKeyCTsns] = int64(v * 1_000_000)
			}
		}
	}
}

// Close closes the reader and its wrapped reader.
func (r *CookedTraceTranslatingReader) Close() error {
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
func (r *CookedTraceTranslatingReader) TotalRowsReturned() int64 {
	if r.wrapped != nil {
		return r.wrapped.TotalRowsReturned()
	}
	return 0
}

// GetSchema returns schema information augmented with columns added during translation.
// This reader adds chq_tsns (timestamp in nanoseconds) derived from chq_timestamp.
func (r *CookedTraceTranslatingReader) GetSchema() *ReaderSchema {
	var baseSchema *ReaderSchema
	if r.wrapped != nil {
		baseSchema = r.wrapped.GetSchema()
	}

	if baseSchema == nil {
		baseSchema = NewReaderSchema()
	}

	// Clone the base schema to avoid modifying the wrapped reader's schema
	augmentedSchema := NewReaderSchema()
	for _, col := range baseSchema.Columns() {
		// Preserve the original name mapping from the base schema
		originalName := baseSchema.GetOriginalName(col.Name)
		augmentedSchema.AddColumn(col.Name, originalName, col.DataType, col.HasNonNull)
	}

	// Add chq_tsns field (derived from chq_timestamp, always added if timestamp exists)
	if !augmentedSchema.HasColumn(wkk.RowKeyValue(wkk.RowKeyCTsns)) {
		// Identity mapping for chq_tsns
		augmentedSchema.AddColumn(wkk.RowKeyCTsns, wkk.RowKeyCTsns, DataTypeInt64, true)
	}

	return augmentedSchema
}
