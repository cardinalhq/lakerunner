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

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// CookedLogTranslatingReader wraps another reader and applies log-specific transformations
// to the data, such as type conversions, validation, and filtering.
// This reader takes ownership of rows it wants to keep using efficient buffer operations.
type CookedLogTranslatingReader struct {
	wrapped Reader
	closed  bool
}

// NewCookedLogTranslatingReader creates a new reader that applies log-specific transformations.
func NewCookedLogTranslatingReader(wrapped Reader) *CookedLogTranslatingReader {
	return &CookedLogTranslatingReader{
		wrapped: wrapped,
	}
}

// Next returns the next batch of transformed log data.
func (r *CookedLogTranslatingReader) Next(ctx context.Context) (*Batch, error) {
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

		// Apply log-specific transformations and validations
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
		attribute.String("reader", "CookedLogTranslatingReader"),
	))

	return batch, nil
}

// shouldDropRow checks if a row should be dropped based on log-specific criteria.
func (r *CookedLogTranslatingReader) shouldDropRow(ctx context.Context, row pipeline.Row) bool {
	// Check for required log fields
	if _, hasTimestamp := row[wkk.RowKeyCTimestamp]; !hasTimestamp {
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "CookedLogTranslatingReader"),
			attribute.String("reason", "missing_timestamp"),
		))
		return true
	}

	return false
}

// transformRow applies log-specific transformations to a row in place.
func (r *CookedLogTranslatingReader) transformRow(row pipeline.Row) {
	// Ensure fingerprint is bytes if present
	if fpValue, exists := row[wkk.RowKeyCFingerprint]; exists {
		if fpStr, ok := fpValue.(string); ok {
			row[wkk.RowKeyCFingerprint] = []byte(fpStr)
		}
	}

	// Normalize message field if needed
	if msgValue, exists := row[wkk.RowKeyCMessage]; exists {
		if msgBytes, ok := msgValue.([]byte); ok {
			// Convert bytes to string for consistency
			row[wkk.RowKeyCMessage] = string(msgBytes)
		}
	}
}

// Close closes the reader and its wrapped reader.
func (r *CookedLogTranslatingReader) Close() error {
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
func (r *CookedLogTranslatingReader) TotalRowsReturned() int64 {
	if r.wrapped != nil {
		return r.wrapped.TotalRowsReturned()
	}
	return 0
}
