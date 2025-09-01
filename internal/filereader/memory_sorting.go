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
	"fmt"
	"io"
	"slices"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
)

// MemorySortingReader reads all rows from an underlying reader,
// then sorts them using a custom sort function and returns them in order.
// This is useful when you need sorted output with flexible sorting criteria.
//
// Memory Impact: HIGH - All rows are loaded into memory at once
// Disk I/O: None (pure in-memory operations)
// Stability: Records are only guaranteed to be sorted by the sort function;
//
//	if the sort function is not stable, the result will not be stable
type MemorySortingReader struct {
	reader      Reader
	keyProvider SortKeyProvider
	closed      bool
	rowCount    int64
	batchSize   int

	// Buffered rows with their sort keys
	allRowsWithKeys []rowWithKey
	currentIndex    int
	sorted          bool
}

// rowWithKey pairs a row with its sort key for efficient sorting
type rowWithKey struct {
	row Row
	key SortKey
}

// NewMemorySortingReader creates a reader that buffers all rows,
// sorts them using the provided key provider, then returns them in order.
//
// Use this for smaller datasets that fit comfortably in memory.
// For large datasets, consider DiskSortingReader to avoid OOM issues.
func NewMemorySortingReader(reader Reader, keyProvider SortKeyProvider, batchSize int) (*MemorySortingReader, error) {
	if reader == nil {
		return nil, fmt.Errorf("reader cannot be nil")
	}
	if keyProvider == nil {
		return nil, fmt.Errorf("keyProvider cannot be nil")
	}

	if batchSize <= 0 {
		batchSize = 1000
	}

	return &MemorySortingReader{
		reader:          reader,
		keyProvider:     keyProvider,
		batchSize:       batchSize,
		allRowsWithKeys: make([]rowWithKey, 0, 1000), // Start with reasonable capacity
	}, nil
}

// loadAndSortAllRows reads all rows from the underlying reader and sorts them.
func (r *MemorySortingReader) loadAndSortAllRows(ctx context.Context) error {
	if r.sorted {
		return nil
	}

	// Read all batches from the underlying reader
	for {
		batch, err := r.reader.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read from underlying reader: %w", err)
		}

		// Track rows read from underlying reader
		rowsInCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
			attribute.String("reader", "MemorySortingReader"),
		))

		// Convert and store rows from batch with their sort keys
		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			// Deep copy the row since we're retaining it beyond the batch lifetime
			copiedRow := make(Row)
			for k, v := range row {
				copiedRow[k] = v
			}
			// Create sort key for this row
			sortKey := r.keyProvider.MakeKey(copiedRow)
			r.allRowsWithKeys = append(r.allRowsWithKeys, rowWithKey{
				row: copiedRow,
				key: sortKey,
			})
		}

		// Return batch to pool since we're done with it
		pipeline.ReturnBatch(batch)
	}

	// Sort using the sort keys
	slices.SortFunc(r.allRowsWithKeys, func(a, b rowWithKey) int {
		return a.key.Compare(b.key)
	})

	r.sorted = true
	return nil
}

// Next returns the next batch of sorted rows from the buffer.
func (r *MemorySortingReader) Next(ctx context.Context) (*Batch, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	// Ensure all rows are loaded and sorted
	if !r.sorted {
		if err := r.loadAndSortAllRows(ctx); err != nil {
			return nil, err
		}
	}

	// Check if we've exhausted all rows
	if r.currentIndex >= len(r.allRowsWithKeys) {
		return nil, io.EOF
	}

	batch := pipeline.GetBatch()

	// Return rows from the sorted buffer
	for batch.Len() < r.batchSize && r.currentIndex < len(r.allRowsWithKeys) {
		// Use batch's AddRow to reuse maps
		row := batch.AddRow()
		for k, v := range r.allRowsWithKeys[r.currentIndex].row {
			row[k] = v
		}
		r.currentIndex++
	}

	if batch.Len() > 0 {
		r.rowCount += int64(batch.Len())
		// Track rows output to downstream
		rowsOutCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
			attribute.String("reader", "MemorySortingReader"),
		))
		return batch, nil
	}

	return nil, io.EOF
}

// Close closes the reader and underlying reader.
func (r *MemorySortingReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Release all sort keys back to their pools
	for i := range r.allRowsWithKeys {
		if r.allRowsWithKeys[i].key != nil {
			r.allRowsWithKeys[i].key.Release()
		}
	}

	// Release buffer
	r.allRowsWithKeys = nil

	return r.reader.Close()
}

// TotalRowsReturned returns the number of rows that have been returned via Next().
func (r *MemorySortingReader) TotalRowsReturned() int64 {
	return r.rowCount
}

// GetOTELMetrics implements the OTELMetricsProvider interface if the underlying reader supports it.
func (r *MemorySortingReader) GetOTELMetrics() (any, error) {
	if provider, ok := r.reader.(interface{ GetOTELMetrics() (any, error) }); ok {
		return provider.GetOTELMetrics()
	}
	return nil, fmt.Errorf("underlying reader does not support OTEL metrics")
}
