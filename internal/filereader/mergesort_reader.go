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
	"errors"
	"fmt"
	"io"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/pipeline"
)

// activeReader represents a reader that has data available for merging
type activeReader struct {
	reader       Reader
	currentKey   SortKey      // Our owned reference to the current sort key
	currentRow   pipeline.Row // Cache of current row
	currentBatch *Batch       // Current batch from reader
	batchIndex   int          // Index within current batch
	index        int          // Original reader index for error reporting
}

// advance loads the next row from this reader, updating the key and row cache
func (ar *activeReader) advance(ctx context.Context, keyProvider SortKeyProvider) error {
	// Release old key
	if ar.currentKey != nil {
		ar.currentKey.Release()
		ar.currentKey = nil
	}
	ar.currentRow = nil

	// Check if we need to load a new batch
	if ar.currentBatch == nil || ar.batchIndex >= ar.currentBatch.Len() {
		// Return old batch to pool
		if ar.currentBatch != nil {
			pipeline.ReturnBatch(ar.currentBatch)
			ar.currentBatch = nil
		}

		batch, err := ar.reader.Next(ctx)
		if err != nil {
			return err // EOF or other error
		}

		// Track rows read from underlying readers
		if batch != nil {
			rowsInCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
				attribute.String("reader", "MergesortReader"),
			))
		}

		if batch.Len() == 0 {
			pipeline.ReturnBatch(batch)
			return io.EOF
		}

		ar.currentBatch = batch
		ar.batchIndex = 0
	}

	// Cache current row and generate key
	ar.currentRow = ar.currentBatch.Get(ar.batchIndex)
	if ar.currentRow != nil {
		ar.currentKey = keyProvider.MakeKey(ar.currentRow)
	}

	return nil
}

// consume advances to the next row in the current batch
func (ar *activeReader) consume() {
	ar.batchIndex++
}

// cleanup releases resources held by this active reader
func (ar *activeReader) cleanup() {
	if ar.currentKey != nil {
		ar.currentKey.Release()
		ar.currentKey = nil
	}
	if ar.currentBatch != nil {
		pipeline.ReturnBatch(ar.currentBatch)
		ar.currentBatch = nil
	}
}

// MergesortReader implements merge-sort style reading across multiple pre-sorted readers.
// It assumes each individual reader returns rows in sorted order according to the
// provided SortKeyProvider.
type MergesortReader struct {
	readers       []Reader        // Original readers
	activeReaders []*activeReader // Readers with data available
	keyProvider   SortKeyProvider
	closed        bool
	rowCount      int64
	batchSize     int
	schema        *ReaderSchema // Merged schema from all readers
}

var _ Reader = (*MergesortReader)(nil)

// NewMergesortReader creates a new MergesortReader that merges rows from multiple readers
// in sorted order using the new algorithm with active reader management.
func NewMergesortReader(ctx context.Context, readers []Reader, keyProvider SortKeyProvider, batchSize int) (*MergesortReader, error) {
	if len(readers) == 0 {
		return nil, errors.New("no readers provided")
	}
	if keyProvider == nil {
		return nil, errors.New("keyProvider cannot be nil")
	}

	or := &MergesortReader{
		readers:     readers,
		keyProvider: keyProvider,
		batchSize:   batchSize,
	}

	// Merge schemas from all readers
	or.schema = mergeSchemas(readers)

	// Prime all readers - read first row from each and create keys
	if err := or.primeReaders(ctx); err != nil {
		_ = or.Close() // Clean up any partially initialized state
		return nil, err
	}

	return or, nil
}

// primeReaders initializes all readers by loading their first row and sort key
func (or *MergesortReader) primeReaders(ctx context.Context) error {
	// Initialize activeReaders slice
	or.activeReaders = make([]*activeReader, 0, len(or.readers))

	// Prime each reader - read first row and create key
	for i, reader := range or.readers {
		ar := &activeReader{
			reader: reader,
			index:  i,
		}

		// Try to advance to first row
		err := ar.advance(ctx, or.keyProvider)
		if err == io.EOF {
			// Reader is empty, skip it
			continue
		} else if err != nil {
			return fmt.Errorf("failed to prime reader %d: %w", i, err)
		}

		// Reader has data, add to active list
		or.activeReaders = append(or.activeReaders, ar)
	}

	return nil
}

// Next returns the next batch of rows in sorted order across all readers.
func (or *MergesortReader) Next(ctx context.Context) (*Batch, error) {
	if or.closed {
		return nil, errors.New("reader is closed")
	}

	batch := pipeline.GetBatch()

	for batch.Len() < or.batchSize {
		// Find the active reader with the smallest sort key
		if len(or.activeReaders) == 0 {
			// No more active readers
			if batch.Len() == 0 {
				pipeline.ReturnBatch(batch)
				return nil, io.EOF
			}
			break
		}

		// Find reader with minimum key
		minIndex := 0
		for i := 1; i < len(or.activeReaders); i++ {
			if or.activeReaders[i].currentKey.Compare(or.activeReaders[minIndex].currentKey) < 0 {
				minIndex = i
			}
		}

		selectedReader := or.activeReaders[minIndex]

		// Copy the row to the batch
		row := batch.AddRow()
		for k, v := range selectedReader.currentRow {
			row[k] = v
		}

		// Apply schema normalization to ensure type consistency
		// Only normalize if we have a non-empty schema (at least one schemafied reader)
		if or.schema != nil && len(or.schema.Columns()) > 0 {
			if err := normalizeRow(ctx, row, or.schema); err != nil {
				pipeline.ReturnBatch(batch)
				return nil, fmt.Errorf("schema normalization failed: %w", err)
			}
		}

		// Advance the selected reader to its next row
		selectedReader.consume()
		err := selectedReader.advance(ctx, or.keyProvider)
		if err == io.EOF {
			// Reader is exhausted, remove from active list
			selectedReader.cleanup()
			or.activeReaders = append(or.activeReaders[:minIndex], or.activeReaders[minIndex+1:]...)
		} else if err != nil {
			// Reader error
			pipeline.ReturnBatch(batch)
			return nil, fmt.Errorf("failed to advance reader %d: %w", selectedReader.index, err)
		}
	}

	// Update row count with successfully read rows
	if batch.Len() > 0 {
		or.rowCount += int64(batch.Len())
		// Track rows output to downstream
		rowsOutCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
			attribute.String("reader", "MergesortReader"),
		))
		return batch, nil
	}

	pipeline.ReturnBatch(batch)
	return nil, io.EOF
}

// Close closes all underlying readers and releases resources.
func (or *MergesortReader) Close() error {
	if or.closed {
		return nil
	}
	or.closed = true

	var errs []error

	// Clean up active readers
	for _, ar := range or.activeReaders {
		ar.cleanup()
	}
	or.activeReaders = nil

	// Close original readers
	for i, reader := range or.readers {
		if err := reader.Close(); err != nil {
			errs = append(errs, fmt.Errorf("reader %d: %w", i, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing readers: %v", errs)
	}
	return nil
}

// TotalRowsReturned returns the total number of rows returned by this reader.
func (or *MergesortReader) TotalRowsReturned() int64 {
	return or.rowCount
}

// ActiveReaderCount returns the number of readers that still have data available.
func (or *MergesortReader) ActiveReaderCount() int {
	return len(or.activeReaders)
}

// GetSchema returns the merged schema from all child readers.
func (or *MergesortReader) GetSchema() *ReaderSchema {
	return or.schema
}

// mergeSchemas merges schemas from multiple readers, performing type promotion
// when the same column appears with different types across readers.
func mergeSchemas(readers []Reader) *ReaderSchema {
	merged := NewReaderSchema()

	for _, reader := range readers {
		readerSchema := reader.GetSchema()
		if readerSchema == nil {
			continue
		}

		// Merge each column from this reader's schema
		for _, col := range readerSchema.Columns() {
			merged.AddColumn(col.Name, col.DataType, col.HasNonNull)
		}
	}

	return merged
}
