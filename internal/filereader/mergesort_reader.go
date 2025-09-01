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

	"github.com/cardinalhq/lakerunner/internal/pipeline"
)

// mergesortReaderState holds the state for a single reader in the ordered merge.
type mergesortReaderState struct {
	reader       Reader
	currentBatch *Batch
	batchIndex   int
	done         bool
	index        int
	err          error
	currentKey   SortKey // cached key for current row
}

// getCurrentRow returns the current row from the reader state, if available.
func (state *mergesortReaderState) getCurrentRow() Row {
	if state.done || state.err != nil || state.currentBatch == nil || state.batchIndex >= state.currentBatch.Len() {
		return nil
	}
	return state.currentBatch.Get(state.batchIndex)
}

// consumeCurrentRow advances to the next row in the current batch.
func (state *mergesortReaderState) consumeCurrentRow() {
	if state.currentBatch != nil && state.batchIndex < state.currentBatch.Len() {
		state.batchIndex++
	}
}

// MergesortReader implements merge-sort style reading across multiple pre-sorted readers.
// It assumes each individual reader returns rows in sorted order according to the
// provided SortKeyProvider.
type MergesortReader struct {
	states      []*mergesortReaderState
	keyProvider SortKeyProvider
	closed      bool
	rowCount    int64
	batchSize   int
}

// NewMergesortReader creates a new MergesortReader that merges rows from multiple readers
// in sorted order. The selector function determines which row should be returned next.
//
// Requirements:
// - Each reader must return rows in sorted order (according to the selector logic)
// - The selector function must be consistent (same inputs -> same output)
// - Readers will be closed when the MergesortReader is closed
func NewMergesortReader(ctx context.Context, readers []Reader, keyProvider SortKeyProvider, batchSize int) (*MergesortReader, error) {
	if len(readers) == 0 {
		return nil, errors.New("at least one reader is required")
	}
	if keyProvider == nil {
		return nil, errors.New("keyProvider is required")
	}

	if batchSize <= 0 {
		batchSize = 1000
	}

	states := make([]*mergesortReaderState, len(readers))
	for i, reader := range readers {
		states[i] = &mergesortReaderState{
			reader: reader,
			index:  i,
		}
	}

	or := &MergesortReader{
		states:      states,
		keyProvider: keyProvider,
		batchSize:   batchSize,
	}

	// Prime all readers by loading their first rows
	if err := or.primeReaders(ctx); err != nil {
		or.Close()
		return nil, fmt.Errorf("failed to prime readers: %w", err)
	}

	return or, nil
}

// primeReaders loads the first batch from each reader.
func (or *MergesortReader) primeReaders(ctx context.Context) error {
	for _, state := range or.states {
		if err := or.advance(ctx, state); err != nil {
			return fmt.Errorf("failed to prime reader %d: %w", state.index, err)
		}
	}
	return nil
}

// advance loads the next batch or moves to the next row in the current batch for the given reader state.
func (or *MergesortReader) advance(ctx context.Context, state *mergesortReaderState) error {
	if state.done || state.err != nil {
		return state.err
	}

	// Release old key if moving to next row
	if state.currentKey != nil {
		state.currentKey.Release()
		state.currentKey = nil
	}

	// Check if we need to load a new batch
	if state.currentBatch == nil || state.batchIndex >= state.currentBatch.Len() {
		// Return old batch to pool before getting a new one
		if state.currentBatch != nil {
			pipeline.ReturnBatch(state.currentBatch)
			state.currentBatch = nil
		}

		batch, err := state.reader.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				state.done = true
				return nil
			}
			state.err = err
			return err
		}

		// Track rows read from underlying readers
		if batch != nil {
			rowsInCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
				attribute.String("reader", "MergesortReader"),
			))
		}

		if batch.Len() == 0 {
			pipeline.ReturnBatch(batch)
			state.done = true
			return nil
		}

		state.currentBatch = batch
		state.batchIndex = 0
	}

	// Compute sort key for current row
	if !state.done && state.currentBatch != nil && state.batchIndex < state.currentBatch.Len() {
		currentRow := state.currentBatch.Get(state.batchIndex)
		state.currentKey = or.keyProvider.MakeKey(currentRow)
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
		// Find the reader state with the smallest sort key
		var selectedState *mergesortReaderState
		var minKey SortKey

		for _, state := range or.states {
			if !state.done && state.err == nil && state.currentKey != nil {
				if minKey == nil || state.currentKey.Compare(minKey) < 0 {
					if minKey != nil {
						minKey.Release()
					}
					selectedState = state
					minKey = state.currentKey
				}
			}
		}

		// No more active readers
		if selectedState == nil {
			// Release minKey if we allocated one
			if minKey != nil {
				minKey.Release()
			}

			// Check if any reader had an error
			for _, state := range or.states {
				if state.err != nil {
					pipeline.ReturnBatch(batch)
					return nil, fmt.Errorf("reader %d error: %w", state.index, state.err)
				}
			}
			if batch.Len() == 0 {
				pipeline.ReturnBatch(batch)
				return nil, io.EOF
			}
			break
		}

		// Release the minKey (we don't need to hold it)
		if minKey != nil {
			minKey.Release()
		}
		currentRow := selectedState.getCurrentRow()
		if currentRow != nil {
			// Copy the row to the batch
			row := batch.AddRow()
			for k, v := range currentRow {
				row[k] = v
			}

			// Consume the current row and advance if needed
			selectedState.consumeCurrentRow()
			if err := or.advance(ctx, selectedState); err != nil {
				pipeline.ReturnBatch(batch)
				return nil, fmt.Errorf("failed to advance reader %d: %w", selectedState.index, err)
			}
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
	for i, state := range or.states {
		// Return any remaining batches to the pool
		if state.currentBatch != nil {
			pipeline.ReturnBatch(state.currentBatch)
			state.currentBatch = nil
		}

		// Release cached sort key
		if state.currentKey != nil {
			state.currentKey.Release()
			state.currentKey = nil
		}

		if state.reader != nil {
			if err := state.reader.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close reader %d: %w", i, err))
			}
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// ActiveReaderCount returns the number of readers that still have data to read.
func (or *MergesortReader) ActiveReaderCount() int {
	if or.closed {
		return 0
	}

	count := 0
	for _, state := range or.states {
		if !state.done && state.err == nil {
			count++
		}
	}
	return count
}

// TotalRowsReturned returns the total number of rows that have been successfully returned via Next() from all readers.
func (or *MergesortReader) TotalRowsReturned() int64 {
	return or.rowCount
}
