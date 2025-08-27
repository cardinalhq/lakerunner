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
	"errors"
	"fmt"
	"io"
)

// readerState holds the state for a single reader in the ordered merge.
type readerState struct {
	reader       Reader
	currentBatch *Batch
	batchIndex   int
	done         bool
	index        int
	err          error
}

// getCurrentRow returns the current row from the reader state, if available.
func (state *readerState) getCurrentRow() Row {
	if state.done || state.err != nil || state.currentBatch == nil || state.batchIndex >= len(state.currentBatch.Rows) {
		return nil
	}
	return state.currentBatch.Rows[state.batchIndex]
}

// consumeCurrentRow advances to the next row in the current batch.
func (state *readerState) consumeCurrentRow() {
	if state.currentBatch != nil && state.batchIndex < len(state.currentBatch.Rows) {
		state.batchIndex++
	}
}

// PreorderedMultisourceReader implements merge-sort style reading across multiple pre-sorted readers.
// It assumes each individual reader returns rows in sorted order according to the
// provided selector function.
type PreorderedMultisourceReader struct {
	states    []*readerState
	selector  SelectFunc
	closed    bool
	rowCount  int64
	batchSize int
}

// NewPreorderedMultisourceReader creates a new PreorderedMultisourceReader that merges rows from multiple readers
// in sorted order. The selector function determines which row should be returned next.
//
// Requirements:
// - Each reader must return rows in sorted order (according to the selector logic)
// - The selector function must be consistent (same inputs -> same output)
// - Readers will be closed when the PreorderedMultisourceReader is closed
func NewPreorderedMultisourceReader(readers []Reader, selector SelectFunc, batchSize int) (*PreorderedMultisourceReader, error) {
	if len(readers) == 0 {
		return nil, errors.New("at least one reader is required")
	}
	if selector == nil {
		return nil, errors.New("selector function is required")
	}

	if batchSize <= 0 {
		batchSize = 1000
	}

	states := make([]*readerState, len(readers))
	for i, reader := range readers {
		states[i] = &readerState{
			reader: reader,
			index:  i,
		}
	}

	or := &PreorderedMultisourceReader{
		states:    states,
		selector:  selector,
		batchSize: batchSize,
	}

	// Prime all readers by loading their first rows
	if err := or.primeReaders(); err != nil {
		or.Close()
		return nil, fmt.Errorf("failed to prime readers: %w", err)
	}

	return or, nil
}

// primeReaders loads the first batch from each reader.
func (or *PreorderedMultisourceReader) primeReaders() error {
	for _, state := range or.states {
		if err := or.advance(state); err != nil {
			return fmt.Errorf("failed to prime reader %d: %w", state.index, err)
		}
	}
	return nil
}

// advance loads the next batch or moves to the next row in the current batch for the given reader state.
func (or *PreorderedMultisourceReader) advance(state *readerState) error {
	if state.done || state.err != nil {
		return state.err
	}

	// Check if we need to load a new batch
	if state.currentBatch == nil || state.batchIndex >= len(state.currentBatch.Rows) {
		batch, err := state.reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				state.done = true
				return nil
			}
			state.err = err
			return err
		}

		if len(batch.Rows) == 0 {
			state.done = true
			return nil
		}

		state.currentBatch = batch
		state.batchIndex = 0
	}

	return nil
}

// Next returns the next batch of rows in sorted order across all readers.
func (or *PreorderedMultisourceReader) Next() (*Batch, error) {
	if or.closed {
		return nil, errors.New("reader is closed")
	}

	batch := &Batch{
		Rows: make([]Row, 0, or.batchSize),
	}

	for len(batch.Rows) < or.batchSize {
		// Collect all active (non-done, non-error) readers and their current rows
		var activeRows []Row
		var activeStates []*readerState

		for _, state := range or.states {
			if !state.done && state.err == nil {
				currentRow := state.getCurrentRow()
				if currentRow != nil {
					activeRows = append(activeRows, currentRow)
					activeStates = append(activeStates, state)
				}
			}
		}

		// No more active readers
		if len(activeRows) == 0 {
			// Check if any reader had an error
			for _, state := range or.states {
				if state.err != nil {
					return nil, fmt.Errorf("reader %d error: %w", state.index, state.err)
				}
			}
			if len(batch.Rows) == 0 {
				return nil, io.EOF
			}
			break
		}

		// Use selector to determine which row to return
		selectedIdx := or.selector(activeRows)
		if selectedIdx < 0 || selectedIdx >= len(activeStates) {
			return nil, fmt.Errorf("selector returned invalid index %d, expected 0-%d",
				selectedIdx, len(activeStates)-1)
		}

		selectedState := activeStates[selectedIdx]
		currentRow := selectedState.getCurrentRow()
		if currentRow != nil {
			// Copy the row to the batch
			row := make(Row)
			for k, v := range currentRow {
				row[k] = v
			}
			batch.Rows = append(batch.Rows, row)

			// Consume the current row and advance if needed
			selectedState.consumeCurrentRow()
			if err := or.advance(selectedState); err != nil {
				return nil, fmt.Errorf("failed to advance reader %d: %w", selectedState.index, err)
			}
		}
	}

	// Update row count with successfully read rows
	if len(batch.Rows) > 0 {
		or.rowCount += int64(len(batch.Rows))
		return batch, nil
	}

	return nil, io.EOF
}

// Close closes all underlying readers and releases resources.
func (or *PreorderedMultisourceReader) Close() error {
	if or.closed {
		return nil
	}
	or.closed = true

	var errs []error
	for i, state := range or.states {
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
func (or *PreorderedMultisourceReader) ActiveReaderCount() int {
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
func (or *PreorderedMultisourceReader) TotalRowsReturned() int64 {
	return or.rowCount
}
