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
	reader  Reader
	current Row
	done    bool
	index   int
	err     error
}

// OrderedReader implements merge-sort style reading across multiple pre-sorted readers.
// It assumes each individual reader returns rows in sorted order according to the
// provided selector function.
type OrderedReader struct {
	states   []*readerState
	selector SelectFunc
	closed   bool
}

// NewOrderedReader creates a new OrderedReader that merges rows from multiple readers
// in sorted order. The selector function determines which row should be returned next.
//
// Requirements:
// - Each reader must return rows in sorted order (according to the selector logic)
// - The selector function must be consistent (same inputs -> same output)
// - Readers will be closed when the OrderedReader is closed
func NewOrderedReader(readers []Reader, selector SelectFunc) (*OrderedReader, error) {
	if len(readers) == 0 {
		return nil, errors.New("at least one reader is required")
	}
	if selector == nil {
		return nil, errors.New("selector function is required")
	}

	states := make([]*readerState, len(readers))
	for i, reader := range readers {
		states[i] = &readerState{
			reader: reader,
			index:  i,
		}
	}

	or := &OrderedReader{
		states:   states,
		selector: selector,
	}

	// Prime all readers by loading their first rows
	if err := or.primeReaders(); err != nil {
		or.Close()
		return nil, fmt.Errorf("failed to prime readers: %w", err)
	}

	return or, nil
}

// primeReaders loads the first row from each reader.
func (or *OrderedReader) primeReaders() error {
	for _, state := range or.states {
		if err := or.advance(state); err != nil {
			return fmt.Errorf("failed to prime reader %d: %w", state.index, err)
		}
	}
	return nil
}

// advance loads the next row for the given reader state.
func (or *OrderedReader) advance(state *readerState) error {
	if state.done || state.err != nil {
		return state.err
	}

	row, err := state.reader.GetRow()
	if err != nil {
		if errors.Is(err, io.EOF) {
			state.done = true
			return nil
		}
		state.err = err
		return err
	}

	state.current = row
	return nil
}

// GetRow returns the next row in sorted order across all readers.
func (or *OrderedReader) GetRow() (Row, error) {
	if or.closed {
		return nil, errors.New("reader is closed")
	}

	// Collect all active (non-done, non-error) readers and their current rows
	var activeRows []Row
	var activeStates []*readerState

	for _, state := range or.states {
		if !state.done && state.err == nil {
			activeRows = append(activeRows, state.current)
			activeStates = append(activeStates, state)
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
		return nil, io.EOF
	}

	// Use selector to determine which row to return
	selectedIdx := or.selector(activeRows)
	if selectedIdx < 0 || selectedIdx >= len(activeStates) {
		return nil, fmt.Errorf("selector returned invalid index %d, expected 0-%d",
			selectedIdx, len(activeStates)-1)
	}

	selectedState := activeStates[selectedIdx]
	selectedRow := selectedState.current

	// Advance the selected reader to its next row
	if err := or.advance(selectedState); err != nil {
		return nil, fmt.Errorf("failed to advance reader %d: %w", selectedState.index, err)
	}

	return selectedRow, nil
}

// Close closes all underlying readers and releases resources.
func (or *OrderedReader) Close() error {
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
func (or *OrderedReader) ActiveReaderCount() int {
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
