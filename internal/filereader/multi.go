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

// MultiReader reads from multiple readers sequentially in the order provided.
// It reads all rows from the first reader, then all rows from the second reader, etc.
// This is useful when you want to concatenate multiple files without any ordering requirements.
type MultiReader struct {
	readers      []Reader
	currentIndex int
	closed       bool
}

// NewMultiReader creates a new MultiReader that reads from the provided readers sequentially.
// Readers will be closed when the MultiReader is closed.
func NewMultiReader(readers []Reader) (*MultiReader, error) {
	if len(readers) == 0 {
		return nil, errors.New("at least one reader is required")
	}

	// Validate that all readers are non-nil
	for i, reader := range readers {
		if reader == nil {
			return nil, fmt.Errorf("reader at index %d is nil", i)
		}
	}

	return &MultiReader{
		readers:      readers,
		currentIndex: 0,
	}, nil
}

// Read populates the provided slice with as many rows as possible from the current reader,
// advancing to the next reader when the current reader is exhausted.
func (mr *MultiReader) Read(rows []Row) (int, error) {
	if mr.closed {
		return 0, errors.New("reader is closed")
	}

	if len(rows) == 0 {
		return 0, nil
	}

	totalRead := 0

	// Loop through readers until we fill the slice or exhaust all readers
	for totalRead < len(rows) && mr.currentIndex < len(mr.readers) {
		currentReader := mr.readers[mr.currentIndex]

		// Read from current reader into remaining slice space
		remainingRows := rows[totalRead:]
		n, err := currentReader.Read(remainingRows)
		totalRead += n

		if err != nil {
			if errors.Is(err, io.EOF) {
				// Current reader is exhausted, move to the next one
				mr.currentIndex++
				continue
			}
			return totalRead, fmt.Errorf("error reading from reader %d: %w", mr.currentIndex, err)
		}
	}

	// Return EOF if we've exhausted all readers and didn't read any data this call
	if totalRead == 0 && mr.currentIndex >= len(mr.readers) {
		return 0, io.EOF
	}

	return totalRead, nil
}

// Close closes all underlying readers and releases resources.
func (mr *MultiReader) Close() error {
	if mr.closed {
		return nil
	}
	mr.closed = true

	var errs []error
	for i, reader := range mr.readers {
		if reader != nil {
			if err := reader.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close reader %d: %w", i, err))
			}
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// CurrentReaderIndex returns the index of the reader currently being read from.
// Returns -1 if all readers are exhausted or the reader is closed.
func (mr *MultiReader) CurrentReaderIndex() int {
	if mr.closed || mr.currentIndex >= len(mr.readers) {
		return -1
	}
	return mr.currentIndex
}

// TotalReaderCount returns the total number of readers in this MultiReader.
func (mr *MultiReader) TotalReaderCount() int {
	return len(mr.readers)
}

// RemainingReaderCount returns the number of readers that haven't been fully processed yet.
func (mr *MultiReader) RemainingReaderCount() int {
	if mr.closed {
		return 0
	}
	remaining := len(mr.readers) - mr.currentIndex
	if remaining < 0 {
		return 0
	}
	return remaining
}
