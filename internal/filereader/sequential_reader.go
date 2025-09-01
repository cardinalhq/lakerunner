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
)

// SequentialReader reads from multiple readers sequentially in the order provided.
// It reads all rows from the first reader, then all rows from the second reader, etc.
// This is useful when you want to concatenate multiple files without any ordering requirements.
type SequentialReader struct {
	readers      []Reader
	currentIndex int
	closed       bool
	rowCount     int64
	batchSize    int
}

// NewSequentialReader creates a new SequentialReader that reads from the provided readers sequentially.
// Readers will be closed when the SequentialReader is closed.
func NewSequentialReader(readers []Reader, batchSize int) (*SequentialReader, error) {
	if len(readers) == 0 {
		return nil, errors.New("at least one reader is required")
	}

	if batchSize <= 0 {
		batchSize = 1000
	}

	// Validate that all readers are non-nil
	for i, reader := range readers {
		if reader == nil {
			return nil, fmt.Errorf("reader at index %d is nil", i)
		}
	}

	return &SequentialReader{
		readers:      readers,
		currentIndex: 0,
		batchSize:    batchSize,
	}, nil
}

func (sr *SequentialReader) Next(ctx context.Context) (*Batch, error) {
	if sr.closed {
		return nil, io.EOF
	}

	for sr.currentIndex < len(sr.readers) {
		currentReader := sr.readers[sr.currentIndex]

		batch, err := currentReader.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Current reader is exhausted, move to the next one
				sr.currentIndex++
				continue
			}
			return nil, fmt.Errorf("error reading from reader %d: %w", sr.currentIndex, err)
		}

		// Track rows read from underlying readers
		rowsInCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
			attribute.String("reader", "SequentialReader"),
		))

		// Update our row count with successfully read rows
		sr.rowCount += int64(batch.Len())

		// Track rows output to downstream
		rowsOutCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
			attribute.String("reader", "SequentialReader"),
		))

		return batch, nil
	}

	// All readers exhausted
	return nil, io.EOF
}

// Close closes all underlying readers and releases resources.
func (sr *SequentialReader) Close() error {
	if sr.closed {
		return nil
	}
	sr.closed = true

	var errs []error
	for i, reader := range sr.readers {
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
func (sr *SequentialReader) CurrentReaderIndex() int {
	if sr.closed || sr.currentIndex >= len(sr.readers) {
		return -1
	}
	return sr.currentIndex
}

// TotalReaderCount returns the total number of readers in this SequentialReader.
func (sr *SequentialReader) TotalReaderCount() int {
	return len(sr.readers)
}

// RemainingReaderCount returns the number of readers that haven't been fully processed yet.
func (sr *SequentialReader) RemainingReaderCount() int {
	if sr.closed {
		return 0
	}
	remaining := len(sr.readers) - sr.currentIndex
	if remaining < 0 {
		return 0
	}
	return remaining
}

// TotalRowsReturned returns the total number of rows that have been successfully returned via Next() from all readers.
func (sr *SequentialReader) TotalRowsReturned() int64 {
	return sr.rowCount
}
