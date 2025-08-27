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

package pipeline

import (
	"io"
)

// LegacyReader is an interface that matches the old filereader.Reader interface.
// This allows us to wrap old-style readers to work with the new pipeline.
type LegacyReader interface {
	Read(rows []Row) (n int, err error)
	Close() error
	TotalRowsReturned() int64
}

// LegacyWriter is an interface that matches the old parquetwriter interface.
// This allows us to wrap new pipeline streams to work with old-style writers.
type LegacyWriter interface {
	Write(row Row) error
}

// LegacyReaderAdapter adapts an old-style Reader to the new pipeline Reader interface.
type LegacyReaderAdapter struct {
	legacy    LegacyReader
	pool      *batchPool
	batchSize int
	closed    bool
}

// NewLegacyReaderAdapter creates a new adapter for an old-style reader.
func NewLegacyReaderAdapter(legacy LegacyReader, batchSize int) *LegacyReaderAdapter {
	return &LegacyReaderAdapter{
		legacy:    legacy,
		pool:      newBatchPool(batchSize),
		batchSize: batchSize,
	}
}

func (l *LegacyReaderAdapter) Next() (*Batch, error) {
	if l.closed {
		return nil, io.EOF
	}

	// Create a temporary slice for the legacy reader
	rows := make([]Row, l.batchSize)
	for i := range rows {
		rows[i] = make(Row)
	}

	n, err := l.legacy.Read(rows)
	if err != nil {
		if err == io.EOF {
			l.closed = true
		}
		if n == 0 {
			return nil, err
		}
		// We got some rows even with error, return them
	}

	if n == 0 {
		return nil, io.EOF
	}

	// Convert to pipeline batch
	batch := l.pool.Get()
	batch.Rows = make([]Row, n)
	for i := 0; i < n; i++ {
		batch.Rows[i] = rows[i] // Transfer ownership
	}

	return batch, err
}

func (l *LegacyReaderAdapter) Close() error {
	l.closed = true
	return l.legacy.Close()
}

// LegacyWriterAdapter allows consuming from a pipeline Reader and writing to an old-style Writer.
type LegacyWriterAdapter struct {
	reader Reader
	writer LegacyWriter
}

// NewLegacyWriterAdapter creates a new adapter that consumes from a Reader and writes to a LegacyWriter.
func NewLegacyWriterAdapter(reader Reader, writer LegacyWriter) *LegacyWriterAdapter {
	return &LegacyWriterAdapter{
		reader: reader,
		writer: writer,
	}
}

// ConsumeAll reads all data from the reader and writes it to the writer.
func (l *LegacyWriterAdapter) ConsumeAll() error {
	defer l.reader.Close()

	for {
		batch, err := l.reader.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		for _, row := range batch.Rows {
			if err := l.writer.Write(row); err != nil {
				return err
			}
		}
	}
}

// SequentialReader chains multiple readers to be consumed one after another.
type SequentialReader struct {
	readers      []Reader
	currentIndex int
	closed       bool
}

// NewSequentialReader creates a new SequentialReader from multiple readers.
func NewSequentialReader(readers []Reader) *SequentialReader {
	return &SequentialReader{
		readers: readers,
	}
}

func (s *SequentialReader) Next() (*Batch, error) {
	if s.closed {
		return nil, io.EOF
	}

	for s.currentIndex < len(s.readers) {
		batch, err := s.readers[s.currentIndex].Next()
		if err == io.EOF {
			// Current reader is exhausted, move to next
			s.currentIndex++
			continue
		}
		if err != nil {
			return nil, err
		}
		return batch, nil
	}

	// All readers exhausted
	s.closed = true
	return nil, io.EOF
}

func (s *SequentialReader) Close() error {
	s.closed = true
	var firstErr error
	for _, reader := range s.readers {
		if err := reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// TimeOrderedMergeReader merges multiple readers in timestamp order.
type TimeOrderedMergeReader struct {
	readers        []Reader
	pool           *batchPool
	pending        []Row  // one row from each reader
	hasData        []bool // whether we have data from each reader
	timestampField string
	closed         bool
}

// NewTimeOrderedMergeReader creates a reader that merges multiple readers in timestamp order.
func NewTimeOrderedMergeReader(readers []Reader, pool *batchPool, timestampField string) *TimeOrderedMergeReader {
	return &TimeOrderedMergeReader{
		readers:        readers,
		pool:           pool,
		pending:        make([]Row, len(readers)),
		hasData:        make([]bool, len(readers)),
		timestampField: timestampField,
	}
}

func (t *TimeOrderedMergeReader) Next() (*Batch, error) {
	if t.closed {
		return nil, io.EOF
	}

	batch := t.pool.Get()

	for len(batch.Rows) < cap(batch.Rows) {
		// Fill any empty pending slots
		for i, hasData := range t.hasData {
			if !hasData {
				b, err := t.readers[i].Next()
				if err == io.EOF {
					continue // This reader is exhausted
				}
				if err != nil {
					return nil, err
				}
				if len(b.Rows) > 0 {
					t.pending[i] = b.Rows[0] // Take first row from batch
					t.hasData[i] = true
					// TODO: Handle remaining rows in batch
				}
			}
		}

		// Find reader with earliest timestamp
		earliestIdx := -1
		var earliestTs int64
		for i, hasData := range t.hasData {
			if hasData {
				ts := extractTimestamp(t.pending[i], t.timestampField)
				if earliestIdx == -1 || ts < earliestTs {
					earliestIdx = i
					earliestTs = ts
				}
			}
		}

		if earliestIdx == -1 {
			// No more data from any reader
			break
		}

		// Take the earliest row
		batch.Rows = append(batch.Rows, t.pending[earliestIdx])
		t.hasData[earliestIdx] = false
	}

	if len(batch.Rows) == 0 {
		t.closed = true
		return nil, io.EOF
	}

	return batch, nil
}

func (t *TimeOrderedMergeReader) Close() error {
	t.closed = true
	var firstErr error
	for _, reader := range t.readers {
		if err := reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// extractTimestamp extracts a timestamp from a row, handling various numeric types.
func extractTimestamp(row Row, fieldName string) int64 {
	val, exists := row[fieldName]
	if !exists {
		return 0
	}

	switch v := val.(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	default:
		return 0
	}
}
