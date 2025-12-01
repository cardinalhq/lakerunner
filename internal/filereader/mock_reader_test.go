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

	"github.com/cardinalhq/lakerunner/pipeline"
)

// Mock readers for testing the Next() interface

// mockReader is a test implementation of Reader with optional schema support
type mockReader struct {
	rows     []pipeline.Row
	index    int
	closed   bool
	name     string
	rowCount int64
	schema   *ReaderSchema
}

func newMockReader(name string, rows []pipeline.Row, schema *ReaderSchema) *mockReader {
	return &mockReader{
		rows:   rows,
		name:   name,
		schema: schema,
	}
}

func (m *mockReader) Next(ctx context.Context) (*Batch, error) {
	if m.closed {
		return nil, fmt.Errorf("reader %s is closed", m.name)
	}

	if m.index >= len(m.rows) {
		return nil, io.EOF
	}

	batch := pipeline.GetBatch()

	for batch.Len() < 100 && m.index < len(m.rows) {
		// Create new row and copy data from mock row
		row := batch.AddRow()
		for k, v := range m.rows[m.index] {
			row[k] = v
		}

		// Normalize row against schema (like real readers do)
		if m.schema != nil {
			if err := normalizeRow(ctx, row, m.schema); err != nil {
				pipeline.ReturnBatch(batch)
				return nil, fmt.Errorf("mock reader normalization failed: %w", err)
			}
		}

		m.index++
	}

	// Update row count with successfully read rows
	m.rowCount += int64(batch.Len())

	return batch, nil
}

func (m *mockReader) Close() error {
	m.closed = true
	return nil
}

func (m *mockReader) TotalRowsReturned() int64 {
	return m.rowCount
}

func (m *mockReader) GetSchema() *ReaderSchema {
	return m.schema
}

// errorReader is a test reader that always returns errors
type errorReader struct {
	closed   bool
	rowCount int64
}

func (e *errorReader) Next(ctx context.Context) (*Batch, error) {
	if e.closed {
		return nil, fmt.Errorf("reader is closed")
	}
	return nil, fmt.Errorf("test error")
}

func (e *errorReader) Close() error {
	e.closed = true
	return nil
}

func (e *errorReader) TotalRowsReturned() int64 {
	return e.rowCount
}

func (e *errorReader) GetSchema() *ReaderSchema {
	return NewReaderSchema()
}
