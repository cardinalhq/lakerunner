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
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"
)

// ParquetReader reads rows from a generic Parquet stream.
type ParquetReader struct {
	data   []byte
	pf     *parquet.File
	pfr    *parquet.GenericReader[map[string]any]
	closed bool
}

// NewParquetReader creates a new ParquetReader for the given io.Reader.
// The entire content is read into memory since parquet requires random access.
// The caller is responsible for closing the underlying reader.
func NewParquetReader(reader io.Reader) (*ParquetReader, error) {
	// Read entire content into memory since parquet requires random access
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet data: %w", err)
	}

	bytesReader := bytes.NewReader(data)
	pf, err := parquet.OpenFile(bytesReader, int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	pfr := parquet.NewGenericReader[map[string]any](pf)

	return &ParquetReader{
		data: data,
		pf:   pf,
		pfr:  pfr,
	}, nil
}

// GetRow returns the next row from the Parquet stream.
func (r *ParquetReader) GetRow() (Row, error) {
	if r.closed || r.pfr == nil {
		return nil, errors.New("reader is closed or not initialized")
	}

	rows := make([]map[string]any, 1)
	rows[0] = make(map[string]any)

	n, err := r.pfr.Read(rows)

	// Handle case where we get data and EOF on same call
	if n == 1 {
		row := rows[0]
		// If we got data, return it regardless of EOF
		// The next call will return io.EOF with no data
		return Row(row), nil
	}

	// No data was read
	if n == 0 {
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("failed to read row: %w", err)
		}
		// Shouldn't happen - no data and no error
		return nil, fmt.Errorf("no data read and no error")
	}

	// Unexpected case - got more than 1 row
	return nil, fmt.Errorf("expected to read 1 row, got %d", n)
}

// Close closes the reader and releases resources.
func (r *ParquetReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	if r.pfr != nil {
		if err := r.pfr.Close(); err != nil {
			return fmt.Errorf("failed to close parquet reader: %w", err)
		}
		r.pfr = nil
	}
	r.pf = nil
	r.data = nil

	return nil
}

// NumRows returns the total number of rows in the Parquet file, if available.
func (r *ParquetReader) NumRows() int64 {
	if r.pfr == nil {
		return 0
	}
	return r.pfr.NumRows()
}
