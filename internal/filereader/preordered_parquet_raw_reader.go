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
	"maps"

	"github.com/parquet-go/parquet-go"
)

// PreorderedParquetRawReader reads rows from a generic Parquet stream.
type PreorderedParquetRawReader struct {
	pf       *parquet.File
	pfr      *parquet.GenericReader[map[string]any]
	closed   bool
	rowCount int64
}

// NewPreorderedParquetRawReader creates a new PreorderedParquetRawReader for the given io.ReaderAt.
// The caller is responsible for closing the underlying reader.
func NewPreorderedParquetRawReader(reader io.ReaderAt, size int64) (*PreorderedParquetRawReader, error) {
	pf, err := parquet.OpenFile(reader, size)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	// Use the file's schema to create a GenericReader
	pfr := parquet.NewGenericReader[map[string]any](pf, pf.Schema())

	// Check if file has data
	if pf.NumRows() == 0 {
		return nil, fmt.Errorf("parquet file has no rows")
	}

	return &PreorderedParquetRawReader{
		pf:  pf,
		pfr: pfr,
	}, nil
}

// Read populates the provided slice with as many rows as possible.
func (r *PreorderedParquetRawReader) Read(rows []Row) (int, error) {
	if r.closed || r.pfr == nil {
		return 0, errors.New("reader is closed or not initialized")
	}

	if len(rows) == 0 {
		return 0, nil
	}

	// Create fresh maps for parquet reader to populate
	parquetRows := make([]map[string]any, len(rows))
	for i := range parquetRows {
		parquetRows[i] = make(map[string]any)
	}

	n, err := r.pfr.Read(parquetRows)
	if err != nil && err != io.EOF {
		return 0, fmt.Errorf("parquet reader error: %w", err)
	}
	if n == 0 && err == nil {
		// No data available but no error - treat as EOF
		return 0, io.EOF
	}

	// Copy the data back to the provided rows slice
	// TODO see if we can avoid this copy
	for i := range n {
		resetRow(&rows[i])
		// Copy data from parquet row
		maps.Copy(rows[i], parquetRows[i])
	}

	// Only increment rowCount for successfully read rows
	if n > 0 {
		r.rowCount += int64(n)
	}

	return n, err
}

// Close closes the reader and releases resources.
func (r *PreorderedParquetRawReader) Close() error {
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

	return nil
}

// TotalRowsReturned returns the total number of rows that have been successfully returned via Read().
func (r *PreorderedParquetRawReader) TotalRowsReturned() int64 {
	return r.rowCount
}
