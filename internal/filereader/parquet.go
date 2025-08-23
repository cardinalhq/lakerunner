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

	"github.com/parquet-go/parquet-go"
)

// ParquetReader reads rows from a generic Parquet stream.
type ParquetReader struct {
	pf     *parquet.File
	pfr    *parquet.GenericReader[map[string]any]
	closed bool
}

// NewParquetReader creates a new ParquetReader for the given io.ReaderAt.
// The caller is responsible for closing the underlying reader.
func NewParquetReader(reader io.ReaderAt, size int64) (*ParquetReader, error) {
	pf, err := parquet.OpenFile(reader, size)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	// Use the file's schema to create a GenericReader
	pfr := parquet.NewGenericReader[map[string]any](pf, pf.Schema())

	return &ParquetReader{
		pf:  pf,
		pfr: pfr,
	}, nil
}

// Read populates the provided slice with as many rows as possible.
func (r *ParquetReader) Read(rows []Row) (int, error) {
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

	// Copy the data back to the provided rows slice
	for i := 0; i < n; i++ {
		resetRow(&rows[i])
		// Copy data from parquet row
		for k, v := range parquetRows[i] {
			rows[i][k] = v
		}
	}

	return n, err
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

	return nil
}
