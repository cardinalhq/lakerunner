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
	"strconv"

	"github.com/parquet-go/parquet-go"
)

// PreorderedParquetRawReader reads rows from a generic Parquet stream.
type PreorderedParquetRawReader struct {
	pf        *parquet.File
	pfr       *parquet.GenericReader[map[string]any]
	closed    bool
	rowCount  int64
	batchSize int
}

// NewPreorderedParquetRawReader creates a new PreorderedParquetRawReader for the given io.ReaderAt.
// The caller is responsible for closing the underlying reader.
func NewPreorderedParquetRawReader(reader io.ReaderAt, size int64, batchSize int) (*PreorderedParquetRawReader, error) {
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

	if batchSize <= 0 {
		batchSize = 1000
	}

	return &PreorderedParquetRawReader{
		pf:        pf,
		pfr:       pfr,
		batchSize: batchSize,
	}, nil
}

// Next returns the next batch of rows from the parquet file.
func (r *PreorderedParquetRawReader) Next() (*Batch, error) {
	if r.closed || r.pfr == nil {
		return nil, errors.New("reader is closed or not initialized")
	}

	// Create fresh maps for parquet reader to populate
	parquetRows := make([]map[string]any, r.batchSize)
	for i := range parquetRows {
		parquetRows[i] = make(map[string]any)
	}

	n, err := r.pfr.Read(parquetRows)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("parquet reader error: %w", err)
	}
	if n == 0 {
		return nil, io.EOF
	}

	batch := &Batch{
		Rows: make([]Row, n),
	}

	// Transfer ownership instead of copying to reduce memory pressure
	// Also perform defensive conversion of _cardinalhq.tid field
	validRows := 0
	for i := 0; i < n; i++ {
		row := parquetRows[i]

		// Check and convert _cardinalhq.tid field if present
		if tidValue, exists := row["_cardinalhq.tid"]; exists {
			switch v := tidValue.(type) {
			case string:
				// Convert string to int64
				if tidInt64, err := strconv.ParseInt(v, 10, 64); err == nil {
					row["_cardinalhq.tid"] = tidInt64
				} else {
					// Drop row if conversion fails
					continue
				}
			case int64:
				// Already correct type, no conversion needed
			default:
				// Drop row if _cardinalhq.tid is neither string nor int64
				continue
			}
		}

		batch.Rows[validRows] = row // Transfer map ownership, avoid expensive copy
		validRows++
	}

	// Resize batch to only include valid rows
	batch.Rows = batch.Rows[:validRows]

	// Only increment rowCount for successfully processed rows
	r.rowCount += int64(validRows)

	// Return EOF if no valid rows remain
	if validRows == 0 && err == io.EOF {
		return nil, io.EOF
	}
	if validRows == 0 {
		// If we dropped all rows but haven't reached EOF, try reading more
		return r.Next()
	}

	return batch, err
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

// TotalRowsReturned returns the total number of rows that have been successfully returned via Next().
func (r *PreorderedParquetRawReader) TotalRowsReturned() int64 {
	return r.rowCount
}
