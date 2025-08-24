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
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// JSONLinesReader reads rows from a JSON lines stream.
type JSONLinesReader struct {
	scanner  *bufio.Scanner
	rowIndex int
	closed   bool
	rowCount int64
}

// NewJSONLinesReader creates a new JSONLinesReader for the given io.Reader.
// The caller is responsible for closing the underlying reader.
func NewJSONLinesReader(reader io.Reader) (*JSONLinesReader, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024) // 1MB max line size

	return &JSONLinesReader{
		scanner:  scanner,
		rowIndex: 0,
	}, nil
}

// Read populates the provided slice with as many rows as possible.
func (r *JSONLinesReader) Read(rows []Row) (int, error) {
	if r.closed {
		return 0, fmt.Errorf("reader is closed")
	}

	if len(rows) == 0 {
		return 0, nil
	}

	n := 0
	for n < len(rows) {
		if !r.scanner.Scan() {
			// Check for scanner error
			if err := r.scanner.Err(); err != nil {
				return n, fmt.Errorf("scanner error reading at line %d: %w", r.rowIndex+1, err)
			}
			// End of file
			return n, io.EOF
		}

		line := strings.TrimSpace(r.scanner.Text())
		r.rowIndex++

		// Skip empty lines
		if line == "" {
			continue
		}

		resetRow(&rows[n])

		// Parse JSON
		var rowData map[string]any
		if err := json.Unmarshal([]byte(line), &rowData); err != nil {
			return n, fmt.Errorf("failed to parse JSON at line %d: %w", r.rowIndex, err)
		}

		// Copy data to Row
		for k, v := range rowData {
			rows[n][k] = v
		}

		n++
	}

	// Update row count with successfully read rows
	if n > 0 {
		r.rowCount += int64(n)
	}

	return n, nil
}

// Close closes the reader.
// The caller is responsible for closing the underlying reader.
func (r *JSONLinesReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	r.scanner = nil
	return nil
}

// RowCount returns the total number of rows that have been successfully read.
func (r *JSONLinesReader) RowCount() int64 {
	return r.rowCount
}
