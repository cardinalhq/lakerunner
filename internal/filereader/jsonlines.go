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

// GetRow returns the next row from the JSON lines stream.
func (r *JSONLinesReader) GetRow() (Row, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	if !r.scanner.Scan() {
		// Check for scanner error
		if err := r.scanner.Err(); err != nil {
			return nil, fmt.Errorf("scanner error reading at line %d: %w", r.rowIndex+1, err)
		}
		// End of file
		return nil, io.EOF
	}

	line := strings.TrimSpace(r.scanner.Text())
	r.rowIndex++

	// Skip empty lines
	if line == "" {
		return r.GetRow() // Recursive call to get next non-empty line
	}

	// Parse JSON
	var row map[string]any
	if err := json.Unmarshal([]byte(line), &row); err != nil {
		return nil, fmt.Errorf("failed to parse JSON at line %d: %w", r.rowIndex, err)
	}

	return Row(row), nil
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

// RowIndex returns the current line number being processed (1-based).
func (r *JSONLinesReader) RowIndex() int {
	return r.rowIndex
}
