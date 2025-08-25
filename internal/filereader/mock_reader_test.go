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
	"fmt"
	"io"
)

// mockReader is a test implementation of Reader
type mockReader struct {
	rows   []Row
	index  int
	closed bool
	name   string
}

func newMockReader(name string, rows []Row) *mockReader {
	return &mockReader{
		rows: rows,
		name: name,
	}
}

func (m *mockReader) Read(rows []Row) (int, error) {
	if m.closed {
		return 0, fmt.Errorf("reader %s is closed", m.name)
	}

	if len(rows) == 0 {
		return 0, nil
	}

	n := 0
	for n < len(rows) && m.index < len(m.rows) {
		resetRow(&rows[n])

		// Copy data from mock row
		for k, v := range m.rows[m.index] {
			rows[n][k] = v
		}

		m.index++
		n++
	}

	if n == 0 {
		return 0, io.EOF
	}

	return n, nil
}

func (m *mockReader) Close() error {
	m.closed = true
	return nil
}

// errorReader is a test reader that always returns errors
type errorReader struct {
	closed bool
}

func (e *errorReader) Read(rows []Row) (int, error) {
	if e.closed {
		return 0, fmt.Errorf("reader is closed")
	}
	return 0, fmt.Errorf("test error")
}

func (e *errorReader) Close() error {
	e.closed = true
	return nil
}
