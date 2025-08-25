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
	"compress/gzip"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readAllRows is a helper function that reads all rows from a reader
func readAllRows(reader Reader) ([]Row, error) {
	var allRows []Row
	buffer := make([]Row, 10) // Read in batches of 10

	for {
		// Initialize Row maps in the buffer
		for i := range buffer {
			buffer[i] = make(Row)
		}

		n, err := reader.Read(buffer)
		for i := 0; i < n; i++ {
			// Copy the row since we're reusing the buffer
			rowCopy := make(Row)
			for k, v := range buffer[i] {
				rowCopy[k] = v
			}
			allRows = append(allRows, rowCopy)
		}
		if errors.Is(err, io.EOF) {
			return allRows, nil
		}
		if err != nil {
			return allRows, err
		}
	}
}

// TestJSONLinesReaderEOFHandling tests that JSONLinesReader correctly handles all data before EOF
func TestJSONLinesReaderEOFHandling(t *testing.T) {
	// Test data - 3 JSON lines without final newline to test EOF edge case
	jsonData := `{"line": 1, "value": "first"}
{"line": 2, "value": "second"}
{"line": 3, "value": "third"}`

	reader, err := NewJSONLinesReader(io.NopCloser(bytes.NewReader([]byte(jsonData))))
	require.NoError(t, err)
	defer reader.Close()

	rows, err := readAllRows(reader)
	require.NoError(t, err)

	// Should have read all 3 rows despite EOF
	assert.Len(t, rows, 3)
	assert.Equal(t, float64(1), rows[0]["line"])
	assert.Equal(t, "first", rows[0]["value"])
	assert.Equal(t, float64(2), rows[1]["line"])
	assert.Equal(t, "second", rows[1]["value"])
	assert.Equal(t, float64(3), rows[2]["line"])
	assert.Equal(t, "third", rows[2]["value"])
}

// TestJSONLinesReaderGzipEOFHandling tests EOF handling with gzipped JSON
func TestJSONLinesReaderGzipEOFHandling(t *testing.T) {
	// Create gzipped JSON data
	jsonData := `{"line": 1, "compressed": true}
{"line": 2, "compressed": true}`

	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	_, err := gzWriter.Write([]byte(jsonData))
	require.NoError(t, err)
	require.NoError(t, gzWriter.Close())

	// Create gzip reader
	gzReader, err := gzip.NewReader(&buf)
	require.NoError(t, err)

	// Test our JSON reader with the gzip reader
	jsonReader, err := NewJSONLinesReader(gzReader)
	require.NoError(t, err)
	defer jsonReader.Close()

	rows, err := readAllRows(jsonReader)
	require.NoError(t, err)

	// Should have read both rows
	assert.Len(t, rows, 2)
	assert.Equal(t, float64(1), rows[0]["line"])
	assert.Equal(t, true, rows[0]["compressed"])
	assert.Equal(t, float64(2), rows[1]["line"])
	assert.Equal(t, true, rows[1]["compressed"])
}

// TestJSONLinesReaderEmptyLinesEOF tests EOF handling with empty lines mixed in
func TestJSONLinesReaderEmptyLinesEOF(t *testing.T) {
	// JSON with empty lines
	jsonData := `{"line": 1}

{"line": 2}

`

	reader, err := NewJSONLinesReader(io.NopCloser(bytes.NewReader([]byte(jsonData))))
	require.NoError(t, err)
	defer reader.Close()

	rows, err := readAllRows(reader)
	require.NoError(t, err)

	// Should skip empty lines and read both JSON objects
	assert.Len(t, rows, 2)
	assert.Equal(t, float64(1), rows[0]["line"])
	assert.Equal(t, float64(2), rows[1]["line"])
}

// MockReaderWithDataAndEOF simulates a reader that returns data and EOF on the same call
type MockReaderWithDataAndEOF struct {
	data   []byte
	called bool
}

func (m *MockReaderWithDataAndEOF) Read(p []byte) (n int, err error) {
	if m.called {
		return 0, io.EOF
	}
	m.called = true

	n = copy(p, m.data)
	// Return both data and EOF on the same call - this is valid per io.Reader contract
	return n, io.EOF
}

func (m *MockReaderWithDataAndEOF) Close() error { return nil }

type mockReadCloser struct {
	io.Reader
	closed bool
}

func (m *mockReadCloser) Close() error {
	m.closed = true
	return nil
}

// TestJSONLinesReaderWithMockEOF tests the specific n>0 && EOF case using a mock
func TestJSONLinesReaderWithMockEOF(t *testing.T) {
	// Create mock reader that returns data and EOF on same call
	jsonLine := `{"test": "data"}`
	mockReader := &MockReaderWithDataAndEOF{data: []byte(jsonLine)}

	reader, err := NewJSONLinesReader(mockReader)
	require.NoError(t, err)
	defer reader.Close()

	// Should read the data successfully
	rows := make([]Row, 1)
	rows[0] = make(Row)
	n, err := reader.Read(rows)
	require.Equal(t, 1, n)
	require.NoError(t, err)
	assert.Equal(t, "data", rows[0]["test"])

	// Next call should return EOF
	rows[0] = make(Row)
	n, err = reader.Read(rows)
	assert.Equal(t, 0, n)
	assert.True(t, errors.Is(err, io.EOF))
}

// TestJSONLinesReaderClose tests that Close works properly
func TestJSONLinesReaderClose(t *testing.T) {
	jsonData := `{"test": "data"}`
	mock := &mockReadCloser{Reader: bytes.NewReader([]byte(jsonData))}
	reader, err := NewJSONLinesReader(mock)
	require.NoError(t, err)

	// Should be able to read before closing
	rows := make([]Row, 1)
	rows[0] = make(Row)
	n, err := reader.Read(rows)
	require.Equal(t, 1, n)
	require.NoError(t, err)
	assert.Equal(t, "data", rows[0]["test"])

	// Close should work
	err = reader.Close()
	assert.NoError(t, err)
	assert.True(t, mock.closed)

	// Reading after close should return error
	rows[0] = make(Row)
	_, err = reader.Read(rows)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	// Close should be idempotent
	err = reader.Close()
	assert.NoError(t, err)
}

// TestJSONLinesReaderBatchProcessing tests reading multiple rows in batches
func TestJSONLinesReaderBatchProcessing(t *testing.T) {
	jsonData := `{"line": 1}
{"line": 2}
{"line": 3}
{"line": 4}
{"line": 5}`

	reader, err := NewJSONLinesReader(io.NopCloser(bytes.NewReader([]byte(jsonData))))
	require.NoError(t, err)
	defer reader.Close()

	// Read in batch of 3
	rows := make([]Row, 3)
	for i := range rows {
		rows[i] = make(Row)
	}

	n, err := reader.Read(rows)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, float64(1), rows[0]["line"])
	assert.Equal(t, float64(2), rows[1]["line"])
	assert.Equal(t, float64(3), rows[2]["line"])

	// Read remaining 2
	for i := range rows {
		rows[i] = make(Row)
	}
	n, err = reader.Read(rows)
	// May return EOF with data or just the data
	if err != nil {
		require.True(t, errors.Is(err, io.EOF), "Expected EOF or no error, got %v", err)
	}
	assert.Equal(t, 2, n)
	assert.Equal(t, float64(4), rows[0]["line"])
	assert.Equal(t, float64(5), rows[1]["line"])

	// Next read should return EOF
	for i := range rows {
		rows[i] = make(Row)
	}
	n, err = reader.Read(rows)
	assert.Equal(t, 0, n)
	assert.True(t, errors.Is(err, io.EOF))
}
