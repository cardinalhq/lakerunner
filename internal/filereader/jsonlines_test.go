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
	"context"
	"errors"
	"io"
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// readAllRows is a helper function that reads all rows from a reader
func readAllRows(reader Reader) ([]pipeline.Row, error) {
	var allRows []pipeline.Row
	for {
		batch, err := reader.Next(context.TODO())
		if batch != nil {
			// Copy the rows since they are owned by the reader
			for i := 0; i < batch.Len(); i++ {
				row := batch.Get(i)
				rowCopy := make(pipeline.Row)
				maps.Copy(rowCopy, row)
				allRows = append(allRows, rowCopy)
			}
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

	reader, err := NewJSONLinesReader(io.NopCloser(bytes.NewReader([]byte(jsonData))), 100)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	rows, err := readAllRows(reader)
	require.NoError(t, err)

	// Should have read all 3 rows despite EOF
	assert.Len(t, rows, 3)
	assert.Equal(t, float64(1), rows[0][wkk.NewRowKey("line")])
	assert.Equal(t, "first", rows[0][wkk.NewRowKey("value")])
	assert.Equal(t, float64(2), rows[1][wkk.NewRowKey("line")])
	assert.Equal(t, "second", rows[1][wkk.NewRowKey("value")])
	assert.Equal(t, float64(3), rows[2][wkk.NewRowKey("line")])
	assert.Equal(t, "third", rows[2][wkk.NewRowKey("value")])
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
	jsonReader, err := NewJSONLinesReader(gzReader, 100)
	require.NoError(t, err)
	defer func() { _ = jsonReader.Close() }()

	rows, err := readAllRows(jsonReader)
	require.NoError(t, err)

	// Should have read both rows
	assert.Len(t, rows, 2)
	assert.Equal(t, float64(1), rows[0][wkk.NewRowKey("line")])
	assert.Equal(t, true, rows[0][wkk.NewRowKey("compressed")])
	assert.Equal(t, float64(2), rows[1][wkk.NewRowKey("line")])
	assert.Equal(t, true, rows[1][wkk.NewRowKey("compressed")])
}

// TestJSONLinesReaderEmptyLinesEOF tests EOF handling with empty lines mixed in
func TestJSONLinesReaderEmptyLinesEOF(t *testing.T) {
	// JSON with empty lines
	jsonData := `{"line": 1}

{"line": 2}

`

	reader, err := NewJSONLinesReader(io.NopCloser(bytes.NewReader([]byte(jsonData))), 100)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	rows, err := readAllRows(reader)
	require.NoError(t, err)

	// Should skip empty lines and read both JSON objects
	assert.Len(t, rows, 2)
	assert.Equal(t, float64(1), rows[0][wkk.NewRowKey("line")])
	assert.Equal(t, float64(2), rows[1][wkk.NewRowKey("line")])
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

// seekableReadCloser wraps a bytes.Reader to provide io.ReadCloser with io.Seeker support
type seekableReadCloser struct {
	*bytes.Reader
	closed bool
}

func newSeekableReadCloser(data []byte) *seekableReadCloser {
	return &seekableReadCloser{
		Reader: bytes.NewReader(data),
	}
}

func (s *seekableReadCloser) Close() error {
	s.closed = true
	return nil
}

// TestJSONLinesReaderWithMockEOF tests the specific n>0 && EOF case using a mock
func TestJSONLinesReaderWithMockEOF(t *testing.T) {
	// Create mock reader that returns data and EOF on same call
	jsonLine := `{"test": "data"}`
	mockReader := &MockReaderWithDataAndEOF{data: []byte(jsonLine)}

	reader, err := NewJSONLinesReader(mockReader, 100)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Should read the data successfully
	batch, err := reader.Next(context.TODO())
	require.NoError(t, err)
	require.NotNil(t, batch)
	assert.Equal(t, 1, batch.Len())
	assert.Equal(t, "data", batch.Get(0)[wkk.NewRowKey("test")])

	// Next call should return EOF
	batch, err = reader.Next(context.TODO())
	assert.Nil(t, batch)
	assert.True(t, errors.Is(err, io.EOF))
}

// TestJSONLinesReaderClose tests that Close works properly
func TestJSONLinesReaderClose(t *testing.T) {
	jsonData := `{"test": "data"}`
	mock := &mockReadCloser{Reader: bytes.NewReader([]byte(jsonData))}
	reader, err := NewJSONLinesReader(mock, 100)
	require.NoError(t, err)

	// Should be able to read before closing
	batch, err := reader.Next(context.TODO())
	require.NoError(t, err)
	require.NotNil(t, batch)
	assert.Equal(t, 1, batch.Len())
	assert.Equal(t, "data", batch.Get(0)[wkk.NewRowKey("test")])

	// Close should work
	err = reader.Close()
	assert.NoError(t, err)
	assert.True(t, mock.closed)

	// Reading after close should return EOF
	_, err = reader.Next(context.TODO())
	assert.True(t, errors.Is(err, io.EOF))

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

	reader, err := NewJSONLinesReader(io.NopCloser(bytes.NewReader([]byte(jsonData))), 100)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Read all data using Next()
	allRows, err := readAllRows(reader)
	require.NoError(t, err)
	assert.Len(t, allRows, 5)
	assert.Equal(t, float64(1), allRows[0][wkk.NewRowKey("line")])
	assert.Equal(t, float64(2), allRows[1][wkk.NewRowKey("line")])
	assert.Equal(t, float64(3), allRows[2][wkk.NewRowKey("line")])
	assert.Equal(t, float64(4), allRows[3][wkk.NewRowKey("line")])
	assert.Equal(t, float64(5), allRows[4][wkk.NewRowKey("line")])

	// Next read should return EOF
	batch, err := reader.Next(context.TODO())
	assert.Nil(t, batch)
	assert.True(t, errors.Is(err, io.EOF))
}

// TestJSONLinesReader_GetSchemaSeekable tests schema inference for seekable readers
func TestJSONLinesReader_GetSchemaSeekable(t *testing.T) {
	jsonData := `{"name":"test","value":123,"active":true}
{"name":"test2","value":456,"extra":"field"}`

	// Use seekableReadCloser which implements both io.ReadCloser and io.Seeker
	reader, err := NewJSONLinesReader(newSeekableReadCloser([]byte(jsonData)), 100)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema := reader.GetSchema()
	assert.NotNil(t, schema, "GetSchema must never return nil")

	// Should have discovered all unique keys from both rows
	columns := schema.Columns()
	assert.Equal(t, 4, len(columns), "Should discover all unique keys across all rows")

	// Verify column names and types
	columnsByName := make(map[string]*ColumnSchema)
	for _, col := range columns {
		columnsByName[string(col.Name.Value())] = col
		assert.True(t, col.HasNonNull, "All columns should be marked as HasNonNull=true")
	}

	// Check each column has correct inferred type
	assert.NotNil(t, columnsByName["name"], "Should have 'name' column")
	assert.Equal(t, DataTypeString, columnsByName["name"].DataType, "name should be string")

	assert.NotNil(t, columnsByName["value"], "Should have 'value' column")
	assert.Equal(t, DataTypeInt64, columnsByName["value"].DataType, "value should be int64 (123 and 456 are integers)")

	assert.NotNil(t, columnsByName["active"], "Should have 'active' column")
	assert.Equal(t, DataTypeBool, columnsByName["active"].DataType, "active should be bool")

	assert.NotNil(t, columnsByName["extra"], "Should have 'extra' column")
	assert.Equal(t, DataTypeString, columnsByName["extra"].DataType, "extra should be string")
}

// TestJSONLinesReader_GetSchemaNonSeekable tests that non-seekable readers return empty schema
func TestJSONLinesReader_GetSchemaNonSeekable(t *testing.T) {
	jsonData := `{"name":"test","value":123}`

	// Create a gzip reader which doesn't implement io.Seeker
	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	_, err := gzWriter.Write([]byte(jsonData))
	require.NoError(t, err)
	require.NoError(t, gzWriter.Close())

	gzReader, err := gzip.NewReader(&buf)
	require.NoError(t, err)

	reader, err := NewJSONLinesReader(gzReader, 100)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Non-seekable readers should return empty schema
	schema := reader.GetSchema()
	assert.NotNil(t, schema, "GetSchema must never return nil")
	assert.Equal(t, 0, len(schema.Columns()), "Non-seekable readers should return empty schema")
}

// TestJSONLinesReader_SchemaInferenceAndReading tests that schema inference doesn't affect reading
func TestJSONLinesReader_SchemaInferenceAndReading(t *testing.T) {
	jsonData := `{"line": 1, "value": "first"}
{"line": 2, "value": "second"}
{"line": 3, "value": "third", "extra": "data"}`

	reader, err := NewJSONLinesReader(newSeekableReadCloser([]byte(jsonData)), 100)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// First verify schema was inferred
	schema := reader.GetSchema()
	require.NotNil(t, schema)
	columns := schema.Columns()
	assert.Equal(t, 3, len(columns), "Should discover all unique keys: line, value, extra")

	// Then verify we can still read all the data correctly
	rows, err := readAllRows(reader)
	require.NoError(t, err)
	assert.Len(t, rows, 3, "Should read all rows after schema inference")

	// Verify data integrity
	assert.Equal(t, float64(1), rows[0][wkk.NewRowKey("line")])
	assert.Equal(t, "first", rows[0][wkk.NewRowKey("value")])
	assert.Equal(t, float64(2), rows[1][wkk.NewRowKey("line")])
	assert.Equal(t, "second", rows[1][wkk.NewRowKey("value")])
	assert.Equal(t, float64(3), rows[2][wkk.NewRowKey("line")])
	assert.Equal(t, "third", rows[2][wkk.NewRowKey("value")])
	assert.Equal(t, "data", rows[2][wkk.NewRowKey("extra")])
}

// TestJSONLinesReader_TypePromotion tests that int64 is promoted to float64 when needed
func TestJSONLinesReader_TypePromotion(t *testing.T) {
	// First row has integer, second row has float - should promote to float64
	jsonData := `{"id":1,"count":100}
{"id":2,"count":200.5}`

	reader, err := NewJSONLinesReader(newSeekableReadCloser([]byte(jsonData)), 100)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema := reader.GetSchema()
	require.NotNil(t, schema)

	columnsByName := make(map[string]*ColumnSchema)
	for _, col := range schema.Columns() {
		columnsByName[string(col.Name.Value())] = col
	}

	// id is always integer, should be int64
	assert.Equal(t, DataTypeInt64, columnsByName["id"].DataType, "id should stay int64")

	// count starts as int (100) but becomes float (200.5), should promote to float64
	assert.Equal(t, DataTypeFloat64, columnsByName["count"].DataType, "count should be promoted to float64")
}

// TestJSONLinesReader_ComplexTypes tests handling of arrays and objects
func TestJSONLinesReader_ComplexTypes(t *testing.T) {
	jsonData := `{"tags":["a","b"],"metadata":{"key":"value"},"simple":"text"}
{"tags":["c"],"metadata":{"key":"other"},"simple":"more"}`

	reader, err := NewJSONLinesReader(newSeekableReadCloser([]byte(jsonData)), 100)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	schema := reader.GetSchema()
	require.NotNil(t, schema)

	columnsByName := make(map[string]*ColumnSchema)
	for _, col := range schema.Columns() {
		columnsByName[string(col.Name.Value())] = col
	}

	// Arrays and objects should use DataTypeAny
	assert.Equal(t, DataTypeAny, columnsByName["tags"].DataType, "tags (array) should be DataTypeAny")
	assert.Equal(t, DataTypeAny, columnsByName["metadata"].DataType, "metadata (object) should be DataTypeAny")
	assert.Equal(t, DataTypeString, columnsByName["simple"].DataType, "simple should be string")
}
