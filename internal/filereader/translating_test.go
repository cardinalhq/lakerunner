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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test-only NoopTranslator that returns a copy of the row for safe testing
type testNoopTranslator struct{}

func newTestNoopTranslator() *testNoopTranslator {
	return &testNoopTranslator{}
}

func (nt *testNoopTranslator) TranslateRow(row *Row) error {
	// No-op - row is unchanged
	return nil
}

// Mock translator for testing
type mockTranslator struct {
	shouldError bool
	prefix      string
}

func (mt *mockTranslator) TranslateRow(row *Row) error {
	if mt.shouldError {
		return errors.New("mock translation error")
	}

	// Apply prefix to all keys in-place
	newKeys := make(map[string]any)
	for k, v := range *row {
		newKeys[mt.prefix+k] = v
	}

	// Clear original row and replace with prefixed keys
	for k := range *row {
		delete(*row, k)
	}
	for k, v := range newKeys {
		(*row)[k] = v
	}

	return nil
}

func TestNewTranslatingReader(t *testing.T) {
	mockReader := newMockReader("test", []Row{
		{"test": "data"},
	})
	translator := newTestNoopTranslator()

	reader, err := NewTranslatingReader(mockReader, translator)
	require.NoError(t, err)
	require.NotNil(t, reader)
	defer reader.Close()

	// Verify the reader was initialized properly
	assert.NotNil(t, reader.reader)
	assert.NotNil(t, reader.translator)
	assert.False(t, reader.closed)
}

func TestNewTranslatingReader_NilReader(t *testing.T) {
	translator := newTestNoopTranslator()

	_, err := NewTranslatingReader(nil, translator)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reader cannot be nil")
}

func TestNewTranslatingReader_NilTranslator(t *testing.T) {
	mockReader := newMockReader("test", []Row{{"test": "data"}})

	_, err := NewTranslatingReader(mockReader, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "translator cannot be nil")
}

func TestTranslatingReader_NoopTranslator(t *testing.T) {
	testData := []Row{
		{"name": "Alice", "age": 30, "city": "New York"},
		{"name": "Bob", "age": 25, "city": "San Francisco"},
		{"name": "Charlie", "age": 35, "city": "Chicago"},
	}

	mockReader := newMockReader("test", testData)
	translator := NewNoopTranslator()

	reader, err := NewTranslatingReader(mockReader, translator)
	require.NoError(t, err)
	defer reader.Close()

	// Read all rows
	allRows, err := readAllRows(reader)
	require.NoError(t, err)
	require.Equal(t, 3, len(allRows), "Should read exactly 3 rows")

	// Verify data is unchanged by NoopTranslator
	for i, row := range allRows {
		assert.Equal(t, testData[i]["name"], row["name"])
		assert.Equal(t, testData[i]["age"], row["age"])
		assert.Equal(t, testData[i]["city"], row["city"])
	}
}

func TestTranslatingReader_SameReferenceVsDifferentReference(t *testing.T) {
	testData := []Row{
		{"key": "value1"},
		{"key": "value2"},
	}

	t.Run("NoopTranslator_SameReference", func(t *testing.T) {
		mockReader := newMockReader("test", testData)
		translator := NewNoopTranslator()

		reader, err := NewTranslatingReader(mockReader, translator)
		require.NoError(t, err)
		defer reader.Close()

		allRows, err := readAllRows(reader)
		require.NoError(t, err)
		require.Equal(t, 2, len(allRows))

		// Verify data is preserved when translator returns same reference
		assert.Equal(t, "value1", allRows[0]["key"])
		assert.Equal(t, "value2", allRows[1]["key"])
	})

	t.Run("TagsTranslator_DifferentReference", func(t *testing.T) {
		mockReader := newMockReader("test", testData)
		translator := NewTagsTranslator(map[string]string{"tag": "test"})

		reader, err := NewTranslatingReader(mockReader, translator)
		require.NoError(t, err)
		defer reader.Close()

		allRows, err := readAllRows(reader)
		require.NoError(t, err)
		require.Equal(t, 2, len(allRows))

		// Verify data is preserved and tag is added when translator returns different reference
		for i, row := range allRows {
			assert.Equal(t, testData[i]["key"], row["key"])
			assert.Equal(t, "test", row["tag"])
		}
	})

	t.Run("TestNoopTranslator_DifferentReference", func(t *testing.T) {
		mockReader := newMockReader("test", testData)
		translator := newTestNoopTranslator()

		reader, err := NewTranslatingReader(mockReader, translator)
		require.NoError(t, err)
		defer reader.Close()

		allRows, err := readAllRows(reader)
		require.NoError(t, err)
		require.Equal(t, 2, len(allRows))

		// Verify data is preserved when test translator returns different reference
		assert.Equal(t, "value1", allRows[0]["key"])
		assert.Equal(t, "value2", allRows[1]["key"])
	})
}

func TestTranslatingReader_TagsTranslator(t *testing.T) {
	testData := []Row{
		{"metric": "cpu_usage", "value": 75.5},
		{"metric": "memory_usage", "value": 60.2},
	}

	tags := map[string]string{
		"environment": "production",
		"service":     "api-server",
		"version":     "1.2.3",
	}

	mockReader := newMockReader("test", testData)
	translator := NewTagsTranslator(tags)

	reader, err := NewTranslatingReader(mockReader, translator)
	require.NoError(t, err)
	defer reader.Close()

	// Read all rows
	allRows, err := readAllRows(reader)
	require.NoError(t, err)
	require.Equal(t, 2, len(allRows), "Should read exactly 2 rows")

	// Verify original data is preserved and tags are added
	for i, row := range allRows {
		// Original data should be preserved
		assert.Equal(t, testData[i]["metric"], row["metric"])
		assert.Equal(t, testData[i]["value"], row["value"])

		// Tags should be added
		assert.Equal(t, "production", row["environment"])
		assert.Equal(t, "api-server", row["service"])
		assert.Equal(t, "1.2.3", row["version"])
	}
}

func TestTranslatingReader_ChainTranslator(t *testing.T) {
	testData := []Row{
		{"metric": "cpu", "value": 75},
		{"metric": "memory", "value": 60},
	}

	// Create a chain of translators
	tagsTranslator := NewTagsTranslator(map[string]string{
		"environment": "test",
	})

	prefixTranslator := &mockTranslator{prefix: "prefix_"}

	chainTranslator := NewChainTranslator(tagsTranslator, prefixTranslator)

	mockReader := newMockReader("test", testData)
	reader, err := NewTranslatingReader(mockReader, chainTranslator)
	require.NoError(t, err)
	defer reader.Close()

	// Read all rows
	allRows, err := readAllRows(reader)
	require.NoError(t, err)
	require.Equal(t, 2, len(allRows), "Should read exactly 2 rows")

	// Verify both transformations were applied
	for i, row := range allRows {
		// Original data should be prefixed
		expectedMetric := "prefix_metric"
		expectedValue := "prefix_value"
		expectedEnv := "prefix_environment"

		assert.Equal(t, testData[i]["metric"], row[expectedMetric])
		assert.Equal(t, testData[i]["value"], row[expectedValue])
		assert.Equal(t, "test", row[expectedEnv])
	}
}

func TestTranslatingReader_TranslationError(t *testing.T) {
	testData := []Row{
		{"good": "data"},
		{"bad": "data"},
		{"more": "data"},
	}

	mockReader := newMockReader("test", testData)
	translator := &mockTranslator{shouldError: true}

	reader, err := NewTranslatingReader(mockReader, translator)
	require.NoError(t, err)
	defer reader.Close()

	// Read should fail on the first row due to translation error
	rows := make([]Row, 3)
	for i := range rows {
		rows[i] = make(Row)
	}

	n, err := reader.Read(rows)
	assert.Equal(t, 0, n, "Should not read any rows due to translation error")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "translation failed for row 0")
	assert.Contains(t, err.Error(), "mock translation error")
}

func TestTranslatingReader_PartialTranslationError(t *testing.T) {
	testData := []Row{
		{"row": 1},
		{"row": 2},
		{"row": 3},
	}

	// Create a translator that fails on the second row
	translator := &conditionalErrorTranslator{failOnRow: 1}

	mockReader := newMockReader("test", testData)
	reader, err := NewTranslatingReader(mockReader, translator)
	require.NoError(t, err)
	defer reader.Close()

	// Read should succeed for first row, then fail on second
	rows := make([]Row, 3)
	for i := range rows {
		rows[i] = make(Row)
	}

	n, err := reader.Read(rows)
	assert.Equal(t, 1, n, "Should read exactly 1 row before translation error")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "translation failed for row 1")

	// Verify the first row was translated successfully
	assert.Equal(t, "translated_1", rows[0]["row"])
}

func TestTranslatingReader_EmptySlice(t *testing.T) {
	testData := []Row{{"test": "data"}}

	mockReader := newMockReader("test", testData)
	translator := newTestNoopTranslator()

	reader, err := NewTranslatingReader(mockReader, translator)
	require.NoError(t, err)
	defer reader.Close()

	// Read with empty slice
	n, err := reader.Read([]Row{})
	assert.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestTranslatingReader_ReadBatched(t *testing.T) {
	testData := []Row{
		{"id": 1, "value": "a"},
		{"id": 2, "value": "b"},
		{"id": 3, "value": "c"},
		{"id": 4, "value": "d"},
		{"id": 5, "value": "e"},
	}

	tags := map[string]string{"batch": "test"}
	mockReader := newMockReader("test", testData)
	translator := NewTagsTranslator(tags)

	reader, err := NewTranslatingReader(mockReader, translator)
	require.NoError(t, err)
	defer reader.Close()

	// Read in batches of 2
	var totalRows int
	batchSize := 2

	for {
		rows := make([]Row, batchSize)
		for i := range rows {
			rows[i] = make(Row)
		}

		n, err := reader.Read(rows)
		totalRows += n

		// Verify each row that was read has translation applied
		for i := 0; i < n; i++ {
			assert.Greater(t, len(rows[i]), 0, "Row %d should have data", i)
			assert.Contains(t, rows[i], "id", "Row %d should have id field", i)
			assert.Contains(t, rows[i], "value", "Row %d should have value field", i)
			assert.Equal(t, "test", rows[i]["batch"], "Row %d should have batch tag", i)
		}

		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}

	assert.Equal(t, 5, totalRows, "Should read exactly 5 rows in batches")
}

func TestTranslatingReader_TotalRowsReturned(t *testing.T) {
	testData := []Row{
		{"id": 1, "value": "a"},
		{"id": 2, "value": "b"},
		{"id": 3, "value": "c"},
		{"id": 4, "value": "d"},
		{"id": 5, "value": "e"},
	}

	mockReader := newMockReader("test", testData)
	translator := NewTagsTranslator(map[string]string{"batch": "test"})

	reader, err := NewTranslatingReader(mockReader, translator)
	require.NoError(t, err)
	defer reader.Close()

	// Initially should have 0 rows
	assert.Equal(t, int64(0), reader.TotalRowsReturned())

	// Read first batch
	rows := make([]Row, 2)
	for i := range rows {
		rows[i] = make(Row)
	}
	n, err := reader.Read(rows)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, int64(2), reader.TotalRowsReturned())

	// Read second batch
	rows = make([]Row, 2)
	for i := range rows {
		rows[i] = make(Row)
	}
	n, err = reader.Read(rows)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, int64(4), reader.TotalRowsReturned())

	// Read final batch
	rows = make([]Row, 2)
	for i := range rows {
		rows[i] = make(Row)
	}
	n, err = reader.Read(rows)
	assert.Equal(t, 1, n)                                 // Only 1 row left
	require.NoError(t, err)                               // Should not be EOF yet, since we got 1 row
	assert.Equal(t, int64(5), reader.TotalRowsReturned()) // Total should be 5

	// Count should remain stable after EOF
	rows = make([]Row, 1)
	rows[0] = make(Row)
	n, err = reader.Read(rows)
	assert.Equal(t, 0, n)
	assert.True(t, errors.Is(err, io.EOF))
	assert.Equal(t, int64(5), reader.TotalRowsReturned()) // Should still be 5
}

func TestTranslatingReader_Close(t *testing.T) {
	testData := []Row{{"test": "data"}}

	mockReader := newMockReader("test", testData)
	translator := newTestNoopTranslator()

	reader, err := NewTranslatingReader(mockReader, translator)
	require.NoError(t, err)

	// Should be able to read before closing
	rows := make([]Row, 1)
	rows[0] = make(Row)
	n, err := reader.Read(rows)
	require.NoError(t, err)
	require.Equal(t, 1, n, "Should read exactly 1 row before closing")

	// Close should work
	err = reader.Close()
	assert.NoError(t, err)
	assert.True(t, reader.closed)

	// Reading after close should return error
	rows[0] = make(Row)
	_, err = reader.Read(rows)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	// Close should be idempotent
	err = reader.Close()
	assert.NoError(t, err)
}

func TestTranslatingReader_UnderlyingReaderError(t *testing.T) {
	// Create a mock reader that will error
	mockReader := &errorReader{}
	translator := newTestNoopTranslator()

	reader, err := NewTranslatingReader(mockReader, translator)
	require.NoError(t, err)
	defer reader.Close()

	// Read should return error from underlying reader
	rows := make([]Row, 1)
	rows[0] = make(Row)
	_, err = reader.Read(rows)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")
}

func TestTranslatingReader_EOF(t *testing.T) {
	testData := []Row{
		{"final": "row"},
	}

	mockReader := newMockReader("test", testData)
	translator := NewTagsTranslator(map[string]string{"eof": "test"})

	reader, err := NewTranslatingReader(mockReader, translator)
	require.NoError(t, err)
	defer reader.Close()

	// First read should get the row
	rows := make([]Row, 1)
	rows[0] = make(Row)
	n, err := reader.Read(rows)
	assert.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, "row", rows[0]["final"])
	assert.Equal(t, "test", rows[0]["eof"])

	// Second read should return EOF
	rows[0] = make(Row)
	n, err = reader.Read(rows)
	assert.Equal(t, 0, n)
	assert.True(t, errors.Is(err, io.EOF))
}

// Helper translator for testing partial translation errors
type conditionalErrorTranslator struct {
	failOnRow int
	rowCount  int
}

func (cet *conditionalErrorTranslator) TranslateRow(row *Row) error {
	if cet.rowCount == cet.failOnRow {
		return errors.New("conditional translation error")
	}

	// Transform all values by adding prefix
	for k, v := range *row {
		(*row)[k] = fmt.Sprintf("translated_%v", v)
	}

	cet.rowCount++
	return nil
}
