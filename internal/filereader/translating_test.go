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
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
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
	newKeys := make(map[wkk.RowKey]any)
	for k, v := range *row {
		newKeys[wkk.NewRowKey(mt.prefix+string(k.Value()))] = v
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
		{wkk.NewRowKey("test"): "data"},
	})
	translator := newTestNoopTranslator()

	reader, err := NewTranslatingReader(mockReader, translator, 1000)
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

	_, err := NewTranslatingReader(nil, translator, 1000)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "reader cannot be nil")
}

func TestNewTranslatingReader_NilTranslator(t *testing.T) {
	mockReader := newMockReader("test", []Row{{wkk.NewRowKey("test"): "data"}})

	_, err := NewTranslatingReader(mockReader, nil, 1000)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "translator cannot be nil")
}

func TestTranslatingReader_NoopTranslator(t *testing.T) {
	testData := []Row{
		{wkk.NewRowKey("name"): "Alice", wkk.NewRowKey("age"): 30, wkk.NewRowKey("city"): "New York"},
		{wkk.NewRowKey("name"): "Bob", wkk.NewRowKey("age"): 25, wkk.NewRowKey("city"): "San Francisco"},
		{wkk.NewRowKey("name"): "Charlie", wkk.NewRowKey("age"): 35, wkk.NewRowKey("city"): "Chicago"},
	}

	mockReader := newMockReader("test", testData)
	translator := NewNoopTranslator()

	reader, err := NewTranslatingReader(mockReader, translator, 1000)
	require.NoError(t, err)
	defer reader.Close()

	// Read all rows
	allRows, err := readAllRows(reader)
	require.NoError(t, err)
	require.Equal(t, 3, len(allRows), "Should read exactly 3 rows")

	// Verify data is unchanged by NoopTranslator
	for i, row := range allRows {
		assert.Equal(t, testData[i][wkk.NewRowKey("name")], row[wkk.NewRowKey("name")])
		assert.Equal(t, testData[i][wkk.NewRowKey("age")], row[wkk.NewRowKey("age")])
		assert.Equal(t, testData[i][wkk.NewRowKey("city")], row[wkk.NewRowKey("city")])
	}
}

func TestTranslatingReader_SameReferenceVsDifferentReference(t *testing.T) {
	testData := []Row{
		{wkk.NewRowKey("key"): "value1"},
		{wkk.NewRowKey("key"): "value2"},
	}

	t.Run("NoopTranslator_SameReference", func(t *testing.T) {
		mockReader := newMockReader("test", testData)
		translator := NewNoopTranslator()

		reader, err := NewTranslatingReader(mockReader, translator, 1000)
		require.NoError(t, err)
		defer reader.Close()

		allRows, err := readAllRows(reader)
		require.NoError(t, err)
		require.Equal(t, 2, len(allRows))

		// Verify data is preserved when translator returns same reference
		assert.Equal(t, "value1", allRows[0][wkk.NewRowKey("key")])
		assert.Equal(t, "value2", allRows[1][wkk.NewRowKey("key")])
	})

	t.Run("TagsTranslator_DifferentReference", func(t *testing.T) {
		mockReader := newMockReader("test", testData)
		translator := NewTagsTranslator(map[string]string{"tag": "test"})

		reader, err := NewTranslatingReader(mockReader, translator, 1000)
		require.NoError(t, err)
		defer reader.Close()

		allRows, err := readAllRows(reader)
		require.NoError(t, err)
		require.Equal(t, 2, len(allRows))

		// Verify data is preserved and tag is added when translator returns different reference
		for i, row := range allRows {
			assert.Equal(t, testData[i][wkk.NewRowKey("key")], row[wkk.NewRowKey("key")])
			assert.Equal(t, "test", row[wkk.NewRowKey("tag")])
		}
	})

	t.Run("TestNoopTranslator_DifferentReference", func(t *testing.T) {
		mockReader := newMockReader("test", testData)
		translator := newTestNoopTranslator()

		reader, err := NewTranslatingReader(mockReader, translator, 1000)
		require.NoError(t, err)
		defer reader.Close()

		allRows, err := readAllRows(reader)
		require.NoError(t, err)
		require.Equal(t, 2, len(allRows))

		// Verify data is preserved when test translator returns different reference
		assert.Equal(t, "value1", allRows[0][wkk.NewRowKey("key")])
		assert.Equal(t, "value2", allRows[1][wkk.NewRowKey("key")])
	})
}

func TestTranslatingReader_TagsTranslator(t *testing.T) {
	testData := []Row{
		{wkk.NewRowKey("metric"): "cpu_usage", wkk.NewRowKey("value"): 75.5},
		{wkk.NewRowKey("metric"): "memory_usage", wkk.NewRowKey("value"): 60.2},
	}

	tags := map[string]string{
		"environment": "production",
		"service":     "api-server",
		"version":     "1.2.3",
	}

	mockReader := newMockReader("test", testData)
	translator := NewTagsTranslator(tags)

	reader, err := NewTranslatingReader(mockReader, translator, 1000)
	require.NoError(t, err)
	defer reader.Close()

	// Read all rows
	allRows, err := readAllRows(reader)
	require.NoError(t, err)
	require.Equal(t, 2, len(allRows), "Should read exactly 2 rows")

	// Verify original data is preserved and tags are added
	for i, row := range allRows {
		// Original data should be preserved
		assert.Equal(t, testData[i][wkk.NewRowKey("metric")], row[wkk.NewRowKey("metric")])
		assert.Equal(t, testData[i][wkk.NewRowKey("value")], row[wkk.NewRowKey("value")])

		// Tags should be added
		assert.Equal(t, "production", row[wkk.NewRowKey("environment")])
		assert.Equal(t, "api-server", row[wkk.NewRowKey("service")])
		assert.Equal(t, "1.2.3", row[wkk.NewRowKey("version")])
	}
}

func TestTranslatingReader_ChainTranslator(t *testing.T) {
	testData := []Row{
		{wkk.NewRowKey("metric"): "cpu", wkk.NewRowKey("value"): 75},
		{wkk.NewRowKey("metric"): "memory", wkk.NewRowKey("value"): 60},
	}

	// Create a chain of translators
	tagsTranslator := NewTagsTranslator(map[string]string{
		"environment": "test",
	})

	prefixTranslator := &mockTranslator{prefix: "prefix_"}

	chainTranslator := NewChainTranslator(tagsTranslator, prefixTranslator)

	mockReader := newMockReader("test", testData)
	reader, err := NewTranslatingReader(mockReader, chainTranslator, 1000)
	require.NoError(t, err)
	defer reader.Close()

	// Read all rows
	allRows, err := readAllRows(reader)
	require.NoError(t, err)
	require.Equal(t, 2, len(allRows), "Should read exactly 2 rows")

	// Verify both transformations were applied
	for i, row := range allRows {
		// Original data should be prefixed
		expectedMetric := wkk.NewRowKey("prefix_metric")
		expectedValue := wkk.NewRowKey("prefix_value")
		expectedEnv := wkk.NewRowKey("prefix_environment")

		assert.Equal(t, testData[i][wkk.NewRowKey("metric")], row[expectedMetric])
		assert.Equal(t, testData[i][wkk.NewRowKey("value")], row[expectedValue])
		assert.Equal(t, "test", row[expectedEnv])
	}
}

func TestTranslatingReader_TranslationError(t *testing.T) {
	testData := []Row{
		{wkk.NewRowKey("good"): "data"},
		{wkk.NewRowKey("bad"): "data"},
		{wkk.NewRowKey("more"): "data"},
	}

	mockReader := newMockReader("test", testData)
	translator := &mockTranslator{shouldError: true}

	reader, err := NewTranslatingReader(mockReader, translator, 1000)
	require.NoError(t, err)
	defer reader.Close()

	// Read should fail on the first row due to translation error
	batch, err := reader.Next(context.TODO())
	assert.Nil(t, batch, "Should not return any batch due to translation error")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "translation failed for row 0")
	assert.Contains(t, err.Error(), "mock translation error")
}

func TestTranslatingReader_PartialTranslationError(t *testing.T) {
	testData := []Row{
		{wkk.NewRowKey("row"): 1},
		{wkk.NewRowKey("row"): 2},
		{wkk.NewRowKey("row"): 3},
	}

	// Create a translator that fails on the second row
	translator := &conditionalErrorTranslator{failOnRow: 1}

	mockReader := newMockReader("test", testData)
	reader, err := NewTranslatingReader(mockReader, translator, 1000)
	require.NoError(t, err)
	defer reader.Close()

	// Read should succeed for first row, then fail on second
	batch, err := reader.Next(context.TODO())
	assert.NotNil(t, batch, "Should return batch with first row before translation error")
	assert.Equal(t, 1, batch.Len(), "Should read exactly 1 row before translation error")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "translation failed for row 1")

	// Verify the first row was translated successfully
	assert.Equal(t, "translated_1", batch.Get(0)[wkk.NewRowKey("row")])
}

func TestTranslatingReader_EmptySlice(t *testing.T) {
	testData := []Row{{wkk.NewRowKey("test"): "data"}}

	mockReader := newMockReader("test", testData)
	translator := newTestNoopTranslator()

	reader, err := NewTranslatingReader(mockReader, translator, 1000)
	require.NoError(t, err)
	defer reader.Close()

	// Read with empty slice should still work with Next()
	batch, err := reader.Next(context.TODO())
	assert.NoError(t, err)
	assert.NotNil(t, batch)
	assert.Equal(t, 1, batch.Len(), "Should read 1 row")
}

func TestTranslatingReader_ReadBatched(t *testing.T) {
	testData := []Row{
		{wkk.NewRowKey("id"): 1, wkk.NewRowKey("value"): "a"},
		{wkk.NewRowKey("id"): 2, wkk.NewRowKey("value"): "b"},
		{wkk.NewRowKey("id"): 3, wkk.NewRowKey("value"): "c"},
		{wkk.NewRowKey("id"): 4, wkk.NewRowKey("value"): "d"},
		{wkk.NewRowKey("id"): 5, wkk.NewRowKey("value"): "e"},
	}

	tags := map[string]string{"batch": "test"}
	mockReader := newMockReader("test", testData)
	translator := NewTagsTranslator(tags)

	reader, err := NewTranslatingReader(mockReader, translator, 1000)
	require.NoError(t, err)
	defer reader.Close()

	// Read in batches
	var totalRows int

	for {
		batch, err := reader.Next(context.TODO())

		if batch != nil {
			totalRows += batch.Len()

			// Verify each row that was read has translation applied
			for i := 0; i < batch.Len(); i++ {
				row := batch.Get(i)
				assert.Greater(t, len(row), 0, "Row %d should have data", i)
				_, hasId := row[wkk.NewRowKey("id")]
				assert.True(t, hasId, "Row %d should have id field", i)
				_, hasValue := row[wkk.NewRowKey("value")]
				assert.True(t, hasValue, "Row %d should have value field", i)
				assert.Equal(t, "test", row[wkk.NewRowKey("batch")], "Row %d should have batch tag", i)
			}
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
		{wkk.NewRowKey("id"): 1, wkk.NewRowKey("value"): "a"},
		{wkk.NewRowKey("id"): 2, wkk.NewRowKey("value"): "b"},
		{wkk.NewRowKey("id"): 3, wkk.NewRowKey("value"): "c"},
		{wkk.NewRowKey("id"): 4, wkk.NewRowKey("value"): "d"},
		{wkk.NewRowKey("id"): 5, wkk.NewRowKey("value"): "e"},
	}

	mockReader := newMockReader("test", testData)
	translator := NewTagsTranslator(map[string]string{"batch": "test"})

	reader, err := NewTranslatingReader(mockReader, translator, 1000)
	require.NoError(t, err)
	defer reader.Close()

	// Initially should have 0 rows
	assert.Equal(t, int64(0), reader.TotalRowsReturned())

	// Read first batch (all 5 rows)
	batch, err := reader.Next(context.TODO())
	require.NoError(t, err)
	require.NotNil(t, batch)
	assert.Equal(t, 5, batch.Len())                       // All 5 rows in one batch
	assert.Equal(t, int64(5), reader.TotalRowsReturned()) // Total should be 5

	// Count should remain stable after EOF
	batch, err = reader.Next(context.TODO())
	assert.Nil(t, batch)
	assert.True(t, errors.Is(err, io.EOF))
	assert.Equal(t, int64(5), reader.TotalRowsReturned()) // Should still be 5
}

func TestTranslatingReader_Close(t *testing.T) {
	testData := []Row{{wkk.NewRowKey("test"): "data"}}

	mockReader := newMockReader("test", testData)
	translator := newTestNoopTranslator()

	reader, err := NewTranslatingReader(mockReader, translator, 1000)
	require.NoError(t, err)

	// Should be able to read before closing
	batch, err := reader.Next(context.TODO())
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Equal(t, 1, batch.Len(), "Should read exactly 1 row before closing")

	// Close should work
	err = reader.Close()
	assert.NoError(t, err)
	assert.True(t, reader.closed)

	// Reading after close should return error
	_, err = reader.Next(context.TODO())
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

	reader, err := NewTranslatingReader(mockReader, translator, 1000)
	require.NoError(t, err)
	defer reader.Close()

	// Read should return error from underlying reader
	_, err = reader.Next(context.TODO())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")
}

func TestTranslatingReader_EOF(t *testing.T) {
	testData := []Row{
		{wkk.NewRowKey("final"): "row"},
	}

	mockReader := newMockReader("test", testData)
	translator := NewTagsTranslator(map[string]string{"eof": "test"})

	reader, err := NewTranslatingReader(mockReader, translator, 1000)
	require.NoError(t, err)
	defer reader.Close()

	// First read should get the row
	batch, err := reader.Next(context.TODO())
	assert.NoError(t, err)
	assert.NotNil(t, batch)
	assert.Equal(t, 1, batch.Len())
	assert.Equal(t, "row", batch.Get(0)[wkk.NewRowKey("final")])
	assert.Equal(t, "test", batch.Get(0)[wkk.NewRowKey("eof")])

	// Second read should return EOF
	batch, err = reader.Next(context.TODO())
	assert.Nil(t, batch)
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
