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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// TestMergesortReader_SchemaUnion tests that schemas are merged as a union of all columns.
func TestMergesortReader_SchemaUnion(t *testing.T) {
	// Create two readers with different columns
	schema1 := NewReaderSchema()
	schema1.AddColumn(wkk.NewRowKey("col_a"), wkk.NewRowKey("col_a"), DataTypeString, true)
	schema1.AddColumn(wkk.NewRowKey("col_b"), wkk.NewRowKey("col_b"), DataTypeInt64, true)

	schema2 := NewReaderSchema()
	schema2.AddColumn(wkk.NewRowKey("col_b"), wkk.NewRowKey("col_b"), DataTypeInt64, true)
	schema2.AddColumn(wkk.NewRowKey("col_c"), wkk.NewRowKey("col_c"), DataTypeFloat64, true)

	reader1 := newMockReader("reader1", []pipeline.Row{}, schema1)
	reader2 := newMockReader("reader2", []pipeline.Row{}, schema2)

	ctx := context.Background()
	keyProvider := &mockSortKeyProvider{}
	mergeReader, err := NewMergesortReader(ctx, []Reader{reader1, reader2}, keyProvider, 100)
	require.NoError(t, err)
	defer func() { _ = mergeReader.Close() }()

	schema := mergeReader.GetSchema()
	require.NotNil(t, schema)

	// Should have union of all columns: col_a, col_b, col_c
	assert.True(t, schema.HasColumn("col_a"))
	assert.True(t, schema.HasColumn("col_b"))
	assert.True(t, schema.HasColumn("col_c"))

	// Verify types
	assert.Equal(t, DataTypeString, schema.GetColumnType("col_a"))
	assert.Equal(t, DataTypeInt64, schema.GetColumnType("col_b"))
	assert.Equal(t, DataTypeFloat64, schema.GetColumnType("col_c"))
}

// TestMergesortReader_SchemaTypePromotion tests that conflicting types are promoted.
func TestMergesortReader_SchemaTypePromotion(t *testing.T) {
	tests := []struct {
		name           string
		type1          DataType
		type2          DataType
		expectedType   DataType
		expectedString string
	}{
		{"int64 + int64 = int64", DataTypeInt64, DataTypeInt64, DataTypeInt64, "int64"},
		{"int64 + float64 = float64", DataTypeInt64, DataTypeFloat64, DataTypeFloat64, "float64"},
		{"int64 + string = string", DataTypeInt64, DataTypeString, DataTypeString, "string"},
		{"int64 + bool = string", DataTypeInt64, DataTypeBool, DataTypeString, "string"},
		{"float64 + string = string", DataTypeFloat64, DataTypeString, DataTypeString, "string"},
		{"bool + bool = bool", DataTypeBool, DataTypeBool, DataTypeBool, "bool"},
		{"bytes + string = string", DataTypeBytes, DataTypeString, DataTypeString, "string"},
		{"any + int64 = any", DataTypeAny, DataTypeInt64, DataTypeAny, "any"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema1 := NewReaderSchema()
			testCol := wkk.NewRowKey("test_col")
			schema1.AddColumn(testCol, testCol, tt.type1, true)

			schema2 := NewReaderSchema()
			schema2.AddColumn(testCol, testCol, tt.type2, true)

			reader1 := newMockReader("reader1", []pipeline.Row{}, schema1)
			reader2 := newMockReader("reader2", []pipeline.Row{}, schema2)

			ctx := context.Background()
			keyProvider := &mockSortKeyProvider{}
			mergeReader, err := NewMergesortReader(ctx, []Reader{reader1, reader2}, keyProvider, 100)
			require.NoError(t, err)
			defer func() { _ = mergeReader.Close() }()

			schema := mergeReader.GetSchema()
			require.NotNil(t, schema)

			actualType := schema.GetColumnType("test_col")
			assert.Equal(t, tt.expectedType, actualType, "Type should be promoted to %s", tt.expectedString)
		})
	}
}

// TestMergesortReader_SchemaHasNonNull tests that HasNonNull is OR'd across readers.
func TestMergesortReader_SchemaHasNonNull(t *testing.T) {
	// Reader 1: col has HasNonNull=false
	schema1 := NewReaderSchema()
	schema1.AddColumn(wkk.NewRowKey("test_col"), wkk.NewRowKey("test_col"), DataTypeString, false)

	// Reader 2: same col has HasNonNull=true
	schema2 := NewReaderSchema()
	schema2.AddColumn(wkk.NewRowKey("test_col"), wkk.NewRowKey("test_col"), DataTypeString, true)

	reader1 := newMockReader("reader1", []pipeline.Row{}, schema1)
	reader2 := newMockReader("reader2", []pipeline.Row{}, schema2)

	ctx := context.Background()
	keyProvider := &mockSortKeyProvider{}
	mergeReader, err := NewMergesortReader(ctx, []Reader{reader1, reader2}, keyProvider, 100)
	require.NoError(t, err)
	defer func() { _ = mergeReader.Close() }()

	schema := mergeReader.GetSchema()
	require.NotNil(t, schema)

	columns := schema.Columns()
	require.Len(t, columns, 1)

	// HasNonNull should be true (OR of false and true)
	assert.True(t, columns[0].HasNonNull, "HasNonNull should be true if ANY reader has non-null")
}

// TestMergesortReader_SchemaNormalization tests that output rows are normalized.
func TestMergesortReader_SchemaNormalization(t *testing.T) {
	// Reader 1: declares col as int64, but emits string value
	schema1 := NewReaderSchema()
	schema1.AddColumn(wkk.NewRowKey("test_col"), wkk.NewRowKey("test_col"), DataTypeInt64, true)
	schema1.AddColumn(wkk.NewRowKey("_timestamp"), wkk.NewRowKey("_timestamp"), DataTypeInt64, true)

	rows1 := []pipeline.Row{
		{
			wkk.NewRowKey("test_col"):   "123", // String that should be converted to int64
			wkk.NewRowKey("_timestamp"): int64(1000),
		},
	}

	// Reader 2: declares col as int64, emits int64 value
	schema2 := NewReaderSchema()
	schema2.AddColumn(wkk.NewRowKey("test_col"), wkk.NewRowKey("test_col"), DataTypeInt64, true)
	schema2.AddColumn(wkk.NewRowKey("_timestamp"), wkk.NewRowKey("_timestamp"), DataTypeInt64, true)

	rows2 := []pipeline.Row{
		{
			wkk.NewRowKey("test_col"):   int64(456),
			wkk.NewRowKey("_timestamp"): int64(2000),
		},
	}

	reader1 := newMockReader("reader1", rows1, schema1)
	reader2 := newMockReader("reader2", rows2, schema2)

	ctx := context.Background()
	// Use a real sort key provider that sorts by _timestamp
	keyProvider := &LogSortKeyProvider{}
	mergeReader, err := NewMergesortReader(ctx, []Reader{reader1, reader2}, keyProvider, 100)
	require.NoError(t, err)
	defer func() { _ = mergeReader.Close() }()

	// Read all rows
	batch, err := mergeReader.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, batch)
	defer pipeline.ReturnBatch(batch)

	// Should have 2 rows
	assert.Equal(t, 2, batch.Len())

	// First row should have test_col converted from string "123" to int64(123)
	row1 := batch.Get(0)
	val1 := row1[wkk.NewRowKey("test_col")]
	assert.Equal(t, int64(123), val1, "String should be converted to int64")
	assert.IsType(t, int64(0), val1, "Type should be int64")

	// Second row should have test_col as int64(456)
	row2 := batch.Get(1)
	val2 := row2[wkk.NewRowKey("test_col")]
	assert.Equal(t, int64(456), val2)
	assert.IsType(t, int64(0), val2, "Type should be int64")
}

// mockSortKeyProvider is a simple sort key provider for testing.
type mockSortKeyProvider struct{}

func (m *mockSortKeyProvider) MakeKey(row pipeline.Row) SortKey {
	return &mockSortKey{}
}

// mockSortKey is a simple sort key for testing.
type mockSortKey struct{}

func (m *mockSortKey) Compare(other SortKey) int {
	return 0 // All keys are equal for simple tests
}

func (m *mockSortKey) Release() {
	// Nothing to release
}

var _ SortKey = (*mockSortKey)(nil)
var _ SortKeyProvider = (*mockSortKeyProvider)(nil)
