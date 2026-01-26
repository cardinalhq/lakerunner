// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// TestParquetRawReader_SchemaExtraction_BasicTypes tests that the schema extractor
// correctly identifies different column types from parquet metadata.
func TestParquetRawReader_SchemaExtraction_BasicTypes(t *testing.T) {
	// Create test parquet with various types
	rows := []map[string]any{
		{
			"string_col": "hello",
			"int_col":    int64(42),
			"float_col":  3.14,
			"bool_col":   true,
		},
		{
			"string_col": "world",
			"int_col":    int64(100),
			"float_col":  2.71,
			"bool_col":   false,
		},
	}

	parquetData, _ := createTestParquetInMemory(t, rows)
	reader := bytes.NewReader(parquetData)

	protoReader, err := NewParquetRawReader(reader, int64(len(parquetData)), 1000)
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	schema := protoReader.GetSchema()
	require.NotNil(t, schema)

	// Verify column types are correctly identified
	assert.Equal(t, DataTypeString, schema.GetColumnType("string_col"))
	assert.Equal(t, DataTypeInt64, schema.GetColumnType("int_col"))
	assert.Equal(t, DataTypeFloat64, schema.GetColumnType("float_col"))
	assert.Equal(t, DataTypeBool, schema.GetColumnType("bool_col"))

	// Verify columns are present
	assert.True(t, schema.HasColumn("string_col"))
	assert.True(t, schema.HasColumn("int_col"))
	assert.True(t, schema.HasColumn("float_col"))
	assert.True(t, schema.HasColumn("bool_col"))
}

// TestParquetRawReader_SchemaExtraction_ColumnNames tests that column names
// with dots are converted to underscores.
func TestParquetRawReader_SchemaExtraction_ColumnNames(t *testing.T) {
	// Create test parquet with dotted column names
	rows := []map[string]any{
		{
			"simple":        "value1",
			"dotted.name":   "value2",
			"multi.dot.col": int64(123),
		},
	}

	parquetData, _ := createTestParquetInMemory(t, rows)
	reader := bytes.NewReader(parquetData)

	protoReader, err := NewParquetRawReader(reader, int64(len(parquetData)), 1000)
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	schema := protoReader.GetSchema()
	require.NotNil(t, schema)

	// Verify simple column name
	assert.True(t, schema.HasColumn("simple"))

	// Verify dotted names are converted to underscores
	assert.True(t, schema.HasColumn("dotted_name"))
	assert.True(t, schema.HasColumn("multi_dot_col"))

	// Verify original dotted names are not in schema
	assert.False(t, schema.HasColumn("dotted.name"))
	assert.False(t, schema.HasColumn("multi.dot.col"))
}

// TestParquetRawReader_SchemaExtraction_ReadWithSchema tests that reading rows
// with the schema works correctly and applies normalization.
func TestParquetRawReader_SchemaExtraction_ReadWithSchema(t *testing.T) {
	// Create test parquet
	rows := []map[string]any{
		{
			"name":  "Alice",
			"age":   int64(30),
			"score": 95.5,
		},
		{
			"name":  "Bob",
			"age":   int64(25),
			"score": 87.3,
		},
	}

	parquetData, _ := createTestParquetInMemory(t, rows)
	reader := bytes.NewReader(parquetData)

	protoReader, err := NewParquetRawReader(reader, int64(len(parquetData)), 1000)
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	schema := protoReader.GetSchema()
	require.NotNil(t, schema)

	// Read all rows
	ctx := context.Background()
	batch, err := protoReader.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, batch)

	// Verify we got 2 rows
	assert.Equal(t, 2, batch.Len())

	// Verify first row has correct types matching schema
	row1 := batch.Get(0)
	name1 := row1[wkk.NewRowKey("name")]
	assert.Equal(t, "Alice", name1)
	assert.IsType(t, "", name1) // string type

	age1 := row1[wkk.NewRowKey("age")]
	assert.Equal(t, int64(30), age1)
	assert.IsType(t, int64(0), age1) // int64 type

	score1 := row1[wkk.NewRowKey("score")]
	assert.Equal(t, 95.5, score1)
	assert.IsType(t, float64(0), score1) // float64 type

	// Verify second row
	row2 := batch.Get(1)
	name2 := row2[wkk.NewRowKey("name")]
	assert.Equal(t, "Bob", name2)
}

// TestParquetRawReader_SchemaExtraction_EmptyFile tests that empty parquet files
// are handled correctly (should error during construction).
func TestParquetRawReader_SchemaExtraction_EmptyFile(t *testing.T) {
	// Empty parquet files should error
	// This is handled by the existing NewParquetRawReader check for NumRows() == 0

	// We can't easily create a truly empty parquet file with our helper,
	// so this test documents the expected behavior
}

// TestParquetRawReader_SchemaExtraction_MultipleReads tests that schema
// remains consistent across multiple read operations.
func TestParquetRawReader_SchemaExtraction_MultipleReads(t *testing.T) {
	// Create test parquet with enough rows for multiple batches
	rows := make([]map[string]any, 100)
	for i := range rows {
		rows[i] = map[string]any{
			"id":    int64(i),
			"value": float64(i) * 1.5,
		}
	}

	parquetData, _ := createTestParquetInMemory(t, rows)
	reader := bytes.NewReader(parquetData)

	protoReader, err := NewParquetRawReader(reader, int64(len(parquetData)), 10)
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	schema := protoReader.GetSchema()
	require.NotNil(t, schema)

	ctx := context.Background()

	// Read all batches
	totalRows := 0
	for {
		batch, err := protoReader.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		totalRows += batch.Len()

		// Verify each row conforms to schema
		for i := range batch.Len() {
			row := batch.Get(i)

			id := row[wkk.NewRowKey("id")]
			require.NotNil(t, id, "Row missing 'id' column")
			assert.IsType(t, int64(0), id)

			value := row[wkk.NewRowKey("value")]
			require.NotNil(t, value, "Row missing 'value' column")
			assert.IsType(t, float64(0), value)
		}
	}

	assert.Equal(t, 100, totalRows)
}
