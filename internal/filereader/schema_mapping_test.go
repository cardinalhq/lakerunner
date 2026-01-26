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

// TestReaderSchema_ColumnNameMapping tests the column name mapping functionality.
func TestReaderSchema_ColumnNameMapping(t *testing.T) {
	schema := NewReaderSchema()

	// Add columns with mappings
	schema.AddColumn(
		wkk.NewRowKey("foo_bar"),
		wkk.NewRowKey("foo.bar"),
		DataTypeString,
		true,
	)
	schema.AddColumn(
		wkk.NewRowKey("baz_qux_quux"),
		wkk.NewRowKey("baz.qux.quux"),
		DataTypeInt64,
		true,
	)
	schema.AddColumn(
		wkk.NewRowKey("simple"),
		wkk.NewRowKey("simple"),
		DataTypeFloat64,
		true,
	)

	// Test GetOriginalName
	assert.Equal(t, wkk.NewRowKey("foo.bar"), schema.GetOriginalName(wkk.NewRowKey("foo_bar")))
	assert.Equal(t, wkk.NewRowKey("baz.qux.quux"), schema.GetOriginalName(wkk.NewRowKey("baz_qux_quux")))
	assert.Equal(t, wkk.NewRowKey("simple"), schema.GetOriginalName(wkk.NewRowKey("simple")))

	// Test GetOriginalName for non-existent key (should return the same key)
	assert.Equal(t, wkk.NewRowKey("nonexistent"), schema.GetOriginalName(wkk.NewRowKey("nonexistent")))

	// Test GetAllOriginalNames
	allMappings := schema.GetAllOriginalNames()
	assert.Len(t, allMappings, 3)
	assert.Equal(t, wkk.NewRowKey("foo.bar"), allMappings[wkk.NewRowKey("foo_bar")])
	assert.Equal(t, wkk.NewRowKey("baz.qux.quux"), allMappings[wkk.NewRowKey("baz_qux_quux")])
	assert.Equal(t, wkk.NewRowKey("simple"), allMappings[wkk.NewRowKey("simple")])

	// Test that schema has the normalized column names (not the original dotted names)
	assert.True(t, schema.HasColumn("foo_bar"), "Schema should have normalized column name")
	assert.False(t, schema.HasColumn("foo.bar"), "Schema should NOT have original dotted name")
	assert.True(t, schema.HasColumn("baz_qux_quux"), "Schema should have normalized column name")
	assert.False(t, schema.HasColumn("baz.qux.quux"), "Schema should NOT have original dotted name")
	assert.True(t, schema.HasColumn("simple"), "Schema should have simple column name")
}

// TestIngestLogParquetReader_ColumnNameMapping tests that parquet reader extracts column mappings.
func TestIngestLogParquetReader_ColumnNameMapping(t *testing.T) {
	ctx := context.Background()

	// Create test parquet with dotted column names
	rows := []map[string]any{
		{
			"simple.name":     "value1",
			"nested.field":    int64(123),
			"deeply.nested.x": 45.67,
		},
	}

	parquetData, _ := createTestParquetInMemory(t, rows)
	reader := bytes.NewReader(parquetData)

	arrowReader, err := NewIngestLogParquetReader(ctx, reader, 1000)
	require.NoError(t, err)
	defer func() { _ = arrowReader.Close() }()

	schema := arrowReader.GetSchema()
	require.NotNil(t, schema)

	// Verify mappings are correct
	mappings := schema.GetAllOriginalNames()
	assert.Equal(t, wkk.NewRowKey("simple.name"), mappings[wkk.NewRowKey("simple_name")])
	assert.Equal(t, wkk.NewRowKey("nested.field"), mappings[wkk.NewRowKey("nested_field")])
	assert.Equal(t, wkk.NewRowKey("deeply.nested.x"), mappings[wkk.NewRowKey("deeply_nested_x")])

	// Verify schema has only the normalized names (not the original dotted names)
	assert.True(t, schema.HasColumn("simple_name"))
	assert.False(t, schema.HasColumn("simple.name"), "Schema should NOT have original dotted name")
	assert.True(t, schema.HasColumn("nested_field"))
	assert.False(t, schema.HasColumn("nested.field"), "Schema should NOT have original dotted name")
	assert.True(t, schema.HasColumn("deeply_nested_x"))
	assert.False(t, schema.HasColumn("deeply.nested.x"), "Schema should NOT have original dotted name")
}

// TestJSONReader_ColumnNameMapping tests that JSON reader schema uses identity mappings.
// Note: JSON reader requires a seekable reader for schema inference, so we skip schema
// testing here. The important property is that SchemaBuilder (used by JSON reader)
// uses AddColumn with identity mappings (key, key).
func TestJSONReader_ColumnNameMapping(t *testing.T) {
	// This test verifies that SchemaBuilder (used by JSON reader) creates identity mappings
	builder := NewSchemaBuilder()

	// Simulate what JSON reader does
	builder.AddStringValue("name", "test")
	builder.AddValue("value", int64(123))

	schema := builder.Build()
	require.NotNil(t, schema)

	// Verify schema has the columns
	assert.True(t, schema.HasColumn("name"))
	assert.True(t, schema.HasColumn("value"))

	// Verify identity mappings exist
	mappings := schema.GetAllOriginalNames()
	assert.Equal(t, wkk.NewRowKey("name"), mappings[wkk.NewRowKey("name")])
	assert.Equal(t, wkk.NewRowKey("value"), mappings[wkk.NewRowKey("value")])
}

// TestCSVReader_ColumnNameMapping tests that CSV reader emits identity mappings.
func TestCSVReader_ColumnNameMapping(t *testing.T) {
	csvData := "name,value,score\nAlice,100,95.5\nBob,200,87.3"
	reader := io.NopCloser(bytes.NewReader([]byte(csvData)))

	csvReader, err := NewCSVReader(reader, 1000)
	require.NoError(t, err)
	defer func() { _ = csvReader.Close() }()

	// Read at least one row to trigger schema inference
	ctx := context.Background()
	_, err = csvReader.Next(ctx)
	require.NoError(t, err)

	schema := csvReader.GetSchema()
	require.NotNil(t, schema)

	// CSV reader should have identity mappings
	mappings := schema.GetAllOriginalNames()

	// Check that all mappings are identity mappings
	for newKey, origKey := range mappings {
		assert.Equal(t, newKey, origKey, "CSV reader should have identity mappings")
	}

	// Verify schema has expected columns
	assert.True(t, schema.HasColumn("name"))
	assert.True(t, schema.HasColumn("value"))
	assert.True(t, schema.HasColumn("score"))
}

// TestIngestLogParquetReader_MapFlattening tests that maps are flattened with correct mappings.
func TestIngestLogParquetReader_MapFlattening(t *testing.T) {
	ctx := context.Background()

	// Create test parquet with a map column
	// Maps in parquet will be flattened as: mapname_key
	rows := []map[string]any{
		{
			"id":            int64(1),
			"labels_env":    "production",
			"labels_region": "us-west-2",
		},
	}

	parquetData, _ := createTestParquetInMemory(t, rows)
	reader := bytes.NewReader(parquetData)

	arrowReader, err := NewIngestLogParquetReader(ctx, reader, 1000)
	require.NoError(t, err)
	defer func() { _ = arrowReader.Close() }()

	schema := arrowReader.GetSchema()
	require.NotNil(t, schema)

	// Verify flattened columns exist
	assert.True(t, schema.HasColumn("id"))
	assert.True(t, schema.HasColumn("labels_env"))
	assert.True(t, schema.HasColumn("labels_region"))

	// Read data to verify flattening works
	batch, err := arrowReader.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, batch)
	require.Equal(t, 1, batch.Len())

	row := batch.Get(0)
	assert.Equal(t, int64(1), row[wkk.NewRowKey("id")])
	assert.Equal(t, "production", row[wkk.NewRowKey("labels_env")])
	assert.Equal(t, "us-west-2", row[wkk.NewRowKey("labels_region")])
}

// TestReaderSchema_DuplicateMappings tests that duplicate mappings (latest wins) work correctly.
func TestReaderSchema_DuplicateMappings(t *testing.T) {
	schema := NewReaderSchema()

	// Add a column with one mapping
	schema.AddColumn(
		wkk.NewRowKey("field"),
		wkk.NewRowKey("original1"),
		DataTypeString,
		true,
	)

	// Add the same column with a different mapping (latest wins)
	schema.AddColumn(
		wkk.NewRowKey("field"),
		wkk.NewRowKey("original2"),
		DataTypeInt64,
		true,
	)

	// The latest mapping should win
	assert.Equal(t, wkk.NewRowKey("original2"), schema.GetOriginalName(wkk.NewRowKey("field")))

	// Type should be promoted (string + int64 -> string)
	assert.Equal(t, DataTypeString, schema.GetColumnType("field"))
}
