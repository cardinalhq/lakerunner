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

package parquetwriter

import (
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestNewSchemaBuilder(t *testing.T) {
	builder := NewSchemaBuilder()
	require.NotNil(t, builder)
	require.NotNil(t, builder.nodes)
	require.Equal(t, 0, len(builder.nodes))
	require.False(t, builder.HasColumns())
}

func TestSchemaBuilder_AddRow_Basic(t *testing.T) {
	builder := NewSchemaBuilder()

	row := map[string]any{
		"id":     int64(123),
		"name":   "test",
		"active": true,
		"score":  3.14,
		"data":   []byte("binary"),
		"tags":   []string{"a", "b"},
		"values": []int64{1, 2, 3},
	}

	err := builder.AddRow(row)
	require.NoError(t, err)

	// Check that all fields were added
	require.True(t, builder.HasColumns())
	nodes := builder.Build()
	require.Equal(t, 7, len(nodes))

	// Verify specific node types exist
	require.Contains(t, nodes, "id")
	require.Contains(t, nodes, "name")
	require.Contains(t, nodes, "active")
	require.Contains(t, nodes, "score")
	require.Contains(t, nodes, "data")
	require.Contains(t, nodes, "tags")
	require.Contains(t, nodes, "values")
}

func TestSchemaBuilder_AddRow_NilValues(t *testing.T) {
	builder := NewSchemaBuilder()

	// Add row with nil values - should be skipped
	row1 := map[string]any{
		"id":     int64(123),
		"name":   nil,
		"active": nil,
		"score":  3.14,
	}

	err := builder.AddRow(row1)
	require.NoError(t, err)

	nodes := builder.Build()
	require.Equal(t, 2, len(nodes)) // Only id and score should be present
	require.Contains(t, nodes, "id")
	require.Contains(t, nodes, "score")
	require.NotContains(t, nodes, "name")
	require.NotContains(t, nodes, "active")

	// Add another row with values for previously nil fields
	row2 := map[string]any{
		"name":   "test",
		"active": true,
	}

	err = builder.AddRow(row2)
	require.NoError(t, err)

	nodes = builder.Build()
	require.Equal(t, 4, len(nodes)) // Now all fields should be present
	require.Contains(t, nodes, "id")
	require.Contains(t, nodes, "name")
	require.Contains(t, nodes, "active")
	require.Contains(t, nodes, "score")
}

func TestSchemaBuilder_AddRow_TypeConsistency(t *testing.T) {
	builder := NewSchemaBuilder()

	// Add first row with int64
	row1 := map[string]any{
		"id": int64(123),
	}

	err := builder.AddRow(row1)
	require.NoError(t, err)

	// Add second row with same type - should succeed
	row2 := map[string]any{
		"id": int64(456),
	}

	err = builder.AddRow(row2)
	require.NoError(t, err)

	// Add third row with different type - should fail
	row3 := map[string]any{
		"id": "string_value",
	}

	err = builder.AddRow(row3)
	require.Error(t, err)
	require.Contains(t, err.Error(), "type mismatch for field \"id\"")
}

func TestSchemaBuilder_AddRow_TypeMismatchDetails(t *testing.T) {
	builder := NewSchemaBuilder()

	// Add row with float64
	row1 := map[string]any{
		"value": 3.14,
	}
	err := builder.AddRow(row1)
	require.NoError(t, err)

	// Try to add conflicting type
	row2 := map[string]any{
		"value": int64(42),
	}
	err = builder.AddRow(row2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "type mismatch for field \"value\"")
	require.Contains(t, err.Error(), "existing=")
	require.Contains(t, err.Error(), "new=")
}

func TestSchemaBuilder_AddRow_UnsupportedType(t *testing.T) {
	builder := NewSchemaBuilder()

	// Try to add unsupported type
	row := map[string]any{
		"complex": map[string]string{"nested": "data"},
	}

	err := builder.AddRow(row)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to build node for field \"complex\"")
}

func TestSchemaBuilder_AddNodes(t *testing.T) {
	builder := NewSchemaBuilder()

	// Create some predefined nodes
	nodes := map[string]parquet.Node{
		"id":   parquet.Int(64),
		"name": parquet.String(),
	}

	err := builder.AddNodes(nodes)
	require.NoError(t, err)

	result := builder.Build()
	require.Equal(t, 2, len(result))
	require.Contains(t, result, "id")
	require.Contains(t, result, "name")
}

func TestSchemaBuilder_AddNodes_TypeMismatch(t *testing.T) {
	builder := NewSchemaBuilder()

	// Add initial nodes
	nodes1 := map[string]parquet.Node{
		"id": parquet.Int(64),
	}
	err := builder.AddNodes(nodes1)
	require.NoError(t, err)

	// Try to add conflicting nodes
	nodes2 := map[string]parquet.Node{
		"id": parquet.String(), // Different type
	}
	err = builder.AddNodes(nodes2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "type mismatch for field \"id\"")
}

func TestSchemaBuilder_GetColumnNames(t *testing.T) {
	builder := NewSchemaBuilder()

	// Initially empty
	names := builder.GetColumnNames()
	require.Empty(t, names)

	// Add some columns
	row := map[string]any{
		"zebra":   "last",
		"alpha":   "first",
		"bravo":   "second",
		"charlie": "third",
	}

	err := builder.AddRow(row)
	require.NoError(t, err)

	names = builder.GetColumnNames()
	require.Equal(t, 4, len(names))

	// Names should include all columns (order not guaranteed)
	require.Contains(t, names, "zebra")
	require.Contains(t, names, "alpha")
	require.Contains(t, names, "bravo")
	require.Contains(t, names, "charlie")
}

func TestSchemaBuilder_Build_IsolatesInternalState(t *testing.T) {
	builder := NewSchemaBuilder()

	row := map[string]any{
		"id": int64(123),
	}
	err := builder.AddRow(row)
	require.NoError(t, err)

	// Get the built nodes
	nodes1 := builder.Build()
	require.Equal(t, 1, len(nodes1))

	// Modify the returned map
	nodes1["new_field"] = parquet.String()

	// Get nodes again - should not be affected by external modification
	nodes2 := builder.Build()
	require.Equal(t, 1, len(nodes2))
	require.NotContains(t, nodes2, "new_field")
}

func TestSchemaBuilder_ComplexWorkflow(t *testing.T) {
	builder := NewSchemaBuilder()

	// Simulate adding multiple rows with varying schemas
	rows := []map[string]any{
		{
			"timestamp": int64(1000),
			"message":   "first",
		},
		{
			"timestamp": int64(2000),
			"message":   "second",
			"level":     "INFO",
		},
		{
			"timestamp": int64(3000),
			"message":   "third",
			"level":     "ERROR",
			"error":     "something went wrong",
		},
		{
			"timestamp": int64(4000),
			"message":   "fourth",
			"level":     "DEBUG",
			"metadata":  []string{"key1", "key2"},
		},
	}

	for i, row := range rows {
		err := builder.AddRow(row)
		require.NoError(t, err, "Failed on row %d", i)
	}

	// Verify final schema contains all discovered fields
	nodes := builder.Build()
	expectedFields := []string{"timestamp", "message", "level", "error", "metadata"}
	require.Equal(t, len(expectedFields), len(nodes))

	for _, field := range expectedFields {
		require.Contains(t, nodes, field, "Missing field: %s", field)
	}

	// Verify we can get column names
	columnNames := builder.GetColumnNames()
	require.Equal(t, len(expectedFields), len(columnNames))

	sort.Strings(expectedFields)
	sort.Strings(columnNames)
	require.Equal(t, expectedFields, columnNames)
}

func TestSchemaBuilder_EmptyRow(t *testing.T) {
	builder := NewSchemaBuilder()

	// Add empty row
	err := builder.AddRow(map[string]any{})
	require.NoError(t, err)

	// Should have no columns
	require.False(t, builder.HasColumns())
	nodes := builder.Build()
	require.Equal(t, 0, len(nodes))
}

func TestSchemaBuilder_NilRow(t *testing.T) {
	builder := NewSchemaBuilder()

	// AddRow should not panic with nil row
	// The actual behavior depends on how it handles nil - let's test both cases
	err := builder.AddRow(nil)
	// This might error or succeed depending on implementation
	// The key is that it shouldn't panic
	require.NotPanics(t, func() {
		_ = builder.AddRow(nil)
	})

	// If it didn't error, it should have no effect
	if err == nil {
		require.False(t, builder.HasColumns())
		nodes := builder.Build()
		require.Equal(t, 0, len(nodes))
	}
}

func TestSchemaBuilder_Integration_WithParquetSchema(t *testing.T) {
	builder := NewSchemaBuilder()

	// Add sample data
	sampleRows := []map[string]any{
		{
			"id":        int64(1),
			"timestamp": int64(1000000000),
			"message":   "Hello world",
			"active":    true,
			"score":     95.5,
			"tags":      []string{"test", "demo"},
		},
		{
			"id":        int64(2),
			"timestamp": int64(2000000000),
			"message":   "Second message",
			"active":    false,
			"score":     87.3,
			"tags":      []string{"prod", "live"},
		},
	}

	for _, row := range sampleRows {
		err := builder.AddRow(row)
		require.NoError(t, err)
	}

	// Build the schema nodes
	nodes := builder.Build()
	require.Equal(t, 6, len(nodes))

	// Create a parquet schema from the nodes using existing utility
	schema, err := ParquetSchemaFromNodemap("test_schema", nodes)
	require.NoError(t, err)
	require.NotNil(t, schema)
	require.Equal(t, 6, len(schema.Fields()))

	// Verify we can create writer options with this schema (uses existing utility)
	tmpdir := t.TempDir()
	options := WriterOptions(tmpdir, schema)
	require.NotEmpty(t, options)

	// And verify we can create a writer config from those options
	_, err = parquet.NewWriterConfig(options...)
	require.NoError(t, err)
}
