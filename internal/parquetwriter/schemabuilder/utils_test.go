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

package schemabuilder

import (
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestWriterOptions(t *testing.T) {
	tmpdir := t.TempDir()

	nodes := map[string]parquet.Node{
		"test_field": parquet.Int(64),
	}
	schema := parquet.NewSchema("test", parquet.Group(nodes))

	options := WriterOptions(tmpdir, schema)

	// Should have at least 7 options (schema + compression + page buffer size + etc.)
	if len(options) < 7 {
		t.Errorf("Expected at least 7 writer options, got %d", len(options))
	}

	// Test that we can create a writer config with these options
	_, err := parquet.NewWriterConfig(options...)
	if err != nil {
		t.Fatalf("Failed to create writer config: %v", err)
	}
}

func TestWantDictionary(t *testing.T) {
	tests := []struct {
		fieldName string
		expected  bool
	}{
		{"_cardinalhq_message", false}, // Overridden to false
		{"_cardinalhq_tid", false},     // Overridden to false
		{"regular_field", true},        // Default value
		{"another_field", true},        // Default value
	}

	for _, tt := range tests {
		t.Run(tt.fieldName, func(t *testing.T) {
			result := wantDictionary(tt.fieldName)
			if result != tt.expected {
				t.Errorf("wantDictionary(%q) = %v, expected %v", tt.fieldName, result, tt.expected)
			}
		})
	}
}

func TestParquetNodeFromType(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		wantErr  bool
		nodeType string
	}{
		// Basic types we already test
		{"int64_field", int64(123), false, "int64"},
		{"string_field", "test", false, "string"},
		{"bool_field", true, false, "bool"},
		{"float64_field", 3.14, false, "float64"},
		{"bytes_field", []byte("test"), false, "[]byte"},
		{"int_slice", []int64{1, 2, 3}, false, "[]int64"},
		{"string_slice", []string{"a", "b"}, false, "[]string"},
		{"unsupported", map[string]string{"key": "value"}, true, ""},

		// Additional primitive types to improve coverage
		{"byte_field", byte(255), false, "byte"},
		{"int8_field", int8(-128), false, "int8"},
		{"int8_slice", []int8{-1, 0, 1}, false, "[]int8"},
		{"int16_field", int16(-32768), false, "int16"},
		{"int16_slice", []int16{-100, 0, 100}, false, "[]int16"},
		{"int32_field", int32(-2147483648), false, "int32"},
		{"int32_slice", []int32{-1000, 0, 1000}, false, "[]int32"},
		{"float32_field", float32(3.14), false, "float32"},
		{"float32_slice", []float32{1.1, 2.2, 3.3}, false, "[]float32"},
		{"float64_slice", []float64{1.1, 2.2, 3.3}, false, "[]float64"},
		{"bool_slice", []bool{true, false, true}, false, "[]bool"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := ParquetNodeFromType(tt.name, tt.value)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ParquetNodeFromType() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("ParquetNodeFromType() unexpected error: %v", err)
				return
			}

			if node == nil {
				t.Error("ParquetNodeFromType() returned nil node")
			}
		})
	}
}

func TestNodesFromMap(t *testing.T) {
	nodes := make(map[string]parquet.Node)

	sampleData := map[string]any{
		"id":       int64(123),
		"name":     "test",
		"active":   true,
		"score":    3.14,
		"tags":     []string{"a", "b"},
		"null_val": nil, // Should be skipped
	}

	err := NodesFromMap(nodes, sampleData)
	if err != nil {
		t.Fatalf("NodesFromMap() error = %v", err)
	}

	// Should have 5 nodes (null_val should be skipped)
	expectedCount := 5
	if len(nodes) != expectedCount {
		t.Errorf("Expected %d nodes, got %d", expectedCount, len(nodes))
	}

	// Check that expected fields are present
	expectedFields := []string{"id", "name", "active", "score", "tags"}
	for _, field := range expectedFields {
		if _, ok := nodes[field]; !ok {
			t.Errorf("Expected field %q not found in nodes", field)
		}
	}

	// Test type mismatch error
	sampleData2 := map[string]any{
		"id": "string_value", // Different type than the int64 above
	}

	err = NodesFromMap(nodes, sampleData2)
	if err == nil {
		t.Error("Expected type mismatch error but got none")
	}
}

func TestNodesFromMap_UnsupportedType(t *testing.T) {
	nodes := make(map[string]parquet.Node)

	// Test error case where ParquetNodeFromType fails
	sampleData := map[string]any{
		"unsupported_field": map[string]string{"key": "value"}, // Unsupported type
	}

	err := NodesFromMap(nodes, sampleData)
	if err == nil {
		t.Error("Expected error for unsupported type but got none")
	}

	// Should contain error message with field name
	if !strings.Contains(err.Error(), "unsupported_field") {
		t.Errorf("Expected error to mention field name, got: %v", err)
	}
}

func TestNodesFromMap_SameFieldTwice(t *testing.T) {
	nodes := make(map[string]parquet.Node)

	// First call - add the field
	sampleData1 := map[string]any{
		"test_field": int64(123),
	}

	err := NodesFromMap(nodes, sampleData1)
	if err != nil {
		t.Fatalf("First call should succeed: %v", err)
	}

	if len(nodes) != 1 {
		t.Errorf("Expected 1 node after first call, got %d", len(nodes))
	}

	// Second call - same field, same type (should succeed and continue)
	sampleData2 := map[string]any{
		"test_field": int64(456), // Same type, different value
	}

	err = NodesFromMap(nodes, sampleData2)
	if err != nil {
		t.Fatalf("Second call with same type should succeed: %v", err)
	}

	// Should still have only 1 node
	if len(nodes) != 1 {
		t.Errorf("Expected 1 node after second call, got %d", len(nodes))
	}
}

func TestParquetSchemaFromNodemap(t *testing.T) {
	nodes := map[string]parquet.Node{
		"id":   parquet.Int(64),
		"name": parquet.String(),
	}

	schema, err := ParquetSchemaFromNodemap("test_schema", nodes)
	if err != nil {
		t.Fatalf("ParquetSchemaFromNodemap() error = %v", err)
	}

	if schema == nil {
		t.Error("ParquetSchemaFromNodemap() returned nil schema")
	}

	// Verify we can use this schema
	if len(schema.Fields()) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(schema.Fields()))
	}
}

func TestIntegration_SchemaCreationAndUsage(t *testing.T) {
	tmpdir := t.TempDir()

	// Create nodes from sample data
	nodes := make(map[string]parquet.Node)
	sampleData := map[string]any{
		"id":        int64(1),
		"timestamp": int64(1000),
		"message":   "test",
	}

	err := NodesFromMap(nodes, sampleData)
	if err != nil {
		t.Fatalf("Failed to create nodes: %v", err)
	}

	// Create schema
	schema, err := ParquetSchemaFromNodemap("integration_test", nodes)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	// Get writer options
	options := WriterOptions(tmpdir, schema)

	// Create writer config
	config, err := parquet.NewWriterConfig(options...)
	if err != nil {
		t.Fatalf("Failed to create writer config: %v", err)
	}

	// Verify we can create a writer (don't actually write, just test creation)
	if config == nil {
		t.Error("Writer config should not be nil")
	}
}
