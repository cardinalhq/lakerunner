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

// TestPromoteType tests type promotion rules.
func TestPromoteType(t *testing.T) {
	tests := []struct {
		name     string
		a        DataType
		b        DataType
		expected DataType
	}{
		// Same types
		{"int64 + int64", DataTypeInt64, DataTypeInt64, DataTypeInt64},
		{"float64 + float64", DataTypeFloat64, DataTypeFloat64, DataTypeFloat64},
		{"string + string", DataTypeString, DataTypeString, DataTypeString},
		{"bool + bool", DataTypeBool, DataTypeBool, DataTypeBool},
		{"bytes + bytes", DataTypeBytes, DataTypeBytes, DataTypeBytes},

		// Numeric promotions
		{"int64 + float64", DataTypeInt64, DataTypeFloat64, DataTypeFloat64},
		{"float64 + int64", DataTypeFloat64, DataTypeInt64, DataTypeFloat64},

		// String promotions (string is most general)
		{"int64 + string", DataTypeInt64, DataTypeString, DataTypeString},
		{"string + int64", DataTypeString, DataTypeInt64, DataTypeString},
		{"float64 + string", DataTypeFloat64, DataTypeString, DataTypeString},
		{"string + float64", DataTypeString, DataTypeFloat64, DataTypeString},
		{"bool + string", DataTypeBool, DataTypeString, DataTypeString},
		{"string + bool", DataTypeString, DataTypeBool, DataTypeString},

		// Bool promotions (mixed with numeric → string)
		{"int64 + bool", DataTypeInt64, DataTypeBool, DataTypeString},
		{"bool + int64", DataTypeBool, DataTypeInt64, DataTypeString},
		{"float64 + bool", DataTypeFloat64, DataTypeBool, DataTypeString},
		{"bool + float64", DataTypeBool, DataTypeFloat64, DataTypeString},

		// Bytes promotions (bytes with anything else → string)
		{"bytes + string", DataTypeBytes, DataTypeString, DataTypeString},
		{"string + bytes", DataTypeString, DataTypeBytes, DataTypeString},
		{"bytes + int64", DataTypeBytes, DataTypeInt64, DataTypeString},
		{"bytes + float64", DataTypeBytes, DataTypeFloat64, DataTypeString},
		{"bytes + bool", DataTypeBytes, DataTypeBool, DataTypeString},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := promoteType(tt.a, tt.b)
			assert.Equal(t, tt.expected, result, "promoteType(%s, %s) should return %s", tt.a, tt.b, tt.expected)
		})
	}
}

// TestNormalizeRow_TypeConversion tests that normalizeRow correctly converts values to schema types.
func TestNormalizeRow_TypeConversion(t *testing.T) {
	schema := NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("str_col"), DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("int_col"), DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("float_col"), DataTypeFloat64, true)
	schema.AddColumn(wkk.NewRowKey("bool_col"), DataTypeBool, true)

	// Create a row with values that need conversion
	row := pipeline.GetPooledRow()
	row[wkk.NewRowKey("str_col")] = int64(42)  // int64 → string
	row[wkk.NewRowKey("int_col")] = "123"      // string → int64
	row[wkk.NewRowKey("float_col")] = int64(5) // int64 → float64
	row[wkk.NewRowKey("bool_col")] = "true"    // string → bool

	// Normalize in-place
	normalizeRow(context.Background(), row, schema)
	defer pipeline.ReturnPooledRow(row)

	// Verify conversions
	assert.Equal(t, "42", row[wkk.NewRowKey("str_col")])
	assert.Equal(t, int64(123), row[wkk.NewRowKey("int_col")])
	assert.Equal(t, float64(5), row[wkk.NewRowKey("float_col")])
	assert.Equal(t, true, row[wkk.NewRowKey("bool_col")])
}

// TestNormalizeRow_NullHandling tests that normalizeRow removes null values.
func TestNormalizeRow_NullHandling(t *testing.T) {
	schema := NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("col1"), DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("col2"), DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("col3"), DataTypeString, true)

	// Create a row with some null values
	row := pipeline.GetPooledRow()
	row[wkk.NewRowKey("col1")] = "value1"
	row[wkk.NewRowKey("col2")] = nil // null value
	row[wkk.NewRowKey("col3")] = "value3"
	// col4 is not in row at all

	// Normalize in-place
	normalizeRow(context.Background(), row, schema)
	defer pipeline.ReturnPooledRow(row)

	// Verify only non-null values are present
	assert.Equal(t, "value1", row[wkk.NewRowKey("col1")])
	assert.NotContains(t, row, wkk.NewRowKey("col2"), "Null value should be removed")
	assert.Equal(t, "value3", row[wkk.NewRowKey("col3")])
}

// TestNormalizeRow_EmptyStringIsNotNull tests that empty strings are treated as non-null.
func TestNormalizeRow_EmptyStringIsNotNull(t *testing.T) {
	schema := NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("col1"), DataTypeString, true)

	row := pipeline.GetPooledRow()
	row[wkk.NewRowKey("col1")] = "" // empty string

	// Normalize in-place
	normalizeRow(context.Background(), row, schema)
	defer pipeline.ReturnPooledRow(row)

	// Empty string should be kept (it's a value, not null)
	assert.Equal(t, "", row[wkk.NewRowKey("col1")])
}

// TestNormalizeRow_UnknownColumns tests that columns not in schema are passed through.
func TestNormalizeRow_UnknownColumns(t *testing.T) {
	schema := NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("known_col"), DataTypeString, true)

	row := pipeline.GetPooledRow()
	row[wkk.NewRowKey("known_col")] = "known"
	row[wkk.NewRowKey("unknown_col")] = "unknown"

	// Normalize in-place
	normalizeRow(context.Background(), row, schema)
	defer pipeline.ReturnPooledRow(row)

	// Known column should be present, unknown column should be dropped
	assert.Equal(t, "known", row[wkk.NewRowKey("known_col")])
	assert.NotContains(t, row, wkk.NewRowKey("unknown_col"), "Unknown column should be dropped")
}

// TestConvertValue tests individual value conversions.
func TestConvertValue(t *testing.T) {
	tests := []struct {
		name       string
		value      any
		targetType DataType
		expected   any
		expectErr  bool
	}{
		// String conversions
		{"string to string", "hello", DataTypeString, "hello", false},
		{"int64 to string", int64(42), DataTypeString, "42", false},
		{"float64 to string", 3.14, DataTypeString, "3.14", false},
		{"bool to string (true)", true, DataTypeString, "true", false},
		{"bool to string (false)", false, DataTypeString, "false", false},
		{"bytes to string", []byte("test"), DataTypeString, "test", false},

		// Int64 conversions
		{"int64 to int64", int64(42), DataTypeInt64, int64(42), false},
		{"int to int64", 42, DataTypeInt64, int64(42), false},
		{"float64 to int64", 3.14, DataTypeInt64, int64(3), false},
		{"string to int64", "123", DataTypeInt64, int64(123), false},
		{"invalid string to int64", "abc", DataTypeInt64, int64(0), true},

		// Float64 conversions
		{"float64 to float64", 3.14, DataTypeFloat64, 3.14, false},
		{"int64 to float64", int64(42), DataTypeFloat64, float64(42), false},
		{"int to float64", 42, DataTypeFloat64, float64(42), false},
		{"string to float64", "3.14", DataTypeFloat64, 3.14, false},
		{"invalid string to float64", "abc", DataTypeFloat64, float64(0), true},

		// Bool conversions
		{"bool to bool", true, DataTypeBool, true, false},
		{"string to bool (true)", "true", DataTypeBool, true, false},
		{"string to bool (1)", "1", DataTypeBool, true, false},
		{"string to bool (false)", "false", DataTypeBool, false, false},
		{"string to bool (0)", "0", DataTypeBool, false, false},
		{"int64 to bool (non-zero)", int64(42), DataTypeBool, true, false},
		{"int64 to bool (zero)", int64(0), DataTypeBool, false, false},

		// Bytes conversions
		{"bytes to bytes", []byte("test"), DataTypeBytes, []byte("test"), false},
		{"string to bytes", "test", DataTypeBytes, []byte("test"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := convertValue(tt.value, tt.targetType)

			if tt.expectErr {
				assert.Error(t, err, "Expected error for conversion")
			} else {
				require.NoError(t, err, "Unexpected error during conversion")
				assert.Equal(t, tt.expected, result, "Conversion result mismatch")
			}
		})
	}
}

// TestConvertValue_Fallback tests that failed conversions drop the value.
func TestConvertValue_Fallback(t *testing.T) {
	schema := NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("col1"), DataTypeInt64, true)

	// Create a row with a value that can't be converted to int64
	row := pipeline.GetPooledRow()
	row[wkk.NewRowKey("col1")] = "not_a_number"

	// Normalize in-place
	normalizeRow(context.Background(), row, schema)
	defer pipeline.ReturnPooledRow(row)

	// Value should be dropped if conversion fails
	assert.NotContains(t, row, wkk.NewRowKey("col1"), "Value that fails conversion should be dropped")
}

// TestReaderSchema_AddColumn tests schema column addition and type promotion.
func TestReaderSchema_AddColumn(t *testing.T) {
	schema := NewReaderSchema()

	// Add initial column
	key := wkk.NewRowKey("test_col")
	schema.AddColumn(key, DataTypeInt64, true)
	assert.Equal(t, DataTypeInt64, schema.GetColumnType("test_col"))

	// Add same column with different type (should promote)
	schema.AddColumn(key, DataTypeString, true)
	assert.Equal(t, DataTypeString, schema.GetColumnType("test_col"), "Type should be promoted to string")

	// Add same column with float64 (but string is already most general)
	schema.AddColumn(key, DataTypeFloat64, true)
	assert.Equal(t, DataTypeString, schema.GetColumnType("test_col"), "Type should remain string")
}

// TestReaderSchema_HasNonNull tests HasNonNull tracking.
func TestReaderSchema_HasNonNull(t *testing.T) {
	schema := NewReaderSchema()
	key := wkk.NewRowKey("test_col")

	// Add column with HasNonNull=false
	schema.AddColumn(key, DataTypeString, false)
	columns := schema.Columns()
	require.Len(t, columns, 1)
	assert.False(t, columns[0].HasNonNull)

	// Add same column with HasNonNull=true (should update to true)
	schema.AddColumn(key, DataTypeString, true)
	columns = schema.Columns()
	require.Len(t, columns, 1)
	assert.True(t, columns[0].HasNonNull)

	// Add same column with HasNonNull=false (should stay true)
	schema.AddColumn(key, DataTypeString, false)
	columns = schema.Columns()
	require.Len(t, columns, 1)
	assert.True(t, columns[0].HasNonNull, "HasNonNull should not flip back to false")
}
