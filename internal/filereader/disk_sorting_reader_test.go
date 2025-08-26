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
	"fmt"
	"io"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiskSortingReader_BasicSorting(t *testing.T) {
	// Create test data with unsorted metric names, TIDs, and timestamps
	testRows := []Row{
		{
			"_cardinalhq.name":      "metric_z",
			"_cardinalhq.tid":       int64(200),
			"_cardinalhq.timestamp": int64(3000),
			"value":                 float64(1.0),
		},
		{
			"_cardinalhq.name":      "metric_a",
			"_cardinalhq.tid":       int64(100),
			"_cardinalhq.timestamp": int64(1000),
			"value":                 float64(2.0),
		},
		{
			"_cardinalhq.name":      "metric_a",
			"_cardinalhq.tid":       int64(100),
			"_cardinalhq.timestamp": int64(2000),
			"value":                 float64(3.0),
		},
	}

	mockReader := NewMockReader(testRows)
	sortingReader, err := NewDiskSortingReader(mockReader)
	require.NoError(t, err)
	defer sortingReader.Close()

	// Read all rows
	var allRows []Row
	for {
		rows := make([]Row, 10)
		n, err := sortingReader.Read(rows)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := 0; i < n; i++ {
			allRows = append(allRows, rows[i])
		}
	}

	// Verify sorting: metric_a (both rows), then metric_z
	require.Len(t, allRows, 3)

	// First two rows should be metric_a, sorted by timestamp
	assert.Equal(t, "metric_a", allRows[0]["_cardinalhq.name"])
	assert.Equal(t, int64(1000), allRows[0]["_cardinalhq.timestamp"])

	assert.Equal(t, "metric_a", allRows[1]["_cardinalhq.name"])
	assert.Equal(t, int64(2000), allRows[1]["_cardinalhq.timestamp"])

	// Last row should be metric_z
	assert.Equal(t, "metric_z", allRows[2]["_cardinalhq.name"])
	assert.Equal(t, int64(3000), allRows[2]["_cardinalhq.timestamp"])
}

func TestDiskSortingReader_TypePreservation(t *testing.T) {
	// Test various types that need to be preserved through CBOR
	testRow := Row{
		"_cardinalhq.name":      "test_metric",
		"_cardinalhq.tid":       int64(12345),
		"_cardinalhq.timestamp": int64(1640995200000),
		"string_field":          "test_string",
		"int64_field":           int64(9223372036854775807), // Max int64
		"float64_field":         float64(3.14159),
		"byte_slice":            []byte{0x01, 0x02, 0x03},
		"float64_slice":         []float64{1.1, 2.2, 3.3},
		"bool_field":            true,
		"nil_field":             nil,
	}

	mockReader := NewMockReader([]Row{testRow})
	sortingReader, err := NewDiskSortingReader(mockReader)
	require.NoError(t, err)
	defer sortingReader.Close()

	// Read the row back
	rows := make([]Row, 1)
	n, err := sortingReader.Read(rows)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	decoded := rows[0]

	// Verify all types are preserved
	assert.Equal(t, "test_metric", decoded["_cardinalhq.name"])
	assert.Equal(t, int64(12345), decoded["_cardinalhq.tid"])
	assert.Equal(t, int64(1640995200000), decoded["_cardinalhq.timestamp"])
	assert.Equal(t, "test_string", decoded["string_field"])
	assert.Equal(t, int64(9223372036854775807), decoded["int64_field"])
	assert.Equal(t, float64(3.14159), decoded["float64_field"])
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, decoded["byte_slice"])
	assert.Equal(t, []float64{1.1, 2.2, 3.3}, decoded["float64_slice"])
	assert.Equal(t, true, decoded["bool_field"])
	assert.Nil(t, decoded["nil_field"])
}

func TestDiskSortingReader_EmptyInput(t *testing.T) {
	mockReader := NewMockReader([]Row{})
	sortingReader, err := NewDiskSortingReader(mockReader)
	require.NoError(t, err)
	defer sortingReader.Close()

	// Should get EOF immediately
	rows := make([]Row, 1)
	n, err := sortingReader.Read(rows)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
}

func TestDiskSortingReader_MissingFields(t *testing.T) {
	// Test row missing required sort fields
	testRows := []Row{
		{
			"_cardinalhq.name": "metric_a",
			// Missing TID and timestamp
			"value": float64(1.0),
		},
	}

	mockReader := NewMockReader(testRows)
	sortingReader, err := NewDiskSortingReader(mockReader)
	require.NoError(t, err)
	defer sortingReader.Close()

	// Should fail when trying to read due to missing sort fields
	rows := make([]Row, 1)
	_, err = sortingReader.Read(rows)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing required sort key fields")
}

func TestDiskSortingReader_CleanupOnError(t *testing.T) {
	// Create reader that will fail during reading
	mockReader := &MockReader{
		rows:      []Row{{"test": "value"}},
		readError: fmt.Errorf("simulated read error"),
	}

	sortingReader, err := NewDiskSortingReader(mockReader)
	require.NoError(t, err)

	tempFileName := sortingReader.tempFile.Name()

	// Try to read - should fail
	rows := make([]Row, 1)
	_, err = sortingReader.Read(rows)
	assert.Error(t, err)

	// Close should clean up temp file
	err = sortingReader.Close()
	assert.NoError(t, err)

	// Verify temp file was removed
	_, err = os.Stat(tempFileName)
	assert.True(t, os.IsNotExist(err), "Temp file should be removed")
}

// MockReader for testing
type MockReader struct {
	rows       []Row
	currentIdx int
	readError  error
	closed     bool
}

func NewMockReader(rows []Row) *MockReader {
	return &MockReader{rows: rows}
}

func (m *MockReader) Read(rows []Row) (int, error) {
	if m.closed {
		return 0, fmt.Errorf("reader is closed")
	}

	if m.readError != nil {
		return 0, m.readError
	}

	if len(rows) == 0 {
		return 0, nil
	}

	n := 0
	for n < len(rows) && m.currentIdx < len(m.rows) {
		resetRow(&rows[n])
		for k, v := range m.rows[m.currentIdx] {
			rows[n][k] = v
		}
		n++
		m.currentIdx++
	}

	if m.currentIdx >= len(m.rows) && n == 0 {
		return 0, io.EOF
	}

	return n, nil
}

func (m *MockReader) Close() error {
	m.closed = true
	return nil
}

func (m *MockReader) RowCount() int64 {
	return int64(m.currentIdx)
}

func TestDiskSortingReader_CBORIdentity(t *testing.T) {
	// Test comprehensive type identity preservation through CBOR encode/decode
	testCases := []struct {
		name            string
		value           any
		expectedType    string
		expectedValue   any
		allowConversion bool // Some conversions are acceptable
	}{
		// Basic types
		{"string", "test_string", "string", "test_string", false},
		{"empty_string", "", "string", "", false},
		{"bool_true", true, "bool", true, false},
		{"bool_false", false, "bool", false, false},
		{"nil", nil, "<nil>", nil, false},

		// Integer types
		{"int64_positive", int64(9223372036854775807), "int64", int64(9223372036854775807), false},
		{"int64_negative", int64(-9223372036854775808), "int64", int64(-9223372036854775808), false},
		{"int64_zero", int64(0), "int64", int64(0), false},
		{"int32_positive", int32(2147483647), "int64", int64(2147483647), true}, // CBOR converts to int64
		{"int32_negative", int32(-2147483648), "int64", int64(-2147483648), true},
		{"uint32_max", uint32(4294967295), "int64", int64(4294967295), true}, // CBOR converts to int64 when < MaxInt64
		{"uint64_small", uint64(123456789), "int64", int64(123456789), true}, // CBOR converts uint64 < MaxInt64 to int64
		{"int_positive", int(123456), "int64", int64(123456), true},          // CBOR converts to int64
		{"int_negative", int(-123456), "int64", int64(-123456), true},

		// Float types
		{"float64_positive", float64(3.14159265359), "float64", float64(3.14159265359), false},
		{"float64_negative", float64(-2.71828), "float64", float64(-2.71828), false},
		{"float64_zero", float64(0.0), "float64", float64(0.0), false},
		{"float64_inf", math.Inf(1), "float64", math.Inf(1), false},
		{"float64_nan", math.NaN(), "float64", math.NaN(), false},                 // NaN comparison special case
		{"float32_value", float32(3.14), "float64", float64(float32(3.14)), true}, // CBOR converts float32->float64

		// Byte arrays
		{"byte_slice_empty", []byte{}, "[]uint8", []byte{}, false},
		{"byte_slice_data", []byte{0x01, 0x02, 0x03, 0xFF}, "[]uint8", []byte{0x01, 0x02, 0x03, 0xFF}, false},

		// Slice types (more complex conversion cases)
		{"float64_slice_empty", []float64{}, "[]any", []any{}, true},                                   // Empty slices become []any
		{"float64_slice_data", []float64{1.1, 2.2, 3.3}, "[]float64", []float64{1.1, 2.2, 3.3}, false}, // All float64 converts back to []float64
		{"int64_slice", []int64{1, 2, 3}, "[]any", []any{int64(1), int64(2), int64(3)}, true},          // Mixed numeric stays as []any
		{"string_slice", []string{"a", "b", "c"}, "[]any", []any{"a", "b", "c"}, true},                 // String slices stay as []any
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a test row with the value
			testRow := Row{
				"_cardinalhq.name":      "test_metric",
				"_cardinalhq.tid":       int64(1),
				"_cardinalhq.timestamp": int64(1000),
				"test_field":            tc.value,
			}

			mockReader := NewMockReader([]Row{testRow})
			sortingReader, err := NewDiskSortingReader(mockReader)
			require.NoError(t, err)
			defer sortingReader.Close()

			// Read the row back
			rows := make([]Row, 1)
			n, err := sortingReader.Read(rows)
			require.NoError(t, err)
			require.Equal(t, 1, n)

			decoded := rows[0]
			decodedValue := decoded["test_field"]

			// Check type preservation
			actualType := fmt.Sprintf("%T", decodedValue)
			if !tc.allowConversion {
				assert.Equal(t, tc.expectedType, actualType,
					"Type not preserved for %s: expected %s, got %s", tc.name, tc.expectedType, actualType)
			}

			// Check value preservation (with special handling for NaN)
			if tc.expectedType == "float64" && tc.name == "float64_nan" {
				// NaN != NaN, so check if both are NaN
				expectedFloat, ok1 := tc.expectedValue.(float64)
				actualFloat, ok2 := decodedValue.(float64)
				if ok1 && ok2 {
					assert.True(t, math.IsNaN(expectedFloat) && math.IsNaN(actualFloat),
						"Both values should be NaN for %s", tc.name)
				} else {
					t.Errorf("Expected both values to be float64 for NaN test")
				}
			} else {
				assert.Equal(t, tc.expectedValue, decodedValue,
					"Value not preserved for %s", tc.name)
			}
		})
	}
}

func TestDiskSortingReader_CBOREdgeCases(t *testing.T) {
	// Test edge cases that might break CBOR encoding/decoding
	edgeCases := []struct {
		name       string
		value      any
		shouldWork bool
	}{
		// Nested structures (CBOR converts complex slices to []any)
		{"map_in_map", map[string]any{"nested": map[string]any{"key": "value"}}, true},
		{"slice_of_maps", []any{map[string]any{"a": int64(1)}, map[string]any{"b": int64(2)}}, true},

		// Very large numbers
		{"max_int64", int64(9223372036854775807), true},
		{"min_int64", int64(-9223372036854775808), true},
		{"large_uint64", uint64(9223372036854775808), false}, // Just above max int64, will overflow and fail

		// Unicode strings
		{"unicode_string", "Hello 世界 🌍 emoji", true},
		{"empty_unicode", "", true},

		// Large byte arrays
		{"large_byte_array", make([]byte, 10000), true}, // 10KB
	}

	for _, tc := range edgeCases {
		t.Run(tc.name, func(t *testing.T) {
			// For large byte array test, fill with pattern
			if tc.name == "large_byte_array" {
				data := tc.value.([]byte)
				for i := range data {
					data[i] = byte(i % 256)
				}
			}

			testRow := Row{
				"_cardinalhq.name":      "edge_case_metric",
				"_cardinalhq.tid":       int64(1),
				"_cardinalhq.timestamp": int64(2000),
				"edge_value":            tc.value,
			}

			mockReader := NewMockReader([]Row{testRow})
			sortingReader, err := NewDiskSortingReader(mockReader)
			require.NoError(t, err)
			defer sortingReader.Close()

			// Try to read back
			rows := make([]Row, 1)
			n, err := sortingReader.Read(rows)

			if tc.shouldWork {
				require.NoError(t, err, "Edge case %s should work", tc.name)
				require.Equal(t, 1, n)

				// For large byte array, verify pattern
				if tc.name == "large_byte_array" {
					decoded := rows[0]["edge_value"].([]byte)
					require.Len(t, decoded, 10000)
					for i := 0; i < 100; i++ { // Check first 100 bytes
						assert.Equal(t, byte(i), decoded[i], "Byte pattern mismatch at position %d", i)
					}
				} else {
					assert.Equal(t, tc.value, rows[0]["edge_value"], "Value mismatch for edge case %s", tc.name)
				}
			} else {
				assert.Error(t, err, "Edge case %s should fail", tc.name)
			}
		})
	}
}
