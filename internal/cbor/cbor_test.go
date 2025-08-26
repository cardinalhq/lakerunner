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

package cbor

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConfig(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)
	require.NotNil(t, config)
	require.NotNil(t, config.encMode)
	require.NotNil(t, config.decMode)
}

func TestCBOR_Identity(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)

	testCases := []struct {
		name            string
		value           any
		expectedType    string
		expectedValue   any
		allowConversion bool
	}{
		// Basic types
		{"string", "test_string", "string", "test_string", false},
		{"empty_string", "", "string", "", false},
		{"bool_true", true, "bool", true, false},
		{"bool_false", false, "bool", false, false},
		{"nil", nil, "<nil>", nil, false},

		// Integer types (CBOR converts to int64)
		{"int64_positive", int64(9223372036854775807), "int64", int64(9223372036854775807), false},
		{"int64_negative", int64(-9223372036854775808), "int64", int64(-9223372036854775808), false},
		{"int64_zero", int64(0), "int64", int64(0), false},
		{"int32_positive", int32(2147483647), "int64", int64(2147483647), true},
		{"int32_negative", int32(-2147483648), "int64", int64(-2147483648), true},
		{"uint32_max", uint32(4294967295), "int64", int64(4294967295), true},
		{"uint64_small", uint64(123456789), "int64", int64(123456789), true},
		{"int_positive", int(123456), "int64", int64(123456), true},
		{"int_negative", int(-123456), "int64", int64(-123456), true},

		// Float types
		{"float64_positive", float64(3.14159265359), "float64", float64(3.14159265359), false},
		{"float64_negative", float64(-2.71828), "float64", float64(-2.71828), false},
		{"float64_zero", float64(0.0), "float64", float64(0.0), false},
		{"float64_inf", math.Inf(1), "float64", math.Inf(1), false},
		{"float64_nan", math.NaN(), "float64", math.NaN(), false},
		{"float32_value", float32(3.14), "float64", float64(float32(3.14)), true},

		// Byte arrays
		{"byte_slice_empty", []byte{}, "[]uint8", []byte{}, false},
		{"byte_slice_data", []byte{0x01, 0x02, 0x03, 0xFF}, "[]uint8", []byte{0x01, 0x02, 0x03, 0xFF}, false},

		// Slice types
		{"float64_slice_empty", []float64{}, "[]any", []any{}, true},
		{"float64_slice_data", []float64{1.1, 2.2, 3.3}, "[]float64", []float64{1.1, 2.2, 3.3}, false},
		{"int64_slice", []int64{1, 2, 3}, "[]any", []any{int64(1), int64(2), int64(3)}, true},
		{"string_slice", []string{"a", "b", "c"}, "[]any", []any{"a", "b", "c"}, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a test row with the value
			testRow := map[string]any{
				"_cardinalhq.name":      "test_metric",
				"_cardinalhq.tid":       int64(1),
				"_cardinalhq.timestamp": int64(1000),
				"test_field":            tc.value,
			}

			// Encode then decode
			encoded, err := config.Encode(testRow)
			require.NoError(t, err)

			decoded, err := config.Decode(encoded)
			require.NoError(t, err)

			decodedValue := decoded["test_field"]

			// Check type preservation (if strict)
			actualType := getTypeName(decodedValue)
			if !tc.allowConversion {
				assert.Equal(t, tc.expectedType, actualType,
					"Type not preserved for %s: expected %s, got %s", tc.name, tc.expectedType, actualType)
			}

			// Check value preservation (with special handling for NaN)
			if tc.expectedType == "float64" && tc.name == "float64_nan" {
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

func TestCBOR_EdgeCases(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)

	edgeCases := []struct {
		name       string
		value      any
		shouldWork bool
	}{
		// Nested structures (DefaultMapType ensures we get map[string]any)
		{"map_in_map", map[string]any{"nested": map[string]any{"key": "value"}}, true},
		{"slice_of_maps", []any{map[string]any{"a": int64(1)}, map[string]any{"b": int64(2)}}, true},

		// Large numbers
		{"max_int64", int64(9223372036854775807), true},
		{"min_int64", int64(-9223372036854775808), true},
		{"large_uint64_overflow", uint64(9223372036854775808), false}, // Will overflow

		// Unicode strings
		{"unicode_string", "Hello 世界 🌍 emoji", true},
		{"empty_unicode", "", true},

		// Large byte arrays
		{"large_byte_array", make([]byte, 10000), true},
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

			testRow := map[string]any{
				"_cardinalhq.name":      "edge_case_metric",
				"_cardinalhq.tid":       int64(1),
				"_cardinalhq.timestamp": int64(2000),
				"edge_value":            tc.value,
			}

			// Try to encode/decode
			encoded, err := config.Encode(testRow)
			if !tc.shouldWork {
				// Some edge cases should fail during encoding or decoding
				if err != nil {
					return // Expected failure
				}
				// Try decoding, might fail here
				_, err = config.Decode(encoded)
				assert.Error(t, err, "Edge case %s should fail", tc.name)
				return
			}

			require.NoError(t, err, "Edge case %s should work", tc.name)

			decoded, err := config.Decode(encoded)
			require.NoError(t, err, "Edge case %s should work", tc.name)

			// For large byte array, verify pattern
			if tc.name == "large_byte_array" {
				decodedBytes := decoded["edge_value"].([]byte)
				require.Len(t, decodedBytes, 10000)
				for i := 0; i < 100; i++ { // Check first 100 bytes
					assert.Equal(t, byte(i), decodedBytes[i], "Byte pattern mismatch at position %d", i)
				}
			} else {
				assert.Equal(t, tc.value, decoded["edge_value"], "Value mismatch for edge case %s", tc.name)
			}
		})
	}
}

func TestCBOR_EncoderDecoder(t *testing.T) {
	config, err := NewConfig()
	require.NoError(t, err)

	testRow := map[string]any{
		"string_field": "test",
		"int_field":    int64(42),
		"float_field":  3.14159,
		"bool_field":   true,
		"byte_field":   []byte{1, 2, 3},
		"slice_field":  []float64{1.1, 2.2, 3.3},
	}

	// Test using encoder/decoder directly
	var buf bytes.Buffer
	encoder := config.NewEncoder(&buf)

	err = encoder.Encode(testRow)
	require.NoError(t, err)

	decoder := config.NewDecoder(&buf)
	var decoded map[string]any
	err = decoder.Decode(&decoded)
	require.NoError(t, err)

	// Apply type conversion
	converted := make(map[string]any)
	for k, v := range decoded {
		converted[k] = convertCBORTypes(v)
	}

	assert.Equal(t, testRow["string_field"], converted["string_field"])
	assert.Equal(t, testRow["int_field"], converted["int_field"])
	assert.Equal(t, testRow["float_field"], converted["float_field"])
	assert.Equal(t, testRow["bool_field"], converted["bool_field"])
	assert.Equal(t, testRow["byte_field"], converted["byte_field"])
	assert.Equal(t, testRow["slice_field"], converted["slice_field"])
}

// Helper function to get type name for assertions
func getTypeName(v any) string {
	if v == nil {
		return "<nil>"
	}
	switch v.(type) {
	case string:
		return "string"
	case bool:
		return "bool"
	case int64:
		return "int64"
	case uint64:
		return "uint64"
	case float64:
		return "float64"
	case []byte:
		return "[]uint8"
	case []float64:
		return "[]float64"
	case []any:
		return "[]any"
	case map[string]any:
		return "map[string]any"
	default:
		return "unknown"
	}
}
