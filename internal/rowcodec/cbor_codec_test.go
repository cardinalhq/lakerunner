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

package rowcodec

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func TestCBORCodec_BasicTypes(t *testing.T) {
	testCases := []struct {
		name  string
		value any
	}{
		{"nil", nil},
		{"bool_true", true},
		{"bool_false", false},
		{"int64_positive", int64(9223372036854775807)},
		{"int64_negative", int64(-9223372036854775808)},
		{"int64_zero", int64(0)},
		// Note: We expect all codecs to promote int32 to int64
		{"int32_positive", int64(2147483647)},
		{"int32_negative", int64(-2147483648)},
		{"float64_positive", float64(3.14159265359)},
		{"float64_negative", float64(-2.71828)},
		{"float64_zero", float64(0.0)},
		{"float64_inf", math.Inf(1)},
		{"float64_neg_inf", math.Inf(-1)},
		{"float64_nan", math.NaN()},
		// Note: We expect all codecs to promote float32 to float64
		{"float32_positive", float64(float32(3.14))},
		{"float32_negative", float64(float32(-2.71))},
		{"string", "test_string"},
		{"empty_string", ""},
		{"unicode_string", "Hello 世界 🌍"},
		{"byte_slice", []byte{0x01, 0x02, 0x03, 0xFF}},
		{"empty_byte_slice", []byte{}},
		{"int_slice", []int64{1, 2, 3, 4, 5}},
		{"float_slice", []float64{1.1, 2.2, 3.3}},
		{"string_slice", []string{"a", "b", "c"}},
		{"empty_int_slice", []int64{}},
		{"empty_float_slice", []float64{}},
		{"empty_string_slice", []string{}},
	}

	codec, err := NewCBOR()
	require.NoError(t, err)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create test row with the value
			testRow := map[string]any{
				"test_field": tc.value,
			}

			// Encode then decode
			encoded, err := codec.Encode(testRow)
			require.NoError(t, err)

			decoded := make(map[string]any)
			err = codec.Decode(encoded, decoded)
			require.NoError(t, err)

			decodedValue := decoded["test_field"]

			// CBOR may decode integers as uint64 for positive values
			if v, ok := tc.value.(int64); ok && v >= 0 {
				switch actual := decodedValue.(type) {
				case int64:
					assert.Equal(t, v, actual, "Value should be preserved")
				case uint64:
					assert.Equal(t, uint64(v), actual, "Value should be preserved")
				default:
					t.Errorf("Unexpected type %T for positive int64", actual)
				}
			} else if tc.name == "float64_nan" {
				expectedFloat, ok1 := tc.value.(float64)
				actualFloat, ok2 := decodedValue.(float64)
				require.True(t, ok1 && ok2, "Both should be float64")
				assert.True(t, math.IsNaN(expectedFloat) && math.IsNaN(actualFloat), "Both should be NaN")
			} else if sliceVal, ok := tc.value.([]int64); ok {
				// CBOR may decode integers as different types, normalize
				actualSlice, ok := decodedValue.([]any)
				if ok {
					assert.Len(t, actualSlice, len(sliceVal))
					for i, v := range sliceVal {
						// Convert to int64 for comparison
						switch actual := actualSlice[i].(type) {
						case int64:
							assert.Equal(t, v, actual)
						case int:
							assert.Equal(t, v, int64(actual))
						case uint64:
							assert.Equal(t, v, int64(actual))
						default:
							t.Errorf("Unexpected type %T at index %d", actual, i)
						}
					}
				} else {
					assert.Equal(t, tc.value, decodedValue, "Value should be preserved")
				}
			} else if sliceVal, ok := tc.value.([]float64); ok {
				actualSlice, ok := decodedValue.([]any)
				if ok {
					assert.Len(t, actualSlice, len(sliceVal))
					for i, v := range sliceVal {
						assert.Equal(t, v, actualSlice[i].(float64))
					}
				} else {
					assert.Equal(t, tc.value, decodedValue, "Value should be preserved")
				}
			} else if sliceVal, ok := tc.value.([]string); ok {
				actualSlice, ok := decodedValue.([]any)
				if ok {
					assert.Len(t, actualSlice, len(sliceVal))
					for i, v := range sliceVal {
						assert.Equal(t, v, actualSlice[i].(string))
					}
				} else {
					assert.Equal(t, tc.value, decodedValue, "Value should be preserved")
				}
			} else {
				assert.Equal(t, tc.value, decodedValue, "Value should be preserved")
			}
		})
	}
}

func TestCBORCodec_ComplexMap(t *testing.T) {
	codec, err := NewCBOR()
	require.NoError(t, err)

	// Test with complex map containing multiple types
	testRow := map[string]any{
		"string_field":  "test_value",
		"int64_field":   int64(123456789),
		"float64_field": float64(3.14159),
		"bool_field":    true,
		"bytes_field":   []byte{0xDE, 0xAD, 0xBE, 0xEF},
		"nil_field":     nil,
		"int32_field":   int32(987654),
		"float32_field": float32(2.71),
		"nested_map": map[string]any{
			"nested_string": "inner_value",
			"nested_int":    int64(42),
		},
	}

	encoded, err := codec.Encode(testRow)
	require.NoError(t, err)

	decoded := make(map[string]any)
	err = codec.Decode(encoded, decoded)
	require.NoError(t, err)

	// Verify all fields except nested map and numeric types
	assert.Equal(t, testRow["string_field"], decoded["string_field"])
	assert.Equal(t, testRow["bool_field"], decoded["bool_field"])
	assert.Equal(t, testRow["bytes_field"], decoded["bytes_field"])
	assert.Equal(t, testRow["nil_field"], decoded["nil_field"])

	// CBOR may decode integers as uint64
	if v, ok := decoded["int64_field"].(uint64); ok {
		assert.Equal(t, uint64(testRow["int64_field"].(int64)), v)
	} else {
		assert.Equal(t, testRow["int64_field"], decoded["int64_field"])
	}

	// Check float with delta
	assert.InDelta(t, testRow["float64_field"].(float64), decoded["float64_field"].(float64), 0.000001)

	// Check nested map - CBOR may use different map type
	nestedOriginal := testRow["nested_map"].(map[string]any)
	switch nestedDecoded := decoded["nested_map"].(type) {
	case map[string]any:
		assert.Equal(t, nestedOriginal["nested_string"], nestedDecoded["nested_string"])
		if v, ok := nestedDecoded["nested_int"].(uint64); ok {
			assert.Equal(t, uint64(nestedOriginal["nested_int"].(int64)), v)
		} else {
			assert.Equal(t, nestedOriginal["nested_int"], nestedDecoded["nested_int"])
		}
	case map[any]any:
		assert.Equal(t, nestedOriginal["nested_string"], nestedDecoded["nested_string"])
		if v, ok := nestedDecoded["nested_int"].(uint64); ok {
			assert.Equal(t, uint64(nestedOriginal["nested_int"].(int64)), v)
		} else {
			assert.Equal(t, nestedOriginal["nested_int"], nestedDecoded["nested_int"])
		}
	default:
		t.Fatalf("Unexpected nested map type: %T", decoded["nested_map"])
	}
}

func TestCBORCodec_RowEncoding(t *testing.T) {
	codec, err := NewCBOR()
	require.NoError(t, err)

	// Create a Row with RowKey handles
	testRow := pipeline.Row{
		wkk.NewRowKey("key1"): "value1",
		wkk.NewRowKey("key2"): int64(123),
		wkk.NewRowKey("key3"): float64(3.14),
		wkk.NewRowKey("key4"): []byte{1, 2, 3},
	}

	// Encode the Row
	encoded, err := codec.EncodeRow(testRow)
	require.NoError(t, err)

	// Decode back to Row
	decodedRow := make(pipeline.Row)
	err = codec.DecodeRow(encoded, decodedRow)
	require.NoError(t, err)

	// Verify all values are preserved
	assert.Len(t, decodedRow, len(testRow))
	for key, expectedValue := range testRow {
		actualValue, exists := decodedRow[key]
		assert.True(t, exists, "Key %s should exist", key.Value())

		// Handle type conversions that CBOR might do
		switch expected := expectedValue.(type) {
		case int64:
			switch actual := actualValue.(type) {
			case int64:
				assert.Equal(t, expected, actual, "Value for key %s should match", key.Value())
			case uint64:
				if expected >= 0 {
					assert.Equal(t, uint64(expected), actual, "Value for key %s should match", key.Value())
				} else {
					t.Errorf("Negative int64 should not become uint64 for key %s", key.Value())
				}
			default:
				assert.Equal(t, expectedValue, actualValue, "Value for key %s should match", key.Value())
			}
		default:
			assert.Equal(t, expectedValue, actualValue, "Value for key %s should match", key.Value())
		}
	}
}

func TestCBORCodec_EmptyMap(t *testing.T) {
	codec, err := NewCBOR()
	require.NoError(t, err)

	emptyRow := map[string]any{}

	encoded, err := codec.Encode(emptyRow)
	require.NoError(t, err)

	decoded := make(map[string]any)
	err = codec.Decode(encoded, decoded)
	require.NoError(t, err)

	assert.Len(t, decoded, 0, "Decoded map should be empty")
}

func TestCBORCodec_StreamEncoding(t *testing.T) {
	codec, err := NewCBOR()
	require.NoError(t, err)

	testData := []map[string]any{
		{"id": int64(1), "name": "first"},
		{"id": int64(2), "name": "second"},
		{"id": int64(3), "name": "third"},
	}

	// Test streaming encode/decode
	var buf bytes.Buffer
	encoder := codec.NewEncoder(&buf)

	// Encode multiple maps
	for _, data := range testData {
		err := encoder.Encode(data)
		require.NoError(t, err)
	}

	// Decode them back
	decoder := codec.NewDecoder(&buf)
	for i, expected := range testData {
		decoded := make(map[string]any)
		err := decoder.Decode(decoded)
		require.NoError(t, err, "Failed to decode item %d", i)

		// Compare with type flexibility for CBOR
		assert.Equal(t, expected["name"], decoded["name"])

		// Handle int64 that might be decoded as uint64
		expectedID := expected["id"].(int64)
		switch actualID := decoded["id"].(type) {
		case int64:
			assert.Equal(t, expectedID, actualID)
		case uint64:
			assert.Equal(t, uint64(expectedID), actualID)
		default:
			t.Errorf("Unexpected type %T for id", actualID)
		}
	}

	// Should get EOF on next read
	dummyMap := make(map[string]any)
	err = decoder.Decode(dummyMap)
	assert.Equal(t, io.EOF, err)
}

func TestCBORCodec_LargeData(t *testing.T) {
	codec, err := NewCBOR()
	require.NoError(t, err)

	// Create a large map
	largeMap := make(map[string]any)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("field_%d", i)
		switch i % 4 {
		case 0:
			largeMap[key] = fmt.Sprintf("string_value_%d", i)
		case 1:
			largeMap[key] = int64(i * 1000)
		case 2:
			largeMap[key] = float64(i) * 3.14
		case 3:
			largeMap[key] = []byte(fmt.Sprintf("bytes_%d", i))
		}
	}

	// Encode and decode
	encoded, err := codec.Encode(largeMap)
	require.NoError(t, err)

	decoded := make(map[string]any)
	err = codec.Decode(encoded, decoded)
	require.NoError(t, err)

	// Verify size and sample some values
	assert.Len(t, decoded, len(largeMap))
	assert.Equal(t, largeMap["field_0"], decoded["field_0"])
	assert.Equal(t, largeMap["field_500"], decoded["field_500"])
	assert.Equal(t, largeMap["field_999"], decoded["field_999"])
}

func TestCBORCodec_SpecialCharacters(t *testing.T) {
	codec, err := NewCBOR()
	require.NoError(t, err)

	testRow := map[string]any{
		"unicode":     "Hello 世界 🌍 ñ é ü",
		"newlines":    "line1\nline2\rline3\r\n",
		"tabs":        "col1\tcol2\tcol3",
		"quotes":      `"double" and 'single' quotes`,
		"backslash":   `path\to\file and C:\Windows`,
		"null_bytes":  string([]byte{0x00, 0x01, 0x00}),
		"long_string": string(make([]byte, 10000)), // 10KB string
	}

	encoded, err := codec.Encode(testRow)
	require.NoError(t, err)

	decoded := make(map[string]any)
	err = codec.Decode(encoded, decoded)
	require.NoError(t, err)

	for key, expected := range testRow {
		assert.Equal(t, expected, decoded[key], "Field %s should match", key)
	}
}
