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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func TestBinaryCodec_BasicTypes(t *testing.T) {
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
		{"int32_positive", int32(2147483647)},  // Will be promoted to int64
		{"int32_negative", int32(-2147483648)}, // Will be promoted to int64
		{"float64_positive", float64(3.14159265359)},
		{"float64_negative", float64(-2.71828)},
		{"float64_zero", float64(0.0)},
		{"float64_inf", math.Inf(1)},
		{"float64_nan", math.NaN()},
		{"float32_positive", float32(3.14)},  // Will be promoted to float64
		{"float32_negative", float32(-2.71)}, // Will be promoted to float64
		{"string", "test_string"},
		{"empty_string", ""},
		{"byte_slice", []byte{0x01, 0x02, 0x03, 0xFF}},
		{"empty_byte_slice", []byte{}},
	}

	config, err := NewBinary()
	require.NoError(t, err)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create test row with the value
			testRow := map[string]any{
				"test_field": tc.value,
			}

			// Encode then decode
			encoded, err := config.Encode(testRow)
			require.NoError(t, err)

			decoded := make(map[string]any)
			err = config.Decode(encoded, decoded)
			require.NoError(t, err)

			decodedValue := decoded["test_field"]

			// Special handling for NaN
			if tc.name == "float64_nan" {
				expectedFloat, ok1 := tc.value.(float64)
				actualFloat, ok2 := decodedValue.(float64)
				require.True(t, ok1 && ok2, "Both should be float64")
				assert.True(t, math.IsNaN(expectedFloat) && math.IsNaN(actualFloat), "Both should be NaN")
			} else if tc.name == "int32_positive" || tc.name == "int32_negative" {
				// int32 is promoted to int64
				expectedInt32 := tc.value.(int32)
				actualInt64, ok := decodedValue.(int64)
				require.True(t, ok, "int32 should be decoded as int64")
				assert.Equal(t, int64(expectedInt32), actualInt64, "Value should be preserved")
			} else if tc.name == "float32_positive" || tc.name == "float32_negative" {
				// float32 is promoted to float64
				expectedFloat32 := tc.value.(float32)
				actualFloat64, ok := decodedValue.(float64)
				require.True(t, ok, "float32 should be decoded as float64")
				assert.InDelta(t, float64(expectedFloat32), actualFloat64, 0.0001, "Value should be preserved")
			} else {
				assert.Equal(t, tc.value, decodedValue, "Value should be preserved")
			}
		})
	}
}

func TestBinaryCodec_ComplexMap(t *testing.T) {
	config, err := NewBinary()
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
	}

	// Encode then decode
	encoded, err := config.Encode(testRow)
	require.NoError(t, err)

	decoded := make(map[string]any)
	err = config.Decode(encoded, decoded)
	require.NoError(t, err)

	// Verify all fields are preserved (with type promotions)
	assert.Equal(t, testRow["string_field"], decoded["string_field"])
	assert.Equal(t, testRow["int64_field"], decoded["int64_field"])
	assert.Equal(t, testRow["float64_field"], decoded["float64_field"])
	assert.Equal(t, testRow["bool_field"], decoded["bool_field"])
	assert.Equal(t, testRow["bytes_field"], decoded["bytes_field"])
	assert.Equal(t, testRow["nil_field"], decoded["nil_field"])
	// int32 is promoted to int64
	assert.Equal(t, int64(testRow["int32_field"].(int32)), decoded["int32_field"])
	// float32 is promoted to float64
	assert.InDelta(t, float64(testRow["float32_field"].(float32)), decoded["float32_field"].(float64), 0.0001)
}

func TestBinaryCodec_RowEncoding(t *testing.T) {
	config, err := NewBinary()
	require.NoError(t, err)

	// Create test Row
	originalRow := pipeline.Row{
		wkk.NewRowKey("string_field"):  "test_value",
		wkk.NewRowKey("int64_field"):   int64(123456),
		wkk.NewRowKey("float64_field"): float64(3.14159),
		wkk.NewRowKey("bool_field"):    true,
		wkk.NewRowKey("bytes_field"):   []byte{0xFF, 0x00},
	}

	// Encode then decode
	encoded, err := config.EncodeRow(originalRow)
	require.NoError(t, err)

	decoded := make(pipeline.Row)
	err = config.DecodeRow(encoded, decoded)
	require.NoError(t, err)

	// Verify all fields are preserved
	assert.Equal(t, originalRow[wkk.NewRowKey("string_field")], decoded[wkk.NewRowKey("string_field")])
	assert.Equal(t, originalRow[wkk.NewRowKey("int64_field")], decoded[wkk.NewRowKey("int64_field")])
	assert.Equal(t, originalRow[wkk.NewRowKey("float64_field")], decoded[wkk.NewRowKey("float64_field")])
	assert.Equal(t, originalRow[wkk.NewRowKey("bool_field")], decoded[wkk.NewRowKey("bool_field")])
	assert.Equal(t, originalRow[wkk.NewRowKey("bytes_field")], decoded[wkk.NewRowKey("bytes_field")])
}

func TestBinaryCodec_EmptyMap(t *testing.T) {
	config, err := NewBinary()
	require.NoError(t, err)

	// Test empty map
	emptyRow := map[string]any{}

	encoded, err := config.Encode(emptyRow)
	require.NoError(t, err)

	decoded := make(map[string]any)
	err = config.Decode(encoded, decoded)
	require.NoError(t, err)

	assert.Equal(t, 0, len(decoded))
}

func TestBinaryCodec_UnsupportedType(t *testing.T) {
	config, err := NewBinary()
	require.NoError(t, err)

	// Test unsupported type
	testRow := map[string]any{
		"unsupported": []int{1, 2, 3}, // Slice types other than []byte not supported
	}

	_, err = config.Encode(testRow)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported type")
}
