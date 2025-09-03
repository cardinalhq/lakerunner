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

func TestGOBCodec_BasicTypes(t *testing.T) {
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
	}

	codec, err := NewGOB()
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

			// Special handling for NaN
			if tc.name == "float64_nan" {
				expectedFloat, ok1 := tc.value.(float64)
				actualFloat, ok2 := decodedValue.(float64)
				require.True(t, ok1 && ok2, "Both should be float64")
				assert.True(t, math.IsNaN(expectedFloat) && math.IsNaN(actualFloat), "Both should be NaN")
			} else if tc.name == "float32_positive" || tc.name == "float32_negative" {
				// float32 values are expected as float64 due to promotion
				expectedFloat64 := tc.value.(float64)
				actualFloat64, ok := decodedValue.(float64)
				require.True(t, ok, "float32 should be decoded as float64")
				assert.InDelta(t, expectedFloat64, actualFloat64, 0.0001)
			} else if tc.name == "empty_byte_slice" {
				// GOB converts empty byte slice to nil
				assert.Nil(t, decodedValue, "GOB converts empty byte slice to nil")
			} else {
				assert.Equal(t, tc.value, decodedValue, "Value should be preserved")
			}
		})
	}
}

func TestGOBCodec_ComplexMap(t *testing.T) {
	codec, err := NewGOB()
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

	encoded, err := codec.Encode(testRow)
	require.NoError(t, err)

	decoded := make(map[string]any)
	err = codec.Decode(encoded, decoded)
	require.NoError(t, err)

	// Verify all fields - GOB may change numeric types
	assert.Equal(t, testRow["string_field"], decoded["string_field"])
	assert.Equal(t, testRow["bool_field"], decoded["bool_field"])
	assert.Equal(t, testRow["bytes_field"], decoded["bytes_field"])
	assert.Equal(t, testRow["nil_field"], decoded["nil_field"])

	// Check numeric fields with type flexibility
	assert.Equal(t, testRow["int64_field"], decoded["int64_field"])
	assert.InDelta(t, testRow["float64_field"].(float64), decoded["float64_field"].(float64), 0.000001)
	// int32 is promoted to int64
	assert.Equal(t, int64(testRow["int32_field"].(int32)), decoded["int32_field"])
	// float32 is promoted to float64
	assert.InDelta(t, float64(testRow["float32_field"].(float32)), decoded["float32_field"].(float64), 0.0001)
}

func TestGOBCodec_RowEncoding(t *testing.T) {
	codec, err := NewGOB()
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
		assert.Equal(t, expectedValue, actualValue, "Value for key %s should match", key.Value())
	}
}

func TestGOBCodec_EmptyMap(t *testing.T) {
	codec, err := NewGOB()
	require.NoError(t, err)

	emptyRow := map[string]any{}

	encoded, err := codec.Encode(emptyRow)
	require.NoError(t, err)

	decoded := make(map[string]any)
	err = codec.Decode(encoded, decoded)
	require.NoError(t, err)

	assert.Len(t, decoded, 0, "Decoded map should be empty")
}

func TestGOBCodec_StreamEncoding(t *testing.T) {
	codec, err := NewGOB()
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
		assert.Equal(t, expected, decoded)
	}

	// Should get EOF on next read
	dummyMap := make(map[string]any)
	err = decoder.Decode(dummyMap)
	assert.Equal(t, io.EOF, err)
}

func TestGOBCodec_LargeData(t *testing.T) {
	codec, err := NewGOB()
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

func TestGOBCodec_SpecialCharacters(t *testing.T) {
	codec, err := NewGOB()
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

func TestGOBCodec_TypeCompatibility(t *testing.T) {
	codec, err := NewGOB()
	require.NoError(t, err)

	// GOB is more restrictive with types, test what it can handle
	testRow := map[string]any{
		"int":     int(42),
		"int8":    int8(127),
		"int16":   int16(32767),
		"int32":   int32(2147483647),
		"int64":   int64(9223372036854775807),
		"uint":    uint(42),
		"uint8":   uint8(255),
		"uint16":  uint16(65535),
		"uint32":  uint32(4294967295),
		"uint64":  uint64(18446744073709551615),
		"float32": float32(3.14),
		"float64": float64(2.71828),
	}

	encoded, err := codec.Encode(testRow)
	require.NoError(t, err)

	decoded := make(map[string]any)
	err = codec.Decode(encoded, decoded)
	require.NoError(t, err)

	// GOB preserves numeric types better than some formats
	assert.Len(t, decoded, len(testRow))
}
