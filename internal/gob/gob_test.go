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

package gob

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func TestGobIdentity(t *testing.T) {
	// Test comprehensive type identity preservation through gob encode/decode
	testCases := []struct {
		name          string
		value         any
		expectedType  string
		expectedValue any
	}{
		// Basic types
		{"string", "test_string", "string", "test_string"},
		{"empty_string", "", "string", ""},
		{"bool_true", true, "bool", true},
		{"bool_false", false, "bool", false},
		{"nil", nil, "<nil>", nil},

		// Integer types - gob preserves exact types
		{"int64_positive", int64(9223372036854775807), "int64", int64(9223372036854775807)},
		{"int64_negative", int64(-9223372036854775808), "int64", int64(-9223372036854775808)},
		{"int64_zero", int64(0), "int64", int64(0)},
		{"int32_positive", int32(2147483647), "int32", int32(2147483647)},
		{"int32_negative", int32(-2147483648), "int32", int32(-2147483648)},
		{"uint32_max", uint32(4294967295), "uint32", uint32(4294967295)},
		{"uint64_small", uint64(123456789), "uint64", uint64(123456789)},
		{"int_positive", int(123456), "int", int(123456)},
		{"int_negative", int(-123456), "int", int(-123456)},

		// Float types - gob preserves exact types
		{"float64_positive", float64(3.14159265359), "float64", float64(3.14159265359)},
		{"float64_negative", float64(-2.71828), "float64", float64(-2.71828)},
		{"float64_zero", float64(0.0), "float64", float64(0.0)},
		{"float64_inf", math.Inf(1), "float64", math.Inf(1)},
		{"float64_nan", math.NaN(), "float64", math.NaN()},
		{"float32_value", float32(3.14), "float32", float32(3.14)},

		// Byte arrays
		{"byte_slice_data", []byte{0x01, 0x02, 0x03, 0xFF}, "[]uint8", []byte{0x01, 0x02, 0x03, 0xFF}},

		// Slices - gob preserves exact slice types
		{"int64_slice", []int64{1, 2, 3}, "[]int64", []int64{1, 2, 3}},
		{"string_slice", []string{"a", "b", "c"}, "[]string", []string{"a", "b", "c"}},
		{"float64_slice", []float64{1.1, 2.2, 3.3}, "[]float64", []float64{1.1, 2.2, 3.3}},
	}

	config, err := NewConfig()
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

			decoded, err := config.Decode(encoded)
			require.NoError(t, err)

			decodedValue := decoded["test_field"]

			// Check type preservation
			actualType := fmt.Sprintf("%T", decodedValue)
			assert.Equal(t, tc.expectedType, actualType,
				"Type not preserved for %s: expected %s, got %s", tc.name, tc.expectedType, actualType)

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

func TestGobRowIdentity(t *testing.T) {
	// Test Row encode/decode specifically
	config, err := NewConfig()
	require.NoError(t, err)

	// Create test row
	originalRow := pipeline.Row{
		wkk.NewRowKey("string_field"):  "test_value",
		wkk.NewRowKey("int64_field"):   int64(123456),
		wkk.NewRowKey("float64_field"): float64(3.14159),
		wkk.NewRowKey("bool_field"):    true,
	}

	// Encode then decode
	encoded, err := config.EncodeRow(originalRow)
	require.NoError(t, err)

	decoded, err := config.DecodeRow(encoded)
	require.NoError(t, err)

	// Verify all fields are preserved
	assert.Equal(t, originalRow[wkk.NewRowKey("string_field")], decoded[wkk.NewRowKey("string_field")])
	assert.Equal(t, originalRow[wkk.NewRowKey("int64_field")], decoded[wkk.NewRowKey("int64_field")])
	assert.Equal(t, originalRow[wkk.NewRowKey("float64_field")], decoded[wkk.NewRowKey("float64_field")])
	assert.Equal(t, originalRow[wkk.NewRowKey("bool_field")], decoded[wkk.NewRowKey("bool_field")])
}
