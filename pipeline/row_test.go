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

package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func TestToStringMap_KeysArePureStrings(t *testing.T) {
	// Create a row with various RowKeys
	row := make(Row)
	row[wkk.NewRowKey("metric_name")] = "test_metric"
	row[wkk.NewRowKey("chq_tid")] = int64(123)
	row[wkk.NewRowKey("metric_test_field")] = "test_value"
	row[wkk.NewRowKey("resource_service_name")] = "test_service"

	// Convert to string map
	stringMap := ToStringMap(row)

	// Verify we have the expected number of keys
	assert.Equal(t, 4, len(stringMap))

	// Check that all keys exist with expected values
	assert.Equal(t, "test_metric", stringMap["metric_name"])
	assert.Equal(t, int64(123), stringMap["chq_tid"])
	assert.Equal(t, "test_value", stringMap["metric_test_field"])
	assert.Equal(t, "test_service", stringMap["resource_service_name"])

	// Most importantly: verify that the keys are pure Go strings, not string aliases
	for key := range stringMap {
		// Use reflection to verify the key is exactly type string
		keyType := assert.IsType(t, "", key, "Key should be pure string type, not alias")

		// Additional type check using type assertion
		_, isString := any(key).(string)
		assert.True(t, isString, "Key should be castable to string")

		// Verify key is not a RowKey type
		_, isRowKey := any(key).(wkk.RowKey)
		assert.False(t, isRowKey, "Key should not be a RowKey type")

		t.Logf("Key '%s' has correct type: %T", key, key)
		_ = keyType // Use the variable to avoid unused variable warning
	}
}

func TestCopyRow(t *testing.T) {
	original := make(Row)
	original[wkk.NewRowKey("id")] = 1
	original[wkk.NewRowKey("name")] = "test"
	original[wkk.NewRowKey("value")] = 42.5

	copied := CopyRow(original)

	assert.Equal(t, len(original), len(copied))
	assert.Equal(t, original[wkk.NewRowKey("id")], copied[wkk.NewRowKey("id")])
	assert.Equal(t, original[wkk.NewRowKey("name")], copied[wkk.NewRowKey("name")])
	assert.Equal(t, original[wkk.NewRowKey("value")], copied[wkk.NewRowKey("value")])

	// Modify original - copied should be unaffected
	original[wkk.NewRowKey("id")] = 999
	original[wkk.NewRowKey("new_field")] = "added"

	assert.Equal(t, 1, copied[wkk.NewRowKey("id")], "Copied row should not be affected by changes to original")
	assert.Nil(t, copied[wkk.NewRowKey("new_field")], "Copied row should not have new fields added to original")
}

func TestRow_GetString(t *testing.T) {
	tests := []struct {
		name     string
		row      Row
		key      string
		expected string
	}{
		{
			name:     "string value",
			row:      Row{wkk.NewRowKey("name"): "test_value"},
			key:      "name",
			expected: "test_value",
		},
		{
			name:     "empty string",
			row:      Row{wkk.NewRowKey("name"): ""},
			key:      "name",
			expected: "",
		},
		{
			name:     "missing key",
			row:      Row{wkk.NewRowKey("other"): "value"},
			key:      "name",
			expected: "",
		},
		{
			name:     "int value (not a string)",
			row:      Row{wkk.NewRowKey("id"): 123},
			key:      "id",
			expected: "",
		},
		{
			name:     "float value (not a string)",
			row:      Row{wkk.NewRowKey("value"): 42.5},
			key:      "value",
			expected: "",
		},
		{
			name:     "nil value",
			row:      Row{wkk.NewRowKey("nullable"): nil},
			key:      "nullable",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.row.GetString(wkk.NewRowKey(tt.key))
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRow_GetInt64(t *testing.T) {
	tests := []struct {
		name       string
		row        Row
		key        string
		expected   int64
		expectedOk bool
	}{
		{
			name:       "int64 value",
			row:        Row{wkk.NewRowKey("id"): int64(9223372036854775807)},
			key:        "id",
			expected:   9223372036854775807,
			expectedOk: true,
		},
		{
			name:       "int value",
			row:        Row{wkk.NewRowKey("count"): 42},
			key:        "count",
			expected:   42,
			expectedOk: true,
		},
		{
			name:       "float64 value",
			row:        Row{wkk.NewRowKey("value"): float64(123.9)},
			key:        "value",
			expected:   123,
			expectedOk: true,
		},
		{
			name:       "negative int64",
			row:        Row{wkk.NewRowKey("negative"): int64(-999)},
			key:        "negative",
			expected:   -999,
			expectedOk: true,
		},
		{
			name:       "zero value",
			row:        Row{wkk.NewRowKey("zero"): int64(0)},
			key:        "zero",
			expected:   0,
			expectedOk: true,
		},
		{
			name:       "missing key",
			row:        Row{wkk.NewRowKey("other"): int64(123)},
			key:        "missing",
			expected:   0,
			expectedOk: false,
		},
		{
			name:       "string value (not convertible)",
			row:        Row{wkk.NewRowKey("name"): "not_a_number"},
			key:        "name",
			expected:   0,
			expectedOk: false,
		},
		{
			name:       "nil value",
			row:        Row{wkk.NewRowKey("nullable"): nil},
			key:        "nullable",
			expected:   0,
			expectedOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := tt.row.GetInt64(wkk.NewRowKey(tt.key))
			assert.Equal(t, tt.expected, result)
			assert.Equal(t, tt.expectedOk, ok)
		})
	}
}

func TestRow_GetInt32(t *testing.T) {
	tests := []struct {
		name       string
		row        Row
		key        string
		expected   int32
		expectedOk bool
	}{
		{
			name:       "int32 value",
			row:        Row{wkk.NewRowKey("id"): int32(2147483647)},
			key:        "id",
			expected:   2147483647,
			expectedOk: true,
		},
		{
			name:       "int value",
			row:        Row{wkk.NewRowKey("count"): 42},
			key:        "count",
			expected:   42,
			expectedOk: true,
		},
		{
			name:       "int64 value",
			row:        Row{wkk.NewRowKey("big"): int64(1000)},
			key:        "big",
			expected:   1000,
			expectedOk: true,
		},
		{
			name:       "float64 value",
			row:        Row{wkk.NewRowKey("value"): float64(123.9)},
			key:        "value",
			expected:   123,
			expectedOk: true,
		},
		{
			name:       "negative int32",
			row:        Row{wkk.NewRowKey("negative"): int32(-999)},
			key:        "negative",
			expected:   -999,
			expectedOk: true,
		},
		{
			name:       "zero value",
			row:        Row{wkk.NewRowKey("zero"): int32(0)},
			key:        "zero",
			expected:   0,
			expectedOk: true,
		},
		{
			name:       "missing key",
			row:        Row{wkk.NewRowKey("other"): int32(123)},
			key:        "missing",
			expected:   0,
			expectedOk: false,
		},
		{
			name:       "string value (not convertible)",
			row:        Row{wkk.NewRowKey("name"): "not_a_number"},
			key:        "name",
			expected:   0,
			expectedOk: false,
		},
		{
			name:       "nil value",
			row:        Row{wkk.NewRowKey("nullable"): nil},
			key:        "nullable",
			expected:   0,
			expectedOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := tt.row.GetInt32(wkk.NewRowKey(tt.key))
			assert.Equal(t, tt.expected, result)
			assert.Equal(t, tt.expectedOk, ok)
		})
	}
}
