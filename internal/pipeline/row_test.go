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

package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func TestToStringMap_KeysArePureStrings(t *testing.T) {
	// Create a row with various RowKeys
	row := make(Row)
	row[wkk.NewRowKey("_cardinalhq.name")] = "test_metric"
	row[wkk.NewRowKey("_cardinalhq.tid")] = int64(123)
	row[wkk.NewRowKey("metric.test_field")] = "test_value"
	row[wkk.NewRowKey("resource.service.name")] = "test_service"

	// Convert to string map
	stringMap := ToStringMap(row)

	// Verify we have the expected number of keys
	assert.Equal(t, 4, len(stringMap))

	// Check that all keys exist with expected values
	assert.Equal(t, "test_metric", stringMap["_cardinalhq.name"])
	assert.Equal(t, int64(123), stringMap["_cardinalhq.tid"])
	assert.Equal(t, "test_value", stringMap["metric.test_field"])
	assert.Equal(t, "test_service", stringMap["resource.service.name"])

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
	// Create original row
	original := make(Row)
	original[wkk.NewRowKey("id")] = 1
	original[wkk.NewRowKey("name")] = "test"
	original[wkk.NewRowKey("value")] = 42.5

	// Copy the row
	copied := CopyRow(original)

	// Verify deep copy
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
