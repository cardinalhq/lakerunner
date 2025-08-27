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
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func TestGlobalBatchPool(t *testing.T) {
	// Test basic get/return cycle
	batch1 := GetBatch()
	require.NotNil(t, batch1)
	assert.Equal(t, 0, batch1.Len()) // Length should be 0

	// Add some data using new API
	row := batch1.AddRow()
	row[wkk.NewRowKey("test")] = "data"
	assert.Equal(t, 1, batch1.Len())

	// Return to pool
	ReturnBatch(batch1)

	// Get another batch - should be clean
	batch2 := GetBatch()
	require.NotNil(t, batch2)
	assert.Equal(t, 0, batch2.Len(), "Returned batch should be clean")

	// Return second batch
	ReturnBatch(batch2)
}

func TestReturnBatchWithNil(t *testing.T) {
	// Should not panic with nil
	ReturnBatch(nil)
}

func TestBatchPoolReuse(t *testing.T) {
	// Get multiple batches and verify they can be different instances
	batch1 := GetBatch()
	batch2 := GetBatch()

	// They might be different objects initially
	require.NotNil(t, batch1)
	require.NotNil(t, batch2)

	// Return both
	ReturnBatch(batch1)
	ReturnBatch(batch2)

	// Get new ones - should work fine
	batch3 := GetBatch()
	batch4 := GetBatch()

	require.NotNil(t, batch3)
	require.NotNil(t, batch4)

	ReturnBatch(batch3)
	ReturnBatch(batch4)
}

func TestBatchMethods(t *testing.T) {
	batch := GetBatch()

	// Test AddRow
	row1 := batch.AddRow()
	row1[wkk.NewRowKey("id")] = 1
	row1[wkk.NewRowKey("name")] = "test1"

	row2 := batch.AddRow()
	row2[wkk.NewRowKey("id")] = 2
	row2[wkk.NewRowKey("name")] = "test2"

	row3 := batch.AddRow()
	row3[wkk.NewRowKey("id")] = 3
	row3[wkk.NewRowKey("name")] = "test3"

	assert.Equal(t, 3, batch.Len())

	// Test Get
	assert.Equal(t, 1, batch.Get(0)[wkk.NewRowKey("id")])
	assert.Equal(t, 2, batch.Get(1)[wkk.NewRowKey("id")])
	assert.Equal(t, 3, batch.Get(2)[wkk.NewRowKey("id")])

	// Test DeleteRow (delete middle row)
	batch.DeleteRow(1)
	assert.Equal(t, 2, batch.Len())

	// After deletion, row order should be [1, 3] (2 was deleted)
	assert.Equal(t, 1, batch.Get(0)[wkk.NewRowKey("id")])
	assert.Equal(t, 3, batch.Get(1)[wkk.NewRowKey("id")]) // row3 should have moved to position 1

	// Test reusing deleted row slot
	row4 := batch.AddRow()
	row4[wkk.NewRowKey("id")] = 4
	row4[wkk.NewRowKey("name")] = "test4"

	assert.Equal(t, 3, batch.Len())
	assert.Equal(t, 1, batch.Get(0)[wkk.NewRowKey("id")])
	assert.Equal(t, 3, batch.Get(1)[wkk.NewRowKey("id")])
	assert.Equal(t, 4, batch.Get(2)[wkk.NewRowKey("id")])

	ReturnBatch(batch)
}

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
