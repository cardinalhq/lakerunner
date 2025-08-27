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
)

func TestGlobalBatchPool(t *testing.T) {
	// Test basic get/return cycle
	batch1 := GetBatch()
	require.NotNil(t, batch1)
	assert.Equal(t, 0, batch1.Len()) // Length should be 0

	// Add some data using new API
	row := batch1.AddRow()
	row["test"] = "data"
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
	row1["id"] = 1
	row1["name"] = "test1"

	row2 := batch.AddRow()
	row2["id"] = 2
	row2["name"] = "test2"

	row3 := batch.AddRow()
	row3["id"] = 3
	row3["name"] = "test3"

	assert.Equal(t, 3, batch.Len())

	// Test Get
	assert.Equal(t, 1, batch.Get(0)["id"])
	assert.Equal(t, 2, batch.Get(1)["id"])
	assert.Equal(t, 3, batch.Get(2)["id"])

	// Test DeleteRow (delete middle row)
	batch.DeleteRow(1)
	assert.Equal(t, 2, batch.Len())

	// After deletion, row order should be [1, 3] (2 was deleted)
	assert.Equal(t, 1, batch.Get(0)["id"])
	assert.Equal(t, 3, batch.Get(1)["id"]) // row3 should have moved to position 1

	// Test reusing deleted row slot
	row4 := batch.AddRow()
	row4["id"] = 4
	row4["name"] = "test4"

	assert.Equal(t, 3, batch.Len())
	assert.Equal(t, 1, batch.Get(0)["id"])
	assert.Equal(t, 3, batch.Get(1)["id"])
	assert.Equal(t, 4, batch.Get(2)["id"])

	ReturnBatch(batch)
}
