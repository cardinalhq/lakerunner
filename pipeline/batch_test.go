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

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
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

func TestCopyBatch(t *testing.T) {
	// Create original batch with some data
	original := GetBatch()

	row1 := original.AddRow()
	row1[wkk.NewRowKey("id")] = 1
	row1[wkk.NewRowKey("name")] = "test1"

	row2 := original.AddRow()
	row2[wkk.NewRowKey("id")] = 2
	row2[wkk.NewRowKey("name")] = "test2"

	// Copy the batch
	copied := CopyBatch(original)

	// Verify deep copy
	assert.Equal(t, original.Len(), copied.Len())
	assert.Equal(t, original.Get(0)[wkk.NewRowKey("id")], copied.Get(0)[wkk.NewRowKey("id")])
	assert.Equal(t, original.Get(0)[wkk.NewRowKey("name")], copied.Get(0)[wkk.NewRowKey("name")])
	assert.Equal(t, original.Get(1)[wkk.NewRowKey("id")], copied.Get(1)[wkk.NewRowKey("id")])
	assert.Equal(t, original.Get(1)[wkk.NewRowKey("name")], copied.Get(1)[wkk.NewRowKey("name")])

	// Modify original - copied should be unaffected
	original.Get(0)[wkk.NewRowKey("id")] = 999
	original.Get(0)[wkk.NewRowKey("new_field")] = "added"

	assert.Equal(t, 1, copied.Get(0)[wkk.NewRowKey("id")], "Copied batch should not be affected by changes to original")
	assert.Nil(t, copied.Get(0)[wkk.NewRowKey("new_field")], "Copied batch should not have new fields added to original")

	// Clean up
	ReturnBatch(original)
	ReturnBatch(copied)
}

func TestTakeRow(t *testing.T) {
	batch := GetBatch()
	defer ReturnBatch(batch)

	// Add some rows
	row1 := batch.AddRow()
	row1[wkk.NewRowKey("id")] = 1
	row1[wkk.NewRowKey("value")] = "first"

	row2 := batch.AddRow()
	row2[wkk.NewRowKey("id")] = 2
	row2[wkk.NewRowKey("value")] = "second"

	row3 := batch.AddRow()
	row3[wkk.NewRowKey("id")] = 3
	row3[wkk.NewRowKey("value")] = "third"

	assert.Equal(t, 3, batch.Len())

	// Take the middle row
	takenRow := batch.TakeRow(1)
	require.NotNil(t, takenRow)
	assert.Equal(t, 2, takenRow[wkk.NewRowKey("id")])
	assert.Equal(t, "second", takenRow[wkk.NewRowKey("value")])

	// The batch should still have 3 rows (TakeRow replaces with a fresh row)
	assert.Equal(t, 3, batch.Len())

	// The row at position 1 should now be empty/fresh
	replacedRow := batch.Get(1)
	assert.Empty(t, replacedRow, "Row at position 1 should be empty after TakeRow")

	// Other rows should be unchanged
	assert.Equal(t, 1, batch.Get(0)[wkk.NewRowKey("id")])
	assert.Equal(t, 3, batch.Get(2)[wkk.NewRowKey("id")])

	// Test taking row with invalid index
	assert.Nil(t, batch.TakeRow(-1), "TakeRow with negative index should return nil")
	assert.Nil(t, batch.TakeRow(3), "TakeRow with out-of-bounds index should return nil")

	// Clean up taken row
	returnRowToPool(takenRow)
}

func TestReplaceRow(t *testing.T) {
	batch := GetBatch()
	defer ReturnBatch(batch)

	// Add some rows
	row1 := batch.AddRow()
	row1[wkk.NewRowKey("id")] = 1
	row1[wkk.NewRowKey("value")] = "first"

	row2 := batch.AddRow()
	row2[wkk.NewRowKey("id")] = 2
	row2[wkk.NewRowKey("value")] = "second"

	assert.Equal(t, 2, batch.Len())

	// Create a new row to replace with
	newRow := getPooledRow()
	newRow[wkk.NewRowKey("id")] = 99
	newRow[wkk.NewRowKey("value")] = "replaced"

	// Replace row at position 1
	batch.ReplaceRow(1, newRow)

	// Check that row was replaced
	assert.Equal(t, 2, batch.Len())
	assert.Equal(t, 1, batch.Get(0)[wkk.NewRowKey("id")])
	assert.Equal(t, 99, batch.Get(1)[wkk.NewRowKey("id")])
	assert.Equal(t, "replaced", batch.Get(1)[wkk.NewRowKey("value")])

	// Test replacing with nil (should be no-op)
	batch.ReplaceRow(0, nil)
	assert.Equal(t, 1, batch.Get(0)[wkk.NewRowKey("id")], "ReplaceRow with nil should not change the row")

	// Test replacing with invalid index (should be no-op)
	replacementRow := getPooledRow()
	replacementRow[wkk.NewRowKey("id")] = 100
	batch.ReplaceRow(-1, replacementRow)
	batch.ReplaceRow(2, replacementRow)
	assert.Equal(t, 2, batch.Len(), "ReplaceRow with invalid index should not change batch length")
	returnRowToPool(replacementRow)
}

func TestAppendRow(t *testing.T) {
	batch := GetBatch()
	defer ReturnBatch(batch)

	// Test appending to empty batch
	row1 := getPooledRow()
	row1[wkk.NewRowKey("id")] = 1
	row1[wkk.NewRowKey("value")] = "first"
	batch.AppendRow(row1)

	assert.Equal(t, 1, batch.Len())
	assert.Equal(t, 1, batch.Get(0)[wkk.NewRowKey("id")])
	assert.Equal(t, "first", batch.Get(0)[wkk.NewRowKey("value")])

	// Test appending another row
	row2 := getPooledRow()
	row2[wkk.NewRowKey("id")] = 2
	row2[wkk.NewRowKey("value")] = "second"
	batch.AppendRow(row2)

	assert.Equal(t, 2, batch.Len())
	assert.Equal(t, 2, batch.Get(1)[wkk.NewRowKey("id")])
	assert.Equal(t, "second", batch.Get(1)[wkk.NewRowKey("value")])

	// Test appending nil (should be no-op)
	batch.AppendRow(nil)
	assert.Equal(t, 2, batch.Len(), "AppendRow with nil should not change batch length")

	// Test that AppendRow reuses space after DeleteRow
	batch.DeleteRow(1) // Delete the second row
	assert.Equal(t, 1, batch.Len())

	row3 := getPooledRow()
	row3[wkk.NewRowKey("id")] = 3
	row3[wkk.NewRowKey("value")] = "third"
	batch.AppendRow(row3)

	assert.Equal(t, 2, batch.Len())
	assert.Equal(t, 1, batch.Get(0)[wkk.NewRowKey("id")])
	assert.Equal(t, 3, batch.Get(1)[wkk.NewRowKey("id")])
}

func TestTakeRowAndAppendRow(t *testing.T) {
	// Test the pattern used in trace_ingest_processor
	sourceBatch := GetBatch()
	defer ReturnBatch(sourceBatch)

	// Add some rows to source batch
	row1 := sourceBatch.AddRow()
	row1[wkk.NewRowKey("id")] = 1
	row1[wkk.NewRowKey("data")] = "alpha"

	row2 := sourceBatch.AddRow()
	row2[wkk.NewRowKey("id")] = 2
	row2[wkk.NewRowKey("data")] = "beta"

	row3 := sourceBatch.AddRow()
	row3[wkk.NewRowKey("id")] = 3
	row3[wkk.NewRowKey("data")] = "gamma"

	// Create destination batch
	destBatch := GetBatch()
	defer ReturnBatch(destBatch)

	// Transfer rows using TakeRow and AppendRow
	for i := 0; i < sourceBatch.Len(); i++ {
		takenRow := sourceBatch.TakeRow(i)
		if takenRow != nil {
			destBatch.AppendRow(takenRow)
		}
	}

	// Verify destination batch has all the rows
	assert.Equal(t, 3, destBatch.Len())
	assert.Equal(t, 1, destBatch.Get(0)[wkk.NewRowKey("id")])
	assert.Equal(t, "alpha", destBatch.Get(0)[wkk.NewRowKey("data")])
	assert.Equal(t, 2, destBatch.Get(1)[wkk.NewRowKey("id")])
	assert.Equal(t, "beta", destBatch.Get(1)[wkk.NewRowKey("data")])
	assert.Equal(t, 3, destBatch.Get(2)[wkk.NewRowKey("id")])
	assert.Equal(t, "gamma", destBatch.Get(2)[wkk.NewRowKey("data")])

	// Verify source batch rows are now empty (replaced with fresh rows)
	assert.Equal(t, 3, sourceBatch.Len())
	for i := 0; i < sourceBatch.Len(); i++ {
		assert.Empty(t, sourceBatch.Get(i), "Source batch row %d should be empty after TakeRow", i)
	}
}
