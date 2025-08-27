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
	assert.NotNil(t, batch1.Rows)
	assert.Equal(t, 0, len(batch1.Rows))
	assert.GreaterOrEqual(t, cap(batch1.Rows), 1000) // Should have capacity for default batch size

	// Add some data
	batch1.Rows = append(batch1.Rows, Row{"test": "data"})
	assert.Equal(t, 1, len(batch1.Rows))

	// Return to pool
	ReturnBatch(batch1)

	// Get another batch - should be clean
	batch2 := GetBatch()
	require.NotNil(t, batch2)
	assert.Equal(t, 0, len(batch2.Rows), "Returned batch should be clean")

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
