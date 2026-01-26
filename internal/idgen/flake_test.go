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

package idgen

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSonyFlakeGenerator_NextID(t *testing.T) {
	gen, err := newFlakeGenerator()
	require.NoError(t, err, "failed to create SonyFlakeGenerator")

	// Check that subsequent IDs are increasing
	id := gen.NextID()
	id2 := gen.NextID()
	assert.Greater(t, id2, id, "NextID() did not return increasing id")
}

func TestSonyFlakeGenerator_NextBase32ID(t *testing.T) {
	gen, err := newFlakeGenerator()
	require.NoError(t, err, "failed to create SonyFlakeGenerator")

	// Generate base32 IDs
	id1 := gen.NextBase32ID()
	id2 := gen.NextBase32ID()

	// Should be different
	assert.NotEqual(t, id1, id2, "NextBase32ID() returned duplicate IDs")

	// Should not contain padding
	assert.False(t, strings.Contains(id1, "="), "base32 ID should not contain padding")
	assert.False(t, strings.Contains(id2, "="), "base32 ID should not contain padding")

	// Should be non-empty strings
	assert.NotEmpty(t, id1)
	assert.NotEmpty(t, id2)

	// Test the convenience function
	id3 := NextBase32ID()
	assert.NotEmpty(t, id3)
	assert.False(t, strings.Contains(id3, "="), "base32 ID should not contain padding")
}

func TestSonyFlakeGenerator_NextBatchIDs(t *testing.T) {
	gen, err := newFlakeGenerator()
	require.NoError(t, err, "failed to create SonyFlakeGenerator")

	tests := []struct {
		name  string
		count int
	}{
		{"single ID", 1},
		{"small batch", 5},
		{"medium batch", 50},
		{"large batch", 500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ids := gen.NextBatchIDs(tt.count)

			// Check correct count
			assert.Len(t, ids, tt.count, "batch should contain exactly %d IDs", tt.count)

			// Check uniqueness within batch
			seen := make(map[int64]bool)
			for i, id := range ids {
				assert.False(t, seen[id], "duplicate ID %d found at index %d", id, i)
				seen[id] = true

				// Check that IDs are positive
				assert.Positive(t, id, "ID should be positive")
			}
		})
	}
}

func TestSonyFlakeGenerator_NextBatchIDs_EdgeCases(t *testing.T) {
	gen, err := newFlakeGenerator()
	require.NoError(t, err, "failed to create SonyFlakeGenerator")

	// Test zero count
	ids := gen.NextBatchIDs(0)
	assert.Nil(t, ids, "zero count should return nil")

	// Test negative count
	ids = gen.NextBatchIDs(-1)
	assert.Nil(t, ids, "negative count should return nil")
}

func TestGenerateBatchIDs(t *testing.T) {
	// Test the convenience function
	ids := GenerateBatchIDs(10)
	assert.Len(t, ids, 10, "should generate exactly 10 IDs")

	// Check uniqueness
	seen := make(map[int64]bool)
	for i, id := range ids {
		assert.False(t, seen[id], "duplicate ID %d found at index %d", id, i)
		seen[id] = true
		assert.Positive(t, id, "ID should be positive")
	}
}

func TestBatchIDsConcurrency(t *testing.T) {
	// Test that concurrent batch generation doesn't produce duplicates
	const numGoroutines = 10
	const batchSize = 100

	results := make(chan []int64, numGoroutines)

	// Generate batches concurrently
	for i := 0; i < numGoroutines; i++ {
		go func() {
			results <- GenerateBatchIDs(batchSize)
		}()
	}

	// Collect all IDs
	allIDs := make(map[int64]bool)
	for i := 0; i < numGoroutines; i++ {
		batch := <-results
		assert.Len(t, batch, batchSize, "each batch should have correct size")

		for _, id := range batch {
			assert.False(t, allIDs[id], "duplicate ID %d across concurrent batches", id)
			allIDs[id] = true
		}
	}

	// Should have exactly numGoroutines * batchSize unique IDs
	assert.Len(t, allIDs, numGoroutines*batchSize, "total unique IDs should match expected count")
}
