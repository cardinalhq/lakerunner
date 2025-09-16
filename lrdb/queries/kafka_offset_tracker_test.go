//go:build integration

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

package queries

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/testhelpers"
)

func TestKafkaOffsetsAfter(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	// Insert test data
	consumerGroup := "test-consumer"
	topic := "test-topic"
	partitionID := int32(0)

	// Insert first bin with non-contiguous offsets
	err := db.InsertKafkaOffsets(ctx, lrdb.InsertKafkaOffsetsParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		Offsets:       []int64{100, 102, 103, 105}, // gaps at 101, 104
		CreatedAt:     nil,                         // Use default (now())
	})
	require.NoError(t, err)

	// Insert second bin with non-contiguous offsets
	err = db.InsertKafkaOffsets(ctx, lrdb.InsertKafkaOffsetsParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		Offsets:       []int64{110, 112, 113, 115}, // gaps at 111, 114
		CreatedAt:     nil,
	})
	require.NoError(t, err)

	// Insert third bin with larger gaps
	err = db.InsertKafkaOffsets(ctx, lrdb.InsertKafkaOffsetsParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		Offsets:       []int64{120, 125, 130}, // larger gaps
		CreatedAt:     nil,
	})
	require.NoError(t, err)

	// Test 1: Get all offsets >= 103
	result, err := db.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		MinOffset:     103,
	})
	require.NoError(t, err)
	expected := []int64{103, 105, 110, 112, 113, 115, 120, 125, 130} // gaps preserved
	assert.Equal(t, expected, result)

	// Test 2: Get all offsets >= 113
	result, err = db.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		MinOffset:     113,
	})
	require.NoError(t, err)
	expected = []int64{113, 115, 120, 125, 130} // gaps preserved
	assert.Equal(t, expected, result)

	// Test 3: Get offsets >= 130 (should return only 130)
	result, err = db.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		MinOffset:     130,
	})
	require.NoError(t, err)
	assert.Equal(t, []int64{130}, result)

	// Test 4: Different consumer group (should return empty)
	result, err = db.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: "other-consumer",
		Topic:         topic,
		PartitionID:   partitionID,
		MinOffset:     100,
	})
	require.NoError(t, err)
	assert.Empty(t, result)

	// Test 5: Different topic (should return empty)
	result, err = db.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: consumerGroup,
		Topic:         "other-topic",
		PartitionID:   partitionID,
		MinOffset:     100,
	})
	require.NoError(t, err)
	assert.Empty(t, result)

	// Test 6: Different partition (should return empty)
	result, err = db.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   1,
		MinOffset:     100,
	})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestKafkaOffsetsAfter_MultipleRows(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	consumerGroup := "test-consumer"
	topic := "test-topic"
	partitionID := int32(0)

	// Insert first batch with offsets 100-105 (with gaps)
	err := db.InsertKafkaOffsets(ctx, lrdb.InsertKafkaOffsetsParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		Offsets:       []int64{100, 102, 104, 105},
		CreatedAt:     nil,
	})
	require.NoError(t, err)

	// Insert second batch with overlapping offsets (102, 105) and new ones (106, 108)
	err = db.InsertKafkaOffsets(ctx, lrdb.InsertKafkaOffsetsParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		Offsets:       []int64{102, 105, 106, 108},
		CreatedAt:     nil,
	})
	require.NoError(t, err)

	// Insert third batch with more offsets including duplicates
	err = db.InsertKafkaOffsets(ctx, lrdb.InsertKafkaOffsetsParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		Offsets:       []int64{104, 107, 109, 110},
		CreatedAt:     nil,
	})
	require.NoError(t, err)

	// Test 1: Get all offsets >= 103
	// Should return unique offsets sorted: 104, 105, 106, 107, 108, 109, 110
	// Note: 102 is excluded because it's < 103
	result, err := db.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		MinOffset:     103,
	})
	require.NoError(t, err)

	// The query returns duplicates from different rows, but they should be sorted
	// We need to deduplicate them for comparison
	uniqueOffsets := deduplicateAndSort(result)
	expected := []int64{104, 105, 106, 107, 108, 109, 110}
	assert.Equal(t, expected, uniqueOffsets)

	// Test 2: Get all offsets >= 107
	result, err = db.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		MinOffset:     107,
	})
	require.NoError(t, err)
	uniqueOffsets = deduplicateAndSort(result)
	expected = []int64{107, 108, 109, 110}
	assert.Equal(t, expected, uniqueOffsets)

	// Test 3: Get all offsets >= 0 (should return all unique offsets)
	result, err = db.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		MinOffset:     0,
	})
	require.NoError(t, err)
	uniqueOffsets = deduplicateAndSort(result)
	expected = []int64{100, 102, 104, 105, 106, 107, 108, 109, 110}
	assert.Equal(t, expected, uniqueOffsets)

	// Test 4: Different partition should return empty
	result, err = db.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   1,
		MinOffset:     0,
	})
	require.NoError(t, err)
	assert.Empty(t, result)
}

// deduplicateAndSort removes duplicates and sorts the offsets
func deduplicateAndSort(offsets []int64) []int64 {
	if len(offsets) == 0 {
		return offsets
	}

	seen := make(map[int64]bool)
	unique := []int64{}
	for _, offset := range offsets {
		if !seen[offset] {
			seen[offset] = true
			unique = append(unique, offset)
		}
	}

	// Sort the unique offsets
	for i := 0; i < len(unique); i++ {
		for j := i + 1; j < len(unique); j++ {
			if unique[i] > unique[j] {
				unique[i], unique[j] = unique[j], unique[i]
			}
		}
	}

	return unique
}

func TestCleanupKafkaOffsets(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	// Insert test data
	consumerGroup := "test-consumer"
	topic := "test-topic"
	partitionID := int32(0)

	// Insert bins with different offset ranges
	err := db.InsertKafkaOffsets(ctx, lrdb.InsertKafkaOffsetsParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		Offsets:       []int64{100, 101, 102, 103, 104, 105},
		CreatedAt:     nil,
	})
	require.NoError(t, err)

	err = db.InsertKafkaOffsets(ctx, lrdb.InsertKafkaOffsetsParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		Offsets:       []int64{110, 111, 112, 113, 114, 115},
		CreatedAt:     nil,
	})
	require.NoError(t, err)

	err = db.InsertKafkaOffsets(ctx, lrdb.InsertKafkaOffsetsParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		Offsets:       []int64{120, 121, 122, 123, 124, 125},
		CreatedAt:     nil,
	})
	require.NoError(t, err)

	// Test 1: Cleanup with max_offset = 110 (should delete first bin only)
	deleted, err := db.CleanupKafkaOffsets(ctx, lrdb.CleanupKafkaOffsetsParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		MaxOffset:     110,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// Verify remaining offsets
	result, err := db.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		MinOffset:     0,
	})
	require.NoError(t, err)
	expected := []int64{110, 111, 112, 113, 114, 115, 120, 121, 122, 123, 124, 125}
	assert.Equal(t, expected, result)

	// Test 2: Cleanup with max_offset = 115 (should delete second bin)
	deleted, err = db.CleanupKafkaOffsets(ctx, lrdb.CleanupKafkaOffsetsParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		MaxOffset:     115,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// Verify only last bin remains
	result, err = db.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		MinOffset:     0,
	})
	require.NoError(t, err)
	expected = []int64{120, 121, 122, 123, 124, 125}
	assert.Equal(t, expected, result)

	// Test 3: Cleanup with max_offset = 200 (should delete all remaining)
	deleted, err = db.CleanupKafkaOffsets(ctx, lrdb.CleanupKafkaOffsetsParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		MaxOffset:     200,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// Verify no offsets remain
	result, err = db.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		MinOffset:     0,
	})
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestCleanupKafkaOffsetsByAge(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	// Insert test data with specific timestamps
	consumerGroup := "test-consumer"
	topic := "test-topic"
	partitionID := int32(0)

	now := time.Now()
	oldTime := now.Add(-48 * time.Hour)
	recentTime := now.Add(-1 * time.Hour)

	// Insert old bin (2 days ago)
	err := db.InsertKafkaOffsets(ctx, lrdb.InsertKafkaOffsetsParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		Offsets:       []int64{100, 101, 102, 103, 104, 105},
		CreatedAt:     &oldTime,
	})
	require.NoError(t, err)

	// Insert another old bin (2 days ago)
	err = db.InsertKafkaOffsets(ctx, lrdb.InsertKafkaOffsetsParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		Offsets:       []int64{110, 111, 112, 113, 114, 115},
		CreatedAt:     &oldTime,
	})
	require.NoError(t, err)

	// Insert recent bin (1 hour ago)
	err = db.InsertKafkaOffsets(ctx, lrdb.InsertKafkaOffsetsParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		Offsets:       []int64{120, 121, 122, 123, 124, 125},
		CreatedAt:     &recentTime,
	})
	require.NoError(t, err)

	// Test 1: Cleanup entries older than 24 hours (should delete 2 old bins)
	cutoff := now.Add(-24 * time.Hour)
	deleted, err := db.CleanupKafkaOffsetsByAge(ctx, cutoff)
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)

	// Verify only recent bin remains
	result, err := db.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		MinOffset:     0,
	})
	require.NoError(t, err)
	expected := []int64{120, 121, 122, 123, 124, 125}
	assert.Equal(t, expected, result)

	// Test 2: Cleanup with cutoff in future (should delete all)
	futureTime := now.Add(1 * time.Hour)
	deleted, err = db.CleanupKafkaOffsetsByAge(ctx, futureTime)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// Verify no offsets remain
	result, err = db.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: consumerGroup,
		Topic:         topic,
		PartitionID:   partitionID,
		MinOffset:     0,
	})
	require.NoError(t, err)
	assert.Empty(t, result)
}
