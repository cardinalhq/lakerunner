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

package metricsprocessing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// mockOffsetStore implements OffsetTrackerStore for testing
type mockOffsetStore struct {
	offsets       map[int32][]int64
	queryCount    int
	cleanupCount  int
	queriedRanges []struct {
		partition int32
		minOffset int64
	}
	cleanedRanges []struct {
		partition int32
		maxOffset int64
	}
}

func newMockOffsetStore() *mockOffsetStore {
	return &mockOffsetStore{
		offsets: make(map[int32][]int64),
	}
}

func (m *mockOffsetStore) KafkaOffsetsAfter(ctx context.Context, params lrdb.KafkaOffsetsAfterParams) ([]int64, error) {
	m.queryCount++
	m.queriedRanges = append(m.queriedRanges, struct {
		partition int32
		minOffset int64
	}{params.PartitionID, params.MinOffset})

	var result []int64
	if offsets, ok := m.offsets[params.PartitionID]; ok {
		for _, offset := range offsets {
			if offset >= params.MinOffset {
				result = append(result, offset)
			}
		}
	}
	return result, nil
}

func (m *mockOffsetStore) setProcessedOffsets(partition int32, offsets []int64) {
	m.offsets[partition] = offsets
}

func (m *mockOffsetStore) CleanupKafkaOffsets(ctx context.Context, params lrdb.CleanupKafkaOffsetsParams) (int64, error) {
	m.cleanupCount++
	m.cleanedRanges = append(m.cleanedRanges, struct {
		partition int32
		maxOffset int64
	}{params.PartitionID, params.MaxOffset})

	// Simulate cleanup by removing offsets <= maxOffset
	var deleted int64
	if offsets, ok := m.offsets[params.PartitionID]; ok {
		var remaining []int64
		for _, offset := range offsets {
			if offset <= params.MaxOffset {
				deleted++
			} else {
				remaining = append(remaining, offset)
			}
		}
		m.offsets[params.PartitionID] = remaining
	}
	return deleted, nil
}

func (m *mockOffsetStore) InsertKafkaOffsets(ctx context.Context, params lrdb.InsertKafkaOffsetsParams) error {
	existing := m.offsets[params.PartitionID]
	m.offsets[params.PartitionID] = append(existing, params.Offsets...)
	return nil
}

func TestOffsetTracker_FirstMessage(t *testing.T) {
	ctx := context.Background()
	store := newMockOffsetStore()
	tracker := newOffsetTracker(store, "test-group", "test-topic")

	// First message on partition 0
	processed, err := tracker.isOffsetProcessed(ctx, 0, 100)
	require.NoError(t, err)
	assert.False(t, processed, "First message should not be marked as processed")

	// Should have queried the database once
	assert.Equal(t, 1, store.queryCount, "Should query DB on first message")
	assert.Equal(t, int32(0), store.queriedRanges[0].partition)
	assert.Equal(t, int64(100), store.queriedRanges[0].minOffset)
}

func TestOffsetTracker_SequentialMessages(t *testing.T) {
	ctx := context.Background()
	store := newMockOffsetStore()
	tracker := newOffsetTracker(store, "test-group", "test-topic")

	// Process sequential messages
	offsets := []int64{100, 101, 102, 103, 104}
	for i, offset := range offsets {
		processed, err := tracker.isOffsetProcessed(ctx, 0, offset)
		require.NoError(t, err)
		assert.False(t, processed, "Message %d should not be processed", offset)

		if i == 0 {
			// First message triggers a query
			assert.Equal(t, 1, store.queryCount, "Should query on first message")
		} else {
			// Sequential messages should NOT trigger queries
			assert.Equal(t, 1, store.queryCount, "Should not query for sequential messages")
		}
	}
}

func TestOffsetTracker_GapDetection(t *testing.T) {
	ctx := context.Background()
	store := newMockOffsetStore()
	tracker := newOffsetTracker(store, "test-group", "test-topic")

	// First message
	processed, err := tracker.isOffsetProcessed(ctx, 0, 100)
	require.NoError(t, err)
	assert.False(t, processed)
	assert.Equal(t, 1, store.queryCount)

	// Sequential message (no query)
	processed, err = tracker.isOffsetProcessed(ctx, 0, 101)
	require.NoError(t, err)
	assert.False(t, processed)
	assert.Equal(t, 1, store.queryCount)

	// Gap detected - jump from 101 to 110
	processed, err = tracker.isOffsetProcessed(ctx, 0, 110)
	require.NoError(t, err)
	assert.False(t, processed)
	assert.Equal(t, 2, store.queryCount, "Gap should trigger new query")
	assert.Equal(t, int64(110), store.queriedRanges[1].minOffset)
}

func TestOffsetTracker_Deduplication(t *testing.T) {
	ctx := context.Background()
	store := newMockOffsetStore()

	// Pre-populate store with processed offsets
	store.setProcessedOffsets(0, []int64{100, 101, 102, 103, 104})

	tracker := newOffsetTracker(store, "test-group", "test-topic")

	// Try to process already-processed offset 102
	processed, err := tracker.isOffsetProcessed(ctx, 0, 102)
	require.NoError(t, err)
	assert.True(t, processed, "Offset 102 should be marked as already processed")

	// Sequential processing continues - 103 is also in cache
	processed, err = tracker.isOffsetProcessed(ctx, 0, 103)
	require.NoError(t, err)
	assert.True(t, processed, "Offset 103 should also be in cache")
}

func TestOffsetTracker_RestartScenario(t *testing.T) {
	ctx := context.Background()
	store := newMockOffsetStore()

	// Simulate DB has offsets 1-1000 already processed
	var processed []int64
	for i := int64(1); i <= 1000; i++ {
		processed = append(processed, i)
	}
	store.setProcessedOffsets(0, processed)

	tracker := newOffsetTracker(store, "test-group", "test-topic")

	// Consumer restarts and Kafka redelivers from 900
	redeliveryStart := int64(900)
	redeliveryEnd := int64(1000)

	// First redelivered message triggers DB query
	isProcessed, err := tracker.isOffsetProcessed(ctx, 0, redeliveryStart)
	require.NoError(t, err)
	assert.True(t, isProcessed, "Offset 900 should be marked as processed")
	assert.Equal(t, 1, store.queryCount)

	// Sequential redelivered messages (no more queries)
	for offset := redeliveryStart + 1; offset <= redeliveryEnd; offset++ {
		isProcessed, err = tracker.isOffsetProcessed(ctx, 0, offset)
		require.NoError(t, err)
		assert.True(t, isProcessed, "Offset %d should still be in cache", offset)
	}
	assert.Equal(t, 1, store.queryCount, "No additional queries for sequential messages")

	// New message arrives (1001)
	isProcessed, err = tracker.isOffsetProcessed(ctx, 0, 1001)
	require.NoError(t, err)
	assert.False(t, isProcessed, "New offset 1001 should not be processed")
	assert.Equal(t, 1, store.queryCount, "Still no additional query needed")
}

func TestOffsetTracker_MultiplePartitions(t *testing.T) {
	ctx := context.Background()
	store := newMockOffsetStore()
	tracker := newOffsetTracker(store, "test-group", "test-topic")

	// Process messages on different partitions
	partitions := []int32{0, 1, 2}
	for _, partition := range partitions {
		processed, err := tracker.isOffsetProcessed(ctx, partition, 100)
		require.NoError(t, err)
		assert.False(t, processed)
	}

	// Each partition should trigger its own query
	assert.Equal(t, 3, store.queryCount, "Each partition should query once")

	// Verify each partition was queried
	queriedPartitions := make(map[int32]bool)
	for _, r := range store.queriedRanges {
		queriedPartitions[r.partition] = true
	}
	assert.Equal(t, 3, len(queriedPartitions))
}

func TestOffsetTracker_SelfCleaningCache(t *testing.T) {
	ctx := context.Background()
	store := newMockOffsetStore()

	// Pre-populate with future offsets
	store.setProcessedOffsets(0, []int64{105, 106, 107})

	tracker := newOffsetTracker(store, "test-group", "test-topic")

	// Process offset 100 (gap triggers query)
	processed, err := tracker.isOffsetProcessed(ctx, 0, 100)
	require.NoError(t, err)
	assert.False(t, processed)

	// Cache should now contain 105, 106, 107
	state := tracker.partitions[0]
	assert.Equal(t, 3, len(state.dedupeCache))

	// Process 105 - should be deduplicated and removed from cache
	processed, err = tracker.isOffsetProcessed(ctx, 0, 105)
	require.NoError(t, err)
	assert.True(t, processed, "105 should be in cache")

	// Verify it was removed
	tracker.mu.RLock()
	_, exists := tracker.partitions[0].dedupeCache[105]
	tracker.mu.RUnlock()
	assert.False(t, exists, "105 should be removed from cache after deduplication")
}

func TestOffsetTracker_PartitionRebalance(t *testing.T) {
	ctx := context.Background()
	store := newMockOffsetStore()

	// Simulate another consumer processed 5001-5500
	var otherConsumerOffsets []int64
	for i := int64(5001); i <= 5500; i++ {
		otherConsumerOffsets = append(otherConsumerOffsets, i)
	}
	store.setProcessedOffsets(0, otherConsumerOffsets)

	tracker := newOffsetTracker(store, "test-group", "test-topic")

	// We regain partition at offset 5501
	processed, err := tracker.isOffsetProcessed(ctx, 0, 5501)
	require.NoError(t, err)
	assert.False(t, processed, "5501 is new and should not be processed")

	// Should have queried to check for processed offsets
	assert.Equal(t, 1, store.queryCount)

	// Sequential processing continues without queries
	processed, err = tracker.isOffsetProcessed(ctx, 0, 5502)
	require.NoError(t, err)
	assert.False(t, processed)
	assert.Equal(t, 1, store.queryCount, "No additional query for sequential offset")
}

func TestOffsetStore_Cleanup(t *testing.T) {
	ctx := context.Background()
	store := newMockOffsetStore()

	// Set up offsets: 100-200
	var offsets []int64
	for i := int64(100); i <= 200; i++ {
		offsets = append(offsets, i)
	}
	store.setProcessedOffsets(0, offsets)

	// Cleanup offsets <= 150
	deleted, err := store.CleanupKafkaOffsets(ctx, lrdb.CleanupKafkaOffsetsParams{
		ConsumerGroup: "test-group",
		Topic:         "test-topic",
		PartitionID:   0,
		MaxOffset:     150,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(51), deleted) // 100-150 inclusive = 51 offsets

	// Verify cleanup was called
	assert.Equal(t, 1, store.cleanupCount)
	assert.Len(t, store.cleanedRanges, 1)
	assert.Equal(t, int32(0), store.cleanedRanges[0].partition)
	assert.Equal(t, int64(150), store.cleanedRanges[0].maxOffset)

	// Verify remaining offsets are 151-200
	remaining := store.offsets[0]
	assert.Len(t, remaining, 50)
	assert.Equal(t, int64(151), remaining[0])
	assert.Equal(t, int64(200), remaining[len(remaining)-1])
}
