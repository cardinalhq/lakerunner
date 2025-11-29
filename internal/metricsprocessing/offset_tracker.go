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
	"sync"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// OffsetTrackerStore defines the interface for sync mode offset tracking
type OffsetTrackerStore interface {
	KafkaOffsetsAfter(ctx context.Context, params lrdb.KafkaOffsetsAfterParams) ([]int64, error)
	CleanupKafkaOffsets(ctx context.Context, params lrdb.CleanupKafkaOffsetsParams) (int64, error)
	InsertKafkaOffsets(ctx context.Context, params lrdb.InsertKafkaOffsetsParams) error
}

// offsetTracker tracks processed offsets using arrays for deduplication
type offsetTracker struct {
	store         OffsetTrackerStore
	consumerGroup string
	topic         string

	// Per-partition tracking state
	partitions map[int32]*partitionState
	mu         sync.RWMutex
}

// partitionState tracks state for a single partition
type partitionState struct {
	mu               sync.Mutex     // Lock for this partition's state
	lastSeenOffset   int64          // Last offset we read from Kafka
	lastCommitOffset int64          // Last offset we committed to Kafka consumer group
	dedupeCache      map[int64]bool // Future offsets to filter out (self-cleaning)
}

// newOffsetTracker creates a new sync mode offset tracker
func newOffsetTracker(store OffsetTrackerStore, consumerGroup, topic string) *offsetTracker {
	return &offsetTracker{
		store:         store,
		consumerGroup: consumerGroup,
		topic:         topic,
		partitions:    make(map[int32]*partitionState),
	}
}

// isOffsetProcessed checks if an offset has already been processed for a partition
func (s *offsetTracker) isOffsetProcessed(ctx context.Context, partition int32, offset int64) (bool, error) {
	// Fast path: try to get partition state with read lock
	s.mu.RLock()
	state, exists := s.partitions[partition]
	s.mu.RUnlock()

	// Slow path: create partition state if it doesn't exist
	if !exists {
		s.mu.Lock()
		// Double-check after acquiring write lock (another goroutine might have created it)
		state, exists = s.partitions[partition]
		if !exists {
			state = &partitionState{
				lastSeenOffset:   0,
				lastCommitOffset: -1,
				dedupeCache:      make(map[int64]bool),
			}
			s.partitions[partition] = state
		}
		s.mu.Unlock()
	}

	// Now lock only this partition's state for the deduplication work
	state.mu.Lock()
	defer state.mu.Unlock()

	// Check if this is first message, there's a gap, or offset moved backward
	needsQuery := state.lastSeenOffset == 0 || offset > state.lastSeenOffset+1 || offset < state.lastSeenOffset

	if needsQuery {
		// Gap detected - query database for future offsets
		dbOffsets, err := s.store.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
			ConsumerGroup: s.consumerGroup,
			Topic:         s.topic,
			PartitionID:   partition,
			MinOffset:     offset,
		})
		if err != nil {
			return false, err
		}

		// Clear and rebuild dedupe cache with future offsets
		state.dedupeCache = make(map[int64]bool)
		for _, o := range dbOffsets {
			state.dedupeCache[o] = true
		}
	}

	// Update last seen offset
	state.lastSeenOffset = offset

	// Check if this offset was already processed
	if state.dedupeCache[offset] {
		// Remove from cache (self-cleaning)
		delete(state.dedupeCache, offset)
		return true, nil
	}

	return false, nil
}
