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
	"slices"
	"sync"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// OffsetTrackerStore defines the interface for sync mode offset tracking
type OffsetTrackerStore interface {
	KafkaOffsetsAfter(ctx context.Context, params lrdb.KafkaOffsetsAfterParams) ([]int64, error)
}

// offsetTracker tracks processed offsets using arrays for deduplication
type offsetTracker struct {
	store         OffsetTrackerStore
	consumerGroup string
	topic         string

	// Cache of processed offsets per partition, updated as we process messages
	// Map key is partition ID, value is array of processed offsets
	processedOffsets map[int32][]int64
	mu               sync.RWMutex

	// Track the minimum offset we've queried for each partition
	// to avoid redundant database queries
	minQueriedOffset map[int32]int64
}

// newOffsetTracker creates a new sync mode offset tracker
func newOffsetTracker(store OffsetTrackerStore, consumerGroup, topic string) *offsetTracker {
	return &offsetTracker{
		store:            store,
		consumerGroup:    consumerGroup,
		topic:            topic,
		processedOffsets: make(map[int32][]int64),
		minQueriedOffset: make(map[int32]int64),
	}
}

// isOffsetProcessed checks if an offset has already been processed for a partition
func (s *offsetTracker) isOffsetProcessed(ctx context.Context, partition int32, offset int64) (bool, error) {
	s.mu.RLock()

	// Check if we need to query the database for this partition
	minQueried, hasQueried := s.minQueriedOffset[partition]
	cachedOffsets := s.processedOffsets[partition]
	s.mu.RUnlock()

	// If we haven't queried yet, or the offset is before our minimum queried offset,
	// we need to fetch from the database
	if !hasQueried || offset < minQueried {
		// Fetch offsets from database that are >= this offset
		dbOffsets, err := s.store.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
			ConsumerGroup: s.consumerGroup,
			Topic:         s.topic,
			PartitionID:   partition,
			MinOffset:     offset,
		})
		if err != nil {
			return false, err
		}

		// Update our cache with the fetched offsets
		s.mu.Lock()
		if len(dbOffsets) > 0 {
			// Merge database offsets with cached offsets
			offsetSet := make(map[int64]bool)
			for _, o := range cachedOffsets {
				offsetSet[o] = true
			}
			for _, o := range dbOffsets {
				offsetSet[o] = true
			}

			// Convert back to sorted slice
			merged := make([]int64, 0, len(offsetSet))
			for o := range offsetSet {
				merged = append(merged, o)
			}

			// Sort the merged offsets
			for i := 0; i < len(merged); i++ {
				for j := i + 1; j < len(merged); j++ {
					if merged[i] > merged[j] {
						merged[i], merged[j] = merged[j], merged[i]
					}
				}
			}

			s.processedOffsets[partition] = merged
		}

		// Update minimum queried offset
		if !hasQueried || offset < minQueried {
			s.minQueriedOffset[partition] = offset
		}
		s.mu.Unlock()

		// Check if offset is in the fetched data
		if slices.Contains(dbOffsets, offset) {
			return true, nil
		}

		return false, nil
	}

	// Check in our cached offsets
	if slices.Contains(cachedOffsets, offset) {
		return true, nil
	}

	return false, nil
}
