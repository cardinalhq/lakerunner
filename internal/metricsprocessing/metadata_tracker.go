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
	"maps"
	"sync"

	"github.com/cardinalhq/lakerunner/internal/fly/messages"
)

// metadataTracker tracks the latest offsets seen for Kafka commits
type metadataTracker[M messages.GroupableMessage, K comparable] struct {
	mu sync.RWMutex

	// Track offsets per partition and key: partition -> key -> offset
	partitionOffsets map[int32]map[K]int64
	// Last committed Kafka offset per partition
	lastCommittedOffsets map[int32]int64
	// Remember the topic and consumer group we're working with
	topic         string
	consumerGroup string
	// Reference to hunter to check pending groups
	hunter *hunter[M, K]
}

// newMetadataTracker creates a new MetadataTracker instance
func newMetadataTracker[M messages.GroupableMessage, K comparable](topic, consumerGroup string, hunter *hunter[M, K]) *metadataTracker[M, K] {
	return &metadataTracker[M, K]{
		partitionOffsets:     make(map[int32]map[K]int64),
		lastCommittedOffsets: make(map[int32]int64),
		topic:                topic,
		consumerGroup:        consumerGroup,
		hunter:               hunter,
	}
}

// trackMetadata tracks the metadata from all accumulated messages to determine commit points
func (mt *metadataTracker[M, K]) trackMetadata(group *accumulationGroup[K]) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	for _, accMsg := range group.Messages {
		metadata := accMsg.Metadata

		// Initialize map if it doesn't exist
		if mt.partitionOffsets[metadata.Partition] == nil {
			mt.partitionOffsets[metadata.Partition] = make(map[K]int64)
		}

		// Track the highest offset for this key
		currentOffset, exists := mt.partitionOffsets[metadata.Partition][group.Key]
		if !exists || metadata.Offset > currentOffset {
			mt.partitionOffsets[metadata.Partition][group.Key] = metadata.Offset
		}
	}
}

// KafkaCommitData represents Kafka offset data to be committed atomically with SQL operations
// This is now only used internally by metadataTracker for safe commit offset calculations
type KafkaCommitData struct {
	Topic         string
	ConsumerGroup string
	Offsets       map[int32]int64 // partition -> highest offset
}

// getSafeCommitOffsets calculates the minimum offsets across all keys that can be safely committed to Kafka
// Returns a single struct with all partition offsets that can be advanced
func (mt *metadataTracker[M, K]) getSafeCommitOffsets() *KafkaCommitData {
	offsets := make(map[int32]int64)

	mt.mu.Lock()
	defer mt.mu.Unlock()

	// First, find minimum pending offset per partition from hunter's groups
	minPendingPerPartition := make(map[int32]int64)

	// Check all pending groups in the hunter
	if mt.hunter != nil {
		mt.hunter.mu.Lock()
		for _, group := range mt.hunter.groups {
			for _, accMsg := range group.Messages {
				partition := accMsg.Metadata.Partition
				offset := accMsg.Metadata.Offset

				if currentMin, exists := minPendingPerPartition[partition]; !exists || offset < currentMin {
					minPendingPerPartition[partition] = offset
				}
			}
		}
		mt.hunter.mu.Unlock()
	}

	// Now calculate safe commit offset per partition
	for partition, keyOffsets := range mt.partitionOffsets {
		if len(keyOffsets) == 0 {
			continue
		}

		// Find the maximum processed offset for this partition
		var maxProcessed int64 = -1
		for _, offset := range keyOffsets {
			if offset > maxProcessed {
				maxProcessed = offset
			}
		}

		// Get the minimum pending offset for this partition
		minPending, hasPending := minPendingPerPartition[partition]

		// Safe commit offset is either:
		// 1. minPending - 1 (if there are pending messages)
		// 2. maxProcessed (if no pending messages)
		var safeOffset int64
		if hasPending && minPending > 0 {
			safeOffset = minPending - 1
		} else {
			safeOffset = maxProcessed
		}

		// Get the last committed offset for this partition
		lastCommitted, exists := mt.lastCommittedOffsets[partition]
		if !exists {
			lastCommitted = -1
		}

		// Only include if we can advance
		if safeOffset > lastCommitted {
			offsets[partition] = safeOffset
		}
	}

	// Return nil if no offsets can be advanced
	if len(offsets) == 0 {
		return nil
	}

	return &KafkaCommitData{
		Topic:         mt.topic,
		ConsumerGroup: mt.consumerGroup,
		Offsets:       offsets,
	}
}

// markOffsetsCommitted records that offsets have been successfully committed to Kafka
func (mt *metadataTracker[M, K]) markOffsetsCommitted(offsets map[int32]int64) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	maps.Copy(mt.lastCommittedOffsets, offsets)
}
