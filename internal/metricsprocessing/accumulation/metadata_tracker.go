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

package accumulation

import (
	"maps"

	"github.com/google/uuid"
)

// OrgInstanceKey uniquely identifies an organization/instance combination
type OrgInstanceKey struct {
	OrganizationID uuid.UUID
	InstanceNum    int16
}

// MetadataTracker tracks the latest offsets seen for Kafka commits
type MetadataTracker struct {
	// Track offsets per partition and org/instance: partition -> org/instance -> offset
	partitionOffsets map[int32]map[OrgInstanceKey]int64
	// Last committed Kafka offset per partition
	lastCommittedOffsets map[int32]int64
	// Remember the topic and consumer group we're working with
	topic         string
	consumerGroup string
}

// NewMetadataTracker creates a new MetadataTracker instance
func NewMetadataTracker(topic, consumerGroup string) *MetadataTracker {
	return &MetadataTracker{
		partitionOffsets:     make(map[int32]map[OrgInstanceKey]int64),
		lastCommittedOffsets: make(map[int32]int64),
		topic:                topic,
		consumerGroup:        consumerGroup,
	}
}

// TrackMetadata tracks the metadata from all accumulated messages to determine commit points
func (mt *MetadataTracker) TrackMetadata(group *AccumulationGroup[CompactionKey]) {
	orgInstanceKey := OrgInstanceKey{
		OrganizationID: group.Key.OrganizationID,
		InstanceNum:    group.Key.InstanceNum,
	}

	for _, accMsg := range group.Messages {
		metadata := accMsg.Metadata

		// Initialize map if it doesn't exist
		if mt.partitionOffsets[metadata.Partition] == nil {
			mt.partitionOffsets[metadata.Partition] = make(map[OrgInstanceKey]int64)
		}

		// Track the highest offset for this organization/instance combination
		currentOffset, exists := mt.partitionOffsets[metadata.Partition][orgInstanceKey]
		if !exists || metadata.Offset > currentOffset {
			mt.partitionOffsets[metadata.Partition][orgInstanceKey] = metadata.Offset
		}
	}
}

// KafkaCommitData represents Kafka offset data to be committed atomically with SQL operations
type KafkaCommitData struct {
	Topic         string
	ConsumerGroup string
	Offsets       map[int32]int64 // partition -> offset
}

// GetSafeCommitOffsets calculates the minimum offsets across all org/instance combinations that can be safely committed to Kafka
// Returns a single struct with all partition offsets that can be advanced
func (mt *MetadataTracker) GetSafeCommitOffsets() *KafkaCommitData {
	offsets := make(map[int32]int64)

	for partition, orgInstanceOffsets := range mt.partitionOffsets {
		if len(orgInstanceOffsets) == 0 {
			continue
		}

		// Find the minimum offset across all org/instance combinations for this partition
		var minOffset int64 = -1
		for _, offset := range orgInstanceOffsets {
			if minOffset == -1 || offset < minOffset {
				minOffset = offset
			}
		}

		// Get the last committed offset for this partition
		lastCommitted, exists := mt.lastCommittedOffsets[partition]
		if !exists {
			lastCommitted = -1
		}

		// Only include if we can advance
		if minOffset > lastCommitted {
			offsets[partition] = minOffset
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

// MarkOffsetsCommitted records that offsets have been successfully committed to Kafka
func (mt *MetadataTracker) MarkOffsetsCommitted(offsets map[int32]int64) {
	maps.Copy(mt.lastCommittedOffsets, offsets)
}
