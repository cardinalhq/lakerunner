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
	"context"

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/google/uuid"
)

// OffsetKey uniquely identifies a topic/partition/consumer_group/org/instance combination for offset tracking
type OffsetKey struct {
	Topic          string
	Partition      int32
	ConsumerGroup  string
	OrganizationID uuid.UUID
	InstanceNum    int16
}

// OffsetTracker manages lazy-loaded offset state for multiple organization/instance combinations
type OffsetTracker struct {
	offsets map[OffsetKey]int64 // offset_key -> last_seen_offset
	store   OffsetStore
}

// OffsetStore defines the interface for querying Kafka offset information
type OffsetStore interface {
	KafkaJournalGetLastProcessedWithOrgInstance(ctx context.Context, params lrdb.KafkaJournalGetLastProcessedWithOrgInstanceParams) (int64, error)
}

// NewOffsetTracker creates a new OffsetTracker for multiple organization/instance combinations
func NewOffsetTracker(store OffsetStore) *OffsetTracker {
	return &OffsetTracker{
		offsets: make(map[OffsetKey]int64),
		store:   store,
	}
}

// ShouldProcessMessage checks if a message should be processed based on offset tracking
// Returns true if the message should be processed, false if it should be dropped
func (ot *OffsetTracker) ShouldProcessMessage(ctx context.Context, metadata *MessageMetadata, organizationID uuid.UUID, instanceNum int16) (bool, error) {
	offsetKey := OffsetKey{
		Topic:          metadata.Topic,
		Partition:      metadata.Partition,
		ConsumerGroup:  metadata.ConsumerGroup,
		OrganizationID: organizationID,
		InstanceNum:    instanceNum,
	}

	lastSeenOffset, exists := ot.offsets[offsetKey]

	if !exists {
		// First time seeing this org/instance/partition - load from DB
		dbOffset, err := ot.store.KafkaJournalGetLastProcessedWithOrgInstance(ctx, lrdb.KafkaJournalGetLastProcessedWithOrgInstanceParams{
			ConsumerGroup:  metadata.ConsumerGroup,
			Topic:          metadata.Topic,
			Partition:      metadata.Partition,
			OrganizationID: organizationID,
			InstanceNum:    instanceNum,
		})

		if err != nil {
			// If no row found, this is likely the first message for this org/instance/partition
			// Start tracking from this offset
			ot.offsets[offsetKey] = metadata.Offset
			return true, nil
		}

		// Found existing offset in DB
		if metadata.Offset <= dbOffset {
			// Already processed this message
			return false, nil
		}

		// Update our in-memory state and process
		ot.offsets[offsetKey] = metadata.Offset
		return true, nil
	}

	// We have seen this org/instance/partition before
	if metadata.Offset == lastSeenOffset+1 {
		// Expected next message - increment and process
		ot.offsets[offsetKey] = metadata.Offset
		return true, nil
	}

	if metadata.Offset <= lastSeenOffset {
		// Already seen this message or older - drop it
		return false, nil
	}

	// Gap detected - refetch from DB to see if we missed something
	dbOffset, err := ot.store.KafkaJournalGetLastProcessedWithOrgInstance(ctx, lrdb.KafkaJournalGetLastProcessedWithOrgInstanceParams{
		ConsumerGroup:  metadata.ConsumerGroup,
		Topic:          metadata.Topic,
		Partition:      metadata.Partition,
		OrganizationID: organizationID,
		InstanceNum:    instanceNum,
	})

	if err != nil {
		// Error fetching from DB, use our in-memory state
		if metadata.Offset > lastSeenOffset {
			ot.offsets[offsetKey] = metadata.Offset
			return true, nil
		}
		return false, nil
	}

	// Update our view to match DB state
	ot.offsets[offsetKey] = dbOffset

	if metadata.Offset <= dbOffset {
		// Already processed
		return false, nil
	}

	// Update to current message and process
	ot.offsets[offsetKey] = metadata.Offset
	return true, nil
}
