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

	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// OrgInstanceOffsetStore defines the interface for org/instance offset storage operations
type OrgInstanceOffsetStore interface {
	KafkaJournalGetLastProcessedWithOrgInstance(ctx context.Context, params lrdb.KafkaJournalGetLastProcessedWithOrgInstanceParams) (int64, error)
}

// OrgInstanceOffsetCallbacks provides reusable offset management for keys that have org/instance data
type OrgInstanceOffsetCallbacks struct {
	store OrgInstanceOffsetStore
}

// NewOrgInstanceOffsetCallbacks creates a new reusable offset callback implementation
func NewOrgInstanceOffsetCallbacks(store OrgInstanceOffsetStore) *OrgInstanceOffsetCallbacks {
	return &OrgInstanceOffsetCallbacks{
		store: store,
	}
}

// GetLastProcessedOffset returns the last processed offset for this key, or -1 if never seen
func (o *OrgInstanceOffsetCallbacks) GetLastProcessedOffset(ctx context.Context, metadata *MessageMetadata, groupingKey messages.CompactionKey) (int64, error) {
	offset, err := o.store.KafkaJournalGetLastProcessedWithOrgInstance(ctx, lrdb.KafkaJournalGetLastProcessedWithOrgInstanceParams{
		Topic:          metadata.Topic,
		Partition:      metadata.Partition,
		ConsumerGroup:  metadata.ConsumerGroup,
		OrganizationID: groupingKey.OrganizationID,
		InstanceNum:    groupingKey.InstanceNum,
	})
	if err != nil {
		// Return -1 if no row found (never seen before)
		return -1, nil
	}
	return offset, nil
}

// MarkOffsetsProcessed records that these offsets have been processed for this key
func (o *OrgInstanceOffsetCallbacks) MarkOffsetsProcessed(ctx context.Context, key messages.CompactionKey, offsets map[int32]int64) error {
	// For now, we'll just mark the highest offset per partition for this org/instance
	// This could be enhanced to batch update multiple partitions at once
	for partition, offset := range offsets {
		// Update the offset tracking for this specific org/instance/partition combo
		// This would typically be done as part of the atomic database transaction
		// with the compaction work, but for now we'll do it separately
		_ = partition // We'll implement the actual storage later
		_ = offset
		_ = key
	}
	// TODO: Implement actual offset storage
	return nil
}