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
	"time"

	"github.com/cardinalhq/lakerunner/internal/fly/messages"
)

// MessageMetadata contains Kafka metadata for a message
type MessageMetadata struct {
	Topic         string
	Partition     int32
	ConsumerGroup string
	Offset        int64
}

// AccumulatedMessage wraps a GroupableMessage with its Kafka metadata
type AccumulatedMessage struct {
	Message  messages.GroupableMessage
	Metadata *MessageMetadata
}

// AccumulationGroup holds messages and their metadata for a specific key
type AccumulationGroup[K comparable] struct {
	Key              K
	Messages         []*AccumulatedMessage
	TotalRecordCount int64
	LatestOffsets    map[string]map[int32]int64 // topic -> partition -> offset
	CreatedAt        time.Time                  // When this group was first created
	LastUpdatedAt    time.Time                  // When this group was last modified
}

// AccumulationResult contains the accumulated messages and metadata when threshold is reached
type AccumulationResult[K comparable] struct {
	Group            *AccumulationGroup[K]
	TriggeringRecord *AccumulatedMessage // The message that caused the threshold to be exceeded
}

// Hunter accumulates GroupableMessage until a threshold is reached
type Hunter[M messages.GroupableMessage, K comparable] struct {
	groups map[K]*AccumulationGroup[K]
}

// NewHunter creates a new Hunter instance
func NewHunter[M messages.GroupableMessage, K comparable]() *Hunter[M, K] {
	return &Hunter[M, K]{
		groups: make(map[K]*AccumulationGroup[K]),
	}
}

// AddMessage adds a message to the appropriate accumulation group
// Returns an AccumulationResult if adding this message would exceed the targetRecordCount
func (h *Hunter[M, K]) AddMessage(msg M, metadata *MessageMetadata, targetRecordCount int64) *AccumulationResult[K] {
	key := msg.GroupingKey().(K)

	group, exists := h.groups[key]
	now := time.Now()
	if !exists {
		group = &AccumulationGroup[K]{
			Key:           key,
			Messages:      make([]*AccumulatedMessage, 0),
			LatestOffsets: make(map[string]map[int32]int64),
			CreatedAt:     now,
			LastUpdatedAt: now,
		}
		h.groups[key] = group
	} else {
		// Update the last modified timestamp
		group.LastUpdatedAt = now
	}

	accMsg := &AccumulatedMessage{
		Message:  msg,
		Metadata: metadata,
	}

	// Check if adding this message would exceed the target
	newTotalRecordCount := group.TotalRecordCount + msg.RecordCount()
	shouldReturn := newTotalRecordCount >= targetRecordCount && len(group.Messages) > 0

	// Add the message to the group
	group.Messages = append(group.Messages, accMsg)
	group.TotalRecordCount = newTotalRecordCount

	// Update latest offset tracking
	if group.LatestOffsets[metadata.Topic] == nil {
		group.LatestOffsets[metadata.Topic] = make(map[int32]int64)
	}
	if currentOffset, exists := group.LatestOffsets[metadata.Topic][metadata.Partition]; !exists || metadata.Offset > currentOffset {
		group.LatestOffsets[metadata.Topic][metadata.Partition] = metadata.Offset
	}

	if shouldReturn {
		// Remove the group from the hunter since we're returning it
		delete(h.groups, key)

		// Return the original group without copying
		return &AccumulationResult[K]{
			Group:            group,
			TriggeringRecord: accMsg,
		}
	}

	return nil
}

// SelectGroups calls the selector function for each group and returns those where selector returns true
// The groups are removed from the hunter when selected
func (h *Hunter[M, K]) SelectGroups(selector func(key K, group *AccumulationGroup[K]) bool) []*AccumulationGroup[K] {
	var selected []*AccumulationGroup[K]
	var keysToRemove []K

	for key, group := range h.groups {
		if selector(key, group) {
			selected = append(selected, group)
			keysToRemove = append(keysToRemove, key)
		}
	}

	// Remove selected groups from tracking
	for _, key := range keysToRemove {
		delete(h.groups, key)
	}

	return selected
}

// SelectStaleGroups selects all groups that haven't been updated for longer than the specified duration
// This is used for periodic flushing of groups that may never reach the record count threshold
func (h *Hunter[M, K]) SelectStaleGroups(olderThan time.Duration) []*AccumulationGroup[K] {
	cutoff := time.Now().Add(-olderThan)
	return h.SelectGroups(func(key K, group *AccumulationGroup[K]) bool {
		return group.LastUpdatedAt.Before(cutoff)
	})
}
