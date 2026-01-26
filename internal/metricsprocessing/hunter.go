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

package metricsprocessing

import (
	"sync"
	"time"

	"github.com/cardinalhq/lakerunner/internal/fly/messages"
)

// messageMetadata contains Kafka metadata for a message
type messageMetadata struct {
	Topic         string
	Partition     int32
	ConsumerGroup string
	Offset        int64
}

// accumulatedMessage wraps a GroupableMessage with its Kafka metadata
type accumulatedMessage struct {
	Message  messages.GroupableMessage
	Metadata *messageMetadata
}

// accumulationGroup holds messages and their metadata for a specific key
type accumulationGroup[K comparable] struct {
	Key              K
	Messages         []*accumulatedMessage
	TotalRecordCount int64
	LatestOffsets    map[int32]int64 // partition -> offset (single topic only)
	CreatedAt        time.Time       // When this group was first created
	LastUpdatedAt    time.Time       // When this group was last modified
}

// accumulationResult contains the accumulated messages and metadata when threshold is reached
type accumulationResult[K comparable] struct {
	Group *accumulationGroup[K]
}

// hunter accumulates GroupableMessage until a threshold is reached.
// All methods are safe for concurrent use by multiple goroutines.
type hunter[M messages.GroupableMessage, K comparable] struct {
	mu     sync.Mutex
	groups map[K]*accumulationGroup[K]
}

// newHunter creates a new Hunter instance
func newHunter[M messages.GroupableMessage, K comparable]() *hunter[M, K] {
	return &hunter[M, K]{
		groups: make(map[K]*accumulationGroup[K]),
	}
}

// addMessage adds a message to the appropriate accumulation group.
// Returns an AccumulationResult if adding this message would exceed the targetRecordCount.
// Safe for concurrent use.
func (h *hunter[M, K]) addMessage(msg M, metadata *messageMetadata, targetRecordCount int64) *accumulationResult[K] {
	h.mu.Lock()
	defer h.mu.Unlock()

	key := msg.GroupingKey().(K)

	group, exists := h.groups[key]
	now := time.Now()
	if !exists {
		group = &accumulationGroup[K]{
			Key:           key,
			Messages:      make([]*accumulatedMessage, 0),
			LatestOffsets: make(map[int32]int64),
			CreatedAt:     now,
			LastUpdatedAt: now,
		}
		h.groups[key] = group
	} else {
		// Update the last modified timestamp
		group.LastUpdatedAt = now
	}

	accMsg := &accumulatedMessage{
		Message:  msg,
		Metadata: metadata,
	}

	// Add the message to the group
	group.Messages = append(group.Messages, accMsg)
	group.TotalRecordCount += msg.RecordCount()

	// Update latest offset tracking
	if currentOffset, exists := group.LatestOffsets[metadata.Partition]; !exists || metadata.Offset > currentOffset {
		group.LatestOffsets[metadata.Partition] = metadata.Offset
	}

	// Emit the group if either:
	// 1. We've reached the target record count, OR
	// 2. We've accumulated 50 segments (messages) to prevent huge bundles
	const maxSegmentsPerBundle = 50
	if group.TotalRecordCount >= targetRecordCount || len(group.Messages) >= maxSegmentsPerBundle {
		// Remove the group from the hunter since we're returning it
		delete(h.groups, key)

		// Return the original group without copying
		return &accumulationResult[K]{Group: group}
	}

	return nil
}

// selectGroups calls the selector function for each group and returns those where selector returns true.
// The groups are removed from the hunter when selected.
// Safe for concurrent use.
func (h *hunter[M, K]) selectGroups(selector func(key K, group *accumulationGroup[K]) bool) []*accumulationGroup[K] {
	h.mu.Lock()
	defer h.mu.Unlock()

	var selected []*accumulationGroup[K]
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

// selectStaleGroups selects all groups that haven't been updated for longer than lastUpdatedAge duration,
// or that are older than maxAge since creation (if maxAge > 0).
// If lastUpdatedAge is 0, all groups are selected immediately.
// If maxAge is 0, absolute age is not checked.
// This is used for periodic flushing of groups that may never reach the record count threshold.
// Safe for concurrent use.
func (h *hunter[M, K]) selectStaleGroups(lastUpdatedAge, maxAge time.Duration) []*accumulationGroup[K] {
	now := time.Now()

	return h.selectGroups(func(key K, group *accumulationGroup[K]) bool {
		// If lastUpdatedAge is 0, flush all groups immediately
		if lastUpdatedAge == 0 {
			return true
		}

		// Check if last update is stale
		lastUpdatedCutoff := now.Add(-lastUpdatedAge)
		if group.LastUpdatedAt.Before(lastUpdatedCutoff) {
			return true
		}

		// Check if absolute age exceeds maxAge (if maxAge > 0)
		if maxAge > 0 {
			createdCutoff := now.Add(-maxAge)
			if group.CreatedAt.Before(createdCutoff) {
				return true
			}
		}

		return false
	})
}
