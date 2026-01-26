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
	"context"
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type processor[M messages.GroupableMessage, K comparable] interface {
	Process(ctx context.Context, group *accumulationGroup[K], kafkaOffsets []lrdb.KafkaOffsetInfo) error
	GetTargetRecordCount(ctx context.Context, groupingKey K) int64
	// ShouldEmitImmediately returns true if the message should be emitted as its own
	// bundle without being grouped with other messages. This is used to prevent
	// memory issues when processing large files that shouldn't be batched.
	ShouldEmitImmediately(msg M) bool
}

// offsetStore defines the interface for checking processed offsets
type offsetStore interface {
	KafkaOffsetsAfter(ctx context.Context, params lrdb.KafkaOffsetsAfterParams) ([]int64, error)
	CleanupKafkaOffsets(ctx context.Context, params lrdb.CleanupKafkaOffsetsParams) (int64, error)
	InsertKafkaOffsets(ctx context.Context, params lrdb.InsertKafkaOffsetsParams) error
}

// gatherer processes a stream of GroupableMessage from Kafka with sync mode deduplication
type gatherer[M messages.GroupableMessage, K comparable] struct {
	hunter          *hunter[M, K]
	metadataTracker *metadataTracker[M, K]
	processor       processor[M, K]
	syncTracker     *offsetTracker
	store           offsetStore
	topic           string
	consumerGroup   string
}

// newGatherer creates a new gatherer with sync mode deduplication
func newGatherer[M messages.GroupableMessage, K comparable](
	topic, consumerGroup string,
	processor processor[M, K],
	store offsetStore,
) *gatherer[M, K] {
	hunter := newHunter[M, K]()
	return &gatherer[M, K]{
		hunter:          hunter,
		metadataTracker: newMetadataTracker(topic, consumerGroup, hunter),
		processor:       processor,
		syncTracker:     newOffsetTracker(store, consumerGroup, topic),
		store:           store,
		topic:           topic,
		consumerGroup:   consumerGroup,
	}
}

// processMessage processes a single message with optional sync mode deduplication
func (g *gatherer[M, K]) processMessage(ctx context.Context, msg M, metadata *messageMetadata) error {
	ll := logctx.FromContext(ctx)

	// Validate topic and consumer group match expectations
	if metadata.Topic != g.topic {
		return &ConfigMismatchError{
			Field:    "topic",
			Expected: g.topic,
			Got:      metadata.Topic,
		}
	}
	if metadata.ConsumerGroup != g.consumerGroup {
		return &ConfigMismatchError{
			Field:    "consumer_group",
			Expected: g.consumerGroup,
			Got:      metadata.ConsumerGroup,
		}
	}

	// Check if this offset has already been processed (for deduplication)
	processed, err := g.syncTracker.isOffsetProcessed(ctx, metadata.Partition, metadata.Offset)
	if err != nil {
		ll.Error("Failed to check if offset is processed",
			slog.Any("error", err),
			slog.Int("partition", int(metadata.Partition)),
			slog.Int64("offset", metadata.Offset))
		// Continue processing on error to avoid blocking
	} else if processed {
		// Skip this message - already processed
		ll.Debug("Skipping already processed offset",
			slog.Int("partition", int(metadata.Partition)),
			slog.Int64("offset", metadata.Offset))
		return nil
	}

	// Get the grouping key from the message
	groupingKey := msg.GroupingKey().(K)

	// Check if this message should be emitted immediately without grouping
	if g.processor.ShouldEmitImmediately(msg) {
		return g.emitSingleMessage(ctx, msg, metadata, groupingKey)
	}

	// Get dynamic estimate from the processor
	targetRecordCount := g.processor.GetTargetRecordCount(ctx, groupingKey)

	// Process the message through the hunter
	result := g.hunter.addMessage(msg, metadata, targetRecordCount)

	if result != nil {
		// Collect Kafka offsets from the group's messages
		kafkaOffsets := g.collectKafkaOffsetsFromGroup(result.Group)

		// Call the processor with the accumulated group.  If it returns no error, it is
		// responsible for inserting the Kafka offsets into our DB tracking table.
		if err := g.processor.Process(ctx, result.Group, kafkaOffsets); err != nil {
			return err
		}

		// Track the metadata for calculating safe Kafka consumer group commits.
		g.metadataTracker.trackMetadata(result.Group)
	}

	return nil
}

// emitSingleMessage creates a single-message group and processes it immediately.
// This bypasses the hunter accumulation for messages that should not be grouped.
func (g *gatherer[M, K]) emitSingleMessage(ctx context.Context, msg M, metadata *messageMetadata, groupingKey K) error {
	now := time.Now()
	group := &accumulationGroup[K]{
		Key: groupingKey,
		Messages: []*accumulatedMessage{
			{
				Message:  msg,
				Metadata: metadata,
			},
		},
		TotalRecordCount: msg.RecordCount(),
		LatestOffsets:    map[int32]int64{metadata.Partition: metadata.Offset},
		CreatedAt:        now,
		LastUpdatedAt:    now,
	}

	kafkaOffsets := g.collectKafkaOffsetsFromGroup(group)

	if err := g.processor.Process(ctx, group, kafkaOffsets); err != nil {
		return err
	}

	g.metadataTracker.trackMetadata(group)
	return nil
}

// collectKafkaOffsetsFromGroup collects all Kafka offsets from the group's messages
func (g *gatherer[M, K]) collectKafkaOffsetsFromGroup(group *accumulationGroup[K]) []lrdb.KafkaOffsetInfo {
	if len(group.Messages) == 0 {
		return nil
	}

	partitionOffsets := make(map[int32][]int64)
	for _, accMsg := range group.Messages {
		metadata := accMsg.Metadata
		partitionOffsets[metadata.Partition] = append(partitionOffsets[metadata.Partition], metadata.Offset)
	}

	var kafkaOffsets []lrdb.KafkaOffsetInfo
	for partition, offsets := range partitionOffsets {
		kafkaOffsets = append(kafkaOffsets, lrdb.KafkaOffsetInfo{
			ConsumerGroup: g.consumerGroup,
			Topic:         g.topic,
			PartitionID:   partition,
			Offsets:       offsets,
		})
	}

	return kafkaOffsets
}

// processIdleGroups processes all groups that haven't been updated for longer than lastUpdatedAge duration
func (g *gatherer[M, K]) processIdleGroups(ctx context.Context, lastUpdatedAge, maxAge time.Duration) (int, error) {
	staleGroups := g.hunter.selectStaleGroups(lastUpdatedAge, maxAge)

	emitted := 0

	for _, group := range staleGroups {
		kafkaOffsets := g.collectKafkaOffsetsFromGroup(group)

		if err := g.processor.Process(ctx, group, kafkaOffsets); err != nil {
			return emitted, err
		}
		emitted++

		g.metadataTracker.trackMetadata(group)
	}

	// After processing idle groups, clean up old committed offsets
	g.cleanupCommittedOffsets(ctx)

	return emitted, nil
}

// cleanupCommittedOffsets removes old offset tracking records for offsets that have been committed
func (g *gatherer[M, K]) cleanupCommittedOffsets(ctx context.Context) {
	ll := logctx.FromContext(ctx)

	// Get the last committed offsets from the metadata tracker
	g.metadataTracker.mu.Lock()
	committedOffsets := make(map[int32]int64)
	for partition, offset := range g.metadataTracker.lastCommittedOffsets {
		committedOffsets[partition] = offset
	}
	g.metadataTracker.mu.Unlock()

	// Safety buffer to handle potential replays during rebalancing

	// Cleanup old offset tracking records for each partition
	for partition, maxOffset := range committedOffsets {
		// Apply safety buffer
		safeCleanupOffset := maxOffset - offsetCleanupBuffer
		if safeCleanupOffset < 0 {
			// Don't cleanup anything if we haven't processed enough messages yet
			continue
		}

		params := lrdb.CleanupKafkaOffsetsParams{
			ConsumerGroup: g.consumerGroup,
			Topic:         g.topic,
			PartitionID:   partition,
			MaxOffset:     safeCleanupOffset,
		}

		if rowsDeleted, err := g.store.CleanupKafkaOffsets(ctx, params); err != nil {
			ll.Error("Failed to cleanup old Kafka offset tracking records",
				slog.Any("error", err),
				slog.Int("partition", int(partition)),
				slog.Int64("maxOffset", maxOffset))
		} else if rowsDeleted > 0 {
			ll.Debug("Cleaned up old Kafka offset tracking records",
				slog.Int("partition", int(partition)),
				slog.Int64("committedOffset", maxOffset),
				slog.Int64("cleanupOffset", safeCleanupOffset),
				slog.Int64("rowsDeleted", rowsDeleted))
		}
	}
}
