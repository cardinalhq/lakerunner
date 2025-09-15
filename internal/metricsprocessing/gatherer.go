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
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type processor[M messages.GroupableMessage, K comparable] interface {
	Process(ctx context.Context, group *accumulationGroup[K], kafkaCommitData *KafkaCommitData) error
	GetTargetRecordCount(ctx context.Context, groupingKey K) int64
}

// offsetStore defines the interface for checking processed offsets
type offsetStore interface {
	KafkaOffsetsAfter(ctx context.Context, params lrdb.KafkaOffsetsAfterParams) ([]int64, error)
}

// gatherer processes a stream of GroupableMessage from Kafka with sync mode deduplication
type gatherer[M messages.GroupableMessage, K comparable] struct {
	hunter          *hunter[M, K]
	metadataTracker *metadataTracker[K]
	processor       processor[M, K]
	syncTracker     *offsetTracker
	topic           string
	consumerGroup   string
}

// newGatherer creates a new gatherer with sync mode deduplication
func newGatherer[M messages.GroupableMessage, K comparable](
	topic, consumerGroup string,
	processor processor[M, K],
	store offsetStore,
) *gatherer[M, K] {
	return &gatherer[M, K]{
		hunter:          newHunter[M, K](),
		metadataTracker: newMetadataTracker[K](topic, consumerGroup),
		processor:       processor,
		syncTracker:     newOffsetTracker(store, consumerGroup, topic),
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

	// Get dynamic estimate from the processor
	targetRecordCount := g.processor.GetTargetRecordCount(ctx, groupingKey)

	// Process the message through the hunter
	result := g.hunter.addMessage(msg, metadata, targetRecordCount)

	if result != nil {
		// Create Kafka commit data from the actual messages in the Hunter list
		kafkaCommitData := g.createKafkaCommitDataFromGroup(result.Group)

		// Call the processor with the accumulated group.  If it returns no error, it is
		// responsible for inserting the Kafka offsets into our DB tracking table.
		if err := g.processor.Process(ctx, result.Group, kafkaCommitData); err != nil {
			return err
		}

		// Track the metadata for calculating safe Kafka consumer group commits.
		g.metadataTracker.trackMetadata(result.Group)
	}

	return nil
}

// createKafkaCommitDataFromGroup creates KafkaCommitData from the actual messages in a Hunter group
func (g *gatherer[M, K]) createKafkaCommitDataFromGroup(group *accumulationGroup[K]) *KafkaCommitData {
	if len(group.Messages) == 0 {
		return nil
	}

	// Use the first message to get topic and consumer group
	firstMsg := group.Messages[0]
	topic := firstMsg.Metadata.Topic
	consumerGroup := firstMsg.Metadata.ConsumerGroup

	// Build map of highest offset per partition from this group's messages
	offsets := make(map[int32]int64)
	for _, accMsg := range group.Messages {
		metadata := accMsg.Metadata
		if currentOffset, exists := offsets[metadata.Partition]; !exists || metadata.Offset > currentOffset {
			offsets[metadata.Partition] = metadata.Offset
		}
	}

	return &KafkaCommitData{
		Topic:         topic,
		ConsumerGroup: consumerGroup,
		Offsets:       offsets,
	}
}

// processIdleGroups processes all groups that haven't been updated for longer than lastUpdatedAge duration
func (g *gatherer[M, K]) processIdleGroups(ctx context.Context, lastUpdatedAge, maxAge time.Duration) (int, error) {
	staleGroups := g.hunter.selectStaleGroups(lastUpdatedAge, maxAge)

	emitted := 0

	for _, group := range staleGroups {
		// Create Kafka commit data from the actual messages in the group
		kafkaCommitData := g.createKafkaCommitDataFromGroup(group)

		if err := g.processor.Process(ctx, group, kafkaCommitData); err != nil {
			return emitted, err
		}
		emitted++

		// Track the metadata for calculating safe Kafka consumer group commits
		g.metadataTracker.trackMetadata(group)

		// Note: Offset tracking happens automatically when segments are inserted/compacted
	}

	return emitted, nil
}
