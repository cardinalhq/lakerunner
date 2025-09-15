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
	Process(ctx context.Context, group *accumulationGroup[K], kafkaOffsets []lrdb.KafkaOffsetInfo) error
	GetTargetRecordCount(ctx context.Context, groupingKey K) int64
}

// offsetStore defines the interface for checking processed offsets
type offsetStore interface {
	KafkaOffsetsAfter(ctx context.Context, params lrdb.KafkaOffsetsAfterParams) ([]int64, error)
}

// gatherer processes a stream of GroupableMessage from Kafka with sync mode deduplication
type gatherer[M messages.GroupableMessage, K comparable] struct {
	hunter          *hunter[M, K]
	metadataTracker *metadataTracker[M, K]
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
	hunter := newHunter[M, K]()
	return &gatherer[M, K]{
		hunter:          hunter,
		metadataTracker: newMetadataTracker[M, K](topic, consumerGroup, hunter),
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

	return emitted, nil
}
