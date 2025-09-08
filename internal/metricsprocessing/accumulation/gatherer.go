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
	"time"

	"github.com/cardinalhq/lakerunner/internal/fly/messages"
)


type Processor[M messages.GroupableMessage, K comparable] interface {
	Process(ctx context.Context, group *AccumulationGroup[K], kafkaCommitData *KafkaCommitData, recordCountEstimate int64) error
	GetTargetRecordCount(ctx context.Context, groupingKey K) int64
}

// OffsetCallbacks defines simple callbacks for offset storage (implemented by the message source)
type OffsetCallbacks[K comparable] interface {
	// GetLastProcessedOffset returns the last processed offset for this key, or -1 if never seen
	GetLastProcessedOffset(ctx context.Context, metadata *MessageMetadata, groupingKey K) (int64, error)
	// MarkOffsetsProcessed records that these offsets have been processed for this key
	MarkOffsetsProcessed(ctx context.Context, key K, offsets map[int32]int64) error
}

// Gatherer processes a stream of GroupableMessage from Kafka
// and feeds them into a Hunter instance for accumulation
type Gatherer[M messages.GroupableMessage, K comparable] struct {
	hunter          *Hunter[M, K]
	metadataTracker *MetadataTracker[K]
	processor       Processor[M, K]
	offsetCallbacks OffsetCallbacks[K]
}

// NewGatherer creates a new generic Gatherer instance
func NewGatherer[M messages.GroupableMessage, K comparable](topic, consumerGroup string, processor Processor[M, K], offsetCallbacks OffsetCallbacks[K]) *Gatherer[M, K] {
	return &Gatherer[M, K]{
		hunter:          NewHunter[M, K](),
		metadataTracker: NewMetadataTracker[K](topic, consumerGroup),
		processor:       processor,
		offsetCallbacks: offsetCallbacks,
	}
}

// ProcessMessage processes a single GroupableMessage
// Checks offset tracking to determine if message should be processed
// If the target record count threshold is reached, it calls the processor and tracks metadata
func (g *Gatherer[M, K]) ProcessMessage(ctx context.Context, msg M, metadata *MessageMetadata) error {
	// Validate topic and consumer group match expectations
	if metadata.Topic != g.metadataTracker.topic {
		return &ConfigMismatchError{
			Field:    "topic",
			Expected: g.metadataTracker.topic,
			Got:      metadata.Topic,
		}
	}
	if metadata.ConsumerGroup != g.metadataTracker.consumerGroup {
		return &ConfigMismatchError{
			Field:    "consumer_group",
			Expected: g.metadataTracker.consumerGroup,
			Got:      metadata.ConsumerGroup,
		}
	}

	// Get the grouping key from the message
	groupingKey := msg.GroupingKey().(K)

	// Check if we should process this message based on offset tracking
	lastProcessedOffset, err := g.offsetCallbacks.GetLastProcessedOffset(ctx, metadata, groupingKey)
	if err != nil {
		return err
	}

	if metadata.Offset <= lastProcessedOffset {
		// Drop this message - already processed
		return nil
	}

	// Get dynamic estimate from the processor
	targetRecordCount := g.processor.GetTargetRecordCount(ctx, groupingKey)

	// Process the message through the hunter
	result := g.hunter.AddMessage(msg, metadata, targetRecordCount)

	if result != nil {
		// Create Kafka commit data from the actual messages in the Hunter list
		kafkaCommitData := g.createKafkaCommitDataFromGroup(result.Group)

		// Call the processor with the accumulated group, commit data, and record count estimate
		if err := g.processor.Process(ctx, result.Group, kafkaCommitData, result.Group.TotalRecordCount); err != nil {
			return err
		}

		// Track the metadata for calculating safe Kafka consumer group commits
		g.metadataTracker.TrackMetadata(result.Group)
		
		// Mark offsets as processed for this key
		if kafkaCommitData != nil {
			if err := g.offsetCallbacks.MarkOffsetsProcessed(ctx, result.Group.Key, kafkaCommitData.Offsets); err != nil {
				return err
			}
		}
	}

	return nil
}

// createKafkaCommitDataFromGroup creates KafkaCommitData from the actual messages in a Hunter group
func (g *Gatherer[M, K]) createKafkaCommitDataFromGroup(group *AccumulationGroup[K]) *KafkaCommitData {
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

// GetMetadataTracker returns the metadata tracker for accessing commit information
func (g *Gatherer[M, K]) GetMetadataTracker() *MetadataTracker[K] {
	return g.metadataTracker
}

// FlushStaleGroups processes all groups that haven't been updated for longer than the specified duration
// This is used for periodic flushing to handle groups that may never reach the record count threshold
func (g *Gatherer[M, K]) FlushStaleGroups(ctx context.Context, olderThan time.Duration) error {
	staleGroups := g.hunter.SelectStaleGroups(olderThan)

	for _, group := range staleGroups {
		// Create Kafka commit data from the actual messages in the group
		kafkaCommitData := g.createKafkaCommitDataFromGroup(group)

		// Call the processor with the group, commit data, and record count estimate
		if err := g.processor.Process(ctx, group, kafkaCommitData, group.TotalRecordCount); err != nil {
			return err
		}

		// Track the metadata for calculating safe Kafka consumer group commits
		g.metadataTracker.TrackMetadata(group)
		
		// Mark offsets as processed for this key
		if kafkaCommitData != nil {
			if err := g.offsetCallbacks.MarkOffsetsProcessed(ctx, group.Key, kafkaCommitData.Offsets); err != nil {
				return err
			}
		}
	}

	return nil
}
