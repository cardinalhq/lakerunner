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
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/google/uuid"
)

// CompactionKey represents the key for grouping messages for compaction
type CompactionKey struct {
	OrganizationID uuid.UUID
	DateInt        int32
	FrequencyMs    int32
	InstanceNum    int16
}

type Processor interface {
	Process(ctx context.Context, group *AccumulationGroup[CompactionKey], kafkaCommitData *KafkaCommitData, recordCountEstimate int64) error
}

// GathererStore defines the interface for querying Kafka offset information and estimates
type GathererStore interface {
	KafkaJournalGetLastProcessedWithOrgInstance(ctx context.Context, params lrdb.KafkaJournalGetLastProcessedWithOrgInstanceParams) (int64, error)
	GetMetricEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64
}

// Gatherer processes a stream of MetricSegmentNotificationMessage from Kafka
// and feeds them into a Hunter instance for accumulation
type Gatherer struct {
	hunter          *Hunter[CompactionKey]
	metadataTracker *MetadataTracker
	offsetTracker   *OffsetTracker
	compactor       Processor
	store           GathererStore
}

// NewGatherer creates a new Gatherer instance for compaction
func NewGatherer(topic, consumerGroup string, store GathererStore, compactor Processor) *Gatherer {
	keyMapper := func(msg *messages.MetricSegmentNotificationMessage) CompactionKey {
		return CompactionKey{
			OrganizationID: msg.OrganizationID,
			DateInt:        msg.DateInt,
			FrequencyMs:    msg.FrequencyMs,
			InstanceNum:    msg.InstanceNum,
		}
	}

	return &Gatherer{
		hunter:          NewHunter(keyMapper),
		metadataTracker: NewMetadataTracker(topic, consumerGroup),
		offsetTracker:   NewOffsetTracker(store),
		compactor:       compactor,
		store:           store,
	}
}

// ProcessMessage processes a single MetricSegmentNotificationMessage
// Checks offset tracking to determine if message should be processed
// If the target record count threshold is reached, it calls the compactor and tracks metadata
func (g *Gatherer) ProcessMessage(ctx context.Context, msg *messages.MetricSegmentNotificationMessage, metadata *MessageMetadata) error {
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

	// Check if we should process this message based on offset tracking
	shouldProcess, err := g.offsetTracker.ShouldProcessMessage(ctx, metadata, msg.OrganizationID, msg.InstanceNum)
	if err != nil {
		return err
	}

	if !shouldProcess {
		// Drop this message - already processed
		return nil
	}

	// Get dynamic estimate for this org/frequency combination
	targetRecordCount := g.store.GetMetricEstimate(ctx, msg.OrganizationID, msg.FrequencyMs)

	// Process the message through the hunter
	result := g.hunter.AddMessage(msg, metadata, targetRecordCount)

	if result != nil {
		// Create Kafka commit data from the actual messages in the Hunter list
		kafkaCommitData := g.createKafkaCommitDataFromGroup(result.Group)

		// Call the compactor with the accumulated group, commit data, and record count estimate
		if err := g.compactor.Process(ctx, result.Group, kafkaCommitData, result.Group.TotalRecordCount); err != nil {
			return err
		}

		// Track the metadata for calculating safe Kafka consumer group commits
		g.metadataTracker.TrackMetadata(result.Group)
	}

	return nil
}

// createKafkaCommitDataFromGroup creates KafkaCommitData from the actual messages in a Hunter group
func (g *Gatherer) createKafkaCommitDataFromGroup(group *AccumulationGroup[CompactionKey]) *KafkaCommitData {
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
func (g *Gatherer) GetMetadataTracker() *MetadataTracker {
	return g.metadataTracker
}

// FlushStaleGroups processes all groups that haven't been updated for longer than the specified duration
// This is used for periodic flushing to handle groups that may never reach the record count threshold
func (g *Gatherer) FlushStaleGroups(ctx context.Context, olderThan time.Duration) error {
	staleGroups := g.hunter.SelectStaleGroups(olderThan)

	for _, group := range staleGroups {
		// Create Kafka commit data from the actual messages in the group
		kafkaCommitData := g.createKafkaCommitDataFromGroup(group)

		// Call the processor with the group, commit data, and record count estimate
		if err := g.compactor.Process(ctx, group, kafkaCommitData, group.TotalRecordCount); err != nil {
			return err
		}

		// Track the metadata for calculating safe Kafka consumer group commits
		g.metadataTracker.TrackMetadata(group)
	}

	return nil
}
