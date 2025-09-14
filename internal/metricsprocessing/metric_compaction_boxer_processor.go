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
	"fmt"
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// MetricCompactionBoxerProcessor implements the Processor interface for boxing metric compaction bundles
type MetricCompactionBoxerProcessor struct {
	kafkaProducer fly.Producer
	store         BoxerStore
}

// newMetricCompactionBoxerProcessor creates a new metric compaction boxer processor instance
func newMetricCompactionBoxerProcessor(kafkaProducer fly.Producer, store BoxerStore) *MetricCompactionBoxerProcessor {
	return &MetricCompactionBoxerProcessor{
		kafkaProducer: kafkaProducer,
		store:         store,
	}
}

// Process implements the Processor interface and sends the bundle to the compaction topic
func (b *MetricCompactionBoxerProcessor) Process(ctx context.Context, group *accumulationGroup[messages.CompactionKey], kafkaCommitData *KafkaCommitData) error {
	ll := logctx.FromContext(ctx)

	// Create a MetricCompactionBundle to send to the compaction topic
	bundle := &messages.MetricCompactionBundle{
		Version:  1,
		Messages: make([]*messages.MetricCompactionMessage, 0, len(group.Messages)),
		QueuedAt: time.Now(),
	}

	// Convert accumulated messages to MetricCompactionMessage format
	for _, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.MetricCompactionMessage)
		if !ok {
			continue // Skip non-MetricCompactionMessage messages
		}
		bundle.Messages = append(bundle.Messages, msg)
	}

	ll.Info("Boxing compaction bundle for processing",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("dateint", int(group.Key.DateInt)),
		slog.Int("frequencyMs", int(group.Key.FrequencyMs)),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
		slog.Int("messageCount", len(bundle.Messages)))

	// Marshal the bundle
	msgBytes, err := bundle.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal compaction bundle: %w", err)
	}

	bundleMessage := fly.Message{
		Value: msgBytes,
	}

	// Send to compaction topic
	compactionTopic := config.DefaultTopicRegistry().GetTopic(config.TopicSegmentsMetricsCompact)
	if err := b.kafkaProducer.Send(ctx, compactionTopic, bundleMessage); err != nil {
		return fmt.Errorf("failed to send compaction bundle to compaction topic: %w", err)
	}

	ll.Info("Successfully sent compaction bundle to compaction topic",
		slog.String("topic", compactionTopic),
		slog.Int("bundledMessages", len(bundle.Messages)))

	// Commit Kafka offsets in batch after successful message send
	if err := b.commitKafkaOffsets(ctx, group, kafkaCommitData); err != nil {
		return fmt.Errorf("failed to commit Kafka offsets: %w", err)
	}

	return nil
}

// GetTargetRecordCount returns the estimated record count for the frequency
func (b *MetricCompactionBoxerProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.CompactionKey) int64 {
	return b.store.GetMetricEstimate(ctx, groupingKey.OrganizationID, groupingKey.FrequencyMs)
}

// commitKafkaOffsets commits the Kafka offsets for all messages in the group using batch operation
func (b *MetricCompactionBoxerProcessor) commitKafkaOffsets(ctx context.Context, group *accumulationGroup[messages.CompactionKey], kafkaCommitData *KafkaCommitData) error {
	if kafkaCommitData == nil || len(kafkaCommitData.Offsets) == 0 {
		return nil // Nothing to commit
	}

	// Convert kafkaCommitData to batch parameters, organizing by org/instance
	batchParams := make([]lrdb.KafkaJournalBatchUpsertParams, 0)

	for _, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.MetricCompactionMessage)
		if !ok {
			continue
		}

		// Get the offset for this message's partition
		if offset, exists := kafkaCommitData.Offsets[accMsg.Metadata.Partition]; exists {
			batchParams = append(batchParams, lrdb.KafkaJournalBatchUpsertParams{
				ConsumerGroup:       kafkaCommitData.ConsumerGroup,
				Topic:               kafkaCommitData.Topic,
				Partition:           accMsg.Metadata.Partition,
				LastProcessedOffset: offset,
				OrganizationID:      msg.OrganizationID,
				InstanceNum:         msg.InstanceNum,
			})
		}
	}

	if len(batchParams) == 0 {
		return nil // No valid offset updates
	}

	// Sort for consistency to prevent deadlocks
	lrdb.SortKafkaOffsetsBatch(batchParams)

	// Execute batch upsert
	result := b.store.KafkaJournalBatchUpsert(ctx, batchParams)
	var offsetErr error
	result.Exec(func(i int, err error) {
		if err != nil && offsetErr == nil {
			offsetErr = err
		}
	})

	return offsetErr
}
