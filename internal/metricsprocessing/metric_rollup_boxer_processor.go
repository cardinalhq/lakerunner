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

	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// MetricRollupBoxerProcessor implements the Processor interface for boxing metric rollup bundles
type MetricRollupBoxerProcessor struct {
	kafkaProducer fly.Producer
	store         BoxerStore
}

// newMetricBoxerProcessor creates a new metric boxer processor instance
func newMetricBoxerProcessor(kafkaProducer fly.Producer, store BoxerStore) *MetricRollupBoxerProcessor {
	return &MetricRollupBoxerProcessor{
		kafkaProducer: kafkaProducer,
		store:         store,
	}
}

// Process implements the Processor interface and sends the bundle to the rollup topic
func (b *MetricRollupBoxerProcessor) Process(ctx context.Context, group *accumulationGroup[messages.RollupKey], kafkaCommitData *KafkaCommitData) error {
	ll := logctx.FromContext(ctx)

	// Create a MetricRollupBundle to send to the rollup topic
	bundle := &messages.MetricRollupBundle{
		Version:  1,
		Messages: make([]*messages.MetricRollupMessage, 0, len(group.Messages)),
		QueuedAt: time.Now(),
	}

	// Convert accumulated messages to MetricRollupMessage format
	for _, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.MetricRollupMessage)
		if !ok {
			continue // Skip non-MetricRollupMessage messages
		}
		bundle.Messages = append(bundle.Messages, msg)
	}

	ll.Info("Boxing rollup bundle for processing",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("dateint", int(group.Key.DateInt)),
		slog.Int("sourceFrequencyMs", int(group.Key.SourceFrequencyMs)),
		slog.Int("targetFrequencyMs", int(group.Key.TargetFrequencyMs)),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
		slog.Int64("truncatedTimebox", group.Key.TruncatedTimebox),
		slog.Int("messageCount", len(bundle.Messages)))

	// Marshal the bundle
	msgBytes, err := bundle.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal rollup bundle: %w", err)
	}

	bundleMessage := fly.Message{
		Value: msgBytes,
	}

	// Send to rollup topic
	rollupTopic := "lakerunner.segments.metrics.rollup"
	if err := b.kafkaProducer.Send(ctx, rollupTopic, bundleMessage); err != nil {
		return fmt.Errorf("failed to send rollup bundle to rollup topic: %w", err)
	}

	ll.Info("Successfully sent rollup bundle to rollup topic",
		slog.String("topic", rollupTopic),
		slog.Int("bundledMessages", len(bundle.Messages)))

	// Commit Kafka offsets in batch after successful message send
	if err := b.commitKafkaOffsets(ctx, group, kafkaCommitData); err != nil {
		return fmt.Errorf("failed to commit Kafka offsets: %w", err)
	}

	return nil
}

// GetTargetRecordCount returns the estimated record count for the target frequency
func (b *MetricRollupBoxerProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.RollupKey) int64 {
	return b.store.GetMetricEstimate(ctx, groupingKey.OrganizationID, groupingKey.TargetFrequencyMs)
}

// commitKafkaOffsets commits the Kafka offsets for all messages in the group using batch operation
func (b *MetricRollupBoxerProcessor) commitKafkaOffsets(ctx context.Context, group *accumulationGroup[messages.RollupKey], kafkaCommitData *KafkaCommitData) error {
	if kafkaCommitData == nil || len(kafkaCommitData.Offsets) == 0 {
		return nil // Nothing to commit
	}

	// Convert kafkaCommitData to batch parameters, organizing by org/instance
	batchParams := make([]lrdb.KafkaJournalBatchUpsertParams, 0)

	for _, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.MetricRollupMessage)
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
