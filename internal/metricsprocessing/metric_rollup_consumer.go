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
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// Rollup accumulation times to avoid import cycle with config package
var rollupAccumulationTimes = map[int32]time.Duration{
	10_000:    90 * time.Second,  // 10s->60s: wait max 90 seconds
	60_000:    200 * time.Second, // 60s->300s: wait max 200 seconds
	300_000:   5 * time.Minute,   // 5m->20m: wait max 5 minutes
	1_200_000: 5 * time.Minute,   // 20m->1h: wait max 5 minutes
}

// MetricRollupConsumer handles metric rollup Kafka messages using accumulation-based approach
type MetricRollupConsumer struct {
	gatherer      *Gatherer[*messages.MetricRollupMessage, messages.RollupKey]
	consumer      fly.Consumer
	store         RollupStore
	flushTicker   *time.Ticker
	done          chan struct{}
	consumerName  string
	topic         string
	consumerGroup string
}

// NewMetricRollupConsumer creates a new metric rollup consumer
func NewMetricRollupConsumer(
	ctx context.Context,
	factory *fly.Factory,
	store RollupStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
) (*MetricRollupConsumer, error) {
	ll := logctx.FromContext(ctx)

	// Create MetricRollupProcessor
	processor := NewMetricRollupProcessor(store, storageProvider, cmgr)

	// Create Gatherer - using hardcoded consumer group and topic for rollups
	consumerGroup := "lakerunner.rollup.metrics"
	topic := "lakerunner.segments.metrics.rollup"

	// Create Kafka consumer
	consumerName := "lakerunner-rollup-accumulator"
	consumer, err := factory.CreateConsumer(topic, consumerGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	// Get the accumulation time based on rollup frequencies
	// We'll use the longest accumulation time as a default
	maxAccumulationTime := time.Duration(0)
	for _, accTime := range rollupAccumulationTimes {
		maxAccumulationTime = max(maxAccumulationTime, accTime)
	}
	if maxAccumulationTime == 0 {
		maxAccumulationTime = 5 * time.Minute // Fallback
	}

	// Set up periodic flushing - flush more frequently for rollups since they have tighter time windows
	flushInterval := max(maxAccumulationTime/2, 30*time.Second)
	flushTicker := time.NewTicker(flushInterval)

	mrc := &MetricRollupConsumer{
		consumer:      consumer,
		store:         store,
		flushTicker:   flushTicker,
		done:          make(chan struct{}),
		consumerName:  consumerName,
		topic:         topic,
		consumerGroup: consumerGroup,
	}

	// Create Gatherer using the consumer itself as offset callbacks
	mrc.gatherer = NewGatherer[*messages.MetricRollupMessage](topic, consumerGroup, processor, mrc)

	ll.Info("Created new Kafka rollup accumulation consumer",
		slog.String("consumerName", consumerName),
		slog.String("topic", topic),
		slog.String("consumerGroup", consumerGroup),
		slog.Duration("flushInterval", flushInterval))

	return mrc, nil
}

// Run starts the Kafka consumer and periodic flushing
func (c *MetricRollupConsumer) Run(ctx context.Context) error {
	ll := logctx.FromContext(ctx).With("consumer", c.consumerName)
	ll.Info("Starting Kafka rollup accumulation consumer")

	// Start periodic flushing goroutine
	go c.periodicFlush(ctx)

	// Start the Kafka consumer
	err := c.consumer.Consume(ctx, func(ctx context.Context, kafkaMessages []fly.ConsumedMessage) error {
		ll := logctx.FromContext(ctx).With(slog.String("batchID", idgen.GenerateShortBase32ID()))
		ctx = logctx.WithLogger(ctx, ll)

		if len(kafkaMessages) == 0 {
			return nil
		}

		ll.Debug("Processing Kafka rollup message batch",
			slog.Int("messageCount", len(kafkaMessages)))

		// Process each message
		for _, kafkaMsg := range kafkaMessages {
			var notification messages.MetricRollupMessage
			if err := notification.Unmarshal(kafkaMsg.Value); err != nil {
				ll.Error("Failed to unmarshal metric rollup message",
					slog.Any("error", err),
					slog.Int("partition", kafkaMsg.Partition),
					slog.Int64("offset", kafkaMsg.Offset))
				continue // Skip malformed messages
			}

			// Validate message version
			if notification.Version != 1 {
				ll.Warn("Unsupported message version, skipping",
					slog.Int("version", int(notification.Version)),
					slog.Int64("segmentID", notification.SegmentID))
				continue
			}

			// Validate that this is a valid rollup transition
			if !isRollupSourceFrequency(notification.SourceFrequencyMs) {
				ll.Warn("Invalid source frequency for rollup, skipping",
					slog.Int("sourceFrequencyMs", int(notification.SourceFrequencyMs)),
					slog.Int64("segmentID", notification.SegmentID))
				continue
			}

			expectedTarget, exists := getTargetRollupFrequency(notification.SourceFrequencyMs)
			if !exists || expectedTarget != notification.TargetFrequencyMs {
				ll.Warn("Invalid rollup frequency transition, skipping",
					slog.Int("sourceFrequencyMs", int(notification.SourceFrequencyMs)),
					slog.Int("targetFrequencyMs", int(notification.TargetFrequencyMs)),
					slog.Int("expectedTarget", int(expectedTarget)),
					slog.Int64("segmentID", notification.SegmentID))
				continue
			}

			// Create MessageMetadata from kafkaMsg
			metadata := &MessageMetadata{
				Topic:         c.topic,
				Partition:     int32(kafkaMsg.Partition),
				ConsumerGroup: c.consumerGroup,
				Offset:        kafkaMsg.Offset,
			}

			// Process the message through the gatherer
			if err := c.gatherer.ProcessMessage(ctx, &notification, metadata); err != nil {
				ll.Error("Failed to process rollup message",
					slog.Any("error", err),
					slog.String("organizationID", notification.OrganizationID.String()),
					slog.Int("dateint", int(notification.DateInt)),
					slog.Int("sourceFrequencyMs", int(notification.SourceFrequencyMs)),
					slog.Int("targetFrequencyMs", int(notification.TargetFrequencyMs)),
					slog.Int("instanceNum", int(notification.InstanceNum)),
					slog.Int64("segmentID", notification.SegmentID))
				return fmt.Errorf("failed to process rollup message: %w", err)
			}
		}

		// Commit the messages after successful processing
		return c.consumer.CommitMessages(ctx, kafkaMessages...)
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		ll.Error("Kafka rollup consumer stopped with error", slog.Any("error", err))
		return fmt.Errorf("Kafka rollup consumer error: %w", err)
	}

	ll.Info("Kafka rollup accumulation consumer stopped")
	return nil
}

// OffsetCallbacks implementation - MetricRollupConsumer implements OffsetCallbacks interface

// GetLastProcessedOffset returns the last processed offset for this key, or -1 if never seen
func (c *MetricRollupConsumer) GetLastProcessedOffset(ctx context.Context, metadata *MessageMetadata, groupingKey messages.RollupKey) (int64, error) {
	offset, err := c.store.KafkaJournalGetLastProcessedWithOrgInstance(ctx, lrdb.KafkaJournalGetLastProcessedWithOrgInstanceParams{
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

// MarkOffsetsProcessed commits the consumer group offsets to Kafka
func (c *MetricRollupConsumer) MarkOffsetsProcessed(ctx context.Context, key messages.RollupKey, offsets map[int32]int64) error {
	ll := logctx.FromContext(ctx)

	if len(offsets) == 0 {
		return nil
	}

	// Create ConsumedMessage objects for each partition/offset to commit
	commitMessages := make([]fly.ConsumedMessage, 0, len(offsets))

	for partition, offset := range offsets {
		commitMessages = append(commitMessages, fly.ConsumedMessage{
			Topic:     c.topic,
			Partition: int(partition),
			Offset:    offset,
			// We don't need the actual message data for commits, just the offset metadata
		})

		ll.Info("Committing Kafka rollup consumer group offset",
			slog.String("consumerGroup", c.consumerGroup),
			slog.String("topic", c.topic),
			slog.Int("partition", int(partition)),
			slog.Int64("offset", offset),
			slog.String("organizationID", key.OrganizationID.String()),
			slog.Int("instanceNum", int(key.InstanceNum)),
			slog.Int("sourceFrequencyMs", int(key.SourceFrequencyMs)),
			slog.Int("targetFrequencyMs", int(key.TargetFrequencyMs)))
	}

	// Actually commit to Kafka consumer group
	if err := c.consumer.CommitMessages(ctx, commitMessages...); err != nil {
		ll.Error("Failed to commit Kafka rollup consumer group offsets",
			slog.Any("error", err),
			slog.String("organizationID", key.OrganizationID.String()),
			slog.Int("instanceNum", int(key.InstanceNum)))
		return fmt.Errorf("failed to commit rollup consumer group offsets: %w", err)
	}

	ll.Debug("Successfully committed Kafka rollup consumer group offsets",
		slog.Int("offsetCount", len(offsets)),
		slog.String("organizationID", key.OrganizationID.String()),
		slog.Int("instanceNum", int(key.InstanceNum)))

	return nil
}

// Close stops the consumer and cleans up resources
func (c *MetricRollupConsumer) Close() error {
	close(c.done)
	c.flushTicker.Stop()

	if c.consumer != nil {
		return c.consumer.Close()
	}
	return nil
}

// periodicFlush runs periodically and flushes stale groups based on their accumulation time
func (c *MetricRollupConsumer) periodicFlush(ctx context.Context) {
	ll := logctx.FromContext(ctx)

	for {
		select {
		case <-ctx.Done():
			ll.Info("Rollup periodic flush stopping due to context cancellation")
			return
		case <-c.done:
			ll.Info("Rollup periodic flush stopping due to consumer shutdown")
			return
		case <-c.flushTicker.C:
			ll.Debug("Running periodic flush of stale rollup groups")
			// Use a more aggressive flush time for rollups since they have tighter time windows
			// We'll flush groups that are older than half their target accumulation time
			flushAge := 2 * time.Minute // Default aggressive flush
			if err := c.gatherer.FlushStaleGroups(ctx, flushAge); err != nil {
				ll.Error("Failed to flush stale rollup groups", slog.Any("error", err))
			}
		}
	}
}
