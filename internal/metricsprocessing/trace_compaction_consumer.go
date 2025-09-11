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

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// TraceCompactionConsumer handles trace compaction Kafka messages using accumulation-based approach
type TraceCompactionConsumer struct {
	gatherer      *gatherer[*messages.TraceCompactionMessage, messages.TraceCompactionKey]
	consumer      fly.Consumer
	store         TraceCompactionStore
	flushTicker   *time.Ticker
	done          chan struct{}
	consumerName  string
	topic         string
	consumerGroup string
}

// NewTraceCompactionConsumer creates a new trace compaction consumer
func NewTraceCompactionConsumer(
	ctx context.Context,
	factory *fly.Factory,
	cfg *config.Config,
	store TraceCompactionStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
) (*TraceCompactionConsumer, error) {
	ll := logctx.FromContext(ctx)

	// Create TraceCompactor
	compactor := newTraceCompactor(store, storageProvider, cmgr, cfg)

	// Create Gatherer - using hardcoded consumer group and topic
	consumerGroup := "lakerunner.compact.traces"
	topic := "lakerunner.segments.traces.compact"

	// Create Kafka consumer
	consumerName := "lakerunner-trace-compaction"
	consumer, err := factory.CreateConsumer(topic, consumerGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	// Set up periodic flushing (every minute, flush groups older than 5 minutes)
	flushTicker := time.NewTicker(1 * time.Minute)

	tcc := &TraceCompactionConsumer{
		consumer:      consumer,
		store:         store,
		flushTicker:   flushTicker,
		done:          make(chan struct{}),
		consumerName:  consumerName,
		topic:         topic,
		consumerGroup: consumerGroup,
	}

	// Create Gatherer using the consumer itself as offset callbacks
	tcc.gatherer = newGatherer[*messages.TraceCompactionMessage](topic, consumerGroup, compactor, tcc)

	ll.Info("Created new Kafka accumulation consumer",
		slog.String("consumerName", consumerName),
		slog.String("topic", topic),
		slog.String("consumerGroup", consumerGroup))

	return tcc, nil
}

// Run starts the Kafka consumer and periodic flushing
func (c *TraceCompactionConsumer) Run(ctx context.Context) error {
	ll := logctx.FromContext(ctx).With("consumer", c.consumerName)
	ll.Info("Starting Kafka accumulation consumer")

	// Start periodic flushing goroutine
	go c.periodicFlush(ctx)

	// Start the Kafka consumer
	err := c.consumer.Consume(ctx, func(ctx context.Context, kafkaMessages []fly.ConsumedMessage) error {
		ll := logctx.FromContext(ctx).With(slog.String("batchID", idgen.GenerateShortBase32ID()))
		ctx = logctx.WithLogger(ctx, ll)

		if len(kafkaMessages) == 0 {
			return nil
		}

		ll.Debug("Processing Kafka message batch",
			slog.Int("messageCount", len(kafkaMessages)))

		// Process each message
		for _, kafkaMsg := range kafkaMessages {
			var notification messages.TraceCompactionMessage
			if err := notification.Unmarshal(kafkaMsg.Value); err != nil {
				ll.Error("Failed to unmarshal trace compaction message",
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

			// Create MessageMetadata from kafkaMsg
			metadata := &messageMetadata{
				Topic:         c.topic,
				Partition:     int32(kafkaMsg.Partition),
				ConsumerGroup: c.consumerGroup,
				Offset:        kafkaMsg.Offset,
			}

			// Process the message through the gatherer
			if err := c.gatherer.processMessage(ctx, &notification, metadata); err != nil {
				ll.Error("Failed to process message",
					slog.Any("error", err),
					slog.String("organizationID", notification.OrganizationID.String()),
					slog.Int("dateint", int(notification.DateInt)),
					slog.Int("instanceNum", int(notification.InstanceNum)),
					slog.Int64("segmentID", notification.SegmentID))
				return fmt.Errorf("failed to process message: %w", err)
			}
		}

		return nil
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		ll.Error("Kafka consumer stopped with error", slog.Any("error", err))
		return fmt.Errorf("Kafka consumer error: %w", err)
	}

	ll.Info("Kafka accumulation consumer stopped")
	return nil
}

// OffsetCallbacks implementation - TraceCompactionConsumer implements OffsetCallbacks interface

// GetLastProcessedOffset returns the last processed offset for this key, or -1 if never seen
func (c *TraceCompactionConsumer) GetLastProcessedOffset(ctx context.Context, metadata *messageMetadata, groupingKey messages.TraceCompactionKey) (int64, error) {
	offset, err := c.store.KafkaGetLastProcessed(ctx, lrdb.KafkaGetLastProcessedParams{
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
func (c *TraceCompactionConsumer) MarkOffsetsProcessed(ctx context.Context, key messages.TraceCompactionKey, offsets map[int32]int64) error {
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

		ll.Info("Committing Kafka consumer group offset",
			slog.String("consumerGroup", c.consumerGroup),
			slog.String("topic", c.topic),
			slog.Int("partition", int(partition)),
			slog.Int64("offset", offset),
			slog.String("organizationID", key.OrganizationID.String()),
			slog.Int("instanceNum", int(key.InstanceNum)))
	}

	// Actually commit to Kafka consumer group
	if err := c.consumer.CommitMessages(ctx, commitMessages...); err != nil {
		ll.Error("Failed to commit Kafka consumer group offsets",
			slog.Any("error", err),
			slog.String("organizationID", key.OrganizationID.String()),
			slog.Int("instanceNum", int(key.InstanceNum)))
		return fmt.Errorf("failed to commit consumer group offsets: %w", err)
	}

	ll.Debug("Successfully committed Kafka consumer group offsets",
		slog.Int("offsetCount", len(offsets)),
		slog.String("organizationID", key.OrganizationID.String()),
		slog.Int("instanceNum", int(key.InstanceNum)))

	return nil
}

// Close stops the consumer and cleans up resources
func (c *TraceCompactionConsumer) Close() error {
	close(c.done)
	c.flushTicker.Stop()

	if c.consumer != nil {
		return c.consumer.Close()
	}
	return nil
}

// periodicFlush runs every minute and flushes stale groups (older than 5 minutes)
func (c *TraceCompactionConsumer) periodicFlush(ctx context.Context) {
	ll := logctx.FromContext(ctx)

	for {
		select {
		case <-ctx.Done():
			ll.Info("Periodic flush stopping due to context cancellation")
			return
		case <-c.done:
			ll.Info("Periodic flush stopping due to consumer shutdown")
			return
		case <-c.flushTicker.C:
			ll.Debug("Running periodic flush of stale groups")
			if _, err := c.gatherer.flushStaleGroups(ctx, 1*time.Minute, 1*time.Minute); err != nil {
				ll.Error("Failed to flush stale groups", slog.Any("error", err))
			}
		}
	}
}
