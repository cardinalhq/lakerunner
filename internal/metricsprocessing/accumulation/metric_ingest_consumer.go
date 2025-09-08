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
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// MetricIngestConsumer handles object store notification messages for metric ingestion
type MetricIngestConsumer struct {
	gatherer      *Gatherer[*messages.ObjStoreNotificationMessage, messages.IngestKey]
	consumer      fly.Consumer
	store         IngestStore
	flushTicker   *time.Ticker
	done          chan struct{}
	consumerName  string
	topic         string
	consumerGroup string
}

// NewMetricIngestConsumer creates a new metric ingest consumer
func NewMetricIngestConsumer(
	ctx context.Context,
	factory *fly.Factory,
	cfg *config.Config,
	store IngestStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
) (*MetricIngestConsumer, error) {
	ll := logctx.FromContext(ctx)

	// Create MetricIngestProcessor
	processor := NewMetricIngestProcessor(store, storageProvider, cmgr)

	// Create Gatherer - using hardcoded consumer group and topic
	consumerGroup := "lakerunner.ingest.metrics"
	topic := "lakerunner.objstore.notifications"

	// Create Kafka consumer
	consumerName := "lakerunner-ingest-accumulator"
	consumer, err := factory.CreateConsumer(topic, consumerGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	// Set up periodic flushing (every 30 seconds, flush groups older than 2 minutes)
	flushTicker := time.NewTicker(30 * time.Second)

	mic := &MetricIngestConsumer{
		consumer:      consumer,
		store:         store,
		flushTicker:   flushTicker,
		done:          make(chan struct{}),
		consumerName:  consumerName,
		topic:         topic,
		consumerGroup: consumerGroup,
	}

	// Create Gatherer using the consumer itself as offset callbacks
	mic.gatherer = NewGatherer[*messages.ObjStoreNotificationMessage, messages.IngestKey](topic, consumerGroup, processor, mic)

	ll.Info("Created new Kafka ingest consumer",
		slog.String("consumerName", consumerName),
		slog.String("topic", topic),
		slog.String("consumerGroup", consumerGroup))

	return mic, nil
}

// Run starts the Kafka consumer and periodic flushing
func (c *MetricIngestConsumer) Run(ctx context.Context) error {
	ll := logctx.FromContext(ctx).With("consumer", c.consumerName)
	ll.Info("Starting Kafka ingest consumer")

	// Start periodic flushing goroutine
	go c.periodicFlush(ctx)

	// Start the Kafka consumer
	err := c.consumer.Consume(ctx, func(ctx context.Context, kafkaMessages []fly.ConsumedMessage) error {
		ll := logctx.FromContext(ctx).With(slog.String("batchID", "ingest-"+fmt.Sprintf("%d", time.Now().UnixNano())))
		ctx = logctx.WithLogger(ctx, ll)

		if len(kafkaMessages) == 0 {
			return nil
		}

		ll.Debug("Processing Kafka message batch",
			slog.Int("messageCount", len(kafkaMessages)))

		// Process each message
		for _, kafkaMsg := range kafkaMessages {
			var notification messages.ObjStoreNotificationMessage
			if err := notification.Unmarshal(kafkaMsg.Value); err != nil {
				ll.Error("Failed to unmarshal object store notification message",
					slog.Any("error", err),
					slog.Int("partition", kafkaMsg.Partition),
					slog.Int64("offset", kafkaMsg.Offset))
				continue // Skip malformed messages
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
				ll.Error("Failed to process message",
					slog.Any("error", err),
					slog.String("organizationID", notification.OrganizationID.String()),
					slog.Int("instanceNum", int(notification.InstanceNum)),
					slog.String("objectID", notification.ObjectID),
					slog.Int64("fileSize", notification.FileSize))
				return fmt.Errorf("failed to process message: %w", err)
			}
		}

		// Commit the messages after successful processing
		return c.consumer.CommitMessages(ctx, kafkaMessages...)
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		ll.Error("Kafka consumer stopped with error", slog.Any("error", err))
		return fmt.Errorf("Kafka consumer error: %w", err)
	}

	ll.Info("Kafka ingest consumer stopped")
	return nil
}

// OffsetCallbacks implementation - MetricIngestConsumer implements OffsetCallbacks interface

// GetLastProcessedOffset returns the last processed offset for this key, or -1 if never seen
func (c *MetricIngestConsumer) GetLastProcessedOffset(ctx context.Context, metadata *MessageMetadata, groupingKey messages.IngestKey) (int64, error) {
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
func (c *MetricIngestConsumer) MarkOffsetsProcessed(ctx context.Context, key messages.IngestKey, offsets map[int32]int64) error {
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
func (c *MetricIngestConsumer) Close() error {
	close(c.done)
	c.flushTicker.Stop()

	if c.consumer != nil {
		return c.consumer.Close()
	}
	return nil
}

// periodicFlush runs every 30 seconds and flushes stale groups (older than 2 minutes)
func (c *MetricIngestConsumer) periodicFlush(ctx context.Context) {
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
			if err := c.gatherer.FlushStaleGroups(ctx, 2*time.Minute); err != nil {
				ll.Error("Failed to flush stale groups", slog.Any("error", err))
			}
		}
	}
}
