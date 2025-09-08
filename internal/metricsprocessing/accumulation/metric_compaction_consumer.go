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
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

// MetricCompactionConsumer handles metric compaction Kafka messages using accumulation-based approach
type MetricCompactionConsumer struct {
	gatherer      *Gatherer[*messages.MetricCompactionMessage, messages.CompactionKey]
	consumer      fly.Consumer
	flushTicker   *time.Ticker
	done          chan struct{}
	consumerName  string
	topic         string
	consumerGroup string
}

// NewMetricCompactionConsumer creates a new metric compaction consumer
func NewMetricCompactionConsumer(
	ctx context.Context,
	factory *fly.Factory,
	cfg *config.Config,
	store CompactionStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
) (*MetricCompactionConsumer, error) {
	ll := logctx.FromContext(ctx)

	// Create MetricCompactor
	compactor := NewMetricCompactor(store, storageProvider, cmgr)

	// Create Gatherer - using hardcoded consumer group and topic
	consumerGroup := "lakerunner.compact.metrics"
	topic := "lakerunner.segments.metrics.compact"

	// Create Kafka consumer
	consumerName := "lakerunner-compaction-accumulator"
	consumer, err := factory.CreateConsumer(topic, consumerGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	// Set up periodic flushing (every minute, flush groups older than 5 minutes)
	flushTicker := time.NewTicker(1 * time.Minute)

	mcc := &MetricCompactionConsumer{
		consumer:      consumer,
		flushTicker:   flushTicker,
		done:          make(chan struct{}),
		consumerName:  consumerName,
		topic:         topic,
		consumerGroup: consumerGroup,
	}

	// Create reusable offset callbacks for org/instance offset management
	offsetCallbacks := NewOrgInstanceOffsetCallbacks(store)

	// Create Gatherer using the reusable offset callbacks
	mcc.gatherer = NewGatherer[*messages.MetricCompactionMessage, messages.CompactionKey](topic, consumerGroup, compactor, offsetCallbacks)

	ll.Info("Created new Kafka accumulation consumer",
		slog.String("consumerName", consumerName),
		slog.String("topic", topic),
		slog.String("consumerGroup", consumerGroup))

	return mcc, nil
}

// Run starts the Kafka consumer and periodic flushing
func (c *MetricCompactionConsumer) Run(ctx context.Context) error {
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
			var notification messages.MetricCompactionMessage
			if err := notification.Unmarshal(kafkaMsg.Value); err != nil {
				ll.Error("Failed to unmarshal metric compaction message",
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
					slog.Int("dateint", int(notification.DateInt)),
					slog.Int("frequencyMs", int(notification.FrequencyMs)),
					slog.Int("instanceNum", int(notification.InstanceNum)),
					slog.Int64("segmentID", notification.SegmentID))
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

	ll.Info("Kafka accumulation consumer stopped")
	return nil
}

// Close stops the consumer and cleans up resources
func (c *MetricCompactionConsumer) Close() error {
	close(c.done)
	c.flushTicker.Stop()

	if c.consumer != nil {
		return c.consumer.Close()
	}
	return nil
}

// periodicFlush runs every minute and flushes stale groups (older than 5 minutes)
func (c *MetricCompactionConsumer) periodicFlush(ctx context.Context) {
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
			if err := c.gatherer.FlushStaleGroups(ctx, 5*time.Minute); err != nil {
				ll.Error("Failed to flush stale groups", slog.Any("error", err))
			}
		}
	}
}

