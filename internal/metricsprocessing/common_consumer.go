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
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// CommonConsumerConfig holds configuration for a common consumer
type CommonConsumerConfig struct {
	ConsumerName  string
	Topic         string
	ConsumerGroup string
	FlushInterval time.Duration
	StaleAge      time.Duration
	MaxAge        time.Duration
}

// CommonConsumerStore defines the minimal interface required by common consumers
type CommonConsumerStore interface {
	KafkaGetLastProcessed(ctx context.Context, params lrdb.KafkaGetLastProcessedParams) (int64, error)
}

// MessageGatherer defines the interface for processing messages and idle groups
type MessageGatherer[M messages.CompactionMessage, K messages.CompactionKeyInterface] interface {
	processMessage(ctx context.Context, msg M, metadata *messageMetadata) error
	processIdleGroups(ctx context.Context, lastUpdatedAge, maxAge time.Duration) (int, error)
}

// CommonConsumer is a generic consumer for any message processing operations
type CommonConsumer[M messages.CompactionMessage, K messages.CompactionKeyInterface] struct {
	gatherer        MessageGatherer[M, K]
	consumer        fly.Consumer
	store           CommonConsumerStore
	idleCheckTicker *time.Ticker
	done            chan struct{}
	config          CommonConsumerConfig
}

// NewCommonConsumer creates a new generic common consumer
func NewCommonConsumer[M messages.CompactionMessage, K messages.CompactionKeyInterface](
	ctx context.Context,
	factory *fly.Factory,
	cfg *config.Config,
	consumerConfig CommonConsumerConfig,
	store CommonConsumerStore,
	processor processor[M, K],
) (*CommonConsumer[M, K], error) {
	ll := logctx.FromContext(ctx)

	// Create Kafka consumer
	consumer, err := factory.CreateConsumer(consumerConfig.Topic, consumerConfig.ConsumerGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	// Set up periodic flushing
	flushTicker := time.NewTicker(consumerConfig.FlushInterval)

	cc := &CommonConsumer[M, K]{
		consumer:        consumer,
		store:           store,
		idleCheckTicker: flushTicker,
		done:            make(chan struct{}),
		config:          consumerConfig,
	}

	// Create Gatherer using the consumer itself as offset callbacks
	cc.gatherer = newGatherer[M](consumerConfig.Topic, consumerConfig.ConsumerGroup, processor, cc)

	ll.Info("Created new generic common consumer",
		slog.String("consumerName", consumerConfig.ConsumerName),
		slog.String("topic", consumerConfig.Topic),
		slog.String("consumerGroup", consumerConfig.ConsumerGroup))

	return cc, nil
}

// Run starts the Kafka consumer and periodic flushing
func (c *CommonConsumer[M, K]) Run(ctx context.Context) error {
	ll := logctx.FromContext(ctx).With("consumer", c.config.ConsumerName)
	ll.Info("Starting generic common consumer")

	// Start periodic flushing goroutine
	go c.idleCheck(ctx)

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
			// Create a new instance of M for unmarshaling
			notification := new(M)
			if err := (*notification).Unmarshal(kafkaMsg.Value); err != nil {
				ll.Error("Failed to unmarshal compaction message",
					slog.Any("error", err),
					slog.Int("partition", kafkaMsg.Partition),
					slog.Int64("offset", kafkaMsg.Offset))
				continue // Skip malformed messages
			}

			// Create MessageMetadata from kafkaMsg
			metadata := &messageMetadata{
				Topic:         c.config.Topic,
				Partition:     int32(kafkaMsg.Partition),
				ConsumerGroup: c.config.ConsumerGroup,
				Offset:        kafkaMsg.Offset,
			}

			// Process the message through the gatherer
			if err := c.gatherer.processMessage(ctx, *notification, metadata); err != nil {
				ll.Error("Failed to process message",
					slog.Any("error", err),
					slog.Int("partition", kafkaMsg.Partition),
					slog.Int64("offset", kafkaMsg.Offset))
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

	ll.Info("Generic common consumer stopped")
	return nil
}

// GetLastProcessedOffset returns the last processed offset for this key, or -1 if never seen
func (c *CommonConsumer[M, K]) GetLastProcessedOffset(ctx context.Context, metadata *messageMetadata, groupingKey K) (int64, error) {
	orgUUID := groupingKey.GetOrgID()
	instNum := groupingKey.GetInstanceNum()

	offset, err := c.store.KafkaGetLastProcessed(ctx, lrdb.KafkaGetLastProcessedParams{
		Topic:          metadata.Topic,
		Partition:      metadata.Partition,
		ConsumerGroup:  metadata.ConsumerGroup,
		OrganizationID: orgUUID,
		InstanceNum:    instNum,
	})
	if err != nil {
		// Return -1 if no row found (never seen before), but propagate other errors
		if errors.Is(err, sql.ErrNoRows) {
			return -1, nil
		}
		return -1, fmt.Errorf("failed to get last processed offset: %w", err)
	}
	return offset, nil
}

// MarkOffsetsProcessed commits the consumer group offsets to Kafka
func (c *CommonConsumer[M, K]) MarkOffsetsProcessed(ctx context.Context, key K, offsets map[int32]int64) error {
	ll := logctx.FromContext(ctx)

	if len(offsets) == 0 {
		return nil
	}

	// Create ConsumedMessage objects for each partition/offset to commit
	commitMessages := make([]fly.ConsumedMessage, 0, len(offsets))

	for partition, offset := range offsets {
		commitMessages = append(commitMessages, fly.ConsumedMessage{
			Topic:     c.config.Topic,
			Partition: int(partition),
			Offset:    offset,
		})

		orgID := key.GetOrgID()
		instanceNum := key.GetInstanceNum()

		ll.Info("Committing Kafka consumer group offset",
			slog.String("consumerGroup", c.config.ConsumerGroup),
			slog.String("topic", c.config.Topic),
			slog.Int("partition", int(partition)),
			slog.Int64("offset", offset),
			slog.String("organizationID", orgID.String()),
			slog.Int("instanceNum", int(instanceNum)))
	}

	if err := c.consumer.CommitMessages(ctx, commitMessages...); err != nil {
		errOrgID := key.GetOrgID()
		errInstanceNum := key.GetInstanceNum()

		ll.Error("Failed to commit Kafka consumer group offsets",
			slog.Any("error", err),
			slog.String("organizationID", errOrgID.String()),
			slog.Int("instanceNum", int(errInstanceNum)))
		return fmt.Errorf("failed to commit consumer group offsets: %w", err)
	}

	ll.Debug("Successfully committed Kafka consumer group offsets",
		slog.Int("offsetCount", len(offsets)))

	return nil
}

// Close stops the consumer and cleans up resources
func (c *CommonConsumer[M, K]) Close() error {
	close(c.done)
	c.idleCheckTicker.Stop()

	if c.consumer != nil {
		return c.consumer.Close()
	}
	return nil
}

// idleCheck runs at the configured interval and flushes idle groups
func (c *CommonConsumer[M, K]) idleCheck(ctx context.Context) {
	ll := logctx.FromContext(ctx)

	for {
		select {
		case <-ctx.Done():
			ll.Info("Periodic flush stopping due to context cancellation")
			return
		case <-c.done:
			ll.Info("Periodic flush stopping due to consumer shutdown")
			return
		case <-c.idleCheckTicker.C:
			ll.Debug("Running periodic flush of idle groups")
			if _, err := c.gatherer.processIdleGroups(ctx, c.config.StaleAge, c.config.MaxAge); err != nil {
				ll.Error("Failed to flush idle groups", slog.Any("error", err))
			}
		}
	}
}
