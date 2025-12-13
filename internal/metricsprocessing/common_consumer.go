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
	"reflect"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
)

var boxerTracer trace.Tracer = otel.Tracer("github.com/cardinalhq/lakerunner/metricsprocessing")

// CommonConsumerConfig holds configuration for a common consumer
type CommonConsumerConfig struct {
	ConsumerName  string
	Topic         string
	ConsumerGroup string
	FlushInterval time.Duration
	StaleAge      time.Duration
	MaxAge        time.Duration
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
	store           offsetStore
	idleCheckTicker *time.Ticker
	done            chan struct{}
	config          CommonConsumerConfig
}

// FlyConsumerFactory defines the interface for creating Kafka consumers (for testability)
type FlyConsumerFactory interface {
	CreateConsumer(topic, consumerGroup string) (fly.Consumer, error)
}

// NewCommonConsumer creates a new generic common consumer
func NewCommonConsumer[M messages.CompactionMessage, K messages.CompactionKeyInterface](
	ctx context.Context,
	factory FlyConsumerFactory,
	cfg *config.Config,
	consumerConfig CommonConsumerConfig,
	store offsetStore,
	processor processor[M, K],
) (*CommonConsumer[M, K], error) {
	// Create Kafka consumer
	consumer, err := factory.CreateConsumer(consumerConfig.Topic, consumerConfig.ConsumerGroup)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	return NewCommonConsumerWithComponents[M](
		ctx, consumer, consumerConfig, store, processor,
	), nil
}

// NewCommonConsumerWithComponents creates a consumer with provided components (for testability)
func NewCommonConsumerWithComponents[M messages.CompactionMessage, K messages.CompactionKeyInterface](
	ctx context.Context,
	consumer fly.Consumer,
	consumerConfig CommonConsumerConfig,
	store offsetStore,
	processor processor[M, K],
) *CommonConsumer[M, K] {
	// Set up periodic flushing
	flushTicker := time.NewTicker(consumerConfig.FlushInterval)

	cc := &CommonConsumer[M, K]{
		consumer:        consumer,
		store:           store,
		idleCheckTicker: flushTicker,
		done:            make(chan struct{}),
		config:          consumerConfig,
	}

	// Create gatherer with sync mode deduplication
	cc.gatherer = newGatherer[M](consumerConfig.Topic, consumerConfig.ConsumerGroup, processor, store)

	return cc
}

// Run starts the Kafka consumer and periodic flushing
func (c *CommonConsumer[M, K]) Run(ctx context.Context) error {
	ll := logctx.FromContext(ctx).With("consumer", c.config.ConsumerName)
	ll.Info("Starting generic common consumer")

	// Start periodic flushing goroutine
	go c.idleCheck(ctx)

	// Start the Kafka consumer
	err := c.consumer.Consume(ctx, c.buildMessageHandler())

	if err != nil && !errors.Is(err, context.Canceled) {
		ll.Error("Kafka consumer stopped with error", slog.Any("error", err))
		return fmt.Errorf("Kafka consumer error: %w", err)
	}

	ll.Info("Generic common consumer stopped")
	return nil
}

// buildMessageHandler creates the message processing function (extracted for testability)
func (c *CommonConsumer[M, K]) buildMessageHandler() func(context.Context, []fly.ConsumedMessage) error {
	return func(ctx context.Context, kafkaMessages []fly.ConsumedMessage) error {
		return c.processKafkaMessageBatch(ctx, kafkaMessages)
	}
}

// processKafkaMessageBatch handles a batch of Kafka messages (extracted for testability)
func (c *CommonConsumer[M, K]) processKafkaMessageBatch(ctx context.Context, kafkaMessages []fly.ConsumedMessage) error {
	ll := logctx.FromContext(ctx).With(slog.String("batchID", idgen.GenerateShortBase32ID()))
	ctx = logctx.WithLogger(ctx, ll)

	if len(kafkaMessages) == 0 {
		return nil
	}

	ctx, span := boxerTracer.Start(ctx, "boxer.consume.batch", trace.WithAttributes(
		attribute.Int("message_count", len(kafkaMessages)),
		attribute.String("topic", c.config.Topic),
		attribute.String("consumer_group", c.config.ConsumerGroup),
	))
	defer span.End()

	ll.Debug("Processing Kafka message batch",
		slog.Int("messageCount", len(kafkaMessages)))

	// Process each message
	for _, kafkaMsg := range kafkaMessages {
		if err := c.processKafkaMessage(ctx, kafkaMsg, ll); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to process message")
			return err
		}
	}

	return nil
}

// processKafkaMessage handles a single Kafka message (extracted for testability)
func (c *CommonConsumer[M, K]) processKafkaMessage(ctx context.Context, kafkaMsg fly.ConsumedMessage, ll *slog.Logger) error {
	// Create a new instance of M for unmarshaling
	// When M is *SomeMessage, we need to create SomeMessage{}, not **SomeMessage
	var notification M

	// Use reflection to create the underlying struct when M is a pointer type
	// Use (*M)(nil) to get the concrete type even when M is a pointer type
	rt := reflect.TypeOf((*M)(nil)).Elem()
	if rt.Kind() == reflect.Ptr {
		// M is a pointer type, create the underlying struct
		notification = reflect.New(rt.Elem()).Interface().(M)
	} else {
		// M is a value type, create it directly
		notification = reflect.New(rt).Interface().(M)
	}

	if err := notification.Unmarshal(kafkaMsg.Value); err != nil {
		ll.Error("Failed to unmarshal compaction message",
			slog.Any("error", err),
			slog.Int("partition", kafkaMsg.Partition),
			slog.Int64("offset", kafkaMsg.Offset))
		return nil // Skip malformed messages
	}

	// Create MessageMetadata from kafkaMsg
	metadata := &messageMetadata{
		Topic:         c.config.Topic,
		Partition:     int32(kafkaMsg.Partition),
		ConsumerGroup: c.config.ConsumerGroup,
		Offset:        kafkaMsg.Offset,
	}

	// Process the message through the gatherer
	if err := c.gatherer.processMessage(ctx, notification, metadata); err != nil {
		ll.Error("Failed to process message",
			slog.Any("error", err),
			slog.Int("partition", kafkaMsg.Partition),
			slog.Int64("offset", kafkaMsg.Offset))
		return fmt.Errorf("failed to process message: %w", err)
	}

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

			ctx, flushSpan := boxerTracer.Start(ctx, "boxer.idle_flush", trace.WithAttributes(
				attribute.String("topic", c.config.Topic),
				attribute.String("consumer_group", c.config.ConsumerGroup),
			))

			flushed, err := c.gatherer.processIdleGroups(ctx, c.config.StaleAge, c.config.MaxAge)
			if err != nil {
				flushSpan.RecordError(err)
				flushSpan.SetStatus(codes.Error, "failed to flush idle groups")
				ll.Error("Failed to flush idle groups", slog.Any("error", err))
			} else {
				flushSpan.SetAttributes(attribute.Int("groups_flushed", flushed))
			}
			flushSpan.End()
		}
	}
}
