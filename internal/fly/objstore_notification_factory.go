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

package fly

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/internal/fly/messages"
)

// ObjStoreNotificationProducer manages Kafka producer for object storage notifications
type ObjStoreNotificationProducer struct {
	producer Producer
	logger   *slog.Logger
	config   *Config
}

// NewObjStoreNotificationProducer creates a new object storage notification producer
func NewObjStoreNotificationProducer(factory *Factory, logger *slog.Logger) (*ObjStoreNotificationProducer, error) {
	if !factory.IsEnabled() {
		return nil, fmt.Errorf("Kafka is not enabled")
	}

	producer, err := factory.CreateProducer()
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	return &ObjStoreNotificationProducer{
		producer: producer,
		logger:   logger.With("component", "objstore_notification_producer"),
		config:   factory.GetConfig(),
	}, nil
}

// SendNotification sends a single object storage notification to the appropriate Kafka topic
func (p *ObjStoreNotificationProducer) SendNotification(ctx context.Context, signal string, notification *messages.ObjStoreNotificationMessage) error {
	// Set queued timestamp if not set
	if notification.QueuedAt.IsZero() {
		notification.QueuedAt = time.Now()
	}

	// Marshal the message
	data, err := notification.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal notification: %w", err)
	}

	// Create Kafka message with random partitioning (empty key)
	msg := &Message{
		Key:   nil, // Random partitioning as requested
		Value: data,
		Headers: map[string]string{
			"organization_id": notification.OrganizationID.String(),
			"timestamp":       notification.QueuedAt.Format(time.RFC3339),
		},
	}

	// Determine topic based on signal
	topic := fmt.Sprintf("lakerunner.objstore.ingest.%s", signal)
	if err := p.producer.Send(ctx, topic, *msg); err != nil {
		return fmt.Errorf("failed to send notification to Kafka topic %s: %w", topic, err)
	}

	p.logger.Debug("Sent object storage notification to Kafka",
		slog.String("topic", topic),
		slog.String("signal", signal),
		slog.String("organization_id", notification.OrganizationID.String()),
		slog.String("bucket", notification.Bucket),
		slog.String("object_id", notification.ObjectID))

	return nil
}

// SendBatch sends multiple object storage notifications as a batch
func (p *ObjStoreNotificationProducer) SendBatch(ctx context.Context, signal string, notifications []messages.ObjStoreNotificationMessage) error {
	// Determine topic based on signal
	topic := fmt.Sprintf("lakerunner.objstore.ingest.%s", signal)
	messages := make([]Message, 0, len(notifications))

	for _, notification := range notifications {
		// Set defaults
		if notification.QueuedAt.IsZero() {
			notification.QueuedAt = time.Now()
		}

		// Marshal the message
		data, err := notification.Marshal()
		if err != nil {
			p.logger.Error("Failed to marshal notification",
				slog.Any("error", err),
				slog.String("organization_id", notification.OrganizationID.String()))
			continue
		}

		// Create Kafka message
		msg := Message{
			Key:   nil, // Random partitioning
			Value: data,
			Headers: map[string]string{
				"organization_id": notification.OrganizationID.String(),
				"timestamp":       notification.QueuedAt.Format(time.RFC3339),
			},
		}

		messages = append(messages, msg)
	}

	if len(messages) == 0 {
		return fmt.Errorf("no valid messages to send")
	}

	// Send all messages to the topic
	if err := p.producer.BatchSend(ctx, topic, messages); err != nil {
		return fmt.Errorf("failed to send batch to Kafka topic %s: %w", topic, err)
	}
	p.logger.Debug("Sent batch of object storage notifications to Kafka",
		slog.String("topic", topic),
		slog.String("signal", signal),
		slog.Int("count", len(messages)))

	return nil
}

// Close closes the producer
func (p *ObjStoreNotificationProducer) Close() error {
	return p.producer.Close()
}

// ObjStoreNotificationConsumer manages Kafka consumer for object storage notifications
type ObjStoreNotificationConsumer struct {
	consumer Consumer
	signal   string
	logger   *slog.Logger
}

// NewObjStoreNotificationConsumer creates a new object storage notification consumer for a specific signal
func NewObjStoreNotificationConsumer(factory *Factory, signal string, groupID string, logger *slog.Logger) (*ObjStoreNotificationConsumer, error) {
	if !factory.IsEnabled() {
		return nil, fmt.Errorf("Kafka is not enabled")
	}

	// Determine topic based on signal
	var topic string
	switch signal {
	case "metrics":
		topic = "lakerunner.objstore.ingest.metrics"
	case "logs":
		topic = "lakerunner.objstore.ingest.logs"
	case "traces":
		topic = "lakerunner.objstore.ingest.traces"
	default:
		return nil, fmt.Errorf("unsupported signal type: %s", signal)
	}

	consumer, err := factory.CreateConsumer(topic, groupID)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	return &ObjStoreNotificationConsumer{
		consumer: consumer,
		signal:   signal,
		logger:   logger.With("component", "objstore_notification_consumer", "signal", signal),
	}, nil
}

// Consume reads and processes object storage notifications using a handler
func (c *ObjStoreNotificationConsumer) Consume(ctx context.Context, handler func(context.Context, []*messages.ObjStoreNotificationMessage) error) error {
	// Create a message handler that unmarshals object storage notifications
	messageHandler := func(ctx context.Context, consumedMessages []ConsumedMessage) error {
		notifications := make([]*messages.ObjStoreNotificationMessage, 0, len(consumedMessages))
		for _, msg := range consumedMessages {
			notification := &messages.ObjStoreNotificationMessage{}
			if err := notification.Unmarshal(msg.Value); err != nil {
				c.logger.Error("Failed to unmarshal notification", slog.Any("error", err))
				continue
			}
			notifications = append(notifications, notification)
		}

		if len(notifications) > 0 {
			return handler(ctx, notifications)
		}
		return nil
	}

	return c.consumer.Consume(ctx, messageHandler)
}

// ConsumeBatch reads a batch of object storage notifications by running the consumer with a batch handler
func (c *ObjStoreNotificationConsumer) ConsumeBatch(ctx context.Context, maxSize int) ([]*messages.ObjStoreNotificationMessage, error) {
	var result []*messages.ObjStoreNotificationMessage
	var consumeErr error

	// Create a context with timeout for batch collection
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Use the consumer with a handler that collects messages up to maxSize
	err := c.consumer.Consume(timeoutCtx, func(ctx context.Context, consumedMessages []ConsumedMessage) error {
		for _, msg := range consumedMessages {
			if len(result) >= maxSize {
				// Cancel context to stop consuming when we have enough messages
				cancel()
				return nil
			}

			notification := &messages.ObjStoreNotificationMessage{}
			if err := notification.Unmarshal(msg.Value); err != nil {
				c.logger.Error("Failed to unmarshal notification", slog.Any("error", err))
				continue
			}
			result = append(result, notification)
		}

		// Commit the messages after processing
		if err := c.consumer.CommitMessages(ctx, consumedMessages...); err != nil {
			consumeErr = err
			return err
		}

		return nil
	})

	// Check if the error is due to context cancellation (expected when we have enough messages)
	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		return nil, fmt.Errorf("failed to consume batch: %w", err)
	}

	if consumeErr != nil {
		return nil, fmt.Errorf("failed to commit messages: %w", consumeErr)
	}

	return result, nil
}

// Commit commits consumed messages
func (c *ObjStoreNotificationConsumer) Commit(ctx context.Context, messages []ConsumedMessage) error {
	return c.consumer.CommitMessages(ctx, messages...)
}

// Close closes the consumer
func (c *ObjStoreNotificationConsumer) Close() error {
	return c.consumer.Close()
}

// ObjStoreNotificationManager provides high-level management for object storage notification processing
type ObjStoreNotificationManager struct {
	factory   *Factory
	producers map[string]*ObjStoreNotificationProducer // Keyed by source (sqs, http, gcp, azure)
	logger    *slog.Logger
}

// NewObjStoreNotificationManager creates a new object storage notification manager
func NewObjStoreNotificationManager(factory *Factory, logger *slog.Logger) *ObjStoreNotificationManager {
	return &ObjStoreNotificationManager{
		factory:   factory,
		producers: make(map[string]*ObjStoreNotificationProducer),
		logger:    logger.With("component", "objstore_notification_manager"),
	}
}

// GetProducer gets or creates a producer for the given source
func (m *ObjStoreNotificationManager) GetProducer(source string) (*ObjStoreNotificationProducer, error) {
	if producer, exists := m.producers[source]; exists {
		return producer, nil
	}

	producer, err := NewObjStoreNotificationProducer(m.factory, m.logger.With("source", source))
	if err != nil {
		return nil, fmt.Errorf("failed to create producer for source %s: %w", source, err)
	}

	m.producers[source] = producer
	return producer, nil
}

// Close closes all producers
func (m *ObjStoreNotificationManager) Close() error {
	var firstErr error
	for source, producer := range m.producers {
		if err := producer.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close producer for source %s: %w", source, err)
		}
	}
	return firstErr
}
