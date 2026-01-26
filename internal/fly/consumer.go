// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"crypto/tls"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
)

// MessageHandler processes consumed messages
type MessageHandler func(ctx context.Context, messages []ConsumedMessage) error

// Consumer provides high-level Kafka consumer functionality
type Consumer interface {
	// Consume from topic with consumer group
	Consume(ctx context.Context, handler MessageHandler) error

	// CommitMessages after successful processing
	CommitMessages(ctx context.Context, messages ...ConsumedMessage) error

	// CommitPartitionOffsets commits specific offsets for given partitions
	CommitPartitionOffsets(ctx context.Context, offsets map[int32]int64) error

	// Close the consumer
	Close() error
}

// ConsumerConfig contains configuration for the Kafka consumer
type ConsumerConfig struct {
	Brokers     []string
	Topic       string
	GroupID     string
	MinBytes    int
	MaxBytes    int
	MaxWait     time.Duration
	BatchSize   int
	StartOffset int64
	AutoCommit  bool
	CommitBatch bool

	// SASL/SCRAM authentication
	SASLMechanism sasl.Mechanism

	// TLS configuration
	TLSConfig *tls.Config

	// Connection settings
	ConnectionTimeout time.Duration
}

// DefaultConsumerConfig returns a default consumer configuration
func DefaultConsumerConfig(topic, groupID string) ConsumerConfig {
	return ConsumerConfig{
		Brokers:     []string{"localhost:9092"},
		Topic:       topic,
		GroupID:     groupID,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		MaxWait:     500 * time.Millisecond,
		BatchSize:   100,
		StartOffset: kafka.LastOffset,
		AutoCommit:  true, // Default to true for backward compatibility
		CommitBatch: true,

		ConnectionTimeout: 10 * time.Second,
	}
}

// kafkaConsumer implements the Consumer interface using segmentio/kafka-go
type kafkaConsumer struct {
	config ConsumerConfig
	reader *kafka.Reader
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(config ConsumerConfig) Consumer {
	timeout := config.ConnectionTimeout
	if timeout == 0 {
		timeout = 10 * time.Second // Default fallback
	}

	dialer := &kafka.Dialer{
		Timeout:       timeout,
		SASLMechanism: config.SASLMechanism,
		TLS:           config.TLSConfig,
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:        config.Brokers,
		Topic:          config.Topic,
		GroupID:        config.GroupID,
		MinBytes:       config.MinBytes,
		MaxBytes:       config.MaxBytes,
		MaxWait:        config.MaxWait,
		StartOffset:    config.StartOffset,
		Dialer:         dialer,
		CommitInterval: 0, // Synchronous commits only when explicitly called
	}

	return &kafkaConsumer{
		config: config,
		reader: kafka.NewReader(readerConfig),
	}
}

// ConsumerOption is a functional option for creating a consumer
type ConsumerOption func(*ConsumerConfig)

// WithTopic sets the topic for the consumer
func WithTopic(topic string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Topic = topic
	}
}

// WithConsumerGroup sets the consumer group ID
func WithConsumerGroup(groupID string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.GroupID = groupID
	}
}

// WithBatchSize sets the batch size for consumption
func WithBatchSize(size int) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.BatchSize = size
	}
}

// WithBrokers sets the Kafka brokers
func WithBrokers(brokers ...string) ConsumerOption {
	return func(c *ConsumerConfig) {
		c.Brokers = brokers
	}
}

// NewConsumerWithOptions creates a consumer with functional options
func NewConsumerWithOptions(opts ...ConsumerOption) Consumer {
	config := DefaultConsumerConfig("", "")
	for _, opt := range opts {
		opt(&config)
	}
	return NewConsumer(config)
}

func (c *kafkaConsumer) Consume(ctx context.Context, handler MessageHandler) error {
	// Log consumer startup details
	slog.Debug("Starting Kafka consumer consumption loop",
		slog.String("topic", c.config.Topic),
		slog.String("consumerGroup", c.config.GroupID),
		slog.Int64("startOffset", c.config.StartOffset),
		slog.Int("batchSize", c.config.BatchSize),
		slog.Duration("maxWait", c.config.MaxWait),
		slog.Int("minBytes", c.config.MinBytes),
		slog.Int("maxBytes", c.config.MaxBytes))

	batch := make([]ConsumedMessage, 0, c.config.BatchSize)

	for {
		select {
		case <-ctx.Done():
			// Process remaining messages before exiting
			if len(batch) > 0 {
				if err := c.processBatch(ctx, handler, batch); err != nil {
					return fmt.Errorf("failed to process final batch: %w", err)
				}
			}
			return ctx.Err()
		default:
		}

		// Read message with timeout
		readCtx, cancel := context.WithTimeout(ctx, c.config.MaxWait)
		msg, err := c.reader.FetchMessage(readCtx)
		cancel()

		if err != nil {
			if err == context.DeadlineExceeded {
				// Timeout reached, process batch if we have messages
				if len(batch) > 0 {
					if err := c.processBatch(ctx, handler, batch); err != nil {
						return fmt.Errorf("failed to process batch: %w", err)
					}
					batch = batch[:0]
				}
				continue
			}
			return fmt.Errorf("failed to fetch message: %w", err)
		}

		batch = append(batch, FromKafkaMessage(msg))

		// Process batch when full
		if len(batch) >= c.config.BatchSize {
			if err := c.processBatch(ctx, handler, batch); err != nil {
				return fmt.Errorf("failed to process batch: %w", err)
			}
			batch = batch[:0]
		}
	}
}

func (c *kafkaConsumer) processBatch(ctx context.Context, handler MessageHandler, messages []ConsumedMessage) error {
	err := handler(ctx, messages)
	if err != nil {
		return fmt.Errorf("handler failed: %w", err)
	}

	// Success - commit messages if AutoCommit is enabled
	if c.config.AutoCommit {
		if commitErr := c.CommitMessages(ctx, messages...); commitErr != nil {
			return fmt.Errorf("failed to commit messages: %w", commitErr)
		}
	}
	return nil
}

func (c *kafkaConsumer) CommitMessages(ctx context.Context, messages ...ConsumedMessage) error {
	if len(messages) == 0 {
		return nil
	}

	if c.config.CommitBatch {
		// For batch commit, we need to commit the highest offset for EACH partition
		// Group messages by partition and find the highest offset for each
		partitionOffsets := make(map[int]ConsumedMessage)
		for _, msg := range messages {
			existing, ok := partitionOffsets[msg.Partition]
			if !ok || msg.Offset > existing.Offset {
				partitionOffsets[msg.Partition] = msg
			}
		}

		// Create commit messages for each partition's highest offset
		kmsgs := make([]kafka.Message, 0, len(partitionOffsets))
		for _, msg := range partitionOffsets {
			kmsgs = append(kmsgs, kafka.Message{
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
			})
		}

		return c.reader.CommitMessages(ctx, kmsgs...)
	}

	// Commit all messages individually
	kmsgs := make([]kafka.Message, len(messages))
	for i, msg := range messages {
		kmsgs[i] = kafka.Message{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
		}
	}
	return c.reader.CommitMessages(ctx, kmsgs...)
}

func (c *kafkaConsumer) CommitPartitionOffsets(ctx context.Context, offsets map[int32]int64) error {
	if len(offsets) == 0 {
		return nil
	}

	// Create kafka messages for each partition's offset
	kmsgs := make([]kafka.Message, 0, len(offsets))
	for partition, offset := range offsets {
		kmsgs = append(kmsgs, kafka.Message{
			Topic:     c.config.Topic,
			Partition: int(partition),
			Offset:    offset,
		})
	}

	return c.reader.CommitMessages(ctx, kmsgs...)
}

func (c *kafkaConsumer) Close() error {
	return c.reader.Close()
}
