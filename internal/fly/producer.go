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
	"crypto/tls"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
)

// Producer provides high-level Kafka producer functionality
type Producer interface {
	// Send with automatic partitioning
	Send(ctx context.Context, topic string, message Message) error

	// SendToPartition for explicit partition control
	SendToPartition(ctx context.Context, topic string, partition int, message Message) error

	// GetPartitionCount for partition-aware routing
	GetPartitionCount(topic string) (int, error)

	// BatchSend for efficient bulk operations
	BatchSend(ctx context.Context, topic string, messages []Message) error

	// Close the producer
	Close() error
}

// ProducerConfig contains configuration for the Kafka producer
type ProducerConfig struct {
	Brokers      []string
	BatchSize    int
	BatchTimeout time.Duration
	RequiredAcks kafka.RequiredAcks
	Compression  kafka.Compression

	// SASL/SCRAM authentication
	SASLMechanism sasl.Mechanism

	// TLS configuration
	TLSConfig *tls.Config
}

// kafkaProducer implements the Producer interface using segmentio/kafka-go
type kafkaProducer struct {
	config          ProducerConfig
	writers         map[string]*kafka.Writer
	writersMu       sync.RWMutex
	partitionCounts map[string]int
	partitionMu     sync.RWMutex
	dialer          *kafka.Dialer
}

// manualBalancer implements kafka.Balancer to send to a specific partition
type manualBalancer struct {
	partition int
}

func (b *manualBalancer) Balance(msg kafka.Message, partitions ...int) int {
	// Always return the specified partition if it's valid
	if slices.Contains(partitions, b.partition) {
		return b.partition
	}
	// If the partition doesn't exist, fall back to the first available
	if len(partitions) > 0 {
		return partitions[0]
	}
	return 0
}

// NewProducer creates a new Kafka producer
func NewProducer(config ProducerConfig) Producer {
	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		SASLMechanism: config.SASLMechanism,
		TLS:           config.TLSConfig,
	}

	return &kafkaProducer{
		config:          config,
		writers:         make(map[string]*kafka.Writer),
		partitionCounts: make(map[string]int),
		dialer:          dialer,
	}
}

func (p *kafkaProducer) getWriter(topic string) *kafka.Writer {
	p.writersMu.RLock()
	w, ok := p.writers[topic]
	p.writersMu.RUnlock()
	if ok {
		return w
	}

	p.writersMu.Lock()
	defer p.writersMu.Unlock()

	// Double-check after acquiring write lock
	if w, ok := p.writers[topic]; ok {
		return w
	}

	transport := &kafka.Transport{
		SASL: p.config.SASLMechanism,
		TLS:  p.config.TLSConfig,
	}

	w = &kafka.Writer{
		Addr:         kafka.TCP(p.config.Brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    p.config.BatchSize,
		BatchTimeout: p.config.BatchTimeout,
		RequiredAcks: p.config.RequiredAcks,
		Transport:    transport,
		Compression:  p.config.Compression,
	}
	p.writers[topic] = w
	return w
}

func (p *kafkaProducer) Send(ctx context.Context, topic string, message Message) error {
	w := p.getWriter(topic)
	km := message.ToKafkaMessage()
	return w.WriteMessages(ctx, km)
}

func (p *kafkaProducer) SendToPartition(ctx context.Context, topic string, partition int, message Message) error {
	transport := &kafka.Transport{
		SASL: p.config.SASLMechanism,
		TLS:  p.config.TLSConfig,
	}

	// Create a dedicated writer for specific partition using manual balancer
	w := &kafka.Writer{
		Addr:         kafka.TCP(p.config.Brokers...),
		Topic:        topic,
		Balancer:     &manualBalancer{partition: partition},
		BatchSize:    1, // send immediately
		RequiredAcks: p.config.RequiredAcks,
		Transport:    transport,
		Compression:  p.config.Compression,
	}
	defer w.Close()

	km := message.ToKafkaMessage()
	return w.WriteMessages(ctx, km)
}

func (p *kafkaProducer) GetPartitionCount(topic string) (int, error) {
	p.partitionMu.RLock()
	count, ok := p.partitionCounts[topic]
	p.partitionMu.RUnlock()
	if ok {
		return count, nil
	}

	// Fetch partition count from broker
	conn, err := p.dialer.Dial("tcp", p.config.Brokers[0])
	if err != nil {
		return 0, fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return 0, fmt.Errorf("failed to read partitions for topic %s: %w", topic, err)
	}

	count = len(partitions)

	p.partitionMu.Lock()
	p.partitionCounts[topic] = count
	p.partitionMu.Unlock()

	return count, nil
}

func (p *kafkaProducer) BatchSend(ctx context.Context, topic string, messages []Message) error {
	if len(messages) == 0 {
		return nil
	}

	w := p.getWriter(topic)
	kmsgs := make([]kafka.Message, len(messages))
	for i, msg := range messages {
		km := msg.ToKafkaMessage()
		kmsgs[i] = km
	}
	return w.WriteMessages(ctx, kmsgs...)
}

func (p *kafkaProducer) Close() error {
	p.writersMu.Lock()
	defer p.writersMu.Unlock()

	var firstErr error
	for _, w := range p.writers {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.writers = make(map[string]*kafka.Writer)
	return firstErr
}
