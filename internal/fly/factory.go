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
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/cardinalhq/lakerunner/config"
)

// Factory creates Kafka producers and consumers with consistent configuration
type Factory struct {
	config *config.KafkaConfig
}

// NewFactory creates a new factory with the given configuration
func NewFactory(cfg *config.KafkaConfig) *Factory {
	return &Factory{
		config: cfg,
	}
}

// CreateProducer creates a new Kafka producer
func (f *Factory) CreateProducer() (Producer, error) {
	var compression kafka.Compression
	switch strings.ToLower(f.config.ProducerCompression) {
	case "", "none", "uncompressed":
		compression = 0
	case "gzip":
		compression = kafka.Gzip
	case "snappy":
		compression = kafka.Snappy
	case "lz4":
		compression = kafka.Lz4
	case "zstd":
		compression = kafka.Zstd
	default:
		return nil, fmt.Errorf("unsupported compression: %s", f.config.ProducerCompression)
	}

	cfg := ProducerConfig{
		Brokers:           f.config.Brokers,
		BatchSize:         f.config.ProducerBatchSize,
		BatchTimeout:      f.config.ProducerBatchTimeout,
		RequiredAcks:      kafka.RequireNone,
		Compression:       compression,
		ConnectionTimeout: f.config.ConnectionTimeout,
	}

	// Configure SASL/SCRAM if enabled
	if f.config.SASLEnabled {
		mechanism, err := f.createSASLMechanism()
		if err != nil {
			return nil, fmt.Errorf("failed to create SASL mechanism: %w", err)
		}
		cfg.SASLMechanism = mechanism
	}

	// Configure TLS if enabled
	if f.config.TLSEnabled {
		cfg.TLSConfig = &tls.Config{
			InsecureSkipVerify: f.config.TLSSkipVerify,
		}
	}

	return NewProducer(cfg), nil
}

// CreateConsumer creates a new Kafka consumer for the specified topic
func (f *Factory) CreateConsumer(topic string, groupID string) (Consumer, error) {
	cfg := ConsumerConfig{
		Brokers:           f.config.Brokers,
		Topic:             topic,
		GroupID:           groupID,
		MinBytes:          f.config.ConsumerMinBytes,
		MaxBytes:          f.config.ConsumerMaxBytes,
		MaxWait:           f.config.ConsumerMaxWait,
		BatchSize:         f.config.ConsumerBatchSize,
		StartOffset:       kafka.LastOffset,
		AutoCommit:        true, // Default to true for backward compatibility
		CommitBatch:       true,
		ConnectionTimeout: f.config.ConnectionTimeout,
	}

	// Configure SASL/SCRAM if enabled
	if f.config.SASLEnabled {
		mechanism, err := f.createSASLMechanism()
		if err != nil {
			return nil, fmt.Errorf("failed to create SASL mechanism: %w", err)
		}
		cfg.SASLMechanism = mechanism
	}

	// Configure TLS if enabled
	if f.config.TLSEnabled {
		cfg.TLSConfig = &tls.Config{
			InsecureSkipVerify: f.config.TLSSkipVerify,
		}
	}

	return NewConsumer(cfg), nil
}

// CreateConsumerWithService creates a consumer with a service-based group ID
func (f *Factory) CreateConsumerWithService(topic string, service string) (Consumer, error) {
	groupID := f.config.GetConsumerGroup(service)
	return f.CreateConsumer(topic, groupID)
}

// createSASLMechanism creates the appropriate SASL mechanism based on configuration
func (f *Factory) createSASLMechanism() (sasl.Mechanism, error) {
	switch f.config.SASLMechanism {
	case "SCRAM-SHA-256":
		return scram.Mechanism(scram.SHA256, f.config.SASLUsername, f.config.SASLPassword)
	case "SCRAM-SHA-512":
		return scram.Mechanism(scram.SHA512, f.config.SASLUsername, f.config.SASLPassword)
	case "PLAIN":
		// Support for GCP Managed Kafka and other SASL/PLAIN systems
		return plain.Mechanism{
			Username: f.config.SASLUsername,
			Password: f.config.SASLPassword,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", f.config.SASLMechanism)
	}
}

// CreateTransport creates a centralized kafka.Transport with proper SASL and TLS configuration
func (f *Factory) CreateTransport() (*kafka.Transport, error) {
	transport := &kafka.Transport{}

	// Configure SASL if enabled
	if f.config.SASLEnabled {
		mechanism, err := f.createSASLMechanism()
		if err != nil {
			return nil, fmt.Errorf("failed to create SASL mechanism: %w", err)
		}
		transport.SASL = mechanism
	}

	// Configure TLS if enabled
	if f.config.TLSEnabled {
		transport.TLS = &tls.Config{
			InsecureSkipVerify: f.config.TLSSkipVerify,
		}
	}

	return transport, nil
}

// CreateKafkaClient creates a properly configured kafka.Client with centralized transport
func (f *Factory) CreateKafkaClient() (*kafka.Client, error) {
	transport, err := f.CreateTransport()
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	client := &kafka.Client{
		Addr:      kafka.TCP(f.config.Brokers[0]),
		Transport: transport,
	}

	return client, nil
}

// CreateDialer creates an authenticated Kafka dialer for administrative operations
func (f *Factory) CreateDialer() (*kafka.Dialer, error) {
	timeout := f.config.ConnectionTimeout
	if timeout == 0 {
		timeout = 10 * time.Second // Default fallback
	}

	dialer := &kafka.Dialer{
		Timeout: timeout,
	}

	// Configure SASL if enabled
	if f.config.SASLEnabled {
		mechanism, err := f.createSASLMechanism()
		if err != nil {
			return nil, fmt.Errorf("failed to create SASL mechanism: %w", err)
		}
		dialer.SASLMechanism = mechanism
	}

	// Configure TLS if enabled
	if f.config.TLSEnabled {
		dialer.TLS = &tls.Config{
			InsecureSkipVerify: f.config.TLSSkipVerify,
		}
	}

	return dialer, nil
}

// CreateTopicSyncer creates a topic syncer for managing Kafka topics
func (f *Factory) CreateTopicSyncer() *TopicSyncer {
	return newTopicSyncer(f)
}

// GetConfig returns the underlying configuration
func (f *Factory) GetConfig() *config.KafkaConfig {
	return f.config
}

// Manager provides lifecycle management for Kafka components
type Manager struct {
	factory   *Factory
	producers []Producer
	consumers []Consumer
}

// NewManager creates a new Kafka component manager
func NewManager(factory *Factory) *Manager {
	return &Manager{
		factory:   factory,
		producers: make([]Producer, 0),
		consumers: make([]Consumer, 0),
	}
}

// CreateProducer creates and tracks a producer
func (m *Manager) CreateProducer() (Producer, error) {
	p, err := m.factory.CreateProducer()
	if err != nil {
		return nil, err
	}
	m.producers = append(m.producers, p)
	return p, nil
}

// CreateConsumer creates and tracks a consumer
func (m *Manager) CreateConsumer(topic string, groupID string) (Consumer, error) {
	c, err := m.factory.CreateConsumer(topic, groupID)
	if err != nil {
		return nil, err
	}
	m.consumers = append(m.consumers, c)
	return c, nil
}

// CreateConsumerWithService creates and tracks a consumer with service-based group ID
func (m *Manager) CreateConsumerWithService(topic string, service string) (Consumer, error) {
	c, err := m.factory.CreateConsumerWithService(topic, service)
	if err != nil {
		return nil, err
	}
	m.consumers = append(m.consumers, c)
	return c, nil
}

// Close closes all managed components
func (m *Manager) Close() error {
	var firstErr error

	// Close producers
	for _, p := range m.producers {
		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Close consumers
	for _, c := range m.consumers {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}
