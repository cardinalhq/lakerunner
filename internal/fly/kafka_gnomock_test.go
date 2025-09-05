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
	"testing"
	"time"

	"github.com/orlangure/gnomock"
	kafkapreset "github.com/orlangure/gnomock/preset/kafka"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

// KafkaTestContainer wraps a Gnomock Kafka container for testing
type KafkaTestContainer struct {
	container *gnomock.Container
	broker    string
}

// NewKafkaTestContainer creates a new Kafka container for testing with specified topics
func NewKafkaTestContainer(t *testing.T, topics ...string) *KafkaTestContainer {
	t.Helper()

	// Create Kafka preset with topics
	preset := kafkapreset.Preset(kafkapreset.WithTopics(topics...))

	// Start container
	container, err := gnomock.Start(
		preset,
		gnomock.WithDebugMode(), // Enable debug for better troubleshooting
	)
	require.NoError(t, err, "Failed to start Kafka container")

	broker := container.Address(kafkapreset.BrokerPort)
	t.Logf("Kafka container started at: %s", broker)

	return &KafkaTestContainer{
		container: container,
		broker:    broker,
	}
}

// Broker returns the broker address for connecting clients
func (k *KafkaTestContainer) Broker() string {
	return k.broker
}

// Stop stops and removes the Kafka container
func (k *KafkaTestContainer) Stop(t *testing.T) {
	t.Helper()
	if k.container != nil {
		err := gnomock.Stop(k.container)
		require.NoError(t, err, "Failed to stop Kafka container")
		t.Log("Kafka container stopped")
	}
}

// CreateProducerConfig creates a ProducerConfig using this container's broker
func (k *KafkaTestContainer) CreateProducerConfig() ProducerConfig {
	return ProducerConfig{
		Brokers:      []string{k.broker},
		BatchSize:    10,
		BatchTimeout: 100 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}
}

// CreateConsumerConfig creates a ConsumerConfig using this container's broker
func (k *KafkaTestContainer) CreateConsumerConfig(topic, groupID string) ConsumerConfig {
	return ConsumerConfig{
		Brokers:       []string{k.broker},
		Topic:         topic,
		GroupID:       groupID,
		MinBytes:      1,
		MaxBytes:      10e6,
		MaxWait:       100 * time.Millisecond,
		BatchSize:     10,
		StartOffset:   kafka.FirstOffset,
		AutoCommit:    false,
		CommitBatch:   true,
		RetryAttempts: 3,
	}
}