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
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/orlangure/gnomock"
	kafkapreset "github.com/orlangure/gnomock/preset/kafka"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

// Global shared container for all tests
var (
	sharedKafkaContainer *KafkaTestContainer
)

// KafkaTestContainer wraps a Gnomock Kafka container for testing
type KafkaTestContainer struct {
	container *gnomock.Container
	broker    string
}

// TestMain sets up and tears down the shared Kafka container
func TestMain(m *testing.M) {
	// Start shared Kafka container
	preset := kafkapreset.Preset() // No initial topics - we'll create them as needed
	
	container, err := gnomock.Start(
		preset,
		gnomock.WithDebugMode(), // Enable debug for better troubleshooting
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start shared Kafka container: %v\n", err)
		os.Exit(1)
	}

	broker := container.Address(kafkapreset.BrokerPort)
	fmt.Printf("Shared Kafka container started at: %s\n", broker)

	sharedKafkaContainer = &KafkaTestContainer{
		container: container,
		broker:    broker,
	}

	// Run tests
	code := m.Run()

	// Cleanup shared container
	if err := gnomock.Stop(container); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to stop shared Kafka container: %v\n", err)
	}

	os.Exit(code)
}

// NewKafkaTestContainer returns the shared container for testing with specified topics
func NewKafkaTestContainer(t *testing.T, topics ...string) *KafkaTestContainer {
	t.Helper()

	if sharedKafkaContainer == nil {
		t.Fatal("Shared Kafka container not initialized - TestMain should have set it up")
	}

	// Create topics on the shared container
	if len(topics) > 0 {
		sharedKafkaContainer.CreateTopics(t, topics...)
	}

	return sharedKafkaContainer
}

// Broker returns the broker address for connecting clients
func (k *KafkaTestContainer) Broker() string {
	return k.broker
}

// CreateTopics creates topics on the shared container
func (k *KafkaTestContainer) CreateTopics(t *testing.T, topics ...string) {
	t.Helper()
	
	conn, err := kafka.Dial("tcp", k.broker)
	require.NoError(t, err, "Failed to connect to Kafka for topic creation")
	defer conn.Close()

	for _, topic := range topics {
		err = conn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
		if err != nil {
			// Ignore "Topic already exists" errors
			if !strings.Contains(err.Error(), "Topic already exists") {
				require.NoError(t, err, "Failed to create topic %s", topic)
			}
		}
	}
}

// CleanupAfterTest removes topics and consumer groups created during the test
func (k *KafkaTestContainer) CleanupAfterTest(t *testing.T, topics []string, consumerGroups []string) {
	t.Helper()

	// Delete consumer groups first
	for _, groupID := range consumerGroups {
		k.deleteConsumerGroup(t, groupID)
	}

	// Delete topics
	for _, topic := range topics {
		k.deleteTopic(t, topic)
	}
}

// deleteTopic removes a topic from Kafka
func (k *KafkaTestContainer) deleteTopic(t *testing.T, topic string) {
	t.Helper()
	
	conn, err := kafka.Dial("tcp", k.broker)
	if err != nil {
		t.Logf("Failed to connect to Kafka for topic deletion: %v", err)
		return
	}
	defer conn.Close()

	err = conn.DeleteTopics(topic)
	if err != nil && !strings.Contains(err.Error(), "UnknownTopicOrPartition") {
		t.Logf("Failed to delete topic %s: %v", topic, err)
	}
}

// deleteConsumerGroup removes a consumer group
func (k *KafkaTestContainer) deleteConsumerGroup(t *testing.T, groupID string) {
	t.Helper()
	
	// Create a temporary consumer to trigger group coordinator metadata, then close it
	// This helps ensure the group is properly cleaned up
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{k.broker},
		GroupID: groupID,
		Topic:   "__consumer_offsets", // Use internal topic to trigger coordinator
	})
	
	// Close immediately to cleanup the group
	if err := reader.Close(); err != nil {
		t.Logf("Warning: Failed to cleanup consumer group %s: %v", groupID, err)
	}
}

// Stop is now a no-op since we use a shared container
func (k *KafkaTestContainer) Stop(t *testing.T) {
	// No-op - shared container is managed by TestMain
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