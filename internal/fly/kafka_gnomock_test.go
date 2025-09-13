//go:build kafkatest

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
	"os"
	"os/exec"
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

// cleanupExistingKafkaContainers removes any hanging Kafka containers
func cleanupExistingKafkaContainers() {
	// Find and stop any existing fast-data-dev containers
	cmd := exec.Command("docker", "ps", "-a", "--filter", "ancestor=lensesio/fast-data-dev:3.6.1-L0", "--format", "{{.ID}}")
	output, err := cmd.Output()
	if err != nil {
		return // Ignore errors, might not have docker or containers
	}

	containerIDs := strings.Fields(strings.TrimSpace(string(output)))
	if len(containerIDs) > 0 {
		fmt.Printf("Cleaning up %d existing Kafka containers...\n", len(containerIDs))

		// Stop containers
		stopCmd := append([]string{"stop"}, containerIDs...)
		exec.Command("docker", stopCmd...).Run()

		// Remove containers
		rmCmd := append([]string{"rm"}, containerIDs...)
		exec.Command("docker", rmCmd...).Run()

		fmt.Printf("Cleanup completed\n")
	}
}

// TestMain sets up and tears down the shared Kafka container
func TestMain(m *testing.M) {
	// Ensure cleanup happens even on panic or early exit
	var container *gnomock.Container
	defer func() {
		if container != nil {
			if err := gnomock.Stop(container); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to stop shared Kafka container: %v\n", err)
			} else {
				fmt.Printf("Shared Kafka container stopped successfully\n")
			}
		}
	}()

	// Clean up any existing containers that might be hanging around
	cleanupExistingKafkaContainers()

	// Start shared Kafka container
	preset := kafkapreset.Preset() // No initial topics - we'll create them as needed

	var err error
	container, err = gnomock.Start(
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

	// Wait for Kafka to be fully ready with a longer timeout
	if !waitForKafkaReady(broker, 30*time.Second) {
		fmt.Fprintf(os.Stderr, "Kafka container did not become ready within timeout\n")
		os.Exit(1)
	}

	fmt.Printf("Kafka container is ready, running tests...\n")

	// Run tests
	code := m.Run()

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

// CreateTopics creates topics on the shared container and waits for them to be ready
func (k *KafkaTestContainer) CreateTopics(t *testing.T, topics ...string) {
	t.Helper()

	conn, err := kafka.Dial("tcp", k.broker)
	require.NoError(t, err, "Failed to connect to Kafka for topic creation")
	defer conn.Close()

	// Create all topics
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

	// Wait for all topics to be ready
	k.WaitForTopicsReady(t, topics...)
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
		RequiredAcks: kafka.RequireNone,
		Compression:  kafka.Snappy,
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

// WaitForTopicsReady waits for topics to be available and ready
func (k *KafkaTestContainer) WaitForTopicsReady(t *testing.T, topics ...string) {
	t.Helper()

	maxWaitTime := 10 * time.Second
	checkInterval := 100 * time.Millisecond
	deadline := time.Now().Add(maxWaitTime)

	config := &Config{
		Brokers: []string{k.broker},
	}
	adminClient, err := NewAdminClient(config)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), maxWaitTime)
	defer cancel()

	for _, topic := range topics {
		topicReady := false
		for time.Now().Before(deadline) {
			exists, err := adminClient.TopicExists(ctx, topic)
			if err == nil && exists {
				// Verify we can get topic info (ensures metadata is available)
				info, err := adminClient.GetTopicInfo(ctx, topic)
				if err == nil && info != nil && len(info.Partitions) > 0 {
					topicReady = true
					break
				}
			}
			time.Sleep(checkInterval)
		}
		require.True(t, topicReady, "Topic %s was not ready within %v", topic, maxWaitTime)
	}
}

// waitForKafkaReady waits for the Kafka broker to be fully ready
func waitForKafkaReady(broker string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := kafka.Dial("tcp", broker)
		if err == nil {
			// Try to get metadata to ensure broker is fully operational
			_, err = conn.ApiVersions()
			conn.Close()
			if err == nil {
				return true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
