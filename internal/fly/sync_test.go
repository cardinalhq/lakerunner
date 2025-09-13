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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cardinalhq/kafka-sync/kafkasync"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopicSyncerCreateTopics(t *testing.T) {
	// Use shared Kafka container
	kafkaContainer := NewKafkaTestContainer(t, "test-topic-create")
	defer kafkaContainer.CleanupAfterTest(t, []string{"test-topic-create", "test-topic-update"}, []string{})

	// Create Factory with test configuration
	config := &Config{
		Brokers:             []string{kafkaContainer.Broker()},
		SASLEnabled:         false,
		TLSEnabled:          false,
		ConsumerGroupPrefix: "lakerunner",
	}
	factory := NewFactory(config)
	syncer := factory.CreateTopicSyncer()

	// Define test topics configuration
	topicsConfig := &kafkasync.Config{
		Defaults: kafkasync.Defaults{
			PartitionCount:    3,
			ReplicationFactor: 1, // Single broker in test
			TopicConfig: map[string]string{
				"retention.ms": "3600000", // 1 hour
			},
		},
		Topics: []kafkasync.Topic{
			{
				Name:              "test-topic-create",
				PartitionCount:    5,
				ReplicationFactor: 1,
				Config: map[string]string{
					"retention.ms": "7200000", // 2 hours
				},
			},
			{
				Name:              "test-topic-update",
				PartitionCount:    2,
				ReplicationFactor: 1,
				Config: map[string]string{
					"cleanup.policy": "compact",
				},
			},
		},
		OperationTimeout: 60 * time.Second,
	}

	ctx := context.Background()

	// Sync topics (fix mode to create them)
	err := syncer.SyncTopics(ctx, topicsConfig, true)
	require.NoError(t, err, "Failed to sync topics")

	// Verify topics were created correctly
	conn, err := kafka.Dial("tcp", kafkaContainer.Broker())
	require.NoError(t, err)
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	require.NoError(t, err)

	// Organize partitions by topic
	topicPartitions := make(map[string][]kafka.Partition)
	for _, p := range partitions {
		topicPartitions[p.Topic] = append(topicPartitions[p.Topic], p)
	}

	// Verify test-topic-create
	createTopicPartitions, exists := topicPartitions["test-topic-create"]
	assert.True(t, exists, "test-topic-create should exist")
	assert.Len(t, createTopicPartitions, 5, "test-topic-create should have 5 partitions")

	// Verify test-topic-update
	updateTopicPartitions, exists := topicPartitions["test-topic-update"]
	assert.True(t, exists, "test-topic-update should exist")
	assert.Len(t, updateTopicPartitions, 2, "test-topic-update should have 2 partitions")

	t.Logf("Successfully created topics: test-topic-create (%d partitions), test-topic-update (%d partitions)",
		len(createTopicPartitions), len(updateTopicPartitions))
}

func TestTopicSyncerInfoMode(t *testing.T) {
	// Use shared Kafka container - don't pre-create the topic we're testing
	kafkaContainer := NewKafkaTestContainer(t)
	defer kafkaContainer.CleanupAfterTest(t, []string{"info-mode-test"}, []string{})

	// Create Factory with test configuration
	config := &Config{
		Brokers:             []string{kafkaContainer.Broker()},
		SASLEnabled:         false,
		TLSEnabled:          false,
		ConsumerGroupPrefix: "lakerunner",
	}
	factory := NewFactory(config)
	syncer := factory.CreateTopicSyncer()

	// Define topics configuration
	topicsConfig := &kafkasync.Config{
		Defaults: kafkasync.Defaults{
			PartitionCount:    3,
			ReplicationFactor: 1,
			TopicConfig: map[string]string{
				"retention.ms": "3600000",
			},
		},
		Topics: []kafkasync.Topic{
			{
				Name:              "info-mode-test",
				PartitionCount:    3,
				ReplicationFactor: 1,
			},
		},
		OperationTimeout: 60 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// First run info mode - should not create topics
	err := syncer.SyncTopics(ctx, topicsConfig, false)
	require.NoError(t, err, "Info mode should not fail")

	// Verify topic was NOT created
	conn, err := kafka.Dial("tcp", kafkaContainer.Broker())
	require.NoError(t, err)
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	require.NoError(t, err)

	topicExists := false
	for _, p := range partitions {
		if p.Topic == "info-mode-test" {
			topicExists = true
			break
		}
	}
	assert.False(t, topicExists, "Topic should not exist after info mode")

	// Now run fix mode - should create topics
	err = syncer.SyncTopics(ctx, topicsConfig, true)
	require.NoError(t, err, "Fix mode should create topics")

	// Verify topic was created
	partitions, err = conn.ReadPartitions()
	require.NoError(t, err)

	topicExists = false
	partitionCount := 0
	for _, p := range partitions {
		if p.Topic == "info-mode-test" {
			topicExists = true
			partitionCount++
		}
	}
	assert.True(t, topicExists, "Topic should exist after fix mode")
	assert.Equal(t, 3, partitionCount, "Topic should have 3 partitions")

	t.Logf("Info mode test passed: topic created only in fix mode with %d partitions", partitionCount)
}

func TestTopicSyncerFromFile(t *testing.T) {
	// Use shared Kafka container
	kafkaContainer := NewKafkaTestContainer(t, "file-config-test")
	defer kafkaContainer.CleanupAfterTest(t, []string{"file-config-test", "another-topic"}, []string{})

	// Create a temporary kafka-sync config file
	configContent := `
defaults:
  partitionCount: 4
  replicationFactor: 1
  topicConfig:
    retention.ms: "86400000"

topics:
  - name: file-config-test
    partitionCount: 6
    replicationFactor: 1
    config:
      cleanup.policy: "delete"
      retention.ms: "3600000"
  - name: another-topic
    # Uses defaults
`

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "kafka_topics.yaml")
	err := os.WriteFile(configFile, []byte(configContent), 0644)
	require.NoError(t, err, "Failed to write config file")

	// Create Factory with test configuration
	config := &Config{
		Brokers:             []string{kafkaContainer.Broker()},
		SASLEnabled:         false,
		TLSEnabled:          false,
		ConsumerGroupPrefix: "lakerunner",
	}
	factory := NewFactory(config)
	syncer := factory.CreateTopicSyncer()

	// Load topics configuration from file
	topicsConfig, err := LoadTopicsConfig(configFile)
	require.NoError(t, err, "Failed to load topics config from file")

	ctx := context.Background()

	// Sync topics from file
	err = syncer.SyncTopics(ctx, topicsConfig, true)
	require.NoError(t, err, "Failed to sync topics from file")

	// Verify topics were created correctly
	conn, err := kafka.Dial("tcp", kafkaContainer.Broker())
	require.NoError(t, err)
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	require.NoError(t, err)

	// Organize partitions by topic
	topicPartitions := make(map[string][]kafka.Partition)
	for _, p := range partitions {
		topicPartitions[p.Topic] = append(topicPartitions[p.Topic], p)
	}

	// Verify file-config-test (explicit configuration)
	fileTopicPartitions, exists := topicPartitions["file-config-test"]
	assert.True(t, exists, "file-config-test should exist")
	assert.Len(t, fileTopicPartitions, 6, "file-config-test should have 6 partitions (explicit)")

	// Verify another-topic (uses defaults)
	defaultTopicPartitions, exists := topicPartitions["another-topic"]
	assert.True(t, exists, "another-topic should exist")
	assert.Len(t, defaultTopicPartitions, 4, "another-topic should have 4 partitions (from defaults)")

	t.Logf("File-based config test passed: file-config-test (%d partitions), another-topic (%d partitions)",
		len(fileTopicPartitions), len(defaultTopicPartitions))
}

func TestTopicSyncerIdempotent(t *testing.T) {
	// Use shared Kafka container - don't pre-create the topic we're testing
	kafkaContainer := NewKafkaTestContainer(t)
	defer kafkaContainer.CleanupAfterTest(t, []string{"idempotent-test"}, []string{})

	// Create Factory with test configuration
	config := &Config{
		Brokers:             []string{kafkaContainer.Broker()},
		SASLEnabled:         false,
		TLSEnabled:          false,
		ConsumerGroupPrefix: "lakerunner",
	}
	factory := NewFactory(config)
	syncer := factory.CreateTopicSyncer()

	// Define topics configuration
	topicsConfig := &kafkasync.Config{
		Defaults: kafkasync.Defaults{
			PartitionCount:    2,
			ReplicationFactor: 1,
		},
		Topics: []kafkasync.Topic{
			{
				Name:              "idempotent-test",
				PartitionCount:    4,
				ReplicationFactor: 1,
			},
		},
		OperationTimeout: 60 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Run sync multiple times
	for i := 0; i < 3; i++ {
		err := syncer.SyncTopics(ctx, topicsConfig, true)
		require.NoError(t, err, "Sync should be idempotent on run %d", i+1)
	}

	// Verify topic exists and has correct configuration
	conn, err := kafka.Dial("tcp", kafkaContainer.Broker())
	require.NoError(t, err)
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
	require.NoError(t, err)

	partitionCount := 0
	for _, p := range partitions {
		if p.Topic == "idempotent-test" {
			partitionCount++
		}
	}

	assert.Equal(t, 4, partitionCount, "Topic should consistently have 4 partitions")
	t.Logf("Idempotent test passed: topic has %d partitions after multiple sync runs", partitionCount)
}
