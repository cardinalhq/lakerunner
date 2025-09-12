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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cardinalhq/kafka-sync/kafkasync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopicSyncerConstruction(t *testing.T) {
	// Test that we can create a TopicSyncer correctly
	config := &Config{
		Brokers:             []string{"localhost:9092"},
		SASLEnabled:         false,
		TLSEnabled:          false,
		ConsumerGroupPrefix: "lakerunner",
	}
	factory := NewFactory(config)
	syncer := factory.CreateTopicSyncer()

	require.NotNil(t, syncer, "TopicSyncer should be created")
	require.NotNil(t, syncer.factory, "TopicSyncer should have factory reference")
	assert.Equal(t, factory, syncer.factory, "TopicSyncer should reference the same factory")
}

func TestTopicSyncerConnectionConfig(t *testing.T) {
	tests := []struct {
		name          string
		config        *Config
		expectSASL    bool
		expectTLS     bool
		expectBrokers []string
	}{
		{
			name: "no auth",
			config: &Config{
				Brokers:     []string{"broker1:9092", "broker2:9092"},
				SASLEnabled: false,
				TLSEnabled:  false,
			},
			expectSASL:    false,
			expectTLS:     false,
			expectBrokers: []string{"broker1:9092", "broker2:9092"},
		},
		{
			name: "with SASL",
			config: &Config{
				Brokers:       []string{"secure-broker:9092"},
				SASLEnabled:   true,
				SASLMechanism: "SCRAM-SHA-256",
				SASLUsername:  "user",
				SASLPassword:  "pass",
				TLSEnabled:    false,
			},
			expectSASL:    true,
			expectTLS:     false,
			expectBrokers: []string{"secure-broker:9092"},
		},
		{
			name: "with TLS",
			config: &Config{
				Brokers:       []string{"tls-broker:9092"},
				SASLEnabled:   false,
				TLSEnabled:    true,
				TLSSkipVerify: true,
			},
			expectSASL:    false,
			expectTLS:     true,
			expectBrokers: []string{"tls-broker:9092"},
		},
		{
			name: "with SASL and TLS",
			config: &Config{
				Brokers:       []string{"secure-tls-broker:9092"},
				SASLEnabled:   true,
				SASLMechanism: "PLAIN",
				SASLUsername:  "user",
				SASLPassword:  "pass",
				TLSEnabled:    true,
				TLSSkipVerify: false,
			},
			expectSASL:    true,
			expectTLS:     true,
			expectBrokers: []string{"secure-tls-broker:9092"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := NewFactory(tt.config)
			syncer := factory.CreateTopicSyncer()

			connConfig, err := syncer.createConnectionConfig()
			require.NoError(t, err, "Should create connection config without error")

			assert.Equal(t, tt.expectBrokers, connConfig.BootstrapServers, "Brokers should match")

			if tt.expectSASL {
				assert.NotNil(t, connConfig.SASLMechanism, "SASL mechanism should be set")
			} else {
				assert.Nil(t, connConfig.SASLMechanism, "SASL mechanism should be nil")
			}

			if tt.expectTLS {
				assert.NotNil(t, connConfig.TLS, "TLS config should be set")
				assert.Equal(t, tt.config.TLSSkipVerify, connConfig.TLS.InsecureSkipVerify, "TLS skip verify should match")
			} else {
				assert.Nil(t, connConfig.TLS, "TLS config should be nil")
			}
		})
	}
}

func TestLoadTopicsConfigFromFile(t *testing.T) {
	// Create a temporary kafka-sync config file
	configContent := `
defaults:
  partitionCount: 8
  replicationFactor: 2
  topicConfig:
    retention.ms: "86400000"
    cleanup.policy: "delete"

topics:
  - name: test-topic-1
    partitionCount: 12
    replicationFactor: 3
    config:
      cleanup.policy: "compact"
      retention.ms: "3600000"
  - name: test-topic-2
    # Uses defaults
  - name: test-topic-3
    partitionCount: 4
    config:
      max.message.bytes: "1048576"

operationTimeout: 45s
`

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "test_kafka_topics.yaml")
	err := os.WriteFile(configFile, []byte(configContent), 0644)
	require.NoError(t, err, "Failed to write test config file")

	// Load configuration
	config, err := LoadTopicsConfig(configFile)
	require.NoError(t, err, "Failed to load topics config from file")

	// Verify defaults
	assert.Equal(t, 8, config.Defaults.PartitionCount, "Default partition count should be 8")
	assert.Equal(t, 2, config.Defaults.ReplicationFactor, "Default replication factor should be 2")
	assert.Equal(t, "86400000", config.Defaults.TopicConfig["retention.ms"], "Default retention should be set")
	assert.Equal(t, "delete", config.Defaults.TopicConfig["cleanup.policy"], "Default cleanup policy should be set")

	// Verify topics
	require.Len(t, config.Topics, 3, "Should have 3 topics")

	// Verify test-topic-1 (explicit config)
	topic1 := config.Topics[0]
	assert.Equal(t, "test-topic-1", topic1.Name)
	assert.Equal(t, 12, topic1.PartitionCount)
	assert.Equal(t, 3, topic1.ReplicationFactor)
	assert.Equal(t, "compact", topic1.Config["cleanup.policy"])
	assert.Equal(t, "3600000", topic1.Config["retention.ms"])

	// Verify test-topic-2 (uses defaults)
	topic2 := config.Topics[1]
	assert.Equal(t, "test-topic-2", topic2.Name)
	assert.Equal(t, 0, topic2.PartitionCount, "Should use defaults (0 means use default)")
	assert.Equal(t, 0, topic2.ReplicationFactor, "Should use defaults (0 means use default)")

	// Verify test-topic-3 (mixed config)
	topic3 := config.Topics[2]
	assert.Equal(t, "test-topic-3", topic3.Name)
	assert.Equal(t, 4, topic3.PartitionCount)
	assert.Equal(t, 0, topic3.ReplicationFactor, "Should use default")
	assert.Equal(t, "1048576", topic3.Config["max.message.bytes"])

	// Verify operation timeout
	assert.Equal(t, 45*time.Second, config.OperationTimeout)

	t.Logf("Successfully loaded config with %d topics and defaults", len(config.Topics))
}

func TestCreateDefaultTopicsConfig(t *testing.T) {
	// Define test topics
	topics := []kafkasync.Topic{
		{
			Name:              "default-test-1",
			PartitionCount:    5,
			ReplicationFactor: 2,
			Config: map[string]string{
				"retention.ms": "7200000",
			},
		},
		{
			Name:              "default-test-2",
			PartitionCount:    3,
			ReplicationFactor: 1,
		},
	}

	// Create default config
	config := CreateDefaultTopicsConfig(topics)

	// Verify defaults
	assert.Equal(t, 16, config.Defaults.PartitionCount, "Default should be 16 partitions")
	assert.Equal(t, 2, config.Defaults.ReplicationFactor, "Default should be replication factor 2")
	assert.Equal(t, "7200000", config.Defaults.TopicConfig["retention.ms"], "Default retention should be 2 hours")

	// Verify topics were copied correctly
	require.Len(t, config.Topics, 2, "Should have 2 topics")
	assert.Equal(t, topics[0].Name, config.Topics[0].Name)
	assert.Equal(t, topics[0].PartitionCount, config.Topics[0].PartitionCount)
	assert.Equal(t, topics[1].Name, config.Topics[1].Name)
	assert.Equal(t, topics[1].PartitionCount, config.Topics[1].PartitionCount)

	// Verify operation timeout
	assert.Equal(t, 30*time.Second, config.OperationTimeout)

	t.Logf("Created default config with %d topics", len(config.Topics))
}

func TestLoadTopicsConfigNonexistentFile(t *testing.T) {
	// Try to load from a file that doesn't exist
	_, err := LoadTopicsConfig("/nonexistent/path/kafka_topics.yaml")
	assert.Error(t, err, "Should fail when file doesn't exist")
	assert.Contains(t, err.Error(), "no such file or directory", "Error should mention file not found")
}

func TestLoadTopicsConfigInvalidYAML(t *testing.T) {
	// Create a file with invalid YAML
	invalidContent := `
defaults:
  partitionCount: invalid_number
  replicationFactor: "not a number"
topics:
  - name: 
  invalid: yaml: structure
`

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "invalid_kafka_topics.yaml")
	err := os.WriteFile(configFile, []byte(invalidContent), 0644)
	require.NoError(t, err, "Failed to write invalid config file")

	// Try to load invalid configuration
	_, err = LoadTopicsConfig(configFile)
	assert.Error(t, err, "Should fail when YAML is invalid")
}
