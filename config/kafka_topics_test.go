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

package config

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopicRegistry_DefaultConfig(t *testing.T) {
	// Load default config and use its registry
	cfg, err := Load()
	require.NoError(t, err)
	registry := cfg.TopicRegistry

	tests := []struct {
		name                  string
		topicKey              string
		expectedTopicName     string
		expectedConsumerGroup string
		expectedServiceType   string
	}{
		{
			name:                  "objstore ingest logs",
			topicKey:              TopicObjstoreIngestLogs,
			expectedTopicName:     "lakerunner.objstore.ingest.logs",
			expectedConsumerGroup: "lakerunner.ingest.logs",
			expectedServiceType:   "boxer-ingest-logs",
		},
		{
			name:                  "objstore ingest metrics",
			topicKey:              TopicObjstoreIngestMetrics,
			expectedTopicName:     "lakerunner.objstore.ingest.metrics",
			expectedConsumerGroup: "lakerunner.ingest.metrics",
			expectedServiceType:   "boxer-ingest-metrics",
		},
		{
			name:                  "objstore ingest traces",
			topicKey:              TopicObjstoreIngestTraces,
			expectedTopicName:     "lakerunner.objstore.ingest.traces",
			expectedConsumerGroup: "lakerunner.ingest.traces",
			expectedServiceType:   "boxer-ingest-traces",
		},
		{
			name:                  "boxer logs compact",
			topicKey:              TopicBoxerLogsCompact,
			expectedTopicName:     "lakerunner.boxer.logs.compact",
			expectedConsumerGroup: "lakerunner.boxer.logs.compact",
			expectedServiceType:   "boxer-compact-logs",
		},
		{
			name:                  "boxer metrics compact",
			topicKey:              TopicBoxerMetricsCompact,
			expectedTopicName:     "lakerunner.boxer.metrics.compact",
			expectedConsumerGroup: "lakerunner.boxer.metrics.compact",
			expectedServiceType:   "boxer-compact-metrics",
		},
		{
			name:                  "boxer traces compact",
			topicKey:              TopicBoxerTracesCompact,
			expectedTopicName:     "lakerunner.boxer.traces.compact",
			expectedConsumerGroup: "lakerunner.boxer.traces.compact",
			expectedServiceType:   "boxer-compact-traces",
		},
		{
			name:                  "boxer metrics rollup",
			topicKey:              TopicBoxerMetricsRollup,
			expectedTopicName:     "lakerunner.boxer.metrics.rollup",
			expectedConsumerGroup: "lakerunner.boxer.metrics.rollup",
			expectedServiceType:   "boxer-rollup-metrics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test topic name
			topicName := registry.GetTopic(tt.topicKey)
			assert.Equal(t, tt.expectedTopicName, topicName, "Topic name should match expected")

			// Test consumer group
			consumerGroup := registry.GetConsumerGroup(tt.topicKey)
			assert.Equal(t, tt.expectedConsumerGroup, consumerGroup, "Consumer group should match expected")

			// Test service type lookup
			topicByService, found := registry.GetTopicByServiceType(tt.expectedServiceType)
			require.True(t, found, "Should find topic by service type")
			assert.Equal(t, tt.expectedTopicName, topicByService, "Topic by service type should match")

			consumerGroupByService, found := registry.GetConsumerGroupByServiceType(tt.expectedServiceType)
			require.True(t, found, "Should find consumer group by service type")
			assert.Equal(t, tt.expectedConsumerGroup, consumerGroupByService, "Consumer group by service type should match")

			// Test service mapping
			mapping, found := registry.GetServiceMapping(tt.expectedServiceType)
			require.True(t, found, "Should find service mapping")
			assert.Equal(t, ServiceMapping{
				ServiceType:   tt.expectedServiceType,
				Topic:         tt.expectedTopicName,
				ConsumerGroup: tt.expectedConsumerGroup,
			}, mapping, "Service mapping should match expected")
		})
	}
}

func TestTopicRegistry_CustomPrefix(t *testing.T) {
	// Test with custom prefix
	registry := NewTopicRegistry("custom")

	// Test a few key topics to ensure prefix is applied correctly
	assert.Equal(t, "custom.objstore.ingest.logs", registry.GetTopic(TopicObjstoreIngestLogs))
	assert.Equal(t, "custom.ingest.logs", registry.GetConsumerGroup(TopicObjstoreIngestLogs))

	assert.Equal(t, "custom.boxer.metrics.rollup", registry.GetTopic(TopicBoxerMetricsRollup))
	assert.Equal(t, "custom.boxer.metrics.rollup", registry.GetConsumerGroup(TopicBoxerMetricsRollup))
}

func TestTopicRegistry_EmptyPrefix(t *testing.T) {
	// Test that empty prefix defaults to "lakerunner"
	registry := NewTopicRegistry("")

	assert.Equal(t, "lakerunner.objstore.ingest.logs", registry.GetTopic(TopicObjstoreIngestLogs))
	assert.Equal(t, "lakerunner.ingest.logs", registry.GetConsumerGroup(TopicObjstoreIngestLogs))
}

func TestTopicRegistry_GetAllServiceMappings(t *testing.T) {
	registry := NewTopicRegistry("lakerunner")
	mappings := registry.GetAllServiceMappings()

	// Should have mappings for all boxer service types (workers consume from PostgreSQL queue)
	expectedServiceTypes := []string{
		"boxer-ingest-logs", "boxer-ingest-metrics", "boxer-ingest-traces",
		"boxer-compact-logs", "boxer-compact-metrics", "boxer-compact-traces",
		"boxer-rollup-metrics",
	}

	assert.Len(t, mappings, len(expectedServiceTypes), "Should have mapping for each service type")

	foundServiceTypes := make(map[string]bool)
	for _, mapping := range mappings {
		foundServiceTypes[mapping.ServiceType] = true

		// Verify all mappings have proper prefixes
		assert.True(t, mapping.Topic != "", "Topic should not be empty")
		assert.True(t, mapping.ConsumerGroup != "", "Consumer group should not be empty")
		assert.Contains(t, mapping.Topic, "lakerunner.", "Topic should contain prefix")
		assert.Contains(t, mapping.ConsumerGroup, "lakerunner.", "Consumer group should contain prefix")
	}

	for _, expectedServiceType := range expectedServiceTypes {
		assert.True(t, foundServiceTypes[expectedServiceType], "Should have mapping for service type: %s", expectedServiceType)
	}
}

func TestTopicRegistry_GetObjstoreIngestTopic(t *testing.T) {
	registry := NewTopicRegistry("lakerunner")

	tests := []struct {
		signal   string
		expected string
	}{
		{"logs", "lakerunner.objstore.ingest.logs"},
		{"metrics", "lakerunner.objstore.ingest.metrics"},
		{"traces", "lakerunner.objstore.ingest.traces"},
		{"LOGS", "lakerunner.objstore.ingest.logs"},       // Test case insensitive
		{"unknown", "lakerunner.objstore.ingest.unknown"}, // Test fallback
	}

	for _, tt := range tests {
		t.Run(tt.signal, func(t *testing.T) {
			result := registry.GetObjstoreIngestTopic(tt.signal)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTopicRegistry_PanicOnUnknownTopic(t *testing.T) {
	registry := NewTopicRegistry("lakerunner")

	// Test that unknown topic key causes panic
	assert.Panics(t, func() {
		registry.GetTopic("unknown.key")
	}, "Should panic on unknown topic key")

	assert.Panics(t, func() {
		registry.GetConsumerGroup("unknown.key")
	}, "Should panic on unknown topic key")
}

func TestTopicRegistry_GetAllTopics(t *testing.T) {
	registry := NewTopicRegistry("lakerunner")
	topics := registry.GetAllTopics()

	// Should contain all boxer topics (workers consume from PostgreSQL queue)
	expectedTopics := []string{
		"lakerunner.objstore.ingest.logs",
		"lakerunner.objstore.ingest.metrics",
		"lakerunner.objstore.ingest.traces",
		"lakerunner.boxer.logs.compact",
		"lakerunner.boxer.metrics.compact",
		"lakerunner.boxer.traces.compact",
		"lakerunner.boxer.metrics.rollup",
	}

	assert.Len(t, topics, len(expectedTopics), "Should have correct number of topics")

	for _, expectedTopic := range expectedTopics {
		assert.Contains(t, topics, expectedTopic, "Should contain topic: %s", expectedTopic)
	}
}

func TestTopicRegistry_ServiceTypeLookups(t *testing.T) {
	registry := NewTopicRegistry("lakerunner")

	// Test successful lookups for boxer services (workers consume from PostgreSQL queue)
	topic, found := registry.GetTopicByServiceType("boxer-ingest-logs")
	assert.True(t, found, "Should find topic for boxer-ingest-logs service")
	assert.Equal(t, "lakerunner.objstore.ingest.logs", topic)

	consumerGroup, found := registry.GetConsumerGroupByServiceType("boxer-rollup-metrics")
	assert.True(t, found, "Should find consumer group for boxer-rollup-metrics service")
	assert.Equal(t, "lakerunner.boxer.metrics.rollup", consumerGroup)

	// Test failed lookups
	_, found = registry.GetTopicByServiceType("unknown-service")
	assert.False(t, found, "Should not find topic for unknown service")

	_, found = registry.GetConsumerGroupByServiceType("unknown-service")
	assert.False(t, found, "Should not find consumer group for unknown service")

	_, found = registry.GetServiceMapping("unknown-service")
	assert.False(t, found, "Should not find service mapping for unknown service")
}

func TestTopicRegistry_GenerateKafkaSyncConfig(t *testing.T) {
	registry := NewTopicRegistry("lakerunner")

	// Create a config with only boxer topics (workers consume from PostgreSQL queue)
	kafkaConfig := KafkaTopicsConfig{
		TopicPrefix: "lakerunner",
		Defaults: TopicCreationConfig{
			PartitionCount:    testIntPtr(16),
			ReplicationFactor: testIntPtr(2),
			Options: map[string]interface{}{
				"cleanup.policy":    "delete",
				"max.message.bytes": "10485760",
				"retention.ms":      "604800000",
			},
		},
		Topics: map[string]TopicCreationConfig{
			"boxer-ingest-logs": {
				PartitionCount: testIntPtr(32),
			},
			"boxer-compact-logs": {
				PartitionCount: testIntPtr(4),
				Options: map[string]interface{}{
					"cleanup.policy": "compact,delete",
				},
			},
			"boxer-compact-metrics": {
				Options: map[string]interface{}{
					"segment.ms": "3600000",
				},
			},
		},
	}

	// Generate kafka-sync config
	syncConfig := registry.GenerateKafkaSyncConfig(kafkaConfig)

	// Verify defaults
	assert.Equal(t, 16, syncConfig.Defaults.PartitionCount)
	assert.Equal(t, 2, syncConfig.Defaults.ReplicationFactor)
	assert.Equal(t, "delete", syncConfig.Defaults.TopicConfig["cleanup.policy"])
	assert.Equal(t, "10485760", syncConfig.Defaults.TopicConfig["max.message.bytes"])
	assert.Equal(t, "604800000", syncConfig.Defaults.TopicConfig["retention.ms"])

	// Verify we have all boxer topics (workers consume from PostgreSQL queue)
	expectedTopicNames := []string{
		"lakerunner.objstore.ingest.logs",
		"lakerunner.objstore.ingest.metrics",
		"lakerunner.objstore.ingest.traces",
		"lakerunner.boxer.logs.compact",
		"lakerunner.boxer.metrics.compact",
		"lakerunner.boxer.traces.compact",
		"lakerunner.boxer.metrics.rollup",
	}

	assert.Len(t, syncConfig.Topics, len(expectedTopicNames), "Should have all expected topics")

	// Find specific topics to verify overrides
	topicsByName := make(map[string]KafkaSyncTopic)
	for _, topic := range syncConfig.Topics {
		topicsByName[topic.Name] = topic
	}

	// Check boxer-ingest-logs has custom partition count
	ingestLogs := topicsByName["lakerunner.objstore.ingest.logs"]
	assert.Equal(t, 32, ingestLogs.PartitionCount, "boxer-ingest-logs should have custom partition count")

	// Check boxer-compact-logs has custom partition count and options
	compactLogs := topicsByName["lakerunner.boxer.logs.compact"]
	assert.Equal(t, 4, compactLogs.PartitionCount, "boxer-compact-logs should have custom partition count")
	assert.Equal(t, "compact,delete", compactLogs.TopicConfig["cleanup.policy"], "boxer-compact-logs should have custom cleanup policy")

	// Check boxer-compact-metrics has custom options
	boxerMetrics := topicsByName["lakerunner.boxer.metrics.compact"]
	assert.Equal(t, "3600000", boxerMetrics.TopicConfig["segment.ms"], "boxer-compact-metrics should have custom segment.ms")

	// Check topic without overrides uses defaults (no explicit values in sync config)
	ingestMetrics := topicsByName["lakerunner.objstore.ingest.metrics"]
	assert.Equal(t, 0, ingestMetrics.PartitionCount, "topics without overrides should have zero values (use defaults)")
	assert.Nil(t, ingestMetrics.TopicConfig, "topics without overrides should have nil TopicConfig")
}

func TestTopicRegistry_GenerateKafkaSyncConfigFromLoadedConfig(t *testing.T) {
	// Test with actual loaded config
	cfg, err := Load()
	require.NoError(t, err)

	syncConfig := cfg.TopicRegistry.GenerateKafkaSyncConfig(cfg.KafkaTopics)

	// Should have reasonable defaults
	assert.Equal(t, 16, syncConfig.Defaults.PartitionCount)
	assert.Equal(t, 3, syncConfig.Defaults.ReplicationFactor)
	assert.Equal(t, "delete", syncConfig.Defaults.TopicConfig["cleanup.policy"])

	// Should have all topics
	assert.Greater(t, len(syncConfig.Topics), 0, "Should have topics")

	// All topic names should start with the prefix
	for _, topic := range syncConfig.Topics {
		assert.True(t, strings.HasPrefix(topic.Name, "lakerunner."), "All topics should have lakerunner prefix: %s", topic.Name)
	}
}

func TestLoadKafkaTopicsOverride_ValidVersion(t *testing.T) {
	// Create a temporary file with version 2 format
	content := `version: 2
defaults:
  partitionCount: 8
  replicationFactor: 1
  options:
    "cleanup.policy": "delete"
workers:
  boxer-ingest-logs:
    partitionCount: 16
    options:
      "segment.ms": "3600000"
`
	tmpFile, err := os.CreateTemp("", "kafka_topics_*.yaml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	_, err = tmpFile.WriteString(content)
	require.NoError(t, err)
	_ = tmpFile.Close()

	// Load the override
	override, err := LoadKafkaTopicsOverride(tmpFile.Name())
	require.NoError(t, err)

	// Verify parsing (using boxer topics since workers consume from PostgreSQL queue)
	assert.Equal(t, 2, override.Version)
	assert.Equal(t, 8, *override.Defaults.PartitionCount)
	assert.Equal(t, 1, *override.Defaults.ReplicationFactor)
	assert.Equal(t, "delete", override.Defaults.Options["cleanup.policy"])
	assert.Equal(t, 16, *override.Workers["boxer-ingest-logs"].PartitionCount)
	assert.Equal(t, "3600000", override.Workers["boxer-ingest-logs"].Options["segment.ms"])
}

func TestLoadKafkaTopicsOverride_InvalidVersion(t *testing.T) {
	// Test with version 1 (unsupported)
	content := `version: 1
`
	tmpFile, err := os.CreateTemp("", "kafka_topics_*.yaml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	_, err = tmpFile.WriteString(content)
	require.NoError(t, err)
	_ = tmpFile.Close()

	// Should fail with version error
	_, err = LoadKafkaTopicsOverride(tmpFile.Name())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported kafka topics override file version 1, expected version 2")
}

func TestLoadKafkaTopicsOverride_StrictModeFailsOnUnknownField(t *testing.T) {
	// Test with unknown field - should fail in strict mode
	content := `version: 2
unknownField: "should fail"
`
	tmpFile, err := os.CreateTemp("", "kafka_topics_*.yaml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	_, err = tmpFile.WriteString(content)
	require.NoError(t, err)
	_ = tmpFile.Close()

	// Should fail due to unknown field in strict mode
	_, err = LoadKafkaTopicsOverride(tmpFile.Name())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "strict mode")
}

func TestLoadKafkaTopicsOverride_MissingVersion(t *testing.T) {
	// Test with no version field
	content := `defaults:
  partitionCount: 8
`
	tmpFile, err := os.CreateTemp("", "kafka_topics_*.yaml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	_, err = tmpFile.WriteString(content)
	require.NoError(t, err)
	_ = tmpFile.Close()

	// Should fail with version error (0 != 2)
	_, err = LoadKafkaTopicsOverride(tmpFile.Name())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported kafka topics override file version 0, expected version 2")
}

func TestMergeKafkaTopicsOverride(t *testing.T) {
	base := KafkaTopicsConfig{
		TopicPrefix: "lakerunner",
		Defaults: TopicCreationConfig{
			PartitionCount:    testIntPtr(16),
			ReplicationFactor: testIntPtr(3),
			Options: map[string]interface{}{
				"cleanup.policy": "delete",
				"retention.ms":   "604800000",
			},
		},
		Topics: map[string]TopicCreationConfig{
			"boxer-ingest-logs": {
				PartitionCount: testIntPtr(32),
			},
		},
	}

	override := &KafkaTopicsOverrideConfig{
		Version: 2,
		Defaults: TopicCreationOverrideConfig{
			PartitionCount: testIntPtr(8), // Override default
			Options: map[string]interface{}{
				"max.message.bytes": "5242880",    // Add new option
				"retention.ms":      "1209600000", // Override existing option
			},
		},
		Workers: map[string]TopicCreationOverrideConfig{
			"boxer-ingest-logs": { // Override existing topic
				PartitionCount: testIntPtr(64),
			},
			"boxer-ingest-metrics": { // Add new topic
				PartitionCount: testIntPtr(12),
			},
		},
	}

	result := MergeKafkaTopicsOverride(base, override)

	// Verify prefix is NOT overridden (remains from base config)
	assert.Equal(t, "lakerunner", result.TopicPrefix)

	// Verify defaults merge
	assert.Equal(t, 8, *result.Defaults.PartitionCount)                      // Overridden
	assert.Equal(t, 3, *result.Defaults.ReplicationFactor)                   // Kept from base
	assert.Equal(t, "delete", result.Defaults.Options["cleanup.policy"])     // Kept from base
	assert.Equal(t, "1209600000", result.Defaults.Options["retention.ms"])   // Overridden
	assert.Equal(t, "5242880", result.Defaults.Options["max.message.bytes"]) // Added from override

	// Verify topics merge (using boxer topics since workers consume from PostgreSQL queue)
	assert.Equal(t, 64, *result.Topics["boxer-ingest-logs"].PartitionCount)    // Overridden
	assert.Equal(t, 12, *result.Topics["boxer-ingest-metrics"].PartitionCount) // Added from override
}

// testIntPtr helper function for tests (renamed to avoid conflict with config.go)
func testIntPtr(i int) *int {
	return &i
}
