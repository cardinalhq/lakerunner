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

package config

import (
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
		name               string
		topicKey           string
		expectedTopicName  string
		expectedConsumerGroup string
		expectedServiceType string
	}{
		{
			name:                  "objstore ingest logs",
			topicKey:              TopicObjstoreIngestLogs,
			expectedTopicName:     "lakerunner.objstore.ingest.logs",
			expectedConsumerGroup: "lakerunner.ingest.logs",
			expectedServiceType:   "ingest-logs",
		},
		{
			name:                  "objstore ingest metrics",
			topicKey:              TopicObjstoreIngestMetrics,
			expectedTopicName:     "lakerunner.objstore.ingest.metrics",
			expectedConsumerGroup: "lakerunner.ingest.metrics",
			expectedServiceType:   "ingest-metrics",
		},
		{
			name:                  "objstore ingest traces",
			topicKey:              TopicObjstoreIngestTraces,
			expectedTopicName:     "lakerunner.objstore.ingest.traces",
			expectedConsumerGroup: "lakerunner.ingest.traces",
			expectedServiceType:   "ingest-traces",
		},
		{
			name:                  "segments logs compact",
			topicKey:              TopicSegmentsLogsCompact,
			expectedTopicName:     "lakerunner.segments.logs.compact",
			expectedConsumerGroup: "lakerunner.compact.logs",
			expectedServiceType:   "compact-logs",
		},
		{
			name:                  "segments metrics compact",
			topicKey:              TopicSegmentsMetricsCompact,
			expectedTopicName:     "lakerunner.segments.metrics.compact",
			expectedConsumerGroup: "lakerunner.compact.metrics",
			expectedServiceType:   "compact-metrics",
		},
		{
			name:                  "segments traces compact",
			topicKey:              TopicSegmentsTracesCompact,
			expectedTopicName:     "lakerunner.segments.traces.compact",
			expectedConsumerGroup: "lakerunner.compact.traces",
			expectedServiceType:   "compact-traces",
		},
		{
			name:                  "segments metrics rollup",
			topicKey:              TopicSegmentsMetricsRollup,
			expectedTopicName:     "lakerunner.segments.metrics.rollup",
			expectedConsumerGroup: "lakerunner.rollup.metrics",
			expectedServiceType:   "rollup-metrics",
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

	// Should have mappings for all service types
	expectedServiceTypes := []string{
		"ingest-logs", "ingest-metrics", "ingest-traces",
		"compact-logs", "compact-metrics", "compact-traces",
		"rollup-metrics",
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
		{"LOGS", "lakerunner.objstore.ingest.logs"}, // Test case insensitive
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

	// Should contain all expected topics
	expectedTopics := []string{
		"lakerunner.objstore.ingest.logs",
		"lakerunner.objstore.ingest.metrics",
		"lakerunner.objstore.ingest.traces",
		"lakerunner.segments.logs.compact",
		"lakerunner.segments.metrics.compact",
		"lakerunner.segments.traces.compact",
		"lakerunner.segments.metrics.rollup",
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

	// Test successful lookups
	topic, found := registry.GetTopicByServiceType("ingest-logs")
	assert.True(t, found, "Should find topic for ingest-logs service")
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