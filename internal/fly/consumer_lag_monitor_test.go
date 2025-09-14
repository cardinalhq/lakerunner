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

	"github.com/cardinalhq/lakerunner/config"
	"github.com/stretchr/testify/assert"
)

func TestNewConsumerLagMonitor(t *testing.T) {
	kafkaConfig := &Config{
		Brokers:     []string{"localhost:9092"},
		SASLEnabled: false,
		TLSEnabled:  false,
	}

	// Create test app config with test topic registry
	testAppConfig := &config.Config{
		TopicRegistry: config.NewTopicRegistry("test"),
	}

	monitor, err := NewConsumerLagMonitorWithAppConfig(kafkaConfig, time.Minute, testAppConfig)
	assert.NoError(t, err)

	assert.NotNil(t, monitor)
	assert.NotNil(t, monitor.adminClient)
	assert.Equal(t, time.Minute, monitor.pollInterval)
	assert.NotNil(t, monitor.lastMetrics)

	// Check that service mappings are populated
	mappings := monitor.GetServiceMappings()
	assert.Greater(t, len(mappings), 0)

	// Verify some expected mappings
	foundIngestLogs := false
	foundBoxerCompactLogs := false

	for _, mapping := range mappings {
		if mapping.ServiceType == "ingest-logs" {
			assert.Equal(t, "test.objstore.ingest.logs", mapping.Topic)
			assert.Equal(t, "test.ingest.logs", mapping.ConsumerGroup)
			foundIngestLogs = true
		}
		if mapping.ServiceType == "boxer-compact-logs" {
			assert.Equal(t, "test.boxer.logs.compact", mapping.Topic)
			assert.Equal(t, "test.boxer.logs.compact", mapping.ConsumerGroup)
			foundBoxerCompactLogs = true
		}
	}

	assert.True(t, foundIngestLogs, "Should have ingest-logs mapping")
	assert.True(t, foundBoxerCompactLogs, "Should have boxer-compact-logs mapping")
}

func TestConsumerLagMonitor_GetQueueDepth(t *testing.T) {
	kafkaConfig := &Config{
		Brokers:     []string{"localhost:9092"},
		SASLEnabled: false,
		TLSEnabled:  false,
	}

	// Create test app config with test topic registry
	testAppConfig := &config.Config{
		TopicRegistry: config.NewTopicRegistry("test"),
	}

	monitor, err := NewConsumerLagMonitorWithAppConfig(kafkaConfig, time.Minute, testAppConfig)
	assert.NoError(t, err)

	// Test with no data initially - should return 0
	lag, err := monitor.GetQueueDepth("ingest-logs")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), lag)

	// Test with unknown service type - should return error
	lag, err = monitor.GetQueueDepth("unknown-service")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported service type")
	assert.Equal(t, int64(0), lag)
}

func TestConsumerLagMonitor_IsHealthy(t *testing.T) {
	kafkaConfig := &Config{
		Brokers:     []string{"localhost:9092"},
		SASLEnabled: false,
		TLSEnabled:  false,
	}

	// Create test app config with test topic registry
	testAppConfig := &config.Config{
		TopicRegistry: config.NewTopicRegistry("test"),
	}

	monitor, err := NewConsumerLagMonitorWithAppConfig(kafkaConfig, time.Second, testAppConfig)
	assert.NoError(t, err)

	// Should be healthy initially (no errors, no updates expected yet)
	assert.True(t, monitor.IsHealthy())

	// Simulate an error and old timestamp
	monitor.mu.Lock()
	monitor.lastError = assert.AnError
	monitor.lastUpdate = time.Now().Add(-10 * time.Second) // Old update
	monitor.mu.Unlock()

	// Should not be healthy with persistent errors
	assert.False(t, monitor.IsHealthy())
}

func TestConsumerLagMonitor_ServiceMappings(t *testing.T) {
	kafkaConfig := &Config{
		Brokers:     []string{"localhost:9092"},
		SASLEnabled: false,
		TLSEnabled:  false,
	}

	// Create test app config with test topic registry
	testAppConfig := &config.Config{
		TopicRegistry: config.NewTopicRegistry("test"),
	}

	monitor, err := NewConsumerLagMonitorWithAppConfig(kafkaConfig, time.Minute, testAppConfig)
	assert.NoError(t, err)
	mappings := monitor.GetServiceMappings()

	// Check that all expected service types are present
	expectedServiceTypes := []string{
		"ingest-logs", "ingest-metrics", "ingest-traces",
		"compact-logs", "compact-metrics", "compact-traces",
		"rollup-metrics",
		"boxer-compact-logs", "boxer-compact-metrics", "boxer-compact-traces", "boxer-rollup-metrics",
	}

	foundServices := make(map[string]bool)
	for _, mapping := range mappings {
		foundServices[mapping.ServiceType] = true
	}

	for _, expectedService := range expectedServiceTypes {
		assert.True(t, foundServices[expectedService], "Should have mapping for %s", expectedService)
	}
}
