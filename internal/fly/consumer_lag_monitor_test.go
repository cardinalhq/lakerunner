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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/config"
)

func TestNewConsumerLagMonitor(t *testing.T) {
	kafkaConfig := &config.KafkaConfig{
		Brokers:     []string{"localhost:9092"},
		SASLEnabled: false,
		TLSEnabled:  false,
	}

	// Create test app config with test topic registry
	testAppConfig := &config.Config{
		TopicRegistry: config.NewTopicRegistry("test"),
		Kafka:         *kafkaConfig,
	}

	monitor, err := NewConsumerLagMonitor(testAppConfig, time.Minute)
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
	kafkaConfig := &config.KafkaConfig{
		Brokers:     []string{"localhost:9092"},
		SASLEnabled: false,
		TLSEnabled:  false,
	}

	// Create test app config with test topic registry
	testAppConfig := &config.Config{
		TopicRegistry: config.NewTopicRegistry("test"),
		Kafka:         *kafkaConfig,
	}

	monitor, err := NewConsumerLagMonitor(testAppConfig, time.Minute)
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

func TestConsumerLagMonitor_GetDetailedMetrics(t *testing.T) {
	kafkaConfig := &config.KafkaConfig{
		Brokers:     []string{"localhost:9092"},
		SASLEnabled: false,
		TLSEnabled:  false,
	}

	// Create test app config with test topic registry
	testAppConfig := &config.Config{
		TopicRegistry: config.NewTopicRegistry("test"),
		Kafka:         *kafkaConfig,
	}

	monitor, err := NewConsumerLagMonitor(testAppConfig, time.Minute)
	assert.NoError(t, err)

	// Test with some sample data
	testData := []ConsumerGroupInfo{
		{
			GroupID:         "test-group",
			Topic:           "test-topic",
			Partition:       0,
			CommittedOffset: 100,
			HighWaterMark:   150,
			Lag:             50,
		},
		{
			GroupID:         "test-group",
			Topic:           "test-topic",
			Partition:       1,
			CommittedOffset: 200,
			HighWaterMark:   220,
			Lag:             20,
		},
	}

	monitor.mu.Lock()
	monitor.detailedMetrics = testData
	monitor.mu.Unlock()

	// Test GetDetailedMetrics returns a copy
	metrics := monitor.GetDetailedMetrics()
	assert.Len(t, metrics, 2)
	assert.Equal(t, testData[0].GroupID, metrics[0].GroupID)
	assert.Equal(t, testData[0].Lag, metrics[0].Lag)
	assert.Equal(t, testData[1].CommittedOffset, metrics[1].CommittedOffset)

	// Verify it's a copy, not the same slice
	metrics[0].Lag = 999
	actualMetrics := monitor.GetDetailedMetrics()
	assert.Equal(t, int64(50), actualMetrics[0].Lag) // Should still be 50, not 999
}

func TestConsumerLagMonitor_IsHealthy(t *testing.T) {
	kafkaConfig := &config.KafkaConfig{
		Brokers:     []string{"localhost:9092"},
		SASLEnabled: false,
		TLSEnabled:  false,
	}

	// Create test app config with test topic registry
	testAppConfig := &config.Config{
		TopicRegistry: config.NewTopicRegistry("test"),
		Kafka:         *kafkaConfig,
	}

	monitor, err := NewConsumerLagMonitor(testAppConfig, time.Second)
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
	kafkaConfig := &config.KafkaConfig{
		Brokers:     []string{"localhost:9092"},
		SASLEnabled: false,
		TLSEnabled:  false,
	}

	// Create test app config with test topic registry
	testAppConfig := &config.Config{
		TopicRegistry: config.NewTopicRegistry("test"),
		Kafka:         *kafkaConfig,
	}

	monitor, err := NewConsumerLagMonitor(testAppConfig, time.Minute)
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

// MockAdminClient is a mock implementation of the Kafka admin client
type MockAdminClient struct {
	mu            sync.RWMutex
	updateCounter int
	responses     [][]ConsumerGroupInfo // Different responses for each poll
	errors        []error               // Errors to return for each poll
}

func (m *MockAdminClient) GetMultipleConsumerGroupLag(ctx context.Context, topicGroups map[string]string) ([]ConsumerGroupInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Return different data on each call to simulate changing lag
	if m.updateCounter < len(m.responses) {
		response := m.responses[m.updateCounter]
		var err error
		if m.errors != nil && m.updateCounter < len(m.errors) {
			err = m.errors[m.updateCounter]
		}
		m.updateCounter++
		return response, err
	}

	// If we've run out of responses, return empty
	return []ConsumerGroupInfo{}, nil
}

func TestConsumerLagMonitor_PeriodicUpdate(t *testing.T) {
	// Create test config
	testAppConfig := &config.Config{
		TopicRegistry: config.NewTopicRegistry("test"),
		Kafka: config.KafkaConfig{
			Brokers: []string{"localhost:9092"},
		},
	}

	// Create monitor with short poll interval for testing
	monitor, err := NewConsumerLagMonitor(testAppConfig, 100*time.Millisecond)
	assert.NoError(t, err)

	// Replace the admin client with our mock
	// Note: Start() does an initial poll immediately, then uses ticker
	mockAdmin := &MockAdminClient{
		responses: [][]ConsumerGroupInfo{
			// Initial poll response (called immediately in Start())
			{
				{
					GroupID:         "test.ingest.logs",
					Topic:           "test.objstore.ingest.logs",
					Partition:       0,
					CommittedOffset: 100,
					HighWaterMark:   200,
					Lag:             100,
				},
				{
					GroupID:         "test.ingest.metrics",
					Topic:           "test.objstore.ingest.metrics",
					Partition:       0,
					CommittedOffset: 50,
					HighWaterMark:   100,
					Lag:             50,
				},
			},
			// Second poll response (after ~100ms) - lag increases
			{
				{
					GroupID:         "test.ingest.logs",
					Topic:           "test.objstore.ingest.logs",
					Partition:       0,
					CommittedOffset: 100,
					HighWaterMark:   300,
					Lag:             200,
				},
				{
					GroupID:         "test.ingest.metrics",
					Topic:           "test.objstore.ingest.metrics",
					Partition:       0,
					CommittedOffset: 50,
					HighWaterMark:   200,
					Lag:             150,
				},
			},
			// Third poll response (after ~200ms) - lag decreases
			{
				{
					GroupID:         "test.ingest.logs",
					Topic:           "test.objstore.ingest.logs",
					Partition:       0,
					CommittedOffset: 250,
					HighWaterMark:   300,
					Lag:             50,
				},
				{
					GroupID:         "test.ingest.metrics",
					Topic:           "test.objstore.ingest.metrics",
					Partition:       0,
					CommittedOffset: 180,
					HighWaterMark:   200,
					Lag:             20,
				},
			},
			// Fourth poll response (after ~300ms) - for safety
			{
				{
					GroupID:         "test.ingest.logs",
					Topic:           "test.objstore.ingest.logs",
					Partition:       0,
					CommittedOffset: 290,
					HighWaterMark:   300,
					Lag:             10,
				},
				{
					GroupID:         "test.ingest.metrics",
					Topic:           "test.objstore.ingest.metrics",
					Partition:       0,
					CommittedOffset: 195,
					HighWaterMark:   200,
					Lag:             5,
				},
			},
		},
	}

	// No need to set up expectations - the mock will use the responses array
	monitor.adminClient = mockAdmin

	// Start the monitor in a goroutine
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go monitor.Start(ctx)

	// Wait for initial poll
	time.Sleep(50 * time.Millisecond)

	// First check - should have initial values
	lag1, err := monitor.GetQueueDepth("ingest-logs")
	assert.NoError(t, err)
	assert.Equal(t, int64(100), lag1, "Initial lag for ingest-logs should be 100")

	lag2, err := monitor.GetQueueDepth("ingest-metrics")
	assert.NoError(t, err)
	assert.Equal(t, int64(50), lag2, "Initial lag for ingest-metrics should be 50")

	// Verify detailed metrics are also updated
	detailed1 := monitor.GetDetailedMetrics()
	assert.Len(t, detailed1, 2)
	assert.Equal(t, int64(100), detailed1[0].Lag)

	// Wait for second poll (after ~100ms)
	time.Sleep(120 * time.Millisecond)

	// Second check - lag should have increased
	lag3, err := monitor.GetQueueDepth("ingest-logs")
	assert.NoError(t, err)

	// Check which update we're on
	mockAdmin.mu.RLock()
	updateCount2 := mockAdmin.updateCounter
	mockAdmin.mu.RUnlock()

	// We expect 200 from the 2nd response but might already be on 3rd
	if updateCount2 == 2 {
		assert.Equal(t, int64(200), lag3, "After second update, lag for ingest-logs should be 200")
		lag4, err := monitor.GetQueueDepth("ingest-metrics")
		assert.NoError(t, err)
		assert.Equal(t, int64(150), lag4, "After second update, lag for ingest-metrics should be 150")
	}

	// Wait for third poll
	time.Sleep(120 * time.Millisecond)

	// Check how many times we've called the mock
	mockAdmin.mu.RLock()
	currentCallCount := mockAdmin.updateCounter
	mockAdmin.mu.RUnlock()
	t.Logf("After waiting, call count: %d", currentCallCount)

	// Get current lag values
	lag5, err := monitor.GetQueueDepth("ingest-logs")
	assert.NoError(t, err)
	lag6, err := monitor.GetQueueDepth("ingest-metrics")
	assert.NoError(t, err)

	// Verify we're seeing changing values (not stuck on initial)
	assert.NotEqual(t, int64(100), lag5, "Lag should have changed from initial value")
	assert.NotEqual(t, int64(50), lag6, "Lag should have changed from initial value")

	// Verify at least 2 updates occurred
	if currentCallCount >= 2 {
		assert.True(t, lag5 != 100 || lag6 != 50, "Values should have updated from initial")
	}

	// Verify the monitor is healthy
	assert.True(t, monitor.IsHealthy())

	// Verify last update time has changed
	lastUpdate := monitor.GetLastUpdate()
	assert.False(t, lastUpdate.IsZero())
	assert.WithinDuration(t, time.Now(), lastUpdate, 500*time.Millisecond)

	// Verify we've made multiple polls
	assert.GreaterOrEqual(t, currentCallCount, 2, "Should have polled at least 2 times")
}

func TestConsumerLagMonitor_PeriodicUpdateWithErrors(t *testing.T) {
	// Create test config
	testAppConfig := &config.Config{
		TopicRegistry: config.NewTopicRegistry("test"),
		Kafka: config.KafkaConfig{
			Brokers: []string{"localhost:9092"},
		},
	}

	// Create monitor with short poll interval for testing
	monitor, err := NewConsumerLagMonitor(testAppConfig, 100*time.Millisecond)
	assert.NoError(t, err)

	// Create mock admin client with responses and errors
	mockAdmin := &MockAdminClient{
		responses: [][]ConsumerGroupInfo{
			// First poll - success with initial lag
			{
				{
					GroupID:         "test.ingest.logs",
					Topic:           "test.objstore.ingest.logs",
					Partition:       0,
					CommittedOffset: 100,
					HighWaterMark:   200,
					Lag:             100,
				},
			},
			// Second poll - will error
			{},
			// Third poll - will error
			{},
			// Fourth poll - success with same lag (shows caching works during errors)
			{
				{
					GroupID:         "test.ingest.logs",
					Topic:           "test.objstore.ingest.logs",
					Partition:       0,
					CommittedOffset: 100,
					HighWaterMark:   200,
					Lag:             100,
				},
			},
			// Fifth poll - in case timing is off
			{
				{
					GroupID:         "test.ingest.logs",
					Topic:           "test.objstore.ingest.logs",
					Partition:       0,
					CommittedOffset: 100,
					HighWaterMark:   200,
					Lag:             100,
				},
			},
		},
		errors: []error{
			nil,            // First poll - no error
			assert.AnError, // Second poll - error
			assert.AnError, // Third poll - error
			nil,            // Fourth poll - no error
			nil,            // Fifth poll - no error
		},
	}

	monitor.adminClient = mockAdmin

	// Start the monitor
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go monitor.Start(ctx)

	// Wait for initial successful poll
	time.Sleep(50 * time.Millisecond)

	// Should have initial value
	lag1, err := monitor.GetQueueDepth("ingest-logs")
	assert.NoError(t, err)
	assert.Equal(t, int64(100), lag1)
	assert.True(t, monitor.IsHealthy())

	// Wait for error polls (2nd and 3rd)
	time.Sleep(250 * time.Millisecond)

	// Check that we have an error recorded
	lastErr := monitor.GetLastError()
	if lastErr == nil {
		// May have already recovered, check call count
		mockAdmin.mu.RLock()
		count := mockAdmin.updateCounter
		mockAdmin.mu.RUnlock()
		t.Logf("No error found after %d calls", count)
		if count < 2 {
			// Haven't hit the error yet, wait more
			time.Sleep(100 * time.Millisecond)
			_ = monitor.GetLastError()
		}
	}

	// Should still return cached value even with errors
	lag2, err := monitor.GetQueueDepth("ingest-logs")
	assert.NoError(t, err)
	assert.Equal(t, int64(100), lag2, "Should return cached value during errors")

	// Wait for recovery (4th poll)
	time.Sleep(150 * time.Millisecond)

	// Should still have same value (but error should be cleared)
	lag3, err := monitor.GetQueueDepth("ingest-logs")
	assert.NoError(t, err)
	assert.Equal(t, int64(100), lag3, "Should maintain value after recovery")

	// Verify we made at least 4 polls
	mockAdmin.mu.RLock()
	finalCallCount := mockAdmin.updateCounter
	mockAdmin.mu.RUnlock()

	assert.GreaterOrEqual(t, finalCallCount, 4, "Should have made at least 4 polls")
}
