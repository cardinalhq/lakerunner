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
	"log/slog"
	"sync"
	"time"
)

// ServiceMapping represents a mapping between service types and their Kafka topic/consumer group
type ServiceMapping struct {
	ServiceType   string
	Topic         string
	ConsumerGroup string
}

// ConsumerLagMonitor provides resilient monitoring of Kafka consumer lag
type ConsumerLagMonitor struct {
	adminClient     *AdminClient
	serviceMappings []ServiceMapping
	pollInterval    time.Duration

	mu          sync.RWMutex
	lastMetrics map[string]int64 // serviceType -> total lag
	lastUpdate  time.Time
	lastError   error
}

// NewConsumerLagMonitor creates a new consumer lag monitor
func NewConsumerLagMonitor(config *Config, pollInterval time.Duration) (*ConsumerLagMonitor, error) {
	// Default service mappings from README.md
	serviceMappings := []ServiceMapping{
		{"boxer-compact-logs", "lakerunner.boxer.logs.compact", "lakerunner.boxer.logs.compact"},
		{"boxer-compact-metrics", "lakerunner.boxer.metrics.compact", "lakerunner.boxer.metrics.compact"},
		{"boxer-rollup-metrics", "lakerunner.boxer.metrics.rollup", "lakerunner.boxer.metrics.rollup"},
		{"boxer-compact-traces", "lakerunner.boxer.traces.compact", "lakerunner.boxer.traces.compact"},
		{"compact-logs", "lakerunner.segments.logs.compact", "lakerunner.compact.logs"},
		{"compact-metrics", "lakerunner.segments.metrics.compact", "lakerunner.compact.metrics"},
		{"compact-traces", "lakerunner.segments.traces.compact", "lakerunner.compact.traces"},
		{"ingest-logs", "lakerunner.objstore.ingest.logs", "lakerunner.ingest.logs"},
		{"ingest-metrics", "lakerunner.objstore.ingest.metrics", "lakerunner.ingest.metrics"},
		{"ingest-traces", "lakerunner.objstore.ingest.traces", "lakerunner.ingest.traces"},
		{"rollup-metrics", "lakerunner.segments.metrics.rollup", "lakerunner.rollup.metrics"},
	}

	adminClient, err := NewAdminClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create admin client: %w", err)
	}

	return &ConsumerLagMonitor{
		adminClient:     adminClient,
		serviceMappings: serviceMappings,
		pollInterval:    pollInterval,
		lastMetrics:     make(map[string]int64),
	}, nil
}

// Start begins the periodic polling of consumer lag metrics
func (m *ConsumerLagMonitor) Start(ctx context.Context) {
	// Initial poll
	m.poll(ctx)

	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.poll(ctx)
		}
	}
}

// poll fetches the latest consumer lag metrics
func (m *ConsumerLagMonitor) poll(ctx context.Context) {
	// Build topic to groups mapping
	topicGroups := make(map[string]string)
	for _, mapping := range m.serviceMappings {
		topicGroups[mapping.Topic] = mapping.ConsumerGroup
	}

	// Fetch all consumer group lags
	lagInfos, err := m.adminClient.GetMultipleConsumerGroupLag(ctx, topicGroups)

	m.mu.Lock()
	defer m.mu.Unlock()

	if err != nil {
		m.lastError = err
		slog.Warn("Failed to fetch consumer lag metrics", "error", err)
		return
	}

	// Clear previous error on successful fetch
	m.lastError = nil
	m.lastUpdate = time.Now()

	// Calculate total lag per service type
	serviceLags := make(map[string]int64)

	for _, lagInfo := range lagInfos {
		// Find the service type for this topic/group combination
		for _, mapping := range m.serviceMappings {
			if mapping.Topic == lagInfo.Topic && mapping.ConsumerGroup == lagInfo.GroupID {
				serviceLags[mapping.ServiceType] += lagInfo.Lag
				break
			}
		}
	}

	// Update metrics
	m.lastMetrics = serviceLags

	slog.Debug("Updated consumer lag metrics",
		"services", len(serviceLags),
		"lastUpdate", m.lastUpdate)
}

// GetQueueDepth returns the total consumer lag for a service type
func (m *ConsumerLagMonitor) GetQueueDepth(serviceType string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check if we have a recent error
	if m.lastError != nil && time.Since(m.lastUpdate) > 5*m.pollInterval {
		return 0, fmt.Errorf("consumer lag monitor has persistent errors: %w", m.lastError)
	}

	// Check if this is a known service type
	found := false
	for _, mapping := range m.serviceMappings {
		if mapping.ServiceType == serviceType {
			found = true
			break
		}
	}
	if !found {
		return 0, fmt.Errorf("unsupported service type: %s", serviceType)
	}

	// Return cached value, 0 if not found (topic/group may not exist yet)
	lag, exists := m.lastMetrics[serviceType]
	if !exists {
		return 0, nil
	}

	return lag, nil
}

// GetLastUpdate returns when metrics were last successfully updated
func (m *ConsumerLagMonitor) GetLastUpdate() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastUpdate
}

// GetLastError returns the last error encountered
func (m *ConsumerLagMonitor) GetLastError() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastError
}

// IsHealthy returns true if the monitor is functioning properly
func (m *ConsumerLagMonitor) IsHealthy() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Consider healthy if we've had a successful update within the last 3 poll intervals
	// or if we've never had an error and haven't been running long enough
	timeSinceUpdate := time.Since(m.lastUpdate)
	return m.lastError == nil && (timeSinceUpdate < 3*m.pollInterval || m.lastUpdate.IsZero())
}

// GetServiceMappings returns the current service mappings
func (m *ConsumerLagMonitor) GetServiceMappings() []ServiceMapping {
	return m.serviceMappings
}
