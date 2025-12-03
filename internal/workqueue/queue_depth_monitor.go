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

package workqueue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/config"
)

// QueueDepthMonitor monitors PostgreSQL work queue depths and emits metrics
type QueueDepthMonitor struct {
	db           DB
	pollInterval time.Duration
	meter        metric.Meter
	gauge        metric.Int64ObservableGauge
	mu           sync.RWMutex
	lastDepths   map[string]int64
	lastUpdate   time.Time
	lastError    error
}

// QueueDepthByTask represents queue depth for a single task type
type QueueDepthByTask struct {
	TaskName string
	Depth    int64
}

// NewQueueDepthMonitor creates a new queue depth monitor
func NewQueueDepthMonitor(db DB, pollInterval time.Duration) (*QueueDepthMonitor, error) {
	meter := otel.Meter("lakerunner.workqueue")

	monitor := &QueueDepthMonitor{
		db:           db,
		pollInterval: pollInterval,
		meter:        meter,
		lastDepths:   make(map[string]int64),
	}

	// Create observable gauge that reads from cached values
	gauge, err := meter.Int64ObservableGauge(
		"lakerunner.workqueue.lag",
		metric.WithDescription("Number of unclaimed work items in the queue by task name"),
		metric.WithInt64Callback(monitor.observeQueueDepth),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create queue depth gauge: %w", err)
	}
	monitor.gauge = gauge

	return monitor, nil
}

// observeQueueDepth is the callback for OpenTelemetry metrics
func (m *QueueDepthMonitor) observeQueueDepth(ctx context.Context, observer metric.Int64Observer) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fmt.Printf("DEBUG: observeQueueDepth callback invoked, lastDepths=%v\n", m.lastDepths)

	// Report all known task types, even if depth is 0
	allTasks := []string{
		config.BoxerTaskIngestLogs,
		config.BoxerTaskIngestMetrics,
		config.BoxerTaskIngestTraces,
		config.BoxerTaskCompactLogs,
		config.BoxerTaskCompactMetrics,
		config.BoxerTaskCompactTraces,
		config.BoxerTaskRollupMetrics,
	}

	for _, taskName := range allTasks {
		depth := m.lastDepths[taskName] // defaults to 0 if not present
		observer.Observe(depth, metric.WithAttributes(
			attribute.String("task_name", taskName),
		))
	}

	return nil
}

// Start begins periodic polling of queue depths
func (m *QueueDepthMonitor) Start(ctx context.Context) error {
	// Do initial poll immediately
	if err := m.updateDepths(ctx); err != nil {
		// Log but don't fail startup
		fmt.Printf("WARN: Initial queue depth poll failed: %v\n", err)
	}

	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := m.updateDepths(ctx); err != nil {
				fmt.Printf("WARN: Failed to poll queue depths: %v\n", err)
			}
		}
	}
}

// updateDepths queries the database and updates cached depths
func (m *QueueDepthMonitor) updateDepths(ctx context.Context) error {
	rows, err := m.db.WorkQueueDepthAll(ctx)
	if err != nil {
		m.mu.Lock()
		m.lastError = err
		m.mu.Unlock()
		return fmt.Errorf("failed to query queue depths: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Clear old depths
	m.lastDepths = make(map[string]int64)

	// Update with current depths
	for _, row := range rows {
		m.lastDepths[row.TaskName] = row.Depth
	}

	m.lastUpdate = time.Now()
	m.lastError = nil

	return nil
}

// GetQueueDepth returns the cached queue depth for a specific task name
// This is used by KEDA scaler
func (m *QueueDepthMonitor) GetQueueDepth(taskName string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Validate task name
	switch taskName {
	case config.BoxerTaskIngestLogs,
		config.BoxerTaskIngestMetrics,
		config.BoxerTaskIngestTraces,
		config.BoxerTaskCompactLogs,
		config.BoxerTaskCompactMetrics,
		config.BoxerTaskCompactTraces,
		config.BoxerTaskRollupMetrics:
		// Valid task name
	default:
		return 0, fmt.Errorf("unsupported task name: %s", taskName)
	}

	return m.lastDepths[taskName], nil
}

// GetAllQueueDepths returns all cached queue depths
func (m *QueueDepthMonitor) GetAllQueueDepths() []QueueDepthByTask {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return all known task types
	allTasks := []string{
		config.BoxerTaskIngestLogs,
		config.BoxerTaskIngestMetrics,
		config.BoxerTaskIngestTraces,
		config.BoxerTaskCompactLogs,
		config.BoxerTaskCompactMetrics,
		config.BoxerTaskCompactTraces,
		config.BoxerTaskRollupMetrics,
	}

	result := make([]QueueDepthByTask, 0, len(allTasks))
	for _, taskName := range allTasks {
		result = append(result, QueueDepthByTask{
			TaskName: taskName,
			Depth:    m.lastDepths[taskName], // defaults to 0 if not present
		})
	}

	return result
}

// GetLastUpdate returns the timestamp of the last successful update
func (m *QueueDepthMonitor) GetLastUpdate() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastUpdate
}

// GetLastError returns the last error encountered
func (m *QueueDepthMonitor) GetLastError() error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastError
}

// IsHealthy returns true if the monitor is functioning properly
func (m *QueueDepthMonitor) IsHealthy() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Unhealthy if we have a persistent error and no recent updates
	if m.lastError != nil {
		return time.Since(m.lastUpdate) < 5*m.pollInterval
	}

	return true
}
