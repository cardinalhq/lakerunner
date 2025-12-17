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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func TestQueueDepthMonitor_GetQueueDepth(t *testing.T) {
	monitor, err := NewQueueDepthMonitor(nil, time.Minute)
	require.NoError(t, err)

	// Set some test data with priorities
	monitor.mu.Lock()
	monitor.lastDepths = map[depthKey]int64{
		{TaskName: config.BoxerTaskIngestLogs, Priority: 0}:     100,
		{TaskName: config.BoxerTaskCompactMetrics, Priority: 0}: 50,
	}
	monitor.mu.Unlock()

	// Test getting known depths (sums across all priorities)
	depth, err := monitor.GetQueueDepth(config.BoxerTaskIngestLogs)
	assert.NoError(t, err)
	assert.Equal(t, int64(100), depth)

	depth, err = monitor.GetQueueDepth(config.BoxerTaskCompactMetrics)
	assert.NoError(t, err)
	assert.Equal(t, int64(50), depth)

	// Test getting depth for task with no entries (should return 0)
	depth, err = monitor.GetQueueDepth(config.BoxerTaskRollupMetrics)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), depth)

	// Test invalid task name
	_, err = monitor.GetQueueDepth("invalid-task")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported task name")
}

func TestQueueDepthMonitor_GetAllQueueDepths(t *testing.T) {
	monitor, err := NewQueueDepthMonitor(nil, time.Minute)
	require.NoError(t, err)

	// Set some test data with priorities
	monitor.mu.Lock()
	monitor.lastDepths = map[depthKey]int64{
		{TaskName: config.BoxerTaskIngestLogs, Priority: 0}:     100,
		{TaskName: config.BoxerTaskCompactMetrics, Priority: 0}: 50,
		{TaskName: config.BoxerTaskIngestLogs, Priority: 1000}:  25, // low priority items
	}
	monitor.mu.Unlock()

	// Get all depths
	depths := monitor.GetAllQueueDepths()

	// Should return 3 entries (one per unique task/priority combination)
	assert.Len(t, depths, 3)

	// Build a map for easier lookup
	type key struct {
		task     string
		priority int32
	}
	depthMap := make(map[key]int64)
	for _, d := range depths {
		depthMap[key{d.TaskName, d.Priority}] = d.Depth
	}

	assert.Equal(t, int64(100), depthMap[key{config.BoxerTaskIngestLogs, 0}])
	assert.Equal(t, int64(50), depthMap[key{config.BoxerTaskCompactMetrics, 0}])
	assert.Equal(t, int64(25), depthMap[key{config.BoxerTaskIngestLogs, 1000}])
}

func TestQueueDepthMonitor_IsHealthy(t *testing.T) {
	monitor, err := NewQueueDepthMonitor(nil, time.Second)
	require.NoError(t, err)

	// Initially healthy (no errors, no expectations)
	assert.True(t, monitor.IsHealthy())

	// Set recent update with no error
	monitor.mu.Lock()
	monitor.lastUpdate = time.Now()
	monitor.lastError = nil
	monitor.mu.Unlock()
	assert.True(t, monitor.IsHealthy())

	// Set error but recent update - still healthy
	monitor.mu.Lock()
	monitor.lastError = assert.AnError
	monitor.lastUpdate = time.Now()
	monitor.mu.Unlock()
	assert.True(t, monitor.IsHealthy())

	// Set error with old update - unhealthy
	monitor.mu.Lock()
	monitor.lastError = assert.AnError
	monitor.lastUpdate = time.Now().Add(-10 * time.Second)
	monitor.mu.Unlock()
	assert.False(t, monitor.IsHealthy())

	// Clear error - healthy again
	monitor.mu.Lock()
	monitor.lastError = nil
	monitor.mu.Unlock()
	assert.True(t, monitor.IsHealthy())
}

// MockDB for testing
type MockQueueDepthDB struct {
	depths []lrdb.WorkQueueDepthAllRow
	err    error
}

func (m *MockQueueDepthDB) WorkQueueClaim(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
	return lrdb.WorkQueue{}, nil
}

func (m *MockQueueDepthDB) WorkQueueComplete(ctx context.Context, arg lrdb.WorkQueueCompleteParams) error {
	return nil
}

func (m *MockQueueDepthDB) WorkQueueFail(ctx context.Context, arg lrdb.WorkQueueFailParams) (int32, error) {
	return 0, nil
}

func (m *MockQueueDepthDB) WorkQueueHeartbeat(ctx context.Context, arg lrdb.WorkQueueHeartbeatParams) error {
	return nil
}

func (m *MockQueueDepthDB) WorkQueueDepthAll(ctx context.Context) ([]lrdb.WorkQueueDepthAllRow, error) {
	return m.depths, m.err
}

func TestQueueDepthMonitor_UpdateDepths(t *testing.T) {
	mockDB := &MockQueueDepthDB{
		depths: []lrdb.WorkQueueDepthAllRow{
			{TaskName: config.BoxerTaskIngestLogs, Priority: 0, Depth: 100},
			{TaskName: config.BoxerTaskCompactMetrics, Priority: 0, Depth: 50},
			{TaskName: config.BoxerTaskRollupMetrics, Priority: 0, Depth: 25},
			{TaskName: config.BoxerTaskIngestLogs, Priority: 1000, Depth: 10}, // low priority
		},
	}

	monitor, err := NewQueueDepthMonitor(mockDB, time.Minute)
	require.NoError(t, err)

	// Update depths
	ctx := context.Background()
	err = monitor.updateDepths(ctx)
	assert.NoError(t, err)

	// Verify depths were cached (GetQueueDepth sums across all priorities)
	depth, err := monitor.GetQueueDepth(config.BoxerTaskIngestLogs)
	assert.NoError(t, err)
	assert.Equal(t, int64(110), depth) // 100 + 10

	depth, err = monitor.GetQueueDepth(config.BoxerTaskCompactMetrics)
	assert.NoError(t, err)
	assert.Equal(t, int64(50), depth)

	depth, err = monitor.GetQueueDepth(config.BoxerTaskRollupMetrics)
	assert.NoError(t, err)
	assert.Equal(t, int64(25), depth)

	// Tasks not in mock data should return 0
	depth, err = monitor.GetQueueDepth(config.BoxerTaskIngestMetrics)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), depth)

	// Verify last update was set
	assert.False(t, monitor.GetLastUpdate().IsZero())
	assert.Nil(t, monitor.GetLastError())
}

func TestQueueDepthMonitor_UpdateDepthsWithError(t *testing.T) {
	mockDB := &MockQueueDepthDB{
		err: assert.AnError,
	}

	monitor, err := NewQueueDepthMonitor(mockDB, time.Minute)
	require.NoError(t, err)

	// Update depths - should get error
	ctx := context.Background()
	err = monitor.updateDepths(ctx)
	assert.Error(t, err)

	// Verify error was cached
	assert.Equal(t, assert.AnError, monitor.GetLastError())
}
