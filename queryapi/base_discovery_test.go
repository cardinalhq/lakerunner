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

package queryapi

import (
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBaseWorkerDiscovery_GetAllWorkers(t *testing.T) {
	base := &BaseWorkerDiscovery{}

	// Test empty workers
	workers, err := base.GetAllWorkers()
	require.NoError(t, err)
	assert.Empty(t, workers)

	// Test with workers
	testWorkers := []Worker{
		{IP: "10.0.0.1", Port: 8081},
		{IP: "10.0.0.2", Port: 8081},
		{IP: "10.0.0.3", Port: 8081},
	}
	base.SetWorkers(testWorkers)

	workers, err = base.GetAllWorkers()
	require.NoError(t, err)
	assert.Equal(t, testWorkers, workers)

	// Ensure returned slice is a copy (modifications don't affect original)
	workers[0].IP = "modified"
	actualWorkers := base.GetWorkers()
	assert.Equal(t, "10.0.0.1", actualWorkers[0].IP)
}

func TestBaseWorkerDiscovery_GetWorkersForSegments(t *testing.T) {
	base := &BaseWorkerDiscovery{}
	orgID := uuid.New()
	segmentIDs := []int64{1, 2, 3, 4, 5}

	// Test with no workers
	mappings, err := base.GetWorkersForSegments(orgID, segmentIDs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no workers available")
	assert.Nil(t, mappings)

	// Test with workers
	testWorkers := []Worker{
		{IP: "10.0.0.1", Port: 8081},
		{IP: "10.0.0.2", Port: 8081},
		{IP: "10.0.0.3", Port: 8081},
	}
	base.SetWorkers(testWorkers)

	mappings, err = base.GetWorkersForSegments(orgID, segmentIDs)
	require.NoError(t, err)
	assert.Len(t, mappings, len(segmentIDs))

	// Verify each segment is mapped to a worker
	for i, mapping := range mappings {
		assert.Equal(t, segmentIDs[i], mapping.SegmentID)
		assert.Contains(t, testWorkers, mapping.Worker)
	}
}

func TestBaseWorkerDiscovery_ConsistentHashing(t *testing.T) {
	base := &BaseWorkerDiscovery{}
	orgID := uuid.New()
	segmentIDs := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	testWorkers := []Worker{
		{IP: "10.0.0.1", Port: 8081},
		{IP: "10.0.0.2", Port: 8081},
		{IP: "10.0.0.3", Port: 8081},
	}
	base.SetWorkers(testWorkers)

	// Get initial mappings
	mappings1, err := base.GetWorkersForSegments(orgID, segmentIDs)
	require.NoError(t, err)

	// Get mappings again - should be identical (consistent)
	mappings2, err := base.GetWorkersForSegments(orgID, segmentIDs)
	require.NoError(t, err)

	assert.Equal(t, mappings1, mappings2, "Consistent hashing should produce identical results")

	// Test with different org ID - should produce different mappings
	differentOrgID := uuid.New()
	mappings3, err := base.GetWorkersForSegments(differentOrgID, segmentIDs)
	require.NoError(t, err)

	// Should have different distribution for different org
	differentMappings := false
	for i := range mappings1 {
		if mappings1[i].Worker != mappings3[i].Worker {
			differentMappings = true
			break
		}
	}
	assert.True(t, differentMappings, "Different org IDs should produce different worker assignments")
}

func TestBaseWorkerDiscovery_WorkerDistribution(t *testing.T) {
	base := &BaseWorkerDiscovery{}
	orgID := uuid.New()

	// Test with many segments to verify distribution
	var segmentIDs []int64
	for i := int64(1); i <= 100; i++ {
		segmentIDs = append(segmentIDs, i)
	}

	testWorkers := []Worker{
		{IP: "10.0.0.1", Port: 8081},
		{IP: "10.0.0.2", Port: 8081},
		{IP: "10.0.0.3", Port: 8081},
	}
	base.SetWorkers(testWorkers)

	mappings, err := base.GetWorkersForSegments(orgID, segmentIDs)
	require.NoError(t, err)

	// Count assignments per worker
	workerCounts := make(map[Worker]int)
	for _, mapping := range mappings {
		workerCounts[mapping.Worker]++
	}

	// Each worker should get some segments (no worker should be unused)
	for _, worker := range testWorkers {
		count := workerCounts[worker]
		assert.Greater(t, count, 0, "Worker %v should have some segment assignments", worker)
	}

	// Distribution should be reasonably balanced (no worker gets more than 50% of segments)
	for worker, count := range workerCounts {
		percentage := float64(count) / float64(len(segmentIDs))
		assert.Less(t, percentage, 0.5, "Worker %v has too many segments (%d/%d = %.2f%%)", worker, count, len(segmentIDs), percentage*100)
	}
}

func TestBaseWorkerDiscovery_ThreadSafety(t *testing.T) {
	base := &BaseWorkerDiscovery{}

	// Test concurrent access
	var wg sync.WaitGroup
	numGoroutines := 10
	numOperations := 100

	// Start multiple goroutines doing concurrent operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()

			for j := 0; j < numOperations; j++ {
				// Concurrent worker updates
				workers := []Worker{
					{IP: "10.0.0.1", Port: 8081 + workerIndex},
					{IP: "10.0.0.2", Port: 8081 + workerIndex},
				}
				base.SetWorkers(workers)

				// Concurrent reads
				_, _ = base.GetAllWorkers()
				_ = base.GetWorkers()

				// Concurrent running state changes
				base.SetRunning(j%2 == 0)
				_ = base.IsRunning()
			}
		}(i)
	}

	wg.Wait()
	// If we get here without data races, the test passes
}

func TestBaseWorkerDiscovery_RunningState(t *testing.T) {
	base := &BaseWorkerDiscovery{}

	// Initial state should be false
	assert.False(t, base.IsRunning())

	// Set running to true
	base.SetRunning(true)
	assert.True(t, base.IsRunning())

	// Set running to false
	base.SetRunning(false)
	assert.False(t, base.IsRunning())
}

func TestBaseWorkerDiscovery_WorkerStateManagement(t *testing.T) {
	base := &BaseWorkerDiscovery{}

	// Test initial empty state
	workers := base.GetWorkers()
	assert.Empty(t, workers)

	// Test setting workers
	testWorkers := []Worker{
		{IP: "10.0.0.1", Port: 8081},
		{IP: "10.0.0.2", Port: 8082},
	}
	base.SetWorkers(testWorkers)

	workers = base.GetWorkers()
	assert.Equal(t, testWorkers, workers)

	// Test updating workers
	newWorkers := []Worker{
		{IP: "10.0.0.3", Port: 8083},
	}
	base.SetWorkers(newWorkers)

	workers = base.GetWorkers()
	assert.Equal(t, newWorkers, workers)
	assert.Len(t, workers, 1)

	// Test setting empty workers
	base.SetWorkers(nil)
	workers = base.GetWorkers()
	assert.Empty(t, workers)
}

func TestBaseWorkerDiscovery_SingleWorker(t *testing.T) {
	base := &BaseWorkerDiscovery{}
	orgID := uuid.New()
	segmentIDs := []int64{1, 2, 3, 4, 5}

	// Test with single worker - all segments should go to that worker
	singleWorker := []Worker{{IP: "10.0.0.1", Port: 8081}}
	base.SetWorkers(singleWorker)

	mappings, err := base.GetWorkersForSegments(orgID, segmentIDs)
	require.NoError(t, err)
	assert.Len(t, mappings, len(segmentIDs))

	// All segments should be assigned to the single worker
	for _, mapping := range mappings {
		assert.Equal(t, singleWorker[0], mapping.Worker)
	}
}

func TestBaseWorkerDiscovery_EmptySegments(t *testing.T) {
	base := &BaseWorkerDiscovery{}
	orgID := uuid.New()

	testWorkers := []Worker{
		{IP: "10.0.0.1", Port: 8081},
		{IP: "10.0.0.2", Port: 8081},
	}
	base.SetWorkers(testWorkers)

	// Test with empty segment list
	mappings, err := base.GetWorkersForSegments(orgID, []int64{})
	require.NoError(t, err)
	assert.Empty(t, mappings)
}
