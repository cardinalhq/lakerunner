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

//go:build integration
// +build integration

package workqueue_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/dbopen"
	"github.com/cardinalhq/lakerunner/internal/workqueue"
	"github.com/cardinalhq/lakerunner/lrdb"
)

const (
	maxRetries = 3 // Items that fail more than this are considered permanently failed
)

type workOutcome struct {
	workID    int64
	completed bool
	failed    bool
	tries     int32
}

// TestWorkQueueTorture is a high-concurrency stress test that:
// - Rapidly enqueues many work items
// - Spawns multiple concurrent workers
// - Randomly completes or fails work (with retries)
// - Verifies all items are accounted for at the end
func TestWorkQueueTorture(t *testing.T) {
	ctx := context.Background()

	// Connect to test database
	pool, err := lrdb.ConnectTolrdb(ctx, dbopen.SkipMigrationCheck())
	require.NoError(t, err)
	defer pool.Close()

	// Clean up work queue before test
	_, err = pool.Exec(ctx, "DELETE FROM work_queue WHERE task_name = 'torture-test'")
	require.NoError(t, err)

	store := lrdb.New(pool)
	testOrgID := uuid.New()

	// Test parameters
	numWorkItems := 500
	numWorkers := 10
	failureRate := 0.3 // 30% chance of failure

	// Track outcomes
	var outcomesLock sync.Mutex
	outcomes := make(map[int64]*workOutcome)
	var completedCount, permanentlyFailedCount atomic.Int32

	// Phase 1: Rapidly enqueue work items
	t.Logf("Phase 1: Enqueuing %d work items", numWorkItems)
	enqueueStart := time.Now()

	var enqueueWg sync.WaitGroup
	enqueueConcurrency := 20

	for i := range enqueueConcurrency {
		enqueueWg.Add(1)
		go func(workerID int) {
			defer enqueueWg.Done()
			itemsPerWorker := numWorkItems / enqueueConcurrency
			start := workerID * itemsPerWorker
			end := start + itemsPerWorker
			if workerID == enqueueConcurrency-1 {
				end = numWorkItems // Last worker gets remainder
			}

			for j := start; j < end; j++ {
				spec := map[string]any{
					"item_num": j,
					"data":     fmt.Sprintf("work-item-%d", j),
				}
				workID, err := workqueue.Add(ctx, store, "torture-test", testOrgID, int16(j%10), spec)
				if err != nil {
					t.Errorf("Failed to enqueue work item %d: %v", j, err)
					continue
				}

				outcomesLock.Lock()
				outcomes[workID] = &workOutcome{workID: workID}
				outcomesLock.Unlock()
			}
		}(i)
	}

	enqueueWg.Wait()
	t.Logf("Phase 1 complete: Enqueued %d items in %v", len(outcomes), time.Since(enqueueStart))

	// Verify queue depth
	depth, err := workqueue.Depth(ctx, store, "torture-test")
	require.NoError(t, err)
	assert.Equal(t, int64(numWorkItems), depth, "Queue depth should match enqueued items")

	// Phase 2: Spawn workers to process items
	t.Logf("Phase 2: Starting %d workers to process items", numWorkers)
	processStart := time.Now()

	var workerWg sync.WaitGroup
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	for i := range numWorkers {
		workerWg.Add(1)
		go func(workerID int) {
			defer workerWg.Done()

			mgr := workqueue.NewManager(
				store,
				int64(workerID+1000),
				"torture-test",
				workqueue.WithHeartbeatInterval(2*time.Second),
				workqueue.WithMaxRetries(maxRetries),
			)
			mgr.Run(workerCtx)

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for {
				work, err := mgr.RequestWork(workerCtx)
				if err != nil {
					if workerCtx.Err() != nil {
						return // Context canceled, worker shutting down
					}
					t.Logf("Worker %d: Error requesting work: %v", workerID, err)
					time.Sleep(10 * time.Millisecond)
					continue
				}

				if work == nil {
					// No work available
					time.Sleep(10 * time.Millisecond)
					continue
				}

				// Simulate some work
				processingTime := time.Duration(rng.Intn(5)) * time.Millisecond
				time.Sleep(processingTime)

				// Randomly decide to complete or fail
				shouldFail := rng.Float64() < failureRate

				if shouldFail {
					// Fail the work with a reason
					reason := fmt.Sprintf("simulated failure at try %d", work.Tries()+1)
					err = work.Fail(&reason)
					if err != nil {
						t.Errorf("Worker %d: Failed to mark work %d as failed: %v", workerID, work.ID(), err)
					}

					// Track permanent failures (database will set failed=true when tries reaches maxRetries)
					if work.Tries() >= maxRetries-1 {
						outcomesLock.Lock()
						if outcome, exists := outcomes[work.ID()]; exists {
							outcome.failed = true
							outcome.tries = work.Tries() + 1 // tries will be incremented by Fail()
						}
						outcomesLock.Unlock()
						permanentlyFailedCount.Add(1)
					}
				} else {
					// Complete successfully
					err = work.Complete()
					if err != nil {
						t.Errorf("Worker %d: Failed to complete work %d: %v", workerID, work.ID(), err)
					}

					outcomesLock.Lock()
					if outcome, exists := outcomes[work.ID()]; exists {
						outcome.completed = true
						outcome.tries = work.Tries()
					}
					outcomesLock.Unlock()
					completedCount.Add(1)
				}
			}
		}(i)
	}

	// Phase 2.5: While workers are running, enqueue MORE work to test concurrent enqueue/dequeue
	// Wait a bit for workers to start processing
	time.Sleep(100 * time.Millisecond)

	t.Logf("Phase 2.5: Enqueuing 100 additional items while workers are active")
	additionalItems := 100
	var additionalEnqueueWg sync.WaitGroup

	for i := range 5 {
		additionalEnqueueWg.Add(1)
		go func(workerID int) {
			defer additionalEnqueueWg.Done()
			itemsPerWorker := additionalItems / 5
			start := numWorkItems + (workerID * itemsPerWorker)
			end := start + itemsPerWorker

			for j := start; j < end; j++ {
				spec := map[string]any{
					"item_num": j,
					"data":     fmt.Sprintf("additional-work-item-%d", j),
				}
				workID, err := workqueue.Add(ctx, store, "torture-test", testOrgID, int16(j%10), spec)
				if err != nil {
					t.Errorf("Failed to enqueue additional work item %d: %v", j, err)
					continue
				}

				outcomesLock.Lock()
				outcomes[workID] = &workOutcome{workID: workID}
				outcomesLock.Unlock()
			}
		}(i)
	}

	additionalEnqueueWg.Wait()
	t.Logf("Phase 2.5 complete: Enqueued %d additional items while workers active", additionalItems)

	// Update total work items count
	totalWorkItems := numWorkItems + additionalItems

	// Wait for all work to be processed (poll queue depth)
	maxWaitTime := 60 * time.Second
	checkInterval := 100 * time.Millisecond
	deadline := time.Now().Add(maxWaitTime)

	for time.Now().Before(deadline) {
		depth, err := workqueue.Depth(ctx, store, "torture-test")
		require.NoError(t, err)

		completed := completedCount.Load()
		failed := permanentlyFailedCount.Load()
		t.Logf("Queue depth: %d, Completed: %d, Permanently failed: %d", depth, completed, failed)

		// Check if all work is accounted for (not just queue empty, but all completed or failed)
		totalProcessed := completed + failed
		if totalProcessed == int32(totalWorkItems) {
			t.Logf("All work accounted for: %d completed, %d failed", completed, failed)
			break
		}

		time.Sleep(checkInterval)
	}

	// Give a bit more time for any in-flight completions to finish
	time.Sleep(500 * time.Millisecond)

	// Stop workers
	cancelWorkers()
	workerWg.Wait()

	t.Logf("Phase 2 complete: Processed all items in %v", time.Since(processStart))

	// Phase 3: Verify all work is accounted for
	t.Logf("Phase 3: Verifying outcomes")

	finalDepth, err := workqueue.Depth(ctx, store, "torture-test")
	require.NoError(t, err)
	assert.Equal(t, int64(0), finalDepth, "Queue should be empty after processing")

	totalCompleted := completedCount.Load()
	totalFailed := permanentlyFailedCount.Load()
	totalProcessed := totalCompleted + totalFailed

	t.Logf("Results:")
	t.Logf("  Total work items: %d", totalWorkItems)
	t.Logf("  Initial batch: %d", numWorkItems)
	t.Logf("  Additional (enqueued while processing): %d", additionalItems)
	t.Logf("  Completed: %d", totalCompleted)
	t.Logf("  Permanently failed (exceeded %d retries): %d", maxRetries, totalFailed)
	t.Logf("  Total processed: %d", totalProcessed)

	assert.Equal(t, int32(totalWorkItems), totalProcessed, "All work items should be processed")

	// Verify each work item has an outcome
	outcomesLock.Lock()
	defer outcomesLock.Unlock()

	var completedItems, failedItems, unprocessedItems int
	for _, outcome := range outcomes {
		if outcome.completed {
			completedItems++
		} else if outcome.failed {
			failedItems++
		} else {
			unprocessedItems++
			t.Errorf("Work item %d was not processed", outcome.workID)
		}
	}

	assert.Equal(t, 0, unprocessedItems, "All work items should be processed")
	assert.Equal(t, int(totalCompleted), completedItems, "Completed count should match")
	assert.Equal(t, int(totalFailed), failedItems, "Failed count should match")

	t.Logf("Phase 3 complete: All work accounted for")
}

// TestWorkQueueConcurrentClaim tests that multiple workers can claim work concurrently
// without conflicts or lost work items
func TestWorkQueueConcurrentClaim(t *testing.T) {
	ctx := context.Background()

	pool, err := lrdb.ConnectTolrdb(ctx, dbopen.SkipMigrationCheck())
	require.NoError(t, err)
	defer pool.Close()

	// Clean up
	_, err = pool.Exec(ctx, "DELETE FROM work_queue WHERE task_name = 'concurrent-claim-test'")
	require.NoError(t, err)

	store := lrdb.New(pool)
	testOrgID := uuid.New()

	// Enqueue work items
	numItems := 100
	for i := range numItems {
		_, err := workqueue.Add(ctx, store, "concurrent-claim-test", testOrgID, int16(i), map[string]any{"num": i})
		require.NoError(t, err)
	}

	// Start many workers trying to claim simultaneously
	numWorkers := 20
	var claimedIDs sync.Map
	var wg sync.WaitGroup

	for i := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			mgr := workqueue.NewManager(store, int64(workerID+2000), "concurrent-claim-test")
			mgr.Run(ctx)

			for {
				work, err := mgr.RequestWork(ctx)
				if err != nil {
					t.Errorf("Worker %d: Error requesting work: %v", workerID, err)
					return
				}

				if work == nil {
					return // No more work
				}

				// Check if this work ID was already claimed by another worker
				if _, exists := claimedIDs.LoadOrStore(work.ID(), workerID); exists {
					t.Errorf("Work ID %d was claimed by multiple workers!", work.ID())
				}

				// Complete the work
				err = work.Complete()
				require.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify all items were claimed exactly once
	claimedCount := 0
	claimedIDs.Range(func(key, value interface{}) bool {
		claimedCount++
		return true
	})

	assert.Equal(t, numItems, claimedCount, "All items should be claimed exactly once")

	// Verify queue is empty
	depth, err := workqueue.Depth(ctx, store, "concurrent-claim-test")
	require.NoError(t, err)
	assert.Equal(t, int64(0), depth)
}

// TestWorkQueueHeartbeatRecovery tests that work items are recovered when a worker dies
// without completing them (simulated by not heartbeating)
func TestWorkQueueHeartbeatRecovery(t *testing.T) {
	ctx := context.Background()

	pool, err := lrdb.ConnectTolrdb(ctx, dbopen.SkipMigrationCheck())
	require.NoError(t, err)
	defer pool.Close()

	// Clean up
	_, err = pool.Exec(ctx, "DELETE FROM work_queue WHERE task_name = 'heartbeat-recovery-test'")
	require.NoError(t, err)

	store := lrdb.New(pool)
	testOrgID := uuid.New()

	// Enqueue work items
	numItems := 10
	for i := range numItems {
		_, err := workqueue.Add(ctx, store, "heartbeat-recovery-test", testOrgID, 0, map[string]any{"num": i})
		require.NoError(t, err)
	}

	// Worker 1: Claims work but "dies" (doesn't complete or heartbeat)
	mgr1 := workqueue.NewManager(store, 3000, "heartbeat-recovery-test",
		workqueue.WithHeartbeatInterval(30*time.Second)) // Long interval, won't heartbeat in time
	workerCtx1, cancel1 := context.WithCancel(ctx)
	mgr1.Run(workerCtx1)

	// Claim all items
	claimedItems := make([]*workqueue.WorkItem, 0, numItems)
	for range numItems {
		work, err := mgr1.RequestWork(workerCtx1)
		require.NoError(t, err)
		require.NotNil(t, work)
		claimedItems = append(claimedItems, work)
	}

	t.Logf("Worker 1 claimed %d items", len(claimedItems))

	// Verify queue appears empty (all claimed)
	depth, err := workqueue.Depth(ctx, store, "heartbeat-recovery-test")
	require.NoError(t, err)
	assert.Equal(t, int64(0), depth, "All items should be claimed")

	// Simulate worker death by canceling context (no more heartbeats)
	cancel1()

	// Wait for heartbeat timeout to expire
	t.Logf("Waiting for heartbeat timeout to expire")
	time.Sleep(2 * time.Second)

	// Run cleanup to recover dead worker's items (short heartbeat timeout for testing)
	t.Logf("Running cleanup to recover items from dead worker")
	err = workqueue.Cleanup(ctx, store, 1*time.Second)
	require.NoError(t, err)

	// Wait a bit for cleanup to take effect
	time.Sleep(100 * time.Millisecond)

	// Verify items are back in the queue
	depth, err = workqueue.Depth(ctx, store, "heartbeat-recovery-test")
	require.NoError(t, err)
	assert.Equal(t, int64(numItems), depth, "All items should be recovered")

	// Worker 2: Claims and completes the recovered work
	mgr2 := workqueue.NewManager(store, 3001, "heartbeat-recovery-test")
	mgr2.Run(ctx)

	recoveredCount := 0
	for range numItems {
		work, err := mgr2.RequestWork(ctx)
		require.NoError(t, err)
		if work == nil {
			break
		}
		err = work.Complete()
		require.NoError(t, err)
		recoveredCount++
	}

	assert.Equal(t, numItems, recoveredCount, "All items should be recovered and processed")

	// Verify queue is empty
	depth, err = workqueue.Depth(ctx, store, "heartbeat-recovery-test")
	require.NoError(t, err)
	assert.Equal(t, int64(0), depth)
}
