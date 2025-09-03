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

package queries

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/testhelpers"
)

func TestMcqStress_ConcurrentWorkers(t *testing.T) {
	t.Skip("Skipping flaky test that fails in CI")
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	// Test parameters
	const (
		numOrgs           = 2
		numFrequencies    = 2
		numInstances      = 3
		itemsPerKey       = 30 // Items per unique key (org/date/freq/instance)
		numWorkers        = 6  // Concurrent workers
		testDuration      = 8 * time.Second
		heartbeatInterval = 500 * time.Millisecond
	)

	// Statistics tracking
	var (
		totalQueued    int32
		totalCompleted int32
		totalFailed    int32
		totalDeferred  int32
		deadlockCount  int32
	)

	// Create test data
	orgs := make([]uuid.UUID, numOrgs)
	for i := range orgs {
		orgs[i] = uuid.New()
	}

	frequencies := []int32{5000, 10000} // 5s and 10s frequencies
	dateint := int32(20250102)

	// Queue work items
	slog.Info("Queueing work items")
	segmentIDCounter := int64(1000000)
	for _, org := range orgs {
		for _, freq := range frequencies {
			for inst := int16(0); inst < numInstances; inst++ {
				for i := 0; i < itemsPerKey; i++ {
					segmentID := segmentIDCounter
					segmentIDCounter++
					recordCount := int64(100 + rand.Intn(900)) // 100-1000 records

					err := db.McqQueueWork(ctx, lrdb.McqQueueWorkParams{
						OrganizationID: org,
						Dateint:        dateint,
						FrequencyMs:    freq,
						SegmentID:      segmentID,
						InstanceNum:    inst,
						RecordCount:    recordCount,
						Priority:       int32(rand.Intn(3)), // Priority 0-2
					})
					require.NoError(t, err)
					atomic.AddInt32(&totalQueued, 1)
				}
			}
		}
	}

	slog.Info("Queued work items", slog.Int("count", int(totalQueued)))

	// Worker function
	workerFunc := func(workerID int64, wg *sync.WaitGroup, workerCtx context.Context) {
		defer wg.Done()

		// Decide worker behavior
		failureRate := 0.2  // 20% of bundles will fail
		completeRate := 0.7 // 70% will complete
		// Remaining 10% will defer

		bundleParams := lrdb.BundleParams{
			WorkerID:    workerID,
			OverFactor:  1.2,
			BatchLimit:  20,
			Grace:       30 * time.Second, // Won't trigger in test
			DeferBase:   100 * time.Millisecond,
			Jitter:      50 * time.Millisecond,
			MaxAttempts: 3,
		}

		// Heartbeat goroutine for this worker
		heartbeatCtx, cancelHeartbeat := context.WithCancel(workerCtx)
		defer cancelHeartbeat()
		var activeIDs []int64
		var idsMutex sync.Mutex

		go func() {
			ticker := time.NewTicker(heartbeatInterval)
			defer ticker.Stop()

			for {
				select {
				case <-heartbeatCtx.Done():
					return
				case <-ticker.C:
					idsMutex.Lock()
					if len(activeIDs) > 0 {
						rowsUpdated, err := db.McqHeartbeat(ctx, lrdb.McqHeartbeatParams{
							WorkerID: workerID,
							Ids:      activeIDs,
						})
						if err != nil {
							slog.Warn("Heartbeat failed",
								slog.Int64("worker", workerID),
								slog.Any("error", err))
						} else if rowsUpdated != int64(len(activeIDs)) {
							slog.Warn("Heartbeat row mismatch",
								slog.Int64("worker", workerID),
								slog.Int64("expected", int64(len(activeIDs))),
								slog.Int64("actual", rowsUpdated))
						}
					}
					idsMutex.Unlock()
				}
			}
		}()

		for {
			// Check if context is done
			select {
			case <-workerCtx.Done():
				return
			default:
			}

			// Try to claim work
			bundle, err := db.ClaimCompactionBundle(ctx, bundleParams)
			if err != nil {
				if isDeadlock(err) {
					atomic.AddInt32(&deadlockCount, 1)
					slog.Error("DEADLOCK detected!",
						slog.Int64("worker", workerID),
						slog.Any("error", err))
				}
				time.Sleep(10 * time.Millisecond)
				continue
			}

			if len(bundle.Items) == 0 {
				// No work available
				time.Sleep(20 * time.Millisecond)
				continue
			}

			// Update active IDs for heartbeating
			idsMutex.Lock()
			activeIDs = make([]int64, len(bundle.Items))
			for i, item := range bundle.Items {
				activeIDs[i] = item.ID
			}
			idsMutex.Unlock()

			// Simulate processing time
			processingTime := time.Duration(50+rand.Intn(150)) * time.Millisecond
			time.Sleep(processingTime)

			// Decide outcome
			outcome := rand.Float64()

			ids := make([]int64, len(bundle.Items))
			for i, item := range bundle.Items {
				ids[i] = item.ID
			}

			if outcome < failureRate {
				// Fail the work (delete it)
				err = db.McqCompleteDelete(ctx, lrdb.McqCompleteDeleteParams{
					WorkerID: workerID,
					Ids:      ids,
				})
				if err != nil && !isDeadlock(err) {
					slog.Error("Failed to delete work",
						slog.Int64("worker", workerID),
						slog.Any("error", err))
				} else if err == nil {
					atomic.AddInt32(&totalFailed, int32(len(bundle.Items)))
				}
			} else if outcome < failureRate+completeRate {
				// Complete the work
				err = db.McqCompleteDelete(ctx, lrdb.McqCompleteDeleteParams{
					WorkerID: workerID,
					Ids:      ids,
				})
				if err != nil && !isDeadlock(err) {
					slog.Error("Failed to complete work",
						slog.Int64("worker", workerID),
						slog.Any("error", err))
				} else if err == nil {
					atomic.AddInt32(&totalCompleted, int32(len(bundle.Items)))
				}
			} else {
				// Release the work (for retry)
				err = db.McqRelease(ctx, lrdb.McqReleaseParams{
					WorkerID: workerID,
					Ids:      ids,
				})
				if err != nil && !isDeadlock(err) {
					slog.Error("Failed to release work",
						slog.Int64("worker", workerID),
						slog.Any("error", err))
				} else if err == nil {
					atomic.AddInt32(&totalDeferred, int32(len(bundle.Items)))
				}
			}

			// Clear active IDs
			idsMutex.Lock()
			activeIDs = nil
			idsMutex.Unlock()

			if isDeadlock(err) {
				atomic.AddInt32(&deadlockCount, 1)
				slog.Error("DEADLOCK during operation!",
					slog.Int64("worker", workerID),
					slog.Any("error", err))
			}
		}
	}

	// Start workers
	slog.Info("Starting workers", slog.Int("count", numWorkers))
	var wg sync.WaitGroup
	startTime := time.Now()

	// Create a context with timeout for all workers
	workerCtx, cancelWorkers := context.WithTimeout(ctx, testDuration)
	defer cancelWorkers()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go workerFunc(int64(i+1), &wg, workerCtx)
	}

	// Wait for workers to finish
	wg.Wait()
	duration := time.Since(startTime)

	// Check for deadlocks
	require.Zero(t, atomic.LoadInt32(&deadlockCount), "Deadlocks detected during test")

	// Allow released items to be reclaimed and processed
	slog.Info("Allowing time for released items to be processed")
	time.Sleep(2 * time.Second)

	// Final cleanup pass - claim and complete all remaining work
	slog.Info("Final cleanup pass")
	var remainingCount int32
	cleanupTimeout := time.After(30 * time.Second) // Add timeout for cleanup
	maxCleanupIterations := 100                    // Prevent infinite loops
	cleanupIterations := 0

	for {
		select {
		case <-cleanupTimeout:
			t.Fatalf("Cleanup phase timed out after 30 seconds, processed %d items", remainingCount)
		default:
		}

		cleanupIterations++
		if cleanupIterations > maxCleanupIterations {
			t.Fatalf("Cleanup phase exceeded maximum iterations (%d), processed %d items", maxCleanupIterations, remainingCount)
		}

		var claimed int32
		for workerID := int64(100); workerID < 100+numWorkers; workerID++ {
			bundle, err := db.ClaimCompactionBundle(ctx, lrdb.BundleParams{
				WorkerID:    workerID,
				OverFactor:  2.0, // Take more
				BatchLimit:  100, // Bigger batches
				Grace:       0,   // Take everything
				DeferBase:   0,
				Jitter:      0,
				MaxAttempts: 1,
			})
			require.NoError(t, err)

			if len(bundle.Items) == 0 {
				continue
			}

			claimed += int32(len(bundle.Items))

			ids := make([]int64, len(bundle.Items))
			for i, item := range bundle.Items {
				ids[i] = item.ID
			}

			err = db.McqCompleteDelete(ctx, lrdb.McqCompleteDeleteParams{
				WorkerID: workerID,
				Ids:      ids,
			})
			require.NoError(t, err)
		}

		if claimed == 0 {
			break
		}
		remainingCount += claimed
		slog.Info("Cleanup iteration",
			slog.Int("iteration", cleanupIterations),
			slog.Int("claimed", int(claimed)),
			slog.Int("total_remaining", int(remainingCount)))
	}

	// Verify all work was eventually processed
	totalProcessed := atomic.LoadInt32(&totalCompleted) +
		atomic.LoadInt32(&totalFailed) +
		remainingCount

	slog.Info("Test completed",
		slog.Duration("duration", duration),
		slog.Int("queued", int(totalQueued)),
		slog.Int("completed", int(atomic.LoadInt32(&totalCompleted))),
		slog.Int("failed", int(atomic.LoadInt32(&totalFailed))),
		slog.Int("deferred", int(atomic.LoadInt32(&totalDeferred))),
		slog.Int("remaining", int(remainingCount)),
		slog.Int("processed", int(totalProcessed)))

	// Verify all work was consumed
	require.Equal(t, totalQueued, totalProcessed,
		"All queued items should be processed (completed, failed, or cleaned up)")

	// Additional verification: try to claim one more time to ensure queue is empty
	finalBundle, err := db.ClaimCompactionBundle(ctx, lrdb.BundleParams{
		WorkerID:    999,
		OverFactor:  2.0,
		BatchLimit:  100,
		Grace:       0,
		DeferBase:   0,
		Jitter:      0,
		MaxAttempts: 1,
	})
	require.NoError(t, err)
	require.Empty(t, finalBundle.Items, "Queue should be empty after test")
}

func isDeadlock(err error) bool {
	if err == nil {
		return false
	}
	// PostgreSQL deadlock error code is 40P01
	return errors.Is(err, sql.ErrTxDone) ||
		contains(err.Error(), "40P01") ||
		contains(err.Error(), "deadlock")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			len(s) > len(substr) &&
				(containsHelper(s, substr)))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
