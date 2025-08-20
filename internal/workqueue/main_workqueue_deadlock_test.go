//go:build integration

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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/testhelpers"
)

// TestMainWorkQueueDeadlockTorture tests the main work_queue operations for deadlocks
// These operations involve both work_queue and signal_locks tables, creating deadlock potential
func TestMainWorkQueueDeadlockTorture(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	// Torture test configuration
	const (
		numWorkers          = 15  // High concurrency
		operationsPerWorker = 50  // Operations per worker
		testDurationSeconds = 20  // Test duration
		workItemSeedCount   = 100 // Pre-populate work items
		maxOperationTimeout = 3   // Detect hung operations
	)

	// Metrics tracking
	var (
		totalOps    int64
		successOps  int64
		deadlockOps int64
		timeoutOps  int64
		errorOps    int64
	)

	// Create work items for main work_queue operations
	orgID := uuid.New()
	t.Logf("Seeding %d main work_queue items", workItemSeedCount)

	// Create realistic work_queue items with time ranges and frequencies
	baseTime := time.Now()
	for i := 0; i < workItemSeedCount; i++ {
		// Create a time range for this work item
		start := baseTime.Add(time.Duration(i*60) * time.Second)
		end := start.Add(10 * time.Minute)
		tsRange := pgtype.Range[pgtype.Timestamptz]{
			Lower: pgtype.Timestamptz{Time: start, Valid: true},
			Upper: pgtype.Timestamptz{Time: end, Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Valid: true,
		}

		err := db.WorkQueueAddDirect(ctx, lrdb.WorkQueueAddParams{
			OrgID:      orgID,
			Instance:   int16(i % 3), // Vary instance numbers
			Dateint:    int32(20250819 + i%7), // Vary dates
			Frequency:  int32([]int{10000, 60000, 300000}[i%3]), // Common frequencies
			Signal:     lrdb.SignalEnumMetrics, // Use metrics signal
			Action:     lrdb.ActionEnumCompact, // Use compact action
			TsRange:    tsRange,
			RunnableAt: baseTime.Add(-time.Hour), // Make them runnable now
			Priority:   int32(i % 5), // Vary priorities
			SlotID:     int32(i % 10), // Vary slot IDs to create lock contention
		})
		require.NoError(t, err)
	}

	// Context with overall timeout
	testCtx, testCancel := context.WithTimeout(ctx, testDurationSeconds*time.Second)
	defer testCancel()

	// Error collection
	errorChan := make(chan error, numWorkers*operationsPerWorker)

	// Start torture workers
	var wg sync.WaitGroup

	t.Logf("Starting %d main work_queue torture workers for %d seconds", numWorkers, testDurationSeconds)

	for workerID := 1; workerID <= numWorkers; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			mainWorkQueueTortureWorker(testCtx, t, db, int64(id), orgID, operationsPerWorker,
				maxOperationTimeout, errorChan, &totalOps, &successOps)
		}(workerID)
	}

	// Monitor for live deadlocks while test runs
	deadlockDetected := make(chan bool, 1)
	go func() {
		monitorForMainWorkQueueDeadlocks(testCtx, t, db, deadlockDetected)
	}()

	// Wait for completion or deadlock detection
	done := make(chan bool, 1)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		t.Log("All main work_queue torture workers completed successfully")
	case <-deadlockDetected:
		testCancel() // Stop all workers
		<-done       // Wait for cleanup
		t.Error("DEADLOCK DETECTED: Live deadlock monitoring found blocked transactions in main work_queue operations")
	case <-testCtx.Done():
		t.Log("Main work_queue torture test completed by timeout")
		<-done // Wait for workers to finish
	}

	close(errorChan)

	// Analyze collected errors
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
		atomic.AddInt64(&errorOps, 1)

		if isDeadlockError(err) {
			atomic.AddInt64(&deadlockOps, 1)
		} else if isTimeoutError(err) {
			atomic.AddInt64(&timeoutOps, 1)
		}
	}

	// Report comprehensive results
	total := atomic.LoadInt64(&totalOps)
	success := atomic.LoadInt64(&successOps)
	deadlocks := atomic.LoadInt64(&deadlockOps)
	timeouts := atomic.LoadInt64(&timeoutOps)
	errorCount := atomic.LoadInt64(&errorOps)

	t.Logf("=== MAIN WORK_QUEUE TORTURE TEST RESULTS ===")
	t.Logf("Total operations attempted: %d", total)
	t.Logf("Successful operations: %d (%.1f%%)", success, float64(success)/float64(total)*100)
	t.Logf("Deadlock errors: %d", deadlocks)
	t.Logf("Timeout errors: %d", timeouts)
	t.Logf("Other errors: %d", errorCount-deadlocks-timeouts)
	t.Logf("Error rate: %.1f%%", float64(errorCount)/float64(total)*100)

	// Log sample errors for debugging
	if len(errors) > 0 {
		t.Logf("Sample errors (first 5):")
		for i, err := range errors {
			if i >= 5 {
				break
			}
			t.Logf("  %d: %v", i+1, err)
		}
	}

	// Assertions for deadlock detection
	assert.Zero(t, deadlocks, "DEADLOCK DETECTED: Found %d deadlock errors in main work_queue operations. This confirms deadlock issues when global advisory lock is removed.", deadlocks)

	// Allow some timeouts but not excessive
	assert.Less(t, timeouts, int64(total/10), "Excessive timeouts (%d) may indicate lock contention issues", timeouts)

	// Require reasonable success rate
	assert.Greater(t, float64(success)/float64(total), 0.6, "Success rate too low (%.1f%%), may indicate locking issues", float64(success)/float64(total)*100)
}

// mainWorkQueueTortureWorker performs aggressive concurrent main work_queue operations
func mainWorkQueueTortureWorker(ctx context.Context, t *testing.T, db lrdb.StoreFull, workerID int64, orgID uuid.UUID,
	maxOps int, timeoutSec int, errorChan chan<- error, totalOps, successOps *int64) {

	operationCount := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if operationCount >= maxOps {
				return
			}
		}

		atomic.AddInt64(totalOps, 1)
		operationCount++

		// Create operation timeout context
		opCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)

		// Vary operation types for main work_queue operations
		opType := operationCount % 4
		var err error

		switch opType {
		case 0, 1: // Claim and complete (most common pattern)
			work, claimErr := db.WorkQueueClaim(opCtx, lrdb.WorkQueueClaimParams{
				TargetFreqs: []int32{10000, 60000, 300000}, // Multiple frequencies
				Signal:      lrdb.SignalEnumMetrics,
				Action:      lrdb.ActionEnumCompact,
				MinPriority: 0,
				WorkerID:    workerID,
			})
			if claimErr == nil {
				// Simulate brief work processing
				time.Sleep(time.Millisecond * time.Duration(operationCount%3))

				err = db.WorkQueueComplete(opCtx, lrdb.WorkQueueCompleteParams{
					WorkerID: workerID,
					ID:       work.ID,
				})
			} else {
				err = claimErr
			}

		case 2: // Claim and delete
			work, claimErr := db.WorkQueueClaim(opCtx, lrdb.WorkQueueClaimParams{
				TargetFreqs: []int32{10000, 60000},
				Signal:      lrdb.SignalEnumMetrics,
				Action:      lrdb.ActionEnumCompact,
				MinPriority: 0,
				WorkerID:    workerID,
			})
			if claimErr == nil {
				err = db.WorkQueueDelete(opCtx, lrdb.WorkQueueDeleteParams{
					ID:       work.ID,
					WorkerID: workerID,
				})
			} else {
				err = claimErr
			}

		case 3: // Cleanup operations (affect multiple rows)
			interval := pgtype.Interval{Microseconds: 3600000000, Valid: true} // 1 hour
			_, err = db.WorkQueueCleanup(opCtx, interval)
		}

		cancel()

		if err != nil {
			errorChan <- err
		} else {
			atomic.AddInt64(successOps, 1)
		}
	}
}

// monitorForMainWorkQueueDeadlocks checks PostgreSQL for deadlocks in main work_queue operations
func monitorForMainWorkQueueDeadlocks(ctx context.Context, t *testing.T, db lrdb.StoreFull, deadlockDetected chan<- bool) {
	ticker := time.NewTicker(150 * time.Millisecond) // Check more frequently
	defer ticker.Stop()

	pool := testhelpers.SetupTestLRDB(t)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Query for lock waits that might indicate deadlock in work_queue and signal_locks
			var lockWaits int
			err := pool.QueryRow(ctx, `
				SELECT COUNT(*) 
				FROM pg_stat_activity 
				WHERE wait_event_type = 'Lock' 
				  AND state = 'active'
				  AND (query LIKE '%work_queue%' OR query LIKE '%signal_locks%')
				  AND query_start < NOW() - INTERVAL '1 seconds'
			`).Scan(&lockWaits)

			if err == nil && lockWaits > 3 {
				t.Logf("WARNING: %d transactions waiting on work_queue/signal_locks locks for >1s", lockWaits)

				// Check for actual deadlocks in pg_stat_database
				var deadlocks int64
				err := pool.QueryRow(ctx, `
					SELECT deadlocks 
					FROM pg_stat_database 
					WHERE datname = current_database()
				`).Scan(&deadlocks)

				if err == nil && deadlocks > 0 {
					select {
					case deadlockDetected <- true:
					default:
					}
					return
				}
			}
		}
	}
}