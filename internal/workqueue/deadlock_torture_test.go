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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/testhelpers"
)

// TestWorkQueueDeadlockTorture runs high-concurrency operations to detect deadlocks
// This test should PASS when the global advisory lock is in place
// It should FAIL (detect deadlocks) when the global advisory lock is removed
func TestWorkQueueDeadlockTorture(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	// Torture test configuration - aggressive settings to expose deadlocks
	const (
		numWorkers             = 20  // High concurrency
		operationsPerWorker    = 100 // Many operations per worker
		testDurationSeconds    = 30  // Run for 30 seconds
		workItemSeedCount      = 200 // Pre-populate work items
		maxOperationTimeoutSec = 3   // Detect hung operations
	)

	telemetryTypes := []string{"logs", "metrics", "traces"}

	// Metrics tracking
	var (
		totalOps      int64
		successOps    int64
		deadlockOps   int64
		timeoutOps    int64
		errorOps      int64
	)

	// Pre-populate work items to create contention
	orgID := uuid.New()
	t.Logf("Seeding %d work items across %d telemetry types", workItemSeedCount, len(telemetryTypes))
	
	for i := 0; i < workItemSeedCount; i++ {
		for _, telemetryType := range telemetryTypes {
			err := db.PutInqueueWork(ctx, lrdb.PutInqueueWorkParams{
				OrganizationID: orgID,
				CollectorName:  "torture-test",
				Bucket:         "torture-bucket",
				ObjectID:       fmt.Sprintf("object-%d-%s.json", i, telemetryType),
				TelemetryType:  telemetryType,
				Priority:       int32(i % 5), // Vary priorities to create lock ordering issues
			})
			require.NoError(t, err)
		}
	}

	// Context with overall timeout
	testCtx, testCancel := context.WithTimeout(ctx, testDurationSeconds*time.Second)
	defer testCancel()

	// Error collection
	errorChan := make(chan error, numWorkers*operationsPerWorker)
	
	// Start torture workers
	var wg sync.WaitGroup
	
	t.Logf("Starting %d torture workers for %d seconds", numWorkers, testDurationSeconds)
	
	for workerID := 1; workerID <= numWorkers; workerID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tortureWorker(testCtx, t, db, int64(id), orgID, telemetryTypes, operationsPerWorker, 
				maxOperationTimeoutSec, errorChan, &totalOps, &successOps)
		}(workerID)
	}

	// Monitor for live deadlocks while test runs
	deadlockDetected := make(chan bool, 1)
	go func() {
		monitorForDeadlocks(testCtx, t, db, deadlockDetected)
	}()

	// Wait for completion or deadlock detection
	done := make(chan bool, 1)
	go func() {
		wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		t.Log("All torture workers completed successfully")
	case <-deadlockDetected:
		testCancel() // Stop all workers
		<-done       // Wait for cleanup
		t.Error("DEADLOCK DETECTED: Live deadlock monitoring found blocked transactions")
	case <-testCtx.Done():
		t.Log("Torture test completed by timeout")
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

	t.Logf("=== TORTURE TEST RESULTS ===")
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
	assert.Zero(t, deadlocks, "DEADLOCK DETECTED: Found %d deadlock errors. This indicates the workqueue needs better lock ordering when global advisory lock is removed.", deadlocks)
	
	// Allow some timeouts but not excessive
	assert.Less(t, timeouts, int64(total/10), "Excessive timeouts (%d) may indicate lock contention issues", timeouts)
	
	// Require reasonable success rate
	assert.Greater(t, float64(success)/float64(total), 0.8, "Success rate too low (%.1f%%), may indicate locking issues", float64(success)/float64(total)*100)
}

// tortureWorker performs aggressive concurrent operations
func tortureWorker(ctx context.Context, t *testing.T, db lrdb.StoreFull, workerID int64, orgID uuid.UUID, 
	telemetryTypes []string, maxOps int, timeoutSec int, errorChan chan<- error,
	totalOps, successOps *int64) {
	
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
		
		// Vary operation types to create diverse locking patterns
		opType := operationCount % 6
		var err error
		
		switch opType {
		case 0, 1: // Claim and delete (most common operations)
			telemetryType := telemetryTypes[operationCount%len(telemetryTypes)]
			work, claimErr := db.ClaimInqueueWork(opCtx, lrdb.ClaimInqueueWorkParams{
				ClaimedBy:     workerID,
				TelemetryType: telemetryType,
			})
			if claimErr == nil {
				// Simulate brief work processing
				time.Sleep(time.Millisecond * time.Duration(operationCount%5))
				
				err = db.DeleteInqueueWork(opCtx, lrdb.DeleteInqueueWorkParams{
					ID:        work.ID,
					ClaimedBy: workerID,
				})
			} else {
				err = claimErr
			}
			
		case 2: // Journal upsert (creates different lock patterns)
			_, err = db.InqueueJournalUpsert(opCtx, lrdb.InqueueJournalUpsertParams{
				OrganizationID: orgID,
				Bucket:         "torture-bucket",
				ObjectID:       fmt.Sprintf("journal-%d-%d.json", workerID, operationCount),
			})
			
		case 3: // Journal delete
			err = db.InqueueJournalDelete(opCtx, lrdb.InqueueJournalDeleteParams{
				OrganizationID: orgID,
				Bucket:         "torture-bucket",
				ObjectID:       fmt.Sprintf("journal-%d-%d.json", workerID, operationCount-10),
			})
			
		case 4: // Cleanup (affects multiple rows)
			err = db.CleanupInqueueWork(opCtx)
			
		case 5: // Add new work (creates insertion contention)
			err = db.PutInqueueWork(opCtx, lrdb.PutInqueueWorkParams{
				OrganizationID: orgID,
				CollectorName:  "torture-test",
				Bucket:         "torture-bucket",
				ObjectID:       fmt.Sprintf("new-work-%d-%d.json", workerID, operationCount),
				TelemetryType:  telemetryTypes[operationCount%len(telemetryTypes)],
				Priority:       int32(operationCount % 3),
			})
		}
		
		cancel()
		
		if err != nil {
			errorChan <- err
		} else {
			atomic.AddInt64(successOps, 1)
		}
	}
}

// monitorForDeadlocks checks PostgreSQL for live deadlock conditions
func monitorForDeadlocks(ctx context.Context, t *testing.T, db lrdb.StoreFull, deadlockDetected chan<- bool) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	
	pool := testhelpers.SetupTestLRDB(t)
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Query for lock waits that might indicate deadlock
			var lockWaits int
			err := pool.QueryRow(ctx, `
				SELECT COUNT(*) 
				FROM pg_stat_activity 
				WHERE wait_event_type = 'Lock' 
				  AND state = 'active'
				  AND (query LIKE '%inqueue%' OR query LIKE '%work_queue%')
				  AND query_start < NOW() - INTERVAL '2 seconds'
			`).Scan(&lockWaits)
			
			if err == nil && lockWaits > 5 {
				t.Logf("WARNING: %d transactions waiting on locks for >2s", lockWaits)
				
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

// Helper functions for error classification
func isDeadlockError(err error) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "deadlock detected") ||
		strings.Contains(errStr, "40p01") || // PostgreSQL deadlock error code
		strings.Contains(errStr, "deadlock")
}

func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) ||
		strings.Contains(err.Error(), "timeout") ||
		strings.Contains(err.Error(), "deadline exceeded")
}

// TestWorkQueueSpecificDeadlockPatterns tests known problematic patterns
func TestWorkQueueSpecificDeadlockPatterns(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	t.Run("concurrent_claim_delete_same_type", func(t *testing.T) {
		orgID := uuid.New()
		
		// Create work items for contention
		for i := 0; i < 50; i++ {
			err := db.PutInqueueWork(ctx, lrdb.PutInqueueWorkParams{
				OrganizationID: orgID,
				CollectorName:  "pattern-test",
				Bucket:         "test-bucket",
				ObjectID:       fmt.Sprintf("item-%d.json", i),
				TelemetryType:  "logs",
				Priority:       1,
			})
			require.NoError(t, err)
		}
		
		// Launch workers that aggressively claim/delete same telemetry type
		var wg sync.WaitGroup
		errorCount := int64(0)
		
		for workerID := 1; workerID <= 10; workerID++ {
			wg.Add(1)
			go func(id int64) {
				defer wg.Done()
				
				for i := 0; i < 20; i++ {
					opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
					
					work, err := db.ClaimInqueueWork(opCtx, lrdb.ClaimInqueueWorkParams{
						ClaimedBy:     id,
						TelemetryType: "logs",
					})
					
					if err == nil {
						err = db.DeleteInqueueWork(opCtx, lrdb.DeleteInqueueWorkParams{
							ID:        work.ID,
							ClaimedBy: id,
						})
					}
					
					if err != nil && isDeadlockError(err) {
						atomic.AddInt64(&errorCount, 1)
						t.Errorf("Deadlock detected in worker %d: %v", id, err)
					}
					
					cancel()
				}
			}(int64(workerID))
		}
		
		// Wait with reasonable timeout
		done := make(chan bool)
		go func() {
			wg.Wait()
			done <- true
		}()
		
		select {
		case <-done:
			// Success
		case <-time.After(15 * time.Second):
			t.Error("Pattern test timed out - possible deadlock")
		}
		
		assert.Zero(t, atomic.LoadInt64(&errorCount), "Deadlocks detected in specific pattern test")
	})
}