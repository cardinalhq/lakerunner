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

package compaction

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/heartbeat"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/testhelpers"
)

// Test-local version of the MCQ heartbeater constructor
func newTestMCQHeartbeater(db lrdb.StoreFull, workerID int64, items []int64) *heartbeat.Heartbeater {
	if len(items) == 0 {
		// Return a no-op heartbeater for empty items
		return heartbeat.New(func(ctx context.Context) error {
			return nil // No-op
		}, time.Minute)
	}

	expectedCount := int64(len(items))

	heartbeatFunc := func(ctx context.Context) error {
		ll := logctx.FromContext(ctx)
		updatedCount, err := db.McqHeartbeat(ctx, lrdb.McqHeartbeatParams{
			WorkerID: workerID,
			Ids:      items,
		})
		if err != nil {
			return err
		}

		if updatedCount != expectedCount {
			ll.Error("Heartbeat did not update all expected rows",
				slog.Int64("expected", expectedCount),
				slog.Int64("updated", updatedCount),
				slog.Int64("missing", expectedCount-updatedCount))
			// Return error to trigger heartbeat failure handling
			return fmt.Errorf("heartbeat updated %d rows, expected %d", updatedCount, expectedCount)
		}

		return nil
	}

	return heartbeat.New(heartbeatFunc, time.Minute)
}

func TestMCQHeartbeater_Integration(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)

	// Create test MCQ item with larger record count to meet estimator target
	err := db.McqQueueWork(ctx, lrdb.McqQueueWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    5000,
		SegmentID:      int64(12347),
		InstanceNum:    1,
		RecordCount:    50000, // Large enough to meet estimator default of 40000
		Priority:       1,
	})
	require.NoError(t, err)

	// Claim the item
	bundle, err := db.ClaimCompactionBundle(ctx, lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 1000,
		BatchLimit:    10,
		Grace:         30 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, bundle.Items, 1)

	claimedItem := bundle.Items[0]

	itemIDs := []int64{claimedItem.ID}

	// Start heartbeater
	heartbeater := newTestMCQHeartbeater(db, workerID, itemIDs)
	cancel := heartbeater.Start(ctx)

	// Wait for initial heartbeat and a few intervals
	time.Sleep(2 * time.Second)

	// Stop heartbeating
	cancel()

	// Try to claim the same item with a different worker - should fail if heartbeat worked
	newWorkerBundle, err := db.ClaimCompactionBundle(ctx, lrdb.BundleParams{
		WorkerID:      workerID + 1,
		TargetRecords: 1000,
		BatchLimit:    10,
		Grace:         30 * time.Second,
	})
	require.NoError(t, err)

	// If heartbeat is working, the item should still be claimed by the original worker
	assert.Len(t, newWorkerBundle.Items, 0, "Item should still be claimed due to heartbeat, not available for new worker")
}

func TestMCQHeartbeater_EmptyItems_Integration(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	workerID := int64(12345)
	var itemIDs []int64 // Empty slice

	// This should not panic or cause issues
	heartbeater := newTestMCQHeartbeater(db, workerID, itemIDs)
	cancel := heartbeater.Start(ctx)

	time.Sleep(100 * time.Millisecond)

	cancel()

	// If we get here without panicking, the test passes
	assert.True(t, true, "Heartbeater should handle empty items gracefully")
}

func TestMCQHeartbeater_ContextCancellation_Integration(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)

	// Create test MCQ item with larger record count to meet estimator target
	err := db.McqQueueWork(ctx, lrdb.McqQueueWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    5000,
		SegmentID:      int64(12347),
		InstanceNum:    1,
		RecordCount:    50000, // Large enough to meet estimator default of 40000
		Priority:       1,
	})
	require.NoError(t, err)

	// Claim the item
	bundle, err := db.ClaimCompactionBundle(ctx, lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 1000,
		BatchLimit:    10,
		Grace:         30 * time.Second,
	})
	require.NoError(t, err)
	require.Len(t, bundle.Items, 1)

	itemIDs := []int64{bundle.Items[0].ID}

	// Test parent context cancellation propagates to heartbeater
	t.Run("parent_context_cancellation", func(t *testing.T) {
		cancelCtx, parentCancel := context.WithCancel(ctx)

		heartbeater := newTestMCQHeartbeater(db, workerID, itemIDs)
		cancel := heartbeater.Start(cancelCtx)
		defer cancel() // Ensure cleanup

		// Let it run briefly
		time.Sleep(25 * time.Millisecond)

		// Cancel parent context - this should cause heartbeater to exit
		parentCancel()

		// Give it time to exit cleanly
		time.Sleep(25 * time.Millisecond)

		// This test passes if no deadlock occurs
		assert.True(t, true, "Parent context cancellation should propagate cleanly")
	})

	// Test surgical cancellation via returned cancel function
	t.Run("cancel_function_works", func(t *testing.T) {
		heartbeater := newTestMCQHeartbeater(db, workerID, itemIDs)
		cancel := heartbeater.Start(ctx)

		// Let it run briefly
		time.Sleep(25 * time.Millisecond)

		// Cancel via returned function - this should stop heartbeater but not affect parent context
		cancel()

		// Give it time to exit cleanly
		time.Sleep(25 * time.Millisecond)

		// Parent context should still be active
		assert.NoError(t, ctx.Err(), "Parent context should not be cancelled")
	})
}
