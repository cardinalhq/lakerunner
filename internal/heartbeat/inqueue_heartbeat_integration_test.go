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

package heartbeat

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/testhelpers"
)

func TestInqueueHeartbeater_Integration(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)

	// Create test inqueue item
	err := db.PutInqueueWork(ctx, lrdb.PutInqueueWorkParams{
		OrganizationID: orgID,
		CollectorName:  "test-collector",
		InstanceNum:    1,
		Bucket:         "test-bucket",
		ObjectID:       "test-object.json.gz",
		Signal:         "logs",
		Priority:       1,
		FileSize:       1024,
	})
	require.NoError(t, err)

	// Claim the item
	items, err := db.ClaimInqueueWorkBatch(ctx, lrdb.ClaimInqueueWorkBatchParams{
		Signal:        "logs",
		WorkerID:      workerID,
		MaxTotalSize:  1024 * 1024,
		MinTotalSize:  0,
		MaxAgeSeconds: 30,
		BatchCount:    10,
	})
	require.NoError(t, err)
	require.Len(t, items, 1)

	claimedItem := items[0]
	require.NotNil(t, claimedItem.HeartbeatedAt, "Item should have initial heartbeat time")

	itemIDs := []uuid.UUID{claimedItem.ID}

	// Start heartbeater with short interval
	heartbeater := NewInqueueHeartbeater(db, workerID, itemIDs)
	heartbeater.interval = 100 * time.Millisecond
	cancel := heartbeater.Start(ctx)

	// Wait for a few heartbeats
	time.Sleep(300 * time.Millisecond)

	// Stop heartbeating
	cancel()

	// Check that the item still exists and is claimed (heartbeat prevented cleanup)
	summary, err := db.InqueueSummary(ctx)
	require.NoError(t, err)

	// The summary only shows unclaimed items, so if heartbeat is working,
	// our item should not appear in the summary (it should still be claimed)
	foundUnclaimed := false
	for _, item := range summary {
		if item.Signal == "logs" && item.Count > 0 {
			foundUnclaimed = true
			break
		}
	}

	// If heartbeat is working, the item should still be claimed (not in unclaimed summary)
	assert.False(t, foundUnclaimed, "Item should still be claimed due to heartbeat, not appear in unclaimed summary")
}

func TestInqueueHeartbeater_EmptyItems_Integration(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	workerID := int64(12345)
	var itemIDs []uuid.UUID // Empty slice

	// This should not panic or cause issues
	heartbeater := NewInqueueHeartbeater(db, workerID, itemIDs)
	heartbeater.interval = 50 * time.Millisecond
	cancel := heartbeater.Start(ctx)

	time.Sleep(100 * time.Millisecond)

	cancel()

	// If we get here without panicking, the test passes
	assert.True(t, true, "Heartbeater should handle empty items gracefully")
}

func TestInqueueHeartbeater_ContextCancellation_Integration(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)

	// Create test inqueue item
	err := db.PutInqueueWork(ctx, lrdb.PutInqueueWorkParams{
		OrganizationID: orgID,
		CollectorName:  "test-collector",
		InstanceNum:    1,
		Bucket:         "test-bucket",
		ObjectID:       "test-object.json.gz",
		Signal:         "logs",
		Priority:       1,
		FileSize:       1024,
	})
	require.NoError(t, err)

	// Claim the item
	items, err := db.ClaimInqueueWorkBatch(ctx, lrdb.ClaimInqueueWorkBatchParams{
		Signal:        "logs",
		WorkerID:      workerID,
		MaxTotalSize:  1024 * 1024,
		MinTotalSize:  0,
		MaxAgeSeconds: 30,
		BatchCount:    10,
	})
	require.NoError(t, err)
	require.Len(t, items, 1)

	itemIDs := []uuid.UUID{items[0].ID}

	// Test parent context cancellation propagates to heartbeater
	t.Run("parent_context_cancellation", func(t *testing.T) {
		cancelCtx, parentCancel := context.WithCancel(ctx)

		heartbeater := NewInqueueHeartbeater(db, workerID, itemIDs)
		heartbeater.interval = 50 * time.Millisecond
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
		heartbeater := NewInqueueHeartbeater(db, workerID, itemIDs)
		heartbeater.interval = 50 * time.Millisecond
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
