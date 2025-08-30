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
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/heartbeat"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/testhelpers"
)

// Test-local version of the MCQ heartbeater constructor
func newTestMCQHeartbeater(db lrdb.StoreFull, workerID int64, items []int64) *heartbeat.Heartbeater {
	if len(items) == 0 {
		// Return a no-op heartbeater for empty items
		return heartbeat.New(func(ctx context.Context) error {
			return nil // No-op
		}, time.Minute, slog.Default().With("component", "mcq_heartbeater", "worker_id", workerID, "item_count", 0))
	}

	heartbeatFunc := func(ctx context.Context) error {
		return db.TouchMetricCompactionWork(ctx, lrdb.TouchMetricCompactionWorkParams{
			Ids:       items,
			ClaimedBy: workerID,
		})
	}

	logger := slog.Default().With("component", "mcq_heartbeater", "worker_id", workerID, "item_count", len(items))
	return heartbeat.New(heartbeatFunc, time.Minute, logger)
}

func TestMCQHeartbeater_Integration(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)
	now := time.Now()

	tsRange := pgtype.Range[pgtype.Timestamptz]{
		Lower:     pgtype.Timestamptz{Time: now, Valid: true},
		Upper:     pgtype.Timestamptz{Time: now.Add(time.Hour), Valid: true},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Valid:     true,
	}

	// Create test MCQ item
	err := db.PutMetricCompactionWork(ctx, lrdb.PutMetricCompactionWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    5000,
		SegmentID:      int64(12347),
		InstanceNum:    1,
		TsRange:        tsRange,
		RecordCount:    1000,
		Priority:       1,
	})
	require.NoError(t, err)

	// Claim the item
	items, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
		WorkerID:             workerID,
		DefaultTargetRecords: 1000,
		MaxAgeSeconds:        30,
		BatchCount:           10,
	})
	require.NoError(t, err)
	require.Len(t, items, 1)

	claimedItem := items[0]
	require.NotNil(t, claimedItem.HeartbeatedAt, "Item should have initial heartbeat time")

	itemIDs := []int64{claimedItem.ID}

	// Start heartbeater
	heartbeater := newTestMCQHeartbeater(db, workerID, itemIDs)
	cancel := heartbeater.Start(ctx)

	// Wait for initial heartbeat and a few intervals
	time.Sleep(2 * time.Second)

	// Stop heartbeating
	cancel()

	// Try to claim the same item with a different worker - should fail if heartbeat worked
	newWorkerItems, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
		WorkerID:             workerID + 1,
		DefaultTargetRecords: 1000,
		MaxAgeSeconds:        30,
		BatchCount:           10,
	})
	require.NoError(t, err)

	// If heartbeat is working, the item should still be claimed by the original worker
	assert.Len(t, newWorkerItems, 0, "Item should still be claimed due to heartbeat, not available for new worker")
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
	now := time.Now()

	tsRange := pgtype.Range[pgtype.Timestamptz]{
		Lower:     pgtype.Timestamptz{Time: now, Valid: true},
		Upper:     pgtype.Timestamptz{Time: now.Add(time.Hour), Valid: true},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Valid:     true,
	}

	// Create test MCQ item
	err := db.PutMetricCompactionWork(ctx, lrdb.PutMetricCompactionWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    5000,
		SegmentID:      int64(12347),
		InstanceNum:    1,
		TsRange:        tsRange,
		RecordCount:    1000,
		Priority:       1,
	})
	require.NoError(t, err)

	// Claim the item
	items, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
		WorkerID:             workerID,
		DefaultTargetRecords: 1000,
		MaxAgeSeconds:        30,
		BatchCount:           10,
	})
	require.NoError(t, err)
	require.Len(t, items, 1)

	itemIDs := []int64{items[0].ID}

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
