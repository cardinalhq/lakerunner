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

package queries

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

func TestClaimRollupBundle_BigSingleFile(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	workerID := int64(42)

	// Insert a large file that exceeds target
	largeFileID := insertMRQRow(t, store, orgID, 20241201, 5000, 1, 1, 4, 1001, 15000, time.Now().Add(-time.Hour), time.Now().Add(-time.Hour))

	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 10000,
		OverFactor:    1.2,
		BatchLimit:    100,
		Grace:         time.Minute,
		MaxAttempts:   3,
	}

	bundle, err := store.ClaimRollupBundle(ctx, params)
	require.NoError(t, err)
	require.Len(t, bundle, 1, "Should return single large file")
	assert.Equal(t, largeFileID, bundle[0].ID)
	assert.Equal(t, int64(15000), bundle[0].RecordCount)

	// Verify the row is claimed
	assertMRQRowClaimed(t, store, largeFileID, workerID)
}

func TestClaimRollupBundle_GreedyPacking(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	workerID := int64(42)
	now := time.Now().Add(-time.Hour)
	windowClose := now.Add(-time.Minute * 30)

	// Insert multiple small files that sum to target
	id1 := insertMRQRow(t, store, orgID, 20241201, 5000, 1, 1, 4, 1001, 3000, now.Add(-time.Minute*10), windowClose)
	id2 := insertMRQRow(t, store, orgID, 20241201, 5000, 1, 1, 4, 1001, 4000, now.Add(-time.Minute*9), windowClose)
	id3 := insertMRQRow(t, store, orgID, 20241201, 5000, 1, 1, 4, 1001, 3500, now.Add(-time.Minute*8), windowClose)
	id4 := insertMRQRow(t, store, orgID, 20241201, 5000, 1, 1, 4, 1001, 2000, now.Add(-time.Minute*7), windowClose)

	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 10000,
		OverFactor:    1.2,
		BatchLimit:    100,
		Grace:         time.Minute,
		MaxAttempts:   3,
	}

	bundle, err := store.ClaimRollupBundle(ctx, params)
	require.NoError(t, err)
	require.Len(t, bundle, 3, "Should pack first 3 files (3000+4000+3500=10500)")

	// Verify correct files in chronological order by window_close_ts, then queue_ts
	expectedIDs := []int64{id1, id2, id3}
	actualIDs := make([]int64, len(bundle))
	totalRecords := int64(0)
	for i, row := range bundle {
		actualIDs[i] = row.ID
		totalRecords += row.RecordCount
	}

	assert.Equal(t, expectedIDs, actualIDs)
	assert.Equal(t, int64(10500), totalRecords)
	assert.True(t, totalRecords >= 10000 && totalRecords <= 12000, "Should be within target range")

	// Verify all claimed rows
	for _, id := range expectedIDs {
		assertMRQRowClaimed(t, store, id, workerID)
	}

	// Verify unclaimed row remains
	assertMRQRowUnclaimed(t, store, id4)
}

func TestClaimRollupBundle_OverFactorPreventsGreedy(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	workerID := int64(42)
	now := time.Now().Add(-time.Hour)
	windowClose := now.Add(-time.Minute * 30)

	// Insert files where adding the second would exceed over factor
	id1 := insertMRQRow(t, store, orgID, 20241201, 5000, 1, 1, 4, 1001, 8000, now.Add(-time.Minute*10), windowClose)
	id2 := insertMRQRow(t, store, orgID, 20241201, 5000, 1, 1, 4, 1001, 5000, now.Add(-time.Minute*9), windowClose)

	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 10000,
		OverFactor:    1.2, // max = 12000, so 8000+5000=13000 exceeds limit
		BatchLimit:    100,
		Grace:         time.Minute,
		MaxAttempts:   3,
	}

	bundle, err := store.ClaimRollupBundle(ctx, params)
	require.NoError(t, err)
	require.Len(t, bundle, 1, "Should only take first file due to over factor")
	assert.Equal(t, id1, bundle[0].ID)

	assertMRQRowClaimed(t, store, id1, workerID)
	assertMRQRowUnclaimed(t, store, id2)
}

func TestClaimRollupBundle_TailRuleActivation(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	workerID := int64(42)

	// Insert old small files that don't reach target
	oldTime := time.Now().Add(-time.Hour * 2) // beyond grace period
	windowClose := oldTime.Add(-time.Minute * 30)
	id1 := insertMRQRow(t, store, orgID, 20241201, 5000, 1, 1, 4, 1001, 1000, oldTime, windowClose)
	id2 := insertMRQRow(t, store, orgID, 20241201, 5000, 1, 1, 4, 1001, 2000, oldTime.Add(time.Minute), windowClose)

	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 10000,
		OverFactor:    1.2,
		BatchLimit:    100,
		Grace:         time.Hour, // files are older than this
		MaxAttempts:   3,
	}

	bundle, err := store.ClaimRollupBundle(ctx, params)
	require.NoError(t, err)
	require.Len(t, bundle, 2, "Should take all available files due to tail rule")

	totalRecords := int64(0)
	for _, row := range bundle {
		totalRecords += row.RecordCount
	}
	assert.Equal(t, int64(3000), totalRecords)

	assertMRQRowClaimed(t, store, id1, workerID)
	assertMRQRowClaimed(t, store, id2, workerID)
}

func TestClaimRollupBundle_BoundaryRuleActivation(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	workerID := int64(42)

	// Insert recent files (within grace period) but with past window_close_ts (boundary condition)
	recentTime := time.Now().Add(-time.Minute * 30)      // within grace period
	pastWindowClose := time.Now().Add(-time.Minute * 10) // window closed 10 minutes ago
	id1 := insertMRQRow(t, store, orgID, 20241201, 5000, 1, 1, 4, 1001, 1000, recentTime, pastWindowClose)
	id2 := insertMRQRow(t, store, orgID, 20241201, 5000, 1, 1, 4, 1001, 2000, recentTime.Add(time.Minute), pastWindowClose)

	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 10000,
		OverFactor:    1.2,
		BatchLimit:    100,
		Grace:         time.Hour, // files are within grace period
		MaxAttempts:   3,
	}

	bundle, err := store.ClaimRollupBundle(ctx, params)
	require.NoError(t, err)
	require.Len(t, bundle, 2, "Should take all files due to boundary rule (window closed)")

	totalRecords := int64(0)
	for _, row := range bundle {
		totalRecords += row.RecordCount
	}
	assert.Equal(t, int64(3000), totalRecords)

	assertMRQRowClaimed(t, store, id1, workerID)
	assertMRQRowClaimed(t, store, id2, workerID)
}

func TestClaimRollupBundle_DeferYoungFiles(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	workerID := int64(42)

	// Insert recent small files (within grace period) and future window close
	recentTime := time.Now().Add(-time.Minute * 30)       // within 1 hour grace
	futureWindowClose := time.Now().Add(time.Minute * 30) // window not closed yet
	id1 := insertMRQRow(t, store, orgID, 20241201, 5000, 1, 1, 4, 1001, 1000, recentTime, futureWindowClose)
	id2 := insertMRQRow(t, store, orgID, 20241201, 5000, 1, 1, 4, 1001, 2000, recentTime.Add(time.Minute), futureWindowClose)

	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 10000,
		OverFactor:    1.2,
		BatchLimit:    100,
		Grace:         time.Hour,
		DeferBase:     time.Minute * 5,
		MaxAttempts:   3,
	}

	bundle, err := store.ClaimRollupBundle(ctx, params)
	require.NoError(t, err)
	require.Empty(t, bundle, "Should defer young files and return empty")

	// Verify files are still unclaimed but deferred
	assertMRQRowUnclaimed(t, store, id1)
	assertMRQRowUnclaimed(t, store, id2)

	// Check that eligible_at was updated (deferred)
	checkMRQDeferred(t, store, id1, time.Minute*5)
}

func TestClaimRollupBundle_KeyRotation(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	workerID := int64(42)
	recentTime := time.Now().Add(-time.Minute * 30)
	futureWindowClose := time.Now().Add(time.Minute * 30)

	// Insert files for first key (young, will be deferred)
	insertMRQRow(t, store, orgID, 20241201, 5000, 1, 1, 4, 1001, 1000, recentTime, futureWindowClose)

	// Insert files for second key (old, can be claimed) - different slot_id
	oldTime := time.Now().Add(-time.Hour * 2)
	pastWindowClose := time.Now().Add(-time.Minute * 10)
	id2 := insertMRQRow(t, store, orgID, 20241201, 5000, 1, 2, 4, 1002, 3000, oldTime, pastWindowClose) // different slot_id

	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 10000,
		OverFactor:    1.2,
		BatchLimit:    100,
		Grace:         time.Hour,
		DeferBase:     time.Minute * 5,
		MaxAttempts:   3,
	}

	bundle, err := store.ClaimRollupBundle(ctx, params)
	require.NoError(t, err)
	require.Len(t, bundle, 1, "Should find suitable bundle from second key")
	assert.Equal(t, id2, bundle[0].ID)

	assertMRQRowClaimed(t, store, id2, workerID)
}

func TestClaimRollupBundle_ExhaustAllAttempts(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	workerID := int64(42)
	recentTime := time.Now().Add(-time.Minute * 30)
	futureWindowClose := time.Now().Add(time.Minute * 30)

	// Insert only young files across multiple keys
	insertMRQRow(t, store, orgID, 20241201, 5000, 1, 1, 4, 1001, 1000, recentTime, futureWindowClose)
	insertMRQRow(t, store, orgID, 20241201, 5000, 1, 2, 4, 1002, 1000, recentTime, futureWindowClose)
	insertMRQRow(t, store, orgID, 20241201, 5000, 1, 3, 4, 1003, 1000, recentTime, futureWindowClose)

	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 10000,
		OverFactor:    1.2,
		BatchLimit:    100,
		Grace:         time.Hour,
		DeferBase:     time.Minute * 5,
		MaxAttempts:   2, // will try 2 keys, then give up
	}

	bundle, err := store.ClaimRollupBundle(ctx, params)
	require.NoError(t, err)
	require.Empty(t, bundle, "Should return empty after exhausting attempts")
}

func TestClaimRollupBundle_EmptyQueue(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	params := lrdb.BundleParams{
		WorkerID:      42,
		TargetRecords: 10000,
		OverFactor:    1.2,
		BatchLimit:    100,
		Grace:         time.Hour,
		MaxAttempts:   3,
	}

	bundle, err := store.ClaimRollupBundle(ctx, params)
	require.NoError(t, err)
	require.Empty(t, bundle, "Should handle empty queue gracefully")
}

func TestClaimRollupBundle_ConcurrentWorkers(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	now := time.Now().Add(-time.Hour)
	windowClose := now.Add(-time.Minute * 30)

	// Insert files
	id1 := insertMRQRow(t, store, orgID, 20241201, 5000, 1, 1, 4, 1001, 15000, now, windowClose) // big single
	id2 := insertMRQRow(t, store, orgID, 20241201, 5000, 1, 2, 4, 1002, 8000, now, windowClose)  // another key (different slot_id)

	params1 := lrdb.BundleParams{WorkerID: 1, TargetRecords: 10000, OverFactor: 1.2, BatchLimit: 100, Grace: time.Minute, MaxAttempts: 3}
	params2 := lrdb.BundleParams{WorkerID: 2, TargetRecords: 10000, OverFactor: 1.2, BatchLimit: 100, Grace: time.Minute, MaxAttempts: 3}

	// Both workers try to claim concurrently
	bundle1, err1 := store.ClaimRollupBundle(ctx, params1)
	bundle2, err2 := store.ClaimRollupBundle(ctx, params2)

	require.NoError(t, err1)
	require.NoError(t, err2)

	// One should get the first key, other should get the second key
	if len(bundle1) > 0 && len(bundle2) > 0 {
		// Both got work - verify different files
		assert.NotEqual(t, bundle1[0].ID, bundle2[0].ID)
		// Check that we got the expected files
		gotIDs := []int64{bundle1[0].ID, bundle2[0].ID}
		expectedIDs := []int64{id1, id2}
		assert.Contains(t, expectedIDs, gotIDs[0])
		assert.Contains(t, expectedIDs, gotIDs[1])
	} else {
		// One might not get work due to timing, that's ok
		assert.True(t, len(bundle1) > 0 || len(bundle2) > 0, "At least one worker should get work")
		if len(bundle1) > 0 {
			assert.True(t, bundle1[0].ID == id1 || bundle1[0].ID == id2)
		}
		if len(bundle2) > 0 {
			assert.True(t, bundle2[0].ID == id1 || bundle2[0].ID == id2)
		}
	}
}

func TestClaimRollupBundle_WindowOrderingMatters(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	workerID := int64(42)
	now := time.Now().Add(-time.Hour)

	// Insert files with different window close times but same queue times
	// Earlier window close should be prioritized
	earlyWindowClose := now.Add(-time.Minute * 60)
	laterWindowClose := now.Add(-time.Minute * 30)

	id1 := insertMRQRow(t, store, orgID, 20241201, 5000, 1, 1, 4, 1001, 5000, now, laterWindowClose) // later window close
	id2 := insertMRQRow(t, store, orgID, 20241201, 5000, 1, 1, 4, 1001, 6000, now, earlyWindowClose) // earlier window close

	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 10000,
		OverFactor:    1.2,
		BatchLimit:    100,
		Grace:         time.Minute,
		MaxAttempts:   3,
	}

	bundle, err := store.ClaimRollupBundle(ctx, params)
	require.NoError(t, err)
	require.Len(t, bundle, 2, "Should pack both files")

	// Should be ordered by window_close_ts first (earliest first)
	assert.Equal(t, id2, bundle[0].ID, "File with earlier window close should be first")
	assert.Equal(t, id1, bundle[1].ID, "File with later window close should be second")
}

// Helper functions

func insertMRQRow(t *testing.T, store *lrdb.Store, orgID uuid.UUID, dateint int32, freqMs int64, instanceNum int16, slotID, slotCount int32, rollupGroup int64, recordCount int64, queueTs, windowCloseTs time.Time) int64 {
	ctx := context.Background()

	// Insert into metric_rollup_queue table
	query := `
		INSERT INTO public.metric_rollup_queue 
		(organization_id, dateint, frequency_ms, instance_num, slot_id, slot_count, segment_id, record_count, rollup_group, window_close_ts, queue_ts, claimed_by, eligible_at, priority)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, -1, now(), 0)
		RETURNING id`

	var id int64
	err := store.Pool().QueryRow(ctx, query, orgID, dateint, freqMs, instanceNum, slotID, slotCount, recordCount+1000, recordCount, rollupGroup, windowCloseTs, queueTs).Scan(&id)
	require.NoError(t, err)
	return id
}

func assertMRQRowClaimed(t *testing.T, store *lrdb.Store, id int64, expectedWorkerID int64) {
	ctx := context.Background()

	var claimedBy int64
	query := `SELECT claimed_by FROM public.metric_rollup_queue WHERE id = $1`
	err := store.Pool().QueryRow(ctx, query, id).Scan(&claimedBy)
	require.NoError(t, err)
	assert.Equal(t, expectedWorkerID, claimedBy, "Row should be claimed by expected worker")
}

func assertMRQRowUnclaimed(t *testing.T, store *lrdb.Store, id int64) {
	ctx := context.Background()

	var claimedBy int64
	query := `SELECT claimed_by FROM public.metric_rollup_queue WHERE id = $1`
	err := store.Pool().QueryRow(ctx, query, id).Scan(&claimedBy)
	require.NoError(t, err)
	assert.Equal(t, int64(-1), claimedBy, "Row should be unclaimed")
}

func checkMRQDeferred(t *testing.T, store *lrdb.Store, id int64, expectedDeferDuration time.Duration) {
	ctx := context.Background()

	var eligibleAt time.Time
	query := `SELECT eligible_at FROM public.metric_rollup_queue WHERE id = $1`
	err := store.Pool().QueryRow(ctx, query, id).Scan(&eligibleAt)
	require.NoError(t, err)

	// Check that eligible_at is in the future (deferred)
	assert.True(t, eligibleAt.After(time.Now()), "Row should be deferred to future")

	// Allow some tolerance for timing
	minExpected := time.Now().Add(expectedDeferDuration - time.Second*10)
	maxExpected := time.Now().Add(expectedDeferDuration + time.Second*10)
	assert.True(t, eligibleAt.After(minExpected) && eligibleAt.Before(maxExpected),
		"Defer time should be approximately correct")
}
