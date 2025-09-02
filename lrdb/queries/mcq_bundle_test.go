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

func TestClaimCompactionBundle_BigSingleFile(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	workerID := int64(42)

	// Insert a large file that exceeds target
	largeFileID := insertMCQRow(t, store, orgID, 20241201, 5000, 1, 15000, time.Now().Add(-time.Hour))

	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 10000,
		OverFactor:    1.2,
		BatchLimit:    100,
		Grace:         time.Minute,
		MaxAttempts:   3,
	}

	bundle, err := store.ClaimCompactionBundle(ctx, params)
	require.NoError(t, err)
	require.Len(t, bundle, 1, "Should return single large file")
	assert.Equal(t, largeFileID, bundle[0].ID)
	assert.Equal(t, int64(15000), bundle[0].RecordCount)

	// Verify the row is claimed
	assertRowClaimed(t, store, largeFileID, workerID)
}

func TestClaimCompactionBundle_GreedyPacking(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	workerID := int64(42)
	now := time.Now().Add(-time.Hour)

	// Insert multiple small files that sum to target
	id1 := insertMCQRow(t, store, orgID, 20241201, 5000, 1, 3000, now.Add(-time.Minute*10))
	id2 := insertMCQRow(t, store, orgID, 20241201, 5000, 1, 4000, now.Add(-time.Minute*9))
	id3 := insertMCQRow(t, store, orgID, 20241201, 5000, 1, 3500, now.Add(-time.Minute*8))
	id4 := insertMCQRow(t, store, orgID, 20241201, 5000, 1, 2000, now.Add(-time.Minute*7))

	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 10000,
		OverFactor:    1.2,
		BatchLimit:    100,
		Grace:         time.Minute,
		MaxAttempts:   3,
	}

	bundle, err := store.ClaimCompactionBundle(ctx, params)
	require.NoError(t, err)
	require.Len(t, bundle, 3, "Should pack first 3 files (3000+4000+3500=10500)")

	// Verify correct files in chronological order
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
		assertRowClaimed(t, store, id, workerID)
	}

	// Verify unclaimed row remains
	assertRowUnclaimed(t, store, id4)
}

func TestClaimCompactionBundle_OverFactorPreventsGreedy(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	workerID := int64(42)
	now := time.Now().Add(-time.Hour)

	// Insert files where adding the second would exceed over factor
	id1 := insertMCQRow(t, store, orgID, 20241201, 5000, 1, 8000, now.Add(-time.Minute*10))
	id2 := insertMCQRow(t, store, orgID, 20241201, 5000, 1, 5000, now.Add(-time.Minute*9))

	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 10000,
		OverFactor:    1.2, // max = 12000, so 8000+5000=13000 exceeds limit
		BatchLimit:    100,
		Grace:         time.Minute,
		MaxAttempts:   3,
	}

	bundle, err := store.ClaimCompactionBundle(ctx, params)
	require.NoError(t, err)
	require.Len(t, bundle, 1, "Should only take first file due to over factor")
	assert.Equal(t, id1, bundle[0].ID)

	assertRowClaimed(t, store, id1, workerID)
	assertRowUnclaimed(t, store, id2)
}

func TestClaimCompactionBundle_TailRuleActivation(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	workerID := int64(42)

	// Insert old small files that don't reach target
	oldTime := time.Now().Add(-time.Hour * 2) // beyond grace period
	id1 := insertMCQRow(t, store, orgID, 20241201, 5000, 1, 1000, oldTime)
	id2 := insertMCQRow(t, store, orgID, 20241201, 5000, 1, 2000, oldTime.Add(time.Minute))

	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 10000,
		OverFactor:    1.2,
		BatchLimit:    100,
		Grace:         time.Hour, // files are older than this
		MaxAttempts:   3,
	}

	bundle, err := store.ClaimCompactionBundle(ctx, params)
	require.NoError(t, err)
	require.Len(t, bundle, 2, "Should take all available files due to tail rule")

	totalRecords := int64(0)
	for _, row := range bundle {
		totalRecords += row.RecordCount
	}
	assert.Equal(t, int64(3000), totalRecords)

	assertRowClaimed(t, store, id1, workerID)
	assertRowClaimed(t, store, id2, workerID)
}

func TestClaimCompactionBundle_TailRuleForcesSingle(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	workerID := int64(42)

	// Insert single old file that's way below target
	oldTime := time.Now().Add(-time.Hour * 2)
	id1 := insertMCQRow(t, store, orgID, 20241201, 5000, 1, 500, oldTime)

	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 10000,
		OverFactor:    1.2,
		BatchLimit:    100,
		Grace:         time.Hour,
		MaxAttempts:   3,
	}

	bundle, err := store.ClaimCompactionBundle(ctx, params)
	require.NoError(t, err)
	require.Len(t, bundle, 1, "Should force single file due to tail rule")
	assert.Equal(t, id1, bundle[0].ID)
	assert.Equal(t, int64(500), bundle[0].RecordCount)

	assertRowClaimed(t, store, id1, workerID)
}

func TestClaimCompactionBundle_DeferYoungFiles(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	workerID := int64(42)

	// Insert recent small files (within grace period)
	recentTime := time.Now().Add(-time.Minute * 30) // within 1 hour grace
	id1 := insertMCQRow(t, store, orgID, 20241201, 5000, 1, 1000, recentTime)
	id2 := insertMCQRow(t, store, orgID, 20241201, 5000, 1, 2000, recentTime.Add(time.Minute))

	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 10000,
		OverFactor:    1.2,
		BatchLimit:    100,
		Grace:         time.Hour,
		DeferBase:     time.Minute * 5,
		MaxAttempts:   3,
	}

	bundle, err := store.ClaimCompactionBundle(ctx, params)
	require.NoError(t, err)
	require.Empty(t, bundle, "Should defer young files and return empty")

	// Verify files are still unclaimed but deferred
	assertRowUnclaimed(t, store, id1)
	assertRowUnclaimed(t, store, id2)

	// Check that eligible_at was updated (deferred)
	checkDeferred(t, store, id1, time.Minute*5)
}

func TestClaimCompactionBundle_KeyRotation(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	workerID := int64(42)
	recentTime := time.Now().Add(-time.Minute * 30)

	// Insert files for first key (young, will be deferred)
	insertMCQRow(t, store, orgID, 20241201, 5000, 1, 1000, recentTime)

	// Insert files for second key (old, can be claimed)
	oldTime := time.Now().Add(-time.Hour * 2)
	id2 := insertMCQRow(t, store, orgID, 20241201, 5000, 2, 3000, oldTime) // different instance_num

	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 10000,
		OverFactor:    1.2,
		BatchLimit:    100,
		Grace:         time.Hour,
		DeferBase:     time.Minute * 5,
		MaxAttempts:   3,
	}

	bundle, err := store.ClaimCompactionBundle(ctx, params)
	require.NoError(t, err)
	require.Len(t, bundle, 1, "Should find suitable bundle from second key")
	assert.Equal(t, id2, bundle[0].ID)

	assertRowClaimed(t, store, id2, workerID)
}

func TestClaimCompactionBundle_ExhaustAllAttempts(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	workerID := int64(42)
	recentTime := time.Now().Add(-time.Minute * 30)

	// Insert only young files across multiple keys
	insertMCQRow(t, store, orgID, 20241201, 5000, 1, 1000, recentTime)
	insertMCQRow(t, store, orgID, 20241201, 5000, 2, 1000, recentTime)
	insertMCQRow(t, store, orgID, 20241201, 5000, 3, 1000, recentTime)

	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 10000,
		OverFactor:    1.2,
		BatchLimit:    100,
		Grace:         time.Hour,
		DeferBase:     time.Minute * 5,
		MaxAttempts:   2, // will try 2 keys, then give up
	}

	bundle, err := store.ClaimCompactionBundle(ctx, params)
	require.NoError(t, err)
	require.Empty(t, bundle, "Should return empty after exhausting attempts")
}

func TestClaimCompactionBundle_EmptyQueue(t *testing.T) {
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

	bundle, err := store.ClaimCompactionBundle(ctx, params)
	require.NoError(t, err)
	require.Empty(t, bundle, "Should handle empty queue gracefully")
}

func TestClaimCompactionBundle_ConcurrentWorkers(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	now := time.Now().Add(-time.Hour)

	// Insert files
	id1 := insertMCQRow(t, store, orgID, 20241201, 5000, 1, 15000, now) // big single
	id2 := insertMCQRow(t, store, orgID, 20241201, 5000, 2, 8000, now)  // another key

	params1 := lrdb.BundleParams{WorkerID: 1, TargetRecords: 10000, OverFactor: 1.2, BatchLimit: 100, Grace: time.Minute, MaxAttempts: 3}
	params2 := lrdb.BundleParams{WorkerID: 2, TargetRecords: 10000, OverFactor: 1.2, BatchLimit: 100, Grace: time.Minute, MaxAttempts: 3}

	// Both workers try to claim concurrently
	bundle1, err1 := store.ClaimCompactionBundle(ctx, params1)
	bundle2, err2 := store.ClaimCompactionBundle(ctx, params2)

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

// Helper functions

func insertMCQRow(t *testing.T, store *lrdb.Store, orgID uuid.UUID, dateint int32, freqMs int64, instanceNum int16, recordCount int64, queueTs time.Time) int64 {
	ctx := context.Background()

	// Insert into metric_compaction_queue table
	query := `
		INSERT INTO public.metric_compaction_queue 
		(organization_id, dateint, frequency_ms, instance_num, segment_id, record_count, queue_ts, claimed_by, eligible_at, priority)
		VALUES ($1, $2, $3, $4, $5, $6, $7, -1, now(), 0)
		RETURNING id`

	var id int64
	err := store.Pool().QueryRow(ctx, query, orgID, dateint, freqMs, instanceNum, recordCount+1000, recordCount, queueTs).Scan(&id)
	require.NoError(t, err)
	return id
}

func assertRowClaimed(t *testing.T, store *lrdb.Store, id int64, expectedWorkerID int64) {
	ctx := context.Background()

	var claimedBy int64
	query := `SELECT claimed_by FROM public.metric_compaction_queue WHERE id = $1`
	err := store.Pool().QueryRow(ctx, query, id).Scan(&claimedBy)
	require.NoError(t, err)
	assert.Equal(t, expectedWorkerID, claimedBy, "Row should be claimed by expected worker")
}

func assertRowUnclaimed(t *testing.T, store *lrdb.Store, id int64) {
	ctx := context.Background()

	var claimedBy int64
	query := `SELECT claimed_by FROM public.metric_compaction_queue WHERE id = $1`
	err := store.Pool().QueryRow(ctx, query, id).Scan(&claimedBy)
	require.NoError(t, err)
	assert.Equal(t, int64(-1), claimedBy, "Row should be unclaimed")
}

func checkDeferred(t *testing.T, store *lrdb.Store, id int64, expectedDeferDuration time.Duration) {
	ctx := context.Background()

	var eligibleAt time.Time
	query := `SELECT eligible_at FROM public.metric_compaction_queue WHERE id = $1`
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
