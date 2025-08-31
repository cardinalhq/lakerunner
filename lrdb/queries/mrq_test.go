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

func TestPutMetricRollupWork(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()

	err := db.PutMetricRollupWork(ctx, lrdb.PutMetricRollupWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    10000,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      8,
		SegmentID:      12345,
		RecordCount:    1000,
		Priority:       1000,
	})
	require.NoError(t, err)
}

func TestPutMetricRollupWork_MultipleItems(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()

	workItems := []lrdb.PutMetricRollupWorkParams{
		{
			OrganizationID: orgID,
			Dateint:        20250829,
			FrequencyMs:    10000,
			InstanceNum:    1,
			SlotID:         0,
			SlotCount:      8,
			SegmentID:      12346,
			RecordCount:    1000,
			Priority:       1000,
		},
		{
			OrganizationID: orgID,
			Dateint:        20250829,
			FrequencyMs:    60000,
			InstanceNum:    1,
			SlotID:         1,
			SlotCount:      8,
			SegmentID:      12347,
			RecordCount:    2000,
			Priority:       800,
		},
	}

	for _, item := range workItems {
		err := db.PutMetricRollupWork(ctx, item)
		require.NoError(t, err)
	}
}

func TestClaimMetricRollupWork_BasicClaim(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)

	// Create two items in the same group (same slot_id and other grouping fields)
	workItems := []lrdb.PutMetricRollupWorkParams{
		{
			OrganizationID: orgID,
			Dateint:        20250829,
			FrequencyMs:    10000,
			InstanceNum:    1,
			SlotID:         0, // Same slot_id
			SlotCount:      8,
			SegmentID:      12348,
			RecordCount:    500,
			Priority:       1000,
		},
		{
			OrganizationID: orgID,
			Dateint:        20250829,
			FrequencyMs:    10000,
			InstanceNum:    1,
			SlotID:         0, // Same slot_id as first item
			SlotCount:      8,
			SegmentID:      12349,
			RecordCount:    600,
			Priority:       1000,
		},
	}

	for _, item := range workItems {
		err := db.PutMetricRollupWork(ctx, item)
		require.NoError(t, err)
	}

	claimedBatch, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
		WorkerID:      workerID,
		MaxAgeSeconds: 30,
		BatchCount:    5,
	})
	require.NoError(t, err)

	assert.Len(t, claimedBatch, 2)
	for _, item := range claimedBatch {
		assert.Equal(t, orgID, item.OrganizationID)
		assert.Equal(t, workerID, item.ClaimedBy)
		assert.Equal(t, int16(1), item.InstanceNum)
		assert.NotNil(t, item.ClaimedAt)
	}
}

func TestClaimMetricRollupWork_AgeThreshold(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)

	err := db.PutMetricRollupWork(ctx, lrdb.PutMetricRollupWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    10000,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      8,
		SegmentID:      12345,
		RecordCount:    1000,
		Priority:       1000,
	})
	require.NoError(t, err)

	// Sleep to make the item old enough to be claimed
	time.Sleep(2 * time.Second)

	claimedBatch, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
		WorkerID:      workerID,
		MaxAgeSeconds: 1,
		BatchCount:    5,
	})
	require.NoError(t, err)

	assert.Len(t, claimedBatch, 1, "Should claim old items")
	if len(claimedBatch) > 0 {
		assert.Equal(t, int64(10000), claimedBatch[0].FrequencyMs)
	}
}

func TestClaimMetricRollupWork_EmptyQueue(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	workerID := int64(12345)

	claimedBatch, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
		WorkerID:      workerID,
		MaxAgeSeconds: 30,
		BatchCount:    5,
	})

	require.NoError(t, err)
	assert.Len(t, claimedBatch, 0)
}

func TestClaimMetricRollupWork_Priority(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)

	// Add low priority item first
	err := db.PutMetricRollupWork(ctx, lrdb.PutMetricRollupWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    10000,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      8,
		SegmentID:      12350,
		RecordCount:    800,
		Priority:       600,
	})
	require.NoError(t, err)

	// Add high priority item
	err = db.PutMetricRollupWork(ctx, lrdb.PutMetricRollupWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    60000,
		InstanceNum:    1,
		SlotID:         1,
		SlotCount:      8,
		SegmentID:      12351,
		RecordCount:    900,
		Priority:       1000,
	})
	require.NoError(t, err)

	claimedBatch, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
		WorkerID:      workerID,
		MaxAgeSeconds: 30,
		BatchCount:    1,
	})
	require.NoError(t, err)

	assert.Len(t, claimedBatch, 1)
	assert.Equal(t, int32(1000), claimedBatch[0].Priority, "Should claim higher priority item first")
	assert.Equal(t, int64(60000), claimedBatch[0].FrequencyMs, "Should be the 60s frequency item")
}

func TestReleaseMetricRollupWork(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)

	err := db.PutMetricRollupWork(ctx, lrdb.PutMetricRollupWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    10000,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      8,
		SegmentID:      12345,
		RecordCount:    1000,
		Priority:       1000,
	})
	require.NoError(t, err)

	claimedBatch, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
		WorkerID:      workerID,
		MaxAgeSeconds: 30,
		BatchCount:    5,
	})
	require.NoError(t, err)
	require.Len(t, claimedBatch, 1)

	claimedItem := claimedBatch[0]
	originalTries := claimedItem.Tries

	err = db.ReleaseMetricRollupWork(ctx, lrdb.ReleaseMetricRollupWorkParams{
		ID:        claimedItem.ID,
		ClaimedBy: workerID,
	})
	require.NoError(t, err)

	// Try to claim again with a different worker
	claimedBatch2, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
		WorkerID:      workerID + 1,
		MaxAgeSeconds: 30,
		BatchCount:    5,
	})
	require.NoError(t, err)
	require.Len(t, claimedBatch2, 1)

	assert.Equal(t, originalTries+1, claimedBatch2[0].Tries, "Tries should increment after release")
	assert.Equal(t, workerID+1, claimedBatch2[0].ClaimedBy, "Released and reclaimed item should show new worker ID")
}

func TestReleaseMetricRollupWork_OnlyReleasesByCorrectWorker(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)
	wrongWorkerID := int64(54321)

	err := db.PutMetricRollupWork(ctx, lrdb.PutMetricRollupWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    10000,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      8,
		SegmentID:      12345,
		RecordCount:    1000,
		Priority:       1000,
	})
	require.NoError(t, err)

	claimedBatch, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
		WorkerID:      workerID,
		MaxAgeSeconds: 30,
		BatchCount:    5,
	})
	require.NoError(t, err)
	require.Len(t, claimedBatch, 1)

	claimedItem := claimedBatch[0]

	// Try to release with wrong worker ID
	err = db.ReleaseMetricRollupWork(ctx, lrdb.ReleaseMetricRollupWorkParams{
		ID:        claimedItem.ID,
		ClaimedBy: wrongWorkerID,
	})
	require.NoError(t, err)

	// Attempt to claim by a different worker - should fail
	claimedBatch2, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
		WorkerID:      workerID + 1,
		MaxAgeSeconds: 30,
		BatchCount:    5,
	})
	require.NoError(t, err)

	assert.Len(t, claimedBatch2, 0, "Item should not be released by wrong worker")
}

func TestDeleteMetricRollupWork(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)

	err := db.PutMetricRollupWork(ctx, lrdb.PutMetricRollupWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    10000,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      8,
		SegmentID:      12345,
		RecordCount:    1000,
		Priority:       1000,
	})
	require.NoError(t, err)

	claimedBatch, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
		WorkerID:      workerID,
		MaxAgeSeconds: 30,
		BatchCount:    5,
	})
	require.NoError(t, err)
	require.Len(t, claimedBatch, 1)

	claimedItem := claimedBatch[0]

	err = db.DeleteMetricRollupWork(ctx, lrdb.DeleteMetricRollupWorkParams{
		ID:        claimedItem.ID,
		ClaimedBy: workerID,
	})
	require.NoError(t, err)

	// Try to claim again - should be empty since item was deleted
	claimedBatch2, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
		WorkerID:      workerID + 1,
		MaxAgeSeconds: 30,
		BatchCount:    5,
	})
	require.NoError(t, err)

	assert.Len(t, claimedBatch2, 0, "Item should be deleted and not claimable")
}

func TestDeleteMetricRollupWork_OnlyDeletesByCorrectWorker(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)
	wrongWorkerID := int64(54321)

	err := db.PutMetricRollupWork(ctx, lrdb.PutMetricRollupWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    10000,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      8,
		SegmentID:      12345,
		RecordCount:    1000,
		Priority:       1000,
	})
	require.NoError(t, err)

	claimedBatch, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
		WorkerID:      workerID,
		MaxAgeSeconds: 30,
		BatchCount:    5,
	})
	require.NoError(t, err)
	require.Len(t, claimedBatch, 1)

	claimedItem := claimedBatch[0]

	// Try to delete with wrong worker ID
	err = db.DeleteMetricRollupWork(ctx, lrdb.DeleteMetricRollupWorkParams{
		ID:        claimedItem.ID,
		ClaimedBy: wrongWorkerID,
	})
	require.NoError(t, err)

	// Release the item so another worker can claim it
	err = db.ReleaseMetricRollupWork(ctx, lrdb.ReleaseMetricRollupWorkParams{
		ID:        claimedItem.ID,
		ClaimedBy: workerID,
	})
	require.NoError(t, err)

	// Item should still exist since delete was attempted by wrong worker
	claimedBatch2, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
		WorkerID:      workerID + 1,
		MaxAgeSeconds: 30,
		BatchCount:    5,
	})
	require.NoError(t, err)

	assert.Len(t, claimedBatch2, 1, "Item should not be deleted by wrong worker")
}

func TestTouchMetricRollupWork(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)

	err := db.PutMetricRollupWork(ctx, lrdb.PutMetricRollupWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    10000,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      8,
		SegmentID:      12345,
		RecordCount:    1000,
		Priority:       1000,
	})
	require.NoError(t, err)

	claimedBatch, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
		WorkerID:      workerID,
		MaxAgeSeconds: 30,
		BatchCount:    5,
	})
	require.NoError(t, err)
	require.Len(t, claimedBatch, 1)

	claimedItem := claimedBatch[0]
	originalHeartbeat := claimedItem.HeartbeatedAt

	// Wait a moment then touch
	time.Sleep(100 * time.Millisecond)

	err = db.TouchMetricRollupWork(ctx, lrdb.TouchMetricRollupWorkParams{
		NowTs:     nil, // Let it use now()
		Ids:       []int64{claimedItem.ID},
		ClaimedBy: workerID,
	})
	require.NoError(t, err)

	// Re-claim to see if heartbeat was updated
	err = db.ReleaseMetricRollupWork(ctx, lrdb.ReleaseMetricRollupWorkParams{
		ID:        claimedItem.ID,
		ClaimedBy: workerID,
	})
	require.NoError(t, err)

	claimedBatch2, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
		WorkerID:      workerID,
		MaxAgeSeconds: 30,
		BatchCount:    5,
	})
	require.NoError(t, err)
	require.Len(t, claimedBatch2, 1)

	newItem := claimedBatch2[0]
	if originalHeartbeat != nil && newItem.HeartbeatedAt != nil {
		assert.True(t, newItem.HeartbeatedAt.After(*originalHeartbeat), "Heartbeat timestamp should be updated by touch")
	}
}

func TestCleanupMetricRollupWork(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)

	// Create an old work item
	err := db.PutMetricRollupWork(ctx, lrdb.PutMetricRollupWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    10000,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      8,
		SegmentID:      12345,
		RecordCount:    1000,
		Priority:       1000,
	})
	require.NoError(t, err)

	// Claim the item so it has claimed_at set
	claimedBatch, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
		WorkerID:      workerID,
		MaxAgeSeconds: 30,
		BatchCount:    5,
	})
	require.NoError(t, err)
	require.Len(t, claimedBatch, 1)

	// Wait and then cleanup old items (1 second timeout to cleanup items with stale heartbeats)
	time.Sleep(2 * time.Second)
	err = db.CleanupMetricRollupWork(ctx, 1)
	require.NoError(t, err)

	// Note: CleanupMetricRollupWork doesn't return the count of deleted rows
	// Try to claim again from a different worker - should be empty since item was cleaned up
	claimedBatch2, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
		WorkerID:      workerID + 1,
		MaxAgeSeconds: 30,
		BatchCount:    5,
	})
	require.NoError(t, err)

	assert.Len(t, claimedBatch2, 0, "Old claimed item should be cleaned up")
}
