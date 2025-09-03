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
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/testhelpers"
)

func TestClaimInqueueBundleWithLock(t *testing.T) {
	ctx := context.Background()
	pool := testhelpers.SetupTestLRDB(t)
	store := lrdb.NewStore(pool)
	t.Cleanup(func() {
		store.Close()
	})

	// Clean up any existing test data
	_, err := pool.Exec(ctx, "DELETE FROM inqueue WHERE bucket LIKE 'test-%'")
	require.NoError(t, err)

	t.Run("EmptyQueue", func(t *testing.T) {
		result, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
			WorkerID:   1,
			Signal:     "metrics",
			TargetSize: 1000,
			MaxSize:    2000,
		})
		require.NoError(t, err)
		assert.Empty(t, result.Items)
	})

	t.Run("SingleLargeFile", func(t *testing.T) {
		// Insert a large file that exceeds target size
		orgID := uuid.New()
		largeFileID := uuid.New()
		_, err := pool.Exec(ctx, `
			INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
				bucket, object_id, signal, file_size, eligible_at)
			VALUES ($1, $2, 'test-collector', 1, 'test-bucket-large', 'large-file.json',
				'metrics', 5000, NOW())`,
			largeFileID, orgID)
		require.NoError(t, err)

		result, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
			WorkerID:   1,
			Signal:     "metrics",
			TargetSize: 1000,
			MaxSize:    10000,
		})
		require.NoError(t, err)
		require.Len(t, result.Items, 1)
		assert.Equal(t, largeFileID, result.Items[0].ID)
		assert.Equal(t, int64(5000), result.Items[0].FileSize)

		// Cleanup
		_, err = pool.Exec(ctx, "DELETE FROM inqueue WHERE id = $1", largeFileID)
		require.NoError(t, err)
	})

	t.Run("BinpbFileScaling", func(t *testing.T) {
		// Insert .binpb files that should be counted as 1/10th size
		orgID := uuid.New()
		var fileIDs []uuid.UUID

		// Insert 10 binpb files of 1000 bytes each
		// Effective size = 100 bytes each, total = 1000 bytes
		for i := 0; i < 10; i++ {
			fileID := uuid.New()
			fileIDs = append(fileIDs, fileID)
			_, err := pool.Exec(ctx, `
				INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
					bucket, object_id, signal, file_size, eligible_at)
				VALUES ($1, $2, 'test-collector', 2, 'test-bucket-binpb', $3,
					'metrics', 1000, NOW())`,
				fileID, orgID, fmt.Sprintf("file%d.binpb", i))
			require.NoError(t, err)
		}

		result, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
			WorkerID:   2,
			Signal:     "metrics",
			TargetSize: 1000, // Should claim all 10 files (effective size = 1000)
			MaxSize:    2000,
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 10)

		// Cleanup
		for _, id := range fileIDs {
			_, err = pool.Exec(ctx, "DELETE FROM inqueue WHERE id = $1", id)
			require.NoError(t, err)
		}
	})

	t.Run("GreedyBatching", func(t *testing.T) {
		// Test greedy accumulation up to max size
		orgID := uuid.New()
		var fileIDs []uuid.UUID

		// Insert files with varying sizes
		sizes := []int64{300, 400, 200, 500, 100}
		for i, size := range sizes {
			fileID := uuid.New()
			fileIDs = append(fileIDs, fileID)
			_, err := pool.Exec(ctx, `
				INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
					bucket, object_id, signal, file_size, eligible_at)
				VALUES ($1, $2, 'test-collector', 3, 'test-bucket-greedy', $3,
					'logs', $4, NOW())`,
				fileID, orgID, fmt.Sprintf("file%d.json", i), size)
			require.NoError(t, err)
			// Small delay to ensure queue_ts ordering
			time.Sleep(10 * time.Millisecond)
		}

		result, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
			WorkerID:   3,
			Signal:     "logs",
			TargetSize: 900,
			MaxSize:    1000, // Should get first 3 files (300+400+200=900)
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 3)

		// Verify we got the right files in order
		var totalSize int64
		for _, item := range result.Items {
			totalSize += item.FileSize
		}
		assert.LessOrEqual(t, totalSize, int64(1000))

		// Cleanup
		for _, id := range fileIDs {
			_, err = pool.Exec(ctx, "DELETE FROM inqueue WHERE id = $1", id)
			require.NoError(t, err)
		}
	})

	t.Run("GracePeriodPartialBatch", func(t *testing.T) {
		// Test that old items trigger partial batch return
		orgID := uuid.New()
		fileID := uuid.New()

		// Insert an old item
		_, err := pool.Exec(ctx, `
			INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
				bucket, object_id, signal, file_size, queue_ts, eligible_at)
			VALUES ($1, $2, 'test-collector', 4, 'test-bucket-old', 'old-file.json',
				'traces', 100, NOW() - INTERVAL '2 minutes', NOW())`,
			fileID, orgID)
		require.NoError(t, err)

		result, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
			WorkerID:    4,
			Signal:      "traces",
			TargetSize:  1000, // Much larger than available
			MaxSize:     2000,
			GracePeriod: 1 * time.Minute, // Items older than this should be returned
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 1, "Should return partial batch for old items")
		assert.Equal(t, fileID, result.Items[0].ID)

		// Cleanup
		_, err = pool.Exec(ctx, "DELETE FROM inqueue WHERE id = $1", fileID)
		require.NoError(t, err)
	})

	t.Run("DeferItems", func(t *testing.T) {
		// Test that items are deferred when batch can't be formed
		orgID := uuid.New()
		fileID := uuid.New()

		// Insert a fresh small item
		_, err := pool.Exec(ctx, `
			INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
				bucket, object_id, signal, file_size, eligible_at)
			VALUES ($1, $2, 'test-collector', 5, 'test-bucket-defer', 'small-file.json',
				'metrics', 100, NOW())`,
			fileID, orgID)
		require.NoError(t, err)

		// First attempt should defer the item
		result, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
			WorkerID:      5,
			Signal:        "metrics",
			TargetSize:    1000,
			MaxSize:       2000,
			GracePeriod:   1 * time.Minute,
			DeferInterval: 5 * time.Second,
			MaxAttempts:   1, // Only try once
		})
		require.NoError(t, err)
		assert.Empty(t, result.Items, "Should not return items that don't meet target")

		// Check that item was deferred
		var eligibleAt time.Time
		err = pool.QueryRow(ctx,
			"SELECT eligible_at FROM inqueue WHERE id = $1", fileID).Scan(&eligibleAt)
		require.NoError(t, err)
		assert.True(t, eligibleAt.After(time.Now()), "Item should be deferred to future")

		// Cleanup
		_, err = pool.Exec(ctx, "DELETE FROM inqueue WHERE id = $1", fileID)
		require.NoError(t, err)
	})

	t.Run("PriorityOrdering", func(t *testing.T) {
		// Test that lower priority values are processed first
		orgID := uuid.New()
		var fileIDs []uuid.UUID
		priorities := []int32{10, 5, 1, 20} // Priority 1 should be picked first

		for i, priority := range priorities {
			fileID := uuid.New()
			fileIDs = append(fileIDs, fileID)
			_, err := pool.Exec(ctx, `
				INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
					bucket, object_id, signal, file_size, priority, eligible_at)
				VALUES ($1, $2, 'test-collector', 6, 'test-bucket-priority', $3,
					'metrics', 500, $4, NOW())`,
				fileID, orgID, fmt.Sprintf("file%d.json", i), priority)
			require.NoError(t, err)
		}

		result, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
			WorkerID:   6,
			Signal:     "metrics",
			TargetSize: 500,
			MaxSize:    1000,
		})
		require.NoError(t, err)
		require.Len(t, result.Items, 1)
		assert.Equal(t, int32(1), result.Items[0].Priority, "Should pick lowest priority value first")

		// Cleanup
		for _, id := range fileIDs {
			_, err = pool.Exec(ctx, "DELETE FROM inqueue WHERE id = $1", id)
			require.NoError(t, err)
		}
	})

	t.Run("ClaimedItemsNotReclaimable", func(t *testing.T) {
		// Test that claimed items cannot be claimed again
		orgID := uuid.New()
		fileID := uuid.New()

		_, err := pool.Exec(ctx, `
			INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
				bucket, object_id, signal, file_size, eligible_at)
			VALUES ($1, $2, 'test-collector', 7, 'test-bucket-claimed', 'file.json',
				'logs', 1000, NOW())`,
			fileID, orgID)
		require.NoError(t, err)

		// First claim should succeed
		result1, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
			WorkerID:   100,
			Signal:     "logs",
			TargetSize: 1000,
			MaxSize:    2000,
		})
		require.NoError(t, err)
		assert.Len(t, result1.Items, 1)

		// Second claim should get nothing (item already claimed)
		result2, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
			WorkerID:   101,
			Signal:     "logs",
			TargetSize: 1000,
			MaxSize:    2000,
		})
		require.NoError(t, err)
		assert.Empty(t, result2.Items)

		// Cleanup
		_, err = pool.Exec(ctx, "DELETE FROM inqueue WHERE id = $1", fileID)
		require.NoError(t, err)
	})

	t.Run("MultipleOrganizations", func(t *testing.T) {
		// Test that batches are formed per organization
		org1ID := uuid.New()
		org2ID := uuid.New()

		// Insert items for two different organizations
		file1ID := uuid.New()
		file2ID := uuid.New()

		_, err := pool.Exec(ctx, `
			INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
				bucket, object_id, signal, file_size, eligible_at)
			VALUES ($1, $2, 'test-collector', 8, 'test-bucket-org1', 'org1-file.json',
				'metrics', 500, NOW())`,
			file1ID, org1ID)
		require.NoError(t, err)

		_, err = pool.Exec(ctx, `
			INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
				bucket, object_id, signal, file_size, eligible_at)
			VALUES ($1, $2, 'test-collector', 8, 'test-bucket-org2', 'org2-file.json',
				'metrics', 500, NOW())`,
			file2ID, org2ID)
		require.NoError(t, err)

		// Claim should only get items from one organization
		result, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
			WorkerID:   8,
			Signal:     "metrics",
			TargetSize: 500, // Lower target to match file size
			MaxSize:    2000,
		})
		require.NoError(t, err)
		require.Len(t, result.Items, 1)

		// All items should be from the same organization
		firstOrgID := result.Items[0].OrganizationID
		for _, item := range result.Items {
			assert.Equal(t, firstOrgID, item.OrganizationID)
		}

		// Cleanup
		_, err = pool.Exec(ctx, "DELETE FROM inqueue WHERE id IN ($1, $2)", file1ID, file2ID)
		require.NoError(t, err)
	})

	t.Run("MaxAttemptsRespected", func(t *testing.T) {
		// Test that MaxAttempts limits the number of groups tried
		// Insert items for multiple org/instance combinations
		for i := 0; i < 5; i++ {
			orgID := uuid.New()
			fileID := uuid.New()
			_, err := pool.Exec(ctx, `
				INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
					bucket, object_id, signal, file_size, eligible_at)
				VALUES ($1, $2, 'test-collector', $3, 'test-bucket-attempts', $4,
					'traces', 100, NOW())`,
				fileID, orgID, int16(i), fmt.Sprintf("file%d.json", i))
			require.NoError(t, err)
			defer func(id uuid.UUID) {
				_, _ = pool.Exec(ctx, "DELETE FROM inqueue WHERE id = $1", id)
			}(fileID)
		}

		// Should try MaxAttempts groups then give up
		result, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
			WorkerID:      9,
			Signal:        "traces",
			TargetSize:    1000,
			MaxSize:       2000,
			MaxAttempts:   2,             // Only try 2 groups
			GracePeriod:   1 * time.Hour, // Don't trigger grace period
			DeferInterval: 1 * time.Second,
		})
		require.NoError(t, err)
		assert.Empty(t, result.Items, "Should return empty after MaxAttempts")
	})
}
