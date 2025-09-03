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

func TestClaimInqueueBundle_MixedGroupingKeys(t *testing.T) {
	ctx := context.Background()
	pool := testhelpers.SetupTestLRDB(t)
	store := lrdb.NewStore(pool)
	t.Cleanup(func() {
		store.Close()
	})

	// Clean up any existing test data
	_, err := pool.Exec(ctx, "DELETE FROM inqueue WHERE bucket LIKE 'test-mixed-%'")
	require.NoError(t, err)

	// Create organizations and instance numbers for mixed groups
	org1 := uuid.New()
	org2 := uuid.New()
	org3 := uuid.New()

	// Insert files with mixed grouping keys but ordered by time
	// The time-ordered list will have items from different groups interleaved
	testData := []struct {
		orgID       uuid.UUID
		instanceNum int16
		fileName    string
		fileSize    int64
		delayMs     int // Delay before insertion to control queue_ts ordering
	}{
		{org1, 1, "org1_inst1_file1.json", 100, 0},
		{org2, 1, "org2_inst1_file1.json", 200, 10},
		{org1, 2, "org1_inst2_file1.json", 150, 20},
		{org3, 1, "org3_inst1_file1.json", 300, 30},
		{org1, 1, "org1_inst1_file2.json", 250, 40},
		{org2, 2, "org2_inst2_file1.json", 180, 50},
		{org1, 1, "org1_inst1_file3.json", 400, 60},
		{org3, 1, "org3_inst1_file2.json", 220, 70},
		{org2, 1, "org2_inst1_file2.json", 350, 80},
		{org1, 2, "org1_inst2_file2.json", 500, 90},
	}

	var insertedIDs []uuid.UUID
	for _, td := range testData {
		if td.delayMs > 0 {
			time.Sleep(time.Duration(td.delayMs) * time.Millisecond)
		}
		fileID := uuid.New()
		insertedIDs = append(insertedIDs, fileID)
		_, err := pool.Exec(ctx, `
			INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
				bucket, object_id, signal, file_size, eligible_at)
			VALUES ($1, $2, 'test-collector', $3, 'test-mixed-groups', $4,
				'metrics', $5, NOW())`,
			fileID, td.orgID, td.instanceNum, td.fileName, td.fileSize)
		require.NoError(t, err)
	}

	// First claim - should get files from org1, instance 1 (oldest group)
	result1, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
		WorkerID:      1,
		Signal:        "metrics",
		TargetSize:    750, // Will get org1_inst1_file1(100) + org1_inst1_file2(250) + org1_inst1_file3(400)
		MaxSize:       1000,
		GracePeriod:   5 * time.Minute,
		DeferInterval: 10 * time.Second,
		MaxAttempts:   5,
	})
	require.NoError(t, err)
	assert.Len(t, result1.Items, 3)
	// Verify all items are from the same group (org1, instance 1)
	for _, item := range result1.Items {
		assert.Equal(t, org1, item.OrganizationID)
		assert.Equal(t, int16(1), item.InstanceNum)
	}
	totalSize1 := int64(0)
	for _, item := range result1.Items {
		totalSize1 += item.FileSize
	}
	assert.Equal(t, int64(750), totalSize1) // 100 + 250 + 400

	// Second claim - should now get from next oldest ungrouped items
	result2, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
		WorkerID:      2,
		Signal:        "metrics",
		TargetSize:    500,
		MaxSize:       600,
		GracePeriod:   5 * time.Minute,
		DeferInterval: 10 * time.Second,
		MaxAttempts:   5,
	})
	require.NoError(t, err)
	assert.Len(t, result2.Items, 2)
	// Should get org2, instance 1 files (200 + 350 = 550)
	for _, item := range result2.Items {
		assert.Equal(t, org2, item.OrganizationID)
		assert.Equal(t, int16(1), item.InstanceNum)
	}

	// Cleanup
	for _, id := range insertedIDs {
		_, _ = pool.Exec(ctx, "DELETE FROM inqueue WHERE id = $1", id)
	}
}

func TestClaimInqueueBundle_DeferMechanismWithShortBundles(t *testing.T) {
	ctx := context.Background()
	pool := testhelpers.SetupTestLRDB(t)
	store := lrdb.NewStore(pool)
	t.Cleanup(func() {
		store.Close()
	})

	// Clean up any existing test data
	_, err := pool.Exec(ctx, "DELETE FROM inqueue WHERE bucket LIKE 'test-defer-%'")
	require.NoError(t, err)

	// Create multiple groups with varying bundle sizes
	org1 := uuid.New()
	org2 := uuid.New()
	org3 := uuid.New()

	// Group 1: Small bundle that won't meet target (should be deferred)
	smallBundleIDs := []uuid.UUID{}
	for i := 0; i < 2; i++ {
		fileID := uuid.New()
		smallBundleIDs = append(smallBundleIDs, fileID)
		_, err := pool.Exec(ctx, `
			INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
				bucket, object_id, signal, file_size, eligible_at, priority)
			VALUES ($1, $2, 'test-collector', 1, 'test-defer-small', $3,
				'logs', 50, NOW(), 10)`,
			fileID, org1, fmt.Sprintf("small_%d.json", i))
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond)
	}

	// Group 2: Another small bundle (should also be deferred)
	for i := 0; i < 3; i++ {
		fileID := uuid.New()
		smallBundleIDs = append(smallBundleIDs, fileID)
		_, err := pool.Exec(ctx, `
			INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
				bucket, object_id, signal, file_size, eligible_at, priority)
			VALUES ($1, $2, 'test-collector', 1, 'test-defer-small2', $3,
				'logs', 75, NOW(), 10)`,
			fileID, org2, fmt.Sprintf("small2_%d.json", i))
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond)
	}

	// Group 3: Large bundle that meets target (deeper in the queue but should be claimed)
	largeBundleIDs := []uuid.UUID{}
	for i := 0; i < 4; i++ {
		fileID := uuid.New()
		largeBundleIDs = append(largeBundleIDs, fileID)
		_, err := pool.Exec(ctx, `
			INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
				bucket, object_id, signal, file_size, eligible_at, priority)
			VALUES ($1, $2, 'test-collector', 1, 'test-defer-large', $3,
				'logs', 300, NOW(), 10)`,
			fileID, org3, fmt.Sprintf("large_%d.json", i))
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond)
	}

	// Attempt to claim - should skip small bundles and claim the large one
	result, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
		WorkerID:      100,
		Signal:        "logs",
		TargetSize:    1000, // Small bundles won't meet this
		MaxSize:       1500,
		GracePeriod:   10 * time.Minute, // Not old enough for grace period
		DeferInterval: 2 * time.Second,
		Jitter:        100 * time.Millisecond,
		MaxAttempts:   5, // Will try multiple groups
	})
	require.NoError(t, err)

	// Should have claimed the large bundle (4 files, 300 bytes each = 1200 bytes)
	require.Len(t, result.Items, 4)
	for _, item := range result.Items {
		assert.Equal(t, org3, item.OrganizationID)
		assert.Equal(t, int64(300), item.FileSize)
	}

	// Verify small bundles were deferred (check eligible_at is in the future)
	for _, id := range smallBundleIDs {
		var eligibleAt time.Time
		err = pool.QueryRow(ctx,
			"SELECT eligible_at FROM inqueue WHERE id = $1", id).Scan(&eligibleAt)
		require.NoError(t, err)
		assert.True(t, eligibleAt.After(time.Now()),
			"Small bundle items should be deferred to future")
	}

	// Second claim immediately - should get nothing (small bundles still deferred)
	result2, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
		WorkerID:      101,
		Signal:        "logs",
		TargetSize:    1000,
		MaxSize:       1500,
		GracePeriod:   10 * time.Minute,
		DeferInterval: 2 * time.Second,
		MaxAttempts:   5,
	})
	require.NoError(t, err)
	assert.Empty(t, result2.Items, "Should not claim deferred items immediately")

	// Wait for defer period and try again
	time.Sleep(3 * time.Second)

	// Third claim after defer period - small bundles still won't meet target individually
	// But with lower target, one should be claimable
	result3, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
		WorkerID:      102,
		Signal:        "logs",
		TargetSize:    100, // Now small bundles can meet this
		MaxSize:       500,
		GracePeriod:   10 * time.Minute,
		DeferInterval: 2 * time.Second,
		MaxAttempts:   5,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, result3.Items, "Should claim small bundle after defer with lower target")

	// Cleanup
	for _, id := range append(smallBundleIDs, largeBundleIDs...) {
		_, _ = pool.Exec(ctx, "DELETE FROM inqueue WHERE id = $1", id)
	}
}

func TestClaimInqueueBundle_ExactRowCountExpectations(t *testing.T) {
	ctx := context.Background()
	pool := testhelpers.SetupTestLRDB(t)
	store := lrdb.NewStore(pool)
	t.Cleanup(func() {
		store.Close()
	})

	// Clean up any existing test data
	_, err := pool.Exec(ctx, "DELETE FROM inqueue WHERE bucket LIKE 'test-exact-%'")
	require.NoError(t, err)

	// Test scenario 1: Exact count with greedy batching
	t.Run("GreedyBatchingExactCount", func(t *testing.T) {
		org := uuid.New()
		fileIDs := []uuid.UUID{}

		// Insert exactly 7 files of varying sizes
		sizes := []int64{100, 200, 150, 300, 250, 180, 220}
		expectedCount := len(sizes)

		for i, size := range sizes {
			fileID := uuid.New()
			fileIDs = append(fileIDs, fileID)
			_, err := pool.Exec(ctx, `
				INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
					bucket, object_id, signal, file_size, eligible_at)
				VALUES ($1, $2, 'test-collector', 1, 'test-exact-greedy', $3,
					'metrics', $4, NOW())`,
				fileID, org, fmt.Sprintf("exact_%d.json", i), size)
			require.NoError(t, err)
		}

		// Claim with parameters that should get exactly 5 files
		result, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
			WorkerID:      200,
			Signal:        "metrics",
			TargetSize:    950,  // 100+200+150+300+250=1000 (first 5)
			MaxSize:       1000, // Hard limit at 1000
			GracePeriod:   10 * time.Minute,
			MaxCandidates: 10,
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 5, "Should claim exactly 5 files")

		// Verify total size
		totalSize := int64(0)
		for _, item := range result.Items {
			totalSize += item.FileSize
		}
		assert.Equal(t, int64(1000), totalSize, "Total size should be exactly 1000")

		// Wait briefly, then mark remaining items as old to trigger grace period
		time.Sleep(10 * time.Millisecond)
		_, err = pool.Exec(ctx, `
			UPDATE inqueue 
			SET queue_ts = NOW() - INTERVAL '2 minutes'
			WHERE bucket = 'test-exact-greedy' AND claimed_by = -1`)
		require.NoError(t, err)

		// Claim remaining files with grace period
		result2, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
			WorkerID:    201,
			Signal:      "metrics",
			TargetSize:  1000,
			MaxSize:     1000,
			GracePeriod: 1 * time.Minute, // Files are now old enough
		})
		require.NoError(t, err)
		assert.Len(t, result2.Items, 2, "Should claim remaining 2 files")

		// Verify we claimed all files
		totalClaimed := len(result.Items) + len(result2.Items)
		assert.Equal(t, expectedCount, totalClaimed, "Should have claimed all 7 files total")

		// Cleanup
		for _, id := range fileIDs {
			_, _ = pool.Exec(ctx, "DELETE FROM inqueue WHERE id = $1", id)
		}
	})

	// Test scenario 2: Exact count with old files
	t.Run("OldFilesExactCount", func(t *testing.T) {
		org := uuid.New()
		fileIDs := []uuid.UUID{}

		// Insert exactly 10 files, make them old
		expectedCount := 10
		for i := 0; i < expectedCount; i++ {
			fileID := uuid.New()
			fileIDs = append(fileIDs, fileID)
			_, err := pool.Exec(ctx, `
				INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
					bucket, object_id, signal, file_size, queue_ts, eligible_at)
				VALUES ($1, $2, 'test-collector', 2, 'test-exact-old', $3,
					'traces', 100, NOW() - INTERVAL '2 minutes', NOW())`,
				fileID, org, fmt.Sprintf("old_%d.json", i))
			require.NoError(t, err)
		}

		// Claim with grace period - should get all 10 old files
		result, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
			WorkerID:      202,
			Signal:        "traces",
			TargetSize:    5000, // Much higher than total
			MaxSize:       10000,
			GracePeriod:   1 * time.Minute,
			MaxCandidates: 15, // Allow more than we have
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, expectedCount,
			fmt.Sprintf("Should claim exactly %d old files", expectedCount))

		// Verify no files remain
		var remaining int
		err = pool.QueryRow(ctx,
			"SELECT COUNT(*) FROM inqueue WHERE bucket = 'test-exact-old' AND claimed_by = -1").
			Scan(&remaining)
		require.NoError(t, err)
		assert.Equal(t, 0, remaining, "No unclaimed files should remain")

		// Cleanup
		for _, id := range fileIDs {
			_, _ = pool.Exec(ctx, "DELETE FROM inqueue WHERE id = $1", id)
		}
	})

	// Test scenario 3: Multiple groups with exact expectations
	t.Run("MultipleGroupsExactCount", func(t *testing.T) {
		orgs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
		allFileIDs := []uuid.UUID{}
		expectedSizes := []int{3, 5, 4} // Files per org

		// Insert files for each org - make them old immediately
		for orgIdx, org := range orgs {
			for i := 0; i < expectedSizes[orgIdx]; i++ {
				fileID := uuid.New()
				allFileIDs = append(allFileIDs, fileID)
				// Insert with old queue_ts to trigger grace period behavior
				_, err := pool.Exec(ctx, `
					INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
						bucket, object_id, signal, file_size, queue_ts, eligible_at, priority)
					VALUES ($1, $2, 'test-collector', 1, 'test-exact-multi', $3,
						'logs', 200, NOW() - INTERVAL '2 minutes', NOW(), $4)`,
					fileID, org, fmt.Sprintf("org%d_file%d.json", orgIdx, i), 10-orgIdx)
				require.NoError(t, err)
			}
		}

		claimedCounts := []int{}

		// Claim from each group - with old files, should get all files from each group
		for i := 0; i < 3; i++ {
			result, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
				WorkerID:      int64(300 + i),
				Signal:        "logs",
				TargetSize:    100,             // Low target, but files are old so should get them anyway
				MaxSize:       2000,            // High enough to get all files from a group (max 5*200=1000)
				GracePeriod:   1 * time.Minute, // Items are old enough (2 minutes old)
				MaxCandidates: 20,              // Increased to ensure we fetch enough
			})
			require.NoError(t, err)
			if len(result.Items) > 0 {
				claimedCounts = append(claimedCounts, len(result.Items))
				t.Logf("Claim %d got %d items from org %s", i+1, len(result.Items), result.Items[0].OrganizationID)
				// Verify all items are from same org
				firstOrg := result.Items[0].OrganizationID
				for _, item := range result.Items {
					assert.Equal(t, firstOrg, item.OrganizationID,
						"All items in batch should be from same organization")
				}
			}
		}

		// Should have gotten 3 claims, one per org
		assert.Len(t, claimedCounts, 3, "Should have claimed from 3 different groups")

		// Each claim should get multiple files from that org (since they're old)
		for i, count := range claimedCounts {
			assert.GreaterOrEqual(t, count, 2, fmt.Sprintf("Claim %d should have gotten at least 2 files (got %d)", i+1, count))
		}

		// Verify total claimed equals total inserted
		totalClaimed := 0
		for _, count := range claimedCounts {
			totalClaimed += count
		}
		assert.Equal(t, 12, totalClaimed, "Should have claimed all 12 files total")

		// Cleanup
		for _, id := range allFileIDs {
			_, _ = pool.Exec(ctx, "DELETE FROM inqueue WHERE id = $1", id)
		}
	})

	// Test scenario 4: .binpb file counting
	t.Run("BinpbFilesExactCount", func(t *testing.T) {
		org := uuid.New()
		fileIDs := []uuid.UUID{}

		// Insert mix of .binpb and regular files
		// 5 .binpb files at 1000 bytes each (effective: 100 bytes each)
		// 3 regular files at 200 bytes each
		// Total effective: 500 + 600 = 1100 bytes

		for i := 0; i < 5; i++ {
			fileID := uuid.New()
			fileIDs = append(fileIDs, fileID)
			_, err := pool.Exec(ctx, `
				INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
					bucket, object_id, signal, file_size, eligible_at)
				VALUES ($1, $2, 'test-collector', 3, 'test-exact-binpb', $3,
					'metrics', 1000, NOW())`,
				fileID, org, fmt.Sprintf("data_%d.binpb", i))
			require.NoError(t, err)
		}

		for i := 0; i < 3; i++ {
			fileID := uuid.New()
			fileIDs = append(fileIDs, fileID)
			_, err := pool.Exec(ctx, `
				INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
					bucket, object_id, signal, file_size, eligible_at)
				VALUES ($1, $2, 'test-collector', 3, 'test-exact-binpb', $3,
					'metrics', 200, NOW())`,
				fileID, org, fmt.Sprintf("data_%d.json", i))
			require.NoError(t, err)
		}

		// Claim with target that should get all files
		result, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
			WorkerID:      400,
			Signal:        "metrics",
			TargetSize:    1100, // Matches total effective size
			MaxSize:       1500,
			GracePeriod:   10 * time.Minute,
			MaxCandidates: 10,
		})
		require.NoError(t, err)
		assert.Len(t, result.Items, 8, "Should claim exactly 8 files (5 binpb + 3 json)")

		// Verify file types and sizes
		binpbCount := 0
		jsonCount := 0
		for _, item := range result.Items {
			if item.ObjectID[len(item.ObjectID)-6:] == ".binpb" {
				binpbCount++
				assert.Equal(t, int64(1000), item.FileSize)
			} else {
				jsonCount++
				assert.Equal(t, int64(200), item.FileSize)
			}
		}
		assert.Equal(t, 5, binpbCount, "Should have exactly 5 binpb files")
		assert.Equal(t, 3, jsonCount, "Should have exactly 3 json files")

		// Cleanup
		for _, id := range fileIDs {
			_, _ = pool.Exec(ctx, "DELETE FROM inqueue WHERE id = $1", id)
		}
	})
}
