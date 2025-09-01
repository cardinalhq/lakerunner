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
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/testhelpers"
)

func TestClaimMetricRollupWork_FullBatchLogic(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	workerID := int64(88888)
	targetRecords := int64(40000)

	now := time.Now()
	// Make sure window_close_ts is in the past so items are ready for processing
	windowCloseTime := now.Add(-1 * time.Minute)

	orgID1 := uuid.New()
	orgID2 := uuid.New()
	orgID3 := uuid.New()

	fmt.Printf("=== Setting up MRQ test scenarios ===\n")

	// Scenario 1: Organization with exactly target records (should be processed as full batch when fresh)
	items1 := []lrdb.PutMetricRollupWorkParams{
		{OrganizationID: orgID1, Dateint: 20250901, FrequencyMs: 10000, InstanceNum: 1, SlotID: 0, SlotCount: 8, SegmentID: 1001, RecordCount: 20000, RollupGroup: 1001, Priority: 800, WindowCloseTs: windowCloseTime},
		{OrganizationID: orgID1, Dateint: 20250901, FrequencyMs: 10000, InstanceNum: 1, SlotID: 0, SlotCount: 8, SegmentID: 1002, RecordCount: 20000, RollupGroup: 1001, Priority: 800, WindowCloseTs: windowCloseTime},
	}

	// Scenario 2: Organization with 45,000 records (112.5% of target, within 120% limit)
	items2 := []lrdb.PutMetricRollupWorkParams{
		{OrganizationID: orgID2, Dateint: 20250901, FrequencyMs: 10000, InstanceNum: 1, SlotID: 1, SlotCount: 8, SegmentID: 2001, RecordCount: 22500, RollupGroup: 2001, Priority: 800, WindowCloseTs: windowCloseTime},
		{OrganizationID: orgID2, Dateint: 20250901, FrequencyMs: 10000, InstanceNum: 1, SlotID: 1, SlotCount: 8, SegmentID: 2002, RecordCount: 22500, RollupGroup: 2001, Priority: 800, WindowCloseTs: windowCloseTime},
	}

	// Scenario 3: Organization with 60,000 records (150% of target, over 120% limit)
	items3 := []lrdb.PutMetricRollupWorkParams{
		{OrganizationID: orgID3, Dateint: 20250901, FrequencyMs: 10000, InstanceNum: 1, SlotID: 2, SlotCount: 8, SegmentID: 3001, RecordCount: 30000, RollupGroup: 3001, Priority: 800, WindowCloseTs: windowCloseTime},
		{OrganizationID: orgID3, Dateint: 20250901, FrequencyMs: 10000, InstanceNum: 1, SlotID: 2, SlotCount: 8, SegmentID: 3002, RecordCount: 30000, RollupGroup: 3001, Priority: 800, WindowCloseTs: windowCloseTime},
	}

	// Insert all items
	allItems := append(items1, items2...)
	allItems = append(allItems, items3...)

	for _, item := range allItems {
		err := db.PutMetricRollupWork(ctx, item)
		require.NoError(t, err)
	}

	fmt.Printf("Created test organizations:\n")
	fmt.Printf("  Org1 (%s): 40,000 records (exactly target) - should be eligible\n", orgID1.String()[:8])
	fmt.Printf("  Org2 (%s): 45,000 records (112.5%% of target) - should be eligible\n", orgID2.String()[:8])
	fmt.Printf("  Org3 (%s): 60,000 records (150%% of target) - should NOT be eligible\n", orgID3.String()[:8])

	// Test the current query behavior with fresh items
	queryTime := now.Add(10 * time.Second)

	fmt.Printf("\n=== Testing Current MRQ Behavior (likely inefficient) ===\n")
	fmt.Printf("Query time: %s (fresh items)\n", queryTime.Format(time.RFC3339))

	// Test 1: Current query behavior - likely to claim inefficient batches
	fmt.Printf("\n--- Test 1: Current query (expecting ANY available work) ---\n")
	batch1, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
		WorkerID:             workerID,
		DefaultTargetRecords: targetRecords,
		MaxAgeSeconds:        300, // 5 minutes - items are only 10 seconds old
		BatchCount:           10,
		NowTs:                &queryTime,
	})
	require.NoError(t, err)

	if len(batch1) > 0 {
		totalRecords := int64(0)
		for _, item := range batch1 {
			totalRecords += item.RecordCount
		}
		efficiency := float64(totalRecords) / float64(targetRecords) * 100

		fmt.Printf("Current query claimed %d items, %d total records\n", len(batch1), totalRecords)
		fmt.Printf("Organization: %s\n", batch1[0].OrganizationID.String()[:8])
		fmt.Printf("Efficiency: %.1f%% of target\n", efficiency)

		// This should demonstrate the problem - likely claiming small/inefficient batches
		if efficiency < 100 || efficiency > 120 {
			fmt.Printf("⚠ PROBLEM: Claimed inefficient batch (%.1f%% of target, outside 100-120%% range)\n", efficiency)
		} else {
			fmt.Printf("✓ Efficient batch: %.1f%% of target\n", efficiency)
		}
	} else {
		fmt.Printf("No items claimed\n")
	}

	// Test with old timestamps to see if it claims oversized batches when old
	oldQueryTime := now.Add(10 * time.Minute) // Make items 10 minutes old

	fmt.Printf("\n--- Test 2: With old items (should claim regardless of size) ---\n")

	// Try to claim remaining items as "old"
	batch2, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
		WorkerID:             workerID,
		DefaultTargetRecords: targetRecords,
		MaxAgeSeconds:        300, // 5 minutes
		BatchCount:           10,
		NowTs:                &oldQueryTime,
	})
	require.NoError(t, err)

	if len(batch2) > 0 {
		totalRecords := int64(0)
		for _, item := range batch2 {
			totalRecords += item.RecordCount
		}
		efficiency := float64(totalRecords) / float64(targetRecords) * 100

		fmt.Printf("Claimed %d old items, %d total records\n", len(batch2), totalRecords)
		fmt.Printf("Organization: %s\n", batch2[0].OrganizationID.String()[:8])
		fmt.Printf("Efficiency: %.1f%% of target\n", efficiency)
		fmt.Printf("✓ Old items should be processed regardless of batch size\n")
	} else {
		fmt.Printf("No old items claimed\n")
	}

	fmt.Printf("\n=== Analysis ===\n")
	fmt.Printf("The current MRQ query likely has the same efficiency issues as MCQ had:\n")
	fmt.Printf("1. Claims ANY available work, even very small batches\n")
	fmt.Printf("2. Doesn't enforce full batch logic for fresh items\n")
	fmt.Printf("3. Should only claim fresh items if they make efficient batches (100-120%% of target)\n")
	fmt.Printf("4. Should claim old items regardless of batch size (to prevent starvation)\n")
}

func TestClaimMetricRollupWork_SingleItemScenarios(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	workerID := int64(77777)
	targetRecords := int64(40000)

	now := time.Now()
	windowCloseTime := now.Add(-1 * time.Minute)

	orgID1 := uuid.New()
	orgID2 := uuid.New()
	orgID3 := uuid.New()
	orgID4 := uuid.New()

	fmt.Printf("=== Testing MRQ Single Item Scenarios ===\n")

	// Test scenarios with single items of different sizes
	testCases := []struct {
		orgID          uuid.UUID
		recordCount    int64
		description    string
		expectedResult string
	}{
		{orgID1, 40000, "exactly target records", "should be claimed as full_batch"},
		{orgID2, 45000, "112.5% of target (within 120%)", "should be claimed as full_batch"},
		{orgID3, 48000, "120% of target (at upper limit)", "should be claimed as full_batch"},
		{orgID4, 50000, "125% of target (over 120%)", "should be rejected when fresh, claimed when old"},
	}

	// Create single items for each test case
	for i, tc := range testCases {
		err := db.PutMetricRollupWork(ctx, lrdb.PutMetricRollupWorkParams{
			OrganizationID: tc.orgID,
			Dateint:        20250901,
			FrequencyMs:    10000,
			InstanceNum:    1,
			SlotID:         int32(i), // Different slot IDs to keep them separate
			SlotCount:      8,
			SegmentID:      int64(4000 + i),
			RecordCount:    tc.recordCount,
			RollupGroup:    int64(4000 + i),
			Priority:       800,
			WindowCloseTs:  windowCloseTime,
		})
		require.NoError(t, err)

		fmt.Printf("Created Org%d (%s): %d records - %s\n",
			i+1, tc.orgID.String()[:8], tc.recordCount, tc.description)
	}

	// Test with fresh timestamps (current behavior - probably claims all)
	queryTime := now.Add(10 * time.Second)

	fmt.Printf("\n=== Testing Fresh Items (Current MRQ Behavior) ===\n")

	for i := 0; i < 4; i++ {
		fmt.Printf("\n--- Attempt %d ---\n", i+1)

		batch, err := db.ClaimMetricRollupWork(ctx, lrdb.ClaimMetricRollupWorkParams{
			WorkerID:             workerID,
			DefaultTargetRecords: targetRecords,
			MaxAgeSeconds:        300, // 5 minutes - items are only 10 seconds old
			BatchCount:           10,
			NowTs:                &queryTime,
		})
		require.NoError(t, err)

		if len(batch) > 0 {
			item := batch[0]
			efficiency := float64(item.RecordCount) / float64(targetRecords) * 100

			fmt.Printf("✓ Claimed: %d records (%.1f%% of target)\n", item.RecordCount, efficiency)
			fmt.Printf("  Organization: %s\n", item.OrganizationID.String()[:8])
			fmt.Printf("  Items in batch: %d\n", len(batch))

			// Check efficiency
			if efficiency >= 100 && efficiency <= 120 {
				fmt.Printf("  ✓ Efficient batch (100-120%% range)\n")
			} else {
				fmt.Printf("  ⚠ Inefficient batch (%.1f%% of target, outside 100-120%% range)\n", efficiency)
			}
		} else {
			fmt.Printf("✗ No items claimed\n")
			break
		}
	}

	fmt.Printf("\n=== Summary ===\n")
	fmt.Printf("Current MRQ behavior analysis:\n")
	fmt.Printf("- If it claims items regardless of efficiency, it has the same problem as MCQ\n")
	fmt.Printf("- Improved MRQ should only claim fresh items that make efficient batches\n")
	fmt.Printf("- Or claim old items regardless of size\n")
}
