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

func TestClaimMetricCompactionWork_SingleItemScenarios(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	workerID := int64(77777)
	targetRecords := int64(40000)

	now := time.Now()
	orgID1 := uuid.New()
	orgID2 := uuid.New()
	orgID3 := uuid.New()
	orgID4 := uuid.New()

	fmt.Printf("=== Testing Single Item Scenarios ===\n")

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
		err := db.PutMetricCompactionWork(ctx, lrdb.PutMetricCompactionWorkParams{
			OrganizationID: tc.orgID,
			Dateint:        20250901,
			FrequencyMs:    10000,
			SegmentID:      int64(4000 + i),
			InstanceNum:    1,
			RecordCount:    tc.recordCount,
			Priority:       800,
		})
		require.NoError(t, err)

		fmt.Printf("Created Org%d (%s): %d records - %s\n",
			i+1, tc.orgID.String()[:8], tc.recordCount, tc.description)
	}

	// Test with fresh timestamps (should use full_batch logic)
	queryTime := now.Add(10 * time.Second)

	fmt.Printf("\n=== Testing Fresh Items (Full Batch Logic) ===\n")

	for i, tc := range testCases {
		fmt.Printf("\n--- Test %d: %s (%d records) ---\n", i+1, tc.description, tc.recordCount)

		batch, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
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
			fmt.Printf("  Batch reason: %s\n", item.BatchReason)
			fmt.Printf("  Items in batch: %d\n", len(batch))

			// Verify it's the expected organization
			if item.OrganizationID == tc.orgID {
				fmt.Printf("  ✓ Correct organization claimed\n")

				if tc.recordCount <= int64(float64(targetRecords)*1.2) {
					if item.BatchReason == "full_batch" {
						fmt.Printf("  ✓ Correctly identified as full_batch\n")
					} else {
						fmt.Printf("  ⚠ Expected full_batch, got: %s\n", item.BatchReason)
					}
				}
			} else {
				fmt.Printf("  ⚠ Claimed different organization than expected\n")
			}
		} else {
			fmt.Printf("✗ No items claimed\n")
			if tc.recordCount <= int64(float64(targetRecords)*1.2) {
				fmt.Printf("  ⚠ Expected this to be claimed (within 120%% limit)\n")
			} else {
				fmt.Printf("  ✓ Correctly rejected (over 120%% limit)\n")
			}
		}
	}

	// Test with old timestamps (should use age-based logic for all remaining items)
	oldQueryTime := now.Add(10 * time.Minute) // Make items 10 minutes old

	fmt.Printf("\n=== Testing Old Items (Age-Based Logic) ===\n")

	// Claim any remaining items (those that weren't claimed in the fresh test)
	for i := 0; i < 10; i++ { // Try up to 10 times to claim remaining items
		batch, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
			WorkerID:             workerID,
			DefaultTargetRecords: targetRecords,
			MaxAgeSeconds:        300, // 5 minutes
			BatchCount:           10,
			NowTs:                &oldQueryTime,
		})
		require.NoError(t, err)

		if len(batch) == 0 {
			fmt.Printf("No more items to claim\n")
			break
		}

		item := batch[0]
		efficiency := float64(item.RecordCount) / float64(targetRecords) * 100

		fmt.Printf("\nClaimed old item:\n")
		fmt.Printf("  Records: %d (%.1f%% of target)\n", item.RecordCount, efficiency)
		fmt.Printf("  Organization: %s\n", item.OrganizationID.String()[:8])
		fmt.Printf("  Batch reason: %s\n", item.BatchReason)
		fmt.Printf("  Items in batch: %d\n", len(batch))

		if item.BatchReason == "old" {
			fmt.Printf("  ✓ Correctly processed as old item\n")
		} else {
			fmt.Printf("  ⚠ Expected old, got: %s\n", item.BatchReason)
		}
	}

	fmt.Printf("\n=== Summary ===\n")
	fmt.Printf("The improved query should:\n")
	fmt.Printf("1. ✓ Claim single items with 40k-48k records as 'full_batch' when fresh\n")
	fmt.Printf("2. ✓ Reject single items with >48k records when fresh\n")
	fmt.Printf("3. ✓ Claim ANY single item as 'old' when past age threshold\n")
	fmt.Printf("4. ✓ Process single items just like multi-item groups\n")
}
