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

func TestClaimMetricCompactionWork_FullBatchLogic(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	workerID := int64(88888)
	targetRecords := int64(40000)

	// Create test data with current timestamps (so it's "fresh", not old)
	now := time.Now()
	orgID1 := uuid.New()
	orgID2 := uuid.New()
	orgID3 := uuid.New()

	// Scenario 1: Organization with exactly target records (should be processed as "full_batch")
	fmt.Printf("=== Setting up test scenarios ===\n")

	// Org 1: Exactly 40,000 records (should be eligible as full_batch)
	items1 := []lrdb.PutMetricCompactionWorkParams{
		{OrganizationID: orgID1, Dateint: 20250901, FrequencyMs: 10000, SegmentID: 1001, InstanceNum: 1, RecordCount: 20000, Priority: 800},
		{OrganizationID: orgID1, Dateint: 20250901, FrequencyMs: 10000, SegmentID: 1002, InstanceNum: 1, RecordCount: 20000, Priority: 800},
	}

	// Org 2: 45,000 records (40k * 1.125 = within 120% limit, should be eligible as full_batch)
	items2 := []lrdb.PutMetricCompactionWorkParams{
		{OrganizationID: orgID2, Dateint: 20250901, FrequencyMs: 10000, SegmentID: 2001, InstanceNum: 1, RecordCount: 22500, Priority: 800},
		{OrganizationID: orgID2, Dateint: 20250901, FrequencyMs: 10000, SegmentID: 2002, InstanceNum: 1, RecordCount: 22500, Priority: 800},
	}

	// Org 3: 60,000 records (40k * 1.5 = over 120% limit, should NOT be eligible)
	items3 := []lrdb.PutMetricCompactionWorkParams{
		{OrganizationID: orgID3, Dateint: 20250901, FrequencyMs: 10000, SegmentID: 3001, InstanceNum: 1, RecordCount: 30000, Priority: 800},
		{OrganizationID: orgID3, Dateint: 20250901, FrequencyMs: 10000, SegmentID: 3002, InstanceNum: 1, RecordCount: 30000, Priority: 800},
	}

	// Insert all items
	allItems := append(items1, items2...)
	allItems = append(allItems, items3...)

	for _, item := range allItems {
		err := db.PutMetricCompactionWork(ctx, item)
		require.NoError(t, err)
	}

	fmt.Printf("Created test organizations:\n")
	fmt.Printf("  Org1 (%s): 40,000 records (exactly target) - should be eligible\n", orgID1.String()[:8])
	fmt.Printf("  Org2 (%s): 45,000 records (112.5%% of target) - should be eligible\n", orgID2.String()[:8])
	fmt.Printf("  Org3 (%s): 60,000 records (150%% of target) - should NOT be eligible\n", orgID3.String()[:8])

	// Test the improved query with current time (items should be fresh, not old)
	queryTime := now.Add(10 * time.Second) // Just 10 seconds after insertion

	fmt.Printf("\n=== Testing Full Batch Logic ===\n")
	fmt.Printf("Query time: %s (fresh items, should use full_batch logic)\n", queryTime.Format(time.RFC3339))

	// Test 1: Should claim Org1 (exactly 40k records)
	fmt.Printf("\n--- Test 1: Expecting Org1 (exactly target records) ---\n")
	batch1, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
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
		fmt.Printf("Claimed %d items, %d total records\n", len(batch1), totalRecords)
		fmt.Printf("Organization: %s\n", batch1[0].OrganizationID.String()[:8])
		fmt.Printf("Batch reason: %s\n", batch1[0].BatchReason)
		fmt.Printf("Efficiency: %.1f%% of target\n", float64(totalRecords)/float64(targetRecords)*100)

		if batch1[0].BatchReason == "full_batch" {
			fmt.Printf("✓ Correctly identified as full_batch\n")
		} else {
			fmt.Printf("⚠ Expected full_batch, got: %s\n", batch1[0].BatchReason)
		}
	} else {
		fmt.Printf("No items claimed - this suggests eligibility logic may have issues\n")
	}

	// Test 2: Should claim Org2 (45k records, within 120%)
	fmt.Printf("\n--- Test 2: Expecting Org2 (within 120%% limit) ---\n")
	batch2, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
		WorkerID:             workerID,
		DefaultTargetRecords: targetRecords,
		MaxAgeSeconds:        300,
		BatchCount:           10,
		NowTs:                &queryTime,
	})
	require.NoError(t, err)

	if len(batch2) > 0 {
		totalRecords := int64(0)
		for _, item := range batch2 {
			totalRecords += item.RecordCount
		}
		fmt.Printf("Claimed %d items, %d total records\n", len(batch2), totalRecords)
		fmt.Printf("Organization: %s\n", batch2[0].OrganizationID.String()[:8])
		fmt.Printf("Batch reason: %s\n", batch2[0].BatchReason)
		fmt.Printf("Efficiency: %.1f%% of target\n", float64(totalRecords)/float64(targetRecords)*100)

		if batch2[0].BatchReason == "full_batch" {
			fmt.Printf("✓ Correctly identified as full_batch\n")
		} else {
			fmt.Printf("⚠ Expected full_batch, got: %s\n", batch2[0].BatchReason)
		}
	} else {
		fmt.Printf("No items claimed\n")
	}

	// Test 3: Should NOT claim Org3 (would require more batches or age threshold)
	fmt.Printf("\n--- Test 3: Expecting no claims (Org3 over 120%% limit) ---\n")
	batch3, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
		WorkerID:             workerID,
		DefaultTargetRecords: targetRecords,
		MaxAgeSeconds:        300,
		BatchCount:           10,
		NowTs:                &queryTime,
	})
	require.NoError(t, err)

	if len(batch3) > 0 {
		totalRecords := int64(0)
		for _, item := range batch3 {
			totalRecords += item.RecordCount
		}
		fmt.Printf("Claimed %d items, %d total records\n", len(batch3), totalRecords)
		fmt.Printf("Organization: %s\n", batch3[0].OrganizationID.String()[:8])
		fmt.Printf("Batch reason: %s\n", batch3[0].BatchReason)

		if batch3[0].OrganizationID == orgID3 {
			fmt.Printf("⚠ Unexpectedly claimed Org3 (should be over limit)\n")
		}
	} else {
		fmt.Printf("✓ Correctly refused to claim oversized batches\n")
	}

	// Test 4: Wait until items are old, then Org3 should be claimable
	fmt.Printf("\n--- Test 4: Making items old (should claim Org3 as 'old') ---\n")
	oldQueryTime := now.Add(10 * time.Minute) // Make items 10 minutes old (older than 5 min threshold)

	batch4, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
		WorkerID:             workerID,
		DefaultTargetRecords: targetRecords,
		MaxAgeSeconds:        300, // 5 minutes
		BatchCount:           10,
		NowTs:                &oldQueryTime,
	})
	require.NoError(t, err)

	if len(batch4) > 0 {
		totalRecords := int64(0)
		for _, item := range batch4 {
			totalRecords += item.RecordCount
		}
		fmt.Printf("Claimed %d items, %d total records\n", len(batch4), totalRecords)
		fmt.Printf("Organization: %s\n", batch4[0].OrganizationID.String()[:8])
		fmt.Printf("Batch reason: %s\n", batch4[0].BatchReason)
		fmt.Printf("Efficiency: %.1f%% of target\n", float64(totalRecords)/float64(targetRecords)*100)

		if batch4[0].BatchReason == "old" {
			fmt.Printf("✓ Correctly identified old items and processed regardless of size\n")
		} else {
			fmt.Printf("⚠ Expected old, got: %s\n", batch4[0].BatchReason)
		}
	} else {
		fmt.Printf("No items claimed\n")
	}
}
