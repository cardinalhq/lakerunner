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

	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/testhelpers"
)

func TestClaimMetricCompactionWork_FullBatchFromTestData(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	// Load test data from file
	err := loadMetricCompactionQueueTestData(ctx, db.(*lrdb.Store))
	require.NoError(t, err, "Failed to load test data")

	// Verify data was loaded
	var rowCount int
	err = db.(*lrdb.Store).Pool().QueryRow(ctx, "SELECT COUNT(*) FROM metric_compaction_queue").Scan(&rowCount)
	require.NoError(t, err)
	fmt.Printf("Loaded %d rows into metric_compaction_queue\n", rowCount)

	workerID := int64(99998) // Different worker ID to avoid conflicts
	
	// Test the improved query with the same scenarios
	lastDataTime := time.Date(2025, 9, 1, 15, 12, 55, 809443000, time.UTC)
	testTimes := []struct {
		name   string
		time   time.Time
		maxAge int32
	}{
		{"1 min after, 5min age", lastDataTime.Add(1 * time.Minute), 300},
		{"6 min after, 5min age", lastDataTime.Add(6 * time.Minute), 300},
		{"1 min after, 30sec age", lastDataTime.Add(1 * time.Minute), 30},
	}

	for _, tt := range testTimes {
		fmt.Printf("\n=== IMPROVED QUERY Test: %s ===\n", tt.name)
		fmt.Printf("Query time: %s\n", tt.time.Format(time.RFC3339Nano))
		fmt.Printf("Max age seconds: %d\n", tt.maxAge)
		
		// Run several batches to see what gets claimed
		for batchNum := 1; batchNum <= 5; batchNum++ {
			fmt.Printf("\n--- Batch %d ---\n", batchNum)
			
			// Call the query - it now uses the improved logic
			claimedBatch, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
				WorkerID:             workerID,
				DefaultTargetRecords: 40000,
				MaxAgeSeconds:        tt.maxAge,
				BatchCount:           50,
				NowTs:                &tt.time,
			})
			require.NoError(t, err)

			if len(claimedBatch) == 0 {
				fmt.Printf("No items claimed in batch %d\n", batchNum)
				break
			}

			fmt.Printf("Claimed %d items in batch %d:\n", len(claimedBatch), batchNum)
			
			totalRecords := int64(0)
			for i, item := range claimedBatch {
				if i < 10 { // Show first 10 items details
					fmt.Printf("  [%d] ID=%d, OrgID=%s, Priority=%d, Records=%d, QueueTS=%s\n",
						i+1, item.ID, item.OrganizationID.String()[:8]+"...", 
						item.Priority, item.RecordCount, item.QueueTs.Format("15:04:05.000"))
				}
				totalRecords += item.RecordCount
			}
			
			if len(claimedBatch) > 10 {
				fmt.Printf("  ... and %d more items\n", len(claimedBatch)-10)
			}
			
			fmt.Printf("Total records in batch: %d\n", totalRecords)
			fmt.Printf("Target records used: %d (%.1f%% of target)\n", 
				claimedBatch[0].UsedTargetRecords, 
				float64(totalRecords)/float64(claimedBatch[0].UsedTargetRecords)*100)
			fmt.Printf("Estimate source: %s\n", claimedBatch[0].EstimateSource)
			
			// Group by organization and show distribution
			orgCounts := make(map[string]int)
			for _, item := range claimedBatch {
				orgKey := item.OrganizationID.String()[:8]
				orgCounts[orgKey]++
			}
			
			fmt.Printf("Organization distribution:\n")
			for orgKey, count := range orgCounts {
				fmt.Printf("  %s...: %d items\n", orgKey, count)
			}

			// Check if we're getting efficient batches
			targetRecords := claimedBatch[0].UsedTargetRecords
			efficiency := float64(totalRecords) / float64(targetRecords)
			if efficiency >= 1.0 && efficiency <= 1.2 {
				fmt.Printf("✓ Efficient batch: %.1f%% of target (within 100-120%% range)\n", efficiency*100)
			} else {
				fmt.Printf("⏰ Processed regardless of size (likely old items)\n")
			}
		}
	}
}

