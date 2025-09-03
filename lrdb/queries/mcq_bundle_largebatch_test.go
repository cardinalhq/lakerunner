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

// TestCompactionBundleLargeBatchLimit tests that compaction bundling respects the estimated target
// and doesn't claim too many items even when they're from the same organization/dateint/frequency.
// This test is similar to the rollup test but for metric compaction queue.
func TestCompactionBundleLargeBatchLimit(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	// Test organization and parameters similar to the rollup case
	orgID := uuid.MustParse("65928f26-224b-4acb-8e57-9ee628164694")
	dateint := int32(20250902)
	frequencyMs := int32(10000) // 10 seconds (compaction typically happens at 10s)
	instanceNum := int16(15)

	// Setup: Insert metric pack estimate for this org/frequency
	// This simulates the estimated target of 36,667 records
	estimatedTarget := int64(36667)
	err := store.UpsertMetricPackEstimate(ctx, lrdb.UpsertMetricPackEstimateParams{
		OrganizationID: orgID,
		FrequencyMs:    frequencyMs,
		TargetRecords:  &estimatedTarget,
	})
	require.NoError(t, err)

	// Setup: Queue 24 work items (simulating multiple segments to compact)
	// Each with 43,000 records (except one with 33,222 and one with 22,842)
	recordCounts := []int64{
		43000, 43000, 43000, 43000, 43000, 43000, 43000, 43000,
		43000, 43000, 43000, 43000, 43000, 43000, 43000, 43000,
		43000, 43000, 43000, 43000, 43000, 33222, 22842, 43000,
	}

	// Insert items with old queue_ts so they're eligible
	oldQueueTs := time.Now().Add(-time.Hour)
	for _, recordCount := range recordCounts {
		// Note: insertMCQRow uses recordCount+1000 as segment_id internally
		insertMCQRow(t, store, orgID, dateint, int64(frequencyMs), instanceNum, recordCount, oldQueueTs)
	}

	// Test: Claim bundle with realistic parameters
	workerID := int64(300330702136674004) // From the real case
	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 1000000, // This should be ignored in favor of estimator
		OverFactor:    1.2,
		BatchLimit:    100, // Allow fetching up to 100 candidates
		Grace:         5 * time.Minute,
		DeferBase:     30 * time.Second,
		MaxAttempts:   5,
	}

	result, err := store.ClaimCompactionBundle(ctx, params)
	require.NoError(t, err)
	require.NotEmpty(t, result.Items)

	// Verify: The bundle should respect the estimated target
	assert.Equal(t, estimatedTarget, result.EstimatedTarget, "Should use the estimated target")

	// Calculate total records in claimed bundle
	totalRecords := int64(0)
	for _, item := range result.Items {
		totalRecords += item.RecordCount
	}

	// The bundle should NOT contain all 24 items (which would be 1M+ records)
	// With target of 36,667 and over factor of 1.2, the max should be ~44,000
	maxAllowed := int64(float64(estimatedTarget) * params.OverFactor)

	t.Logf("Claimed %d items with %d total records (target: %d, max: %d)",
		len(result.Items), totalRecords, estimatedTarget, maxAllowed)

	// Key assertions:
	// 1. Should claim only 1 item since first item (43,000) already exceeds target
	assert.Equal(t, 1, len(result.Items), "Should claim only 1 item when first item exceeds target")

	// 2. Total records should not exceed the over limit
	assert.LessOrEqual(t, totalRecords, maxAllowed,
		"Total records should not exceed target * over_factor")

	// 3. Should not claim all 24 items
	assert.Less(t, len(result.Items), 24,
		"Should not claim all items just because they're from same org/dateint/frequency")
}

// TestCompactionBundleWithoutEstimate tests that compaction bundling uses the default
// when no estimate is configured for an organization/frequency
func TestCompactionBundleWithoutEstimate(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	// Test with a new organization that has no estimates
	orgID := uuid.New()
	dateint := int32(20250902)
	frequencyMs := int32(10000) // 10 seconds
	instanceNum := int16(1)

	// Queue 10 work items with 50,000 records each (old enough to be eligible)
	oldQueueTs := time.Now().Add(-time.Hour)
	for i := 0; i < 10; i++ {
		insertMCQRow(t, store, orgID, dateint, int64(frequencyMs), instanceNum, 50000, oldQueueTs)
	}

	// Test: Claim bundle
	workerID := int64(999)
	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 1000000, // This should be ignored
		OverFactor:    1.2,
		BatchLimit:    100,
		Grace:         5 * time.Minute,
		DeferBase:     30 * time.Second,
		MaxAttempts:   5,
	}

	result, err := store.ClaimCompactionBundle(ctx, params)
	require.NoError(t, err)
	require.NotEmpty(t, result.Items)

	// Should use the default from estimator (40,000)
	expectedDefault := int64(40000)
	assert.Equal(t, expectedDefault, result.EstimatedTarget,
		"Should use default target when no estimate exists")

	// With default of 40K and first item having 50K, should claim only 1
	assert.Equal(t, 1, len(result.Items),
		"Should claim only 1 item when it exceeds default target")
}

// TestCompactionBundleMultipleSmallSegments tests compaction with multiple small segments
// that together fit within the target
func TestCompactionBundleMultipleSmallSegments(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	orgID := uuid.New()
	dateint := int32(20250902)
	frequencyMs := int32(10000)
	instanceNum := int16(1)

	// Set up a specific estimate for this test
	estimatedTarget := int64(20000)
	err := store.UpsertMetricPackEstimate(ctx, lrdb.UpsertMetricPackEstimateParams{
		OrganizationID: orgID,
		FrequencyMs:    frequencyMs,
		TargetRecords:  &estimatedTarget,
	})
	require.NoError(t, err)

	// Queue several small segments that can be bundled together
	recordCounts := []int64{5000, 6000, 4000, 3000, 7000, 8000}
	oldQueueTs := time.Now().Add(-time.Hour)

	var segmentIDs []int64
	for i, recordCount := range recordCounts {
		// Add small time differences to ensure order
		queueTs := oldQueueTs.Add(time.Duration(i) * time.Second)
		id := insertMCQRow(t, store, orgID, dateint, int64(frequencyMs), instanceNum, recordCount, queueTs)
		// Extract the actual segment ID from the database
		var segmentID int64
		err := store.Pool().QueryRow(ctx, "SELECT segment_id FROM metric_compaction_queue WHERE id = $1", id).Scan(&segmentID)
		require.NoError(t, err)
		segmentIDs = append(segmentIDs, segmentID)
	}

	// Test: Claim bundle
	workerID := int64(555)
	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 100000, // Should be ignored
		OverFactor:    1.2,    // Max = 20000 * 1.2 = 24000
		BatchLimit:    100,
		Grace:         5 * time.Minute,
		DeferBase:     30 * time.Second,
		MaxAttempts:   5,
	}

	result, err := store.ClaimCompactionBundle(ctx, params)
	require.NoError(t, err)
	require.NotEmpty(t, result.Items)

	// Calculate total records
	totalRecords := int64(0)
	for _, item := range result.Items {
		totalRecords += item.RecordCount
	}

	t.Logf("Claimed %d items with %d total records (target: %d, max: %d)",
		len(result.Items), totalRecords, estimatedTarget, int64(float64(estimatedTarget)*1.2))

	// Should bundle: 5000 + 6000 + 4000 + 3000 = 18000 (4 items)
	// Adding the 5th item (7000) would make it 25000, which exceeds 24000 limit
	assert.Equal(t, 4, len(result.Items), "Should claim first 4 segments")
	assert.Equal(t, int64(18000), totalRecords, "Total should be 18000 records")

	// Verify the correct segments were claimed (in queue_ts order)
	for i := 0; i < 4; i++ {
		assert.Equal(t, segmentIDs[i], result.Items[i].SegmentID,
			"Should claim segments in queue_ts order")
	}
}
