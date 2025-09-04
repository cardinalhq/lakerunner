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

// TestRollupBundleLargeBatchLimit tests that rollup bundling respects the estimated target
// and doesn't claim too many items even when they share the same rollup_group.
// This test reproduces the issue where 24 items with 43K records each (1M+ total)
// were being claimed when the target was only 36,667.
func TestRollupBundleLargeBatchLimit(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	// Test organization and parameters from the real case
	orgID := uuid.MustParse("65928f26-224b-4acb-8e57-9ee628164694")
	dateint := int32(20250902)
	frequencyMs := int32(300000) // 5 minutes
	instanceNum := int16(15)
	slotID := int32(0)
	slotCount := int32(1)
	rollupGroup := int64(1464030)

	// Setup: Insert metric pack estimate for this org/frequency
	// This simulates the estimated target of 36,667 records
	estimatedTarget := int64(36667)
	err := store.UpsertMetricPackEstimate(ctx, lrdb.UpsertMetricPackEstimateParams{
		OrganizationID: orgID,
		FrequencyMs:    frequencyMs,
		TargetRecords:  &estimatedTarget,
	})
	require.NoError(t, err)

	// The store's internal estimator will pick up the new estimate

	// Setup: Queue 24 work items with same rollup_group (simulating the real case)
	// Each with 43,000 records (except one with 33,222 and one with 22,842)
	recordCounts := []int64{
		43000, 43000, 43000, 43000, 43000, 43000, 43000, 43000,
		43000, 43000, 43000, 43000, 43000, 43000, 43000, 43000,
		43000, 43000, 43000, 43000, 43000, 33222, 22842, 43000,
	}

	now := time.Now()
	eligibleAt := now.Add(-10 * time.Minute) // Make them all eligible

	for i, recordCount := range recordCounts {
		segmentID := int64(300326400073403426 + i*100000) // Simulate segment IDs
		err := store.MrqQueueWork(ctx, lrdb.MrqQueueWorkParams{
			OrganizationID: orgID,
			Dateint:        dateint,
			FrequencyMs:    frequencyMs,
			InstanceNum:    instanceNum,
			SlotID:         slotID,
			SlotCount:      slotCount,
			SegmentID:      segmentID,
			RecordCount:    recordCount,
			RollupGroup:    rollupGroup,
			Priority:       0,
			EligibleAt:     eligibleAt,
		})
		require.NoError(t, err)
	}

	// Test: Claim bundle with realistic parameters
	workerID := int64(300330702136674004) // From the real case
	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 40000, // This should be ignored in favor of estimator
		OverFactor:    1.2,
		BatchLimit:    100, // Allow fetching up to 100 candidates
		Grace:         time.Hour,
		DeferBase:     5 * time.Minute,
		MaxAttempts:   5,
	}

	result, err := store.ClaimRollupBundle(ctx, params)
	require.NoError(t, err)
	require.NotNil(t, result)

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
		"Should not claim all items just because they share rollup_group")
}

// TestRollupBundleWithoutEstimate tests that rollup bundling uses the default
// when no estimate is configured for an organization/frequency
func TestRollupBundleWithoutEstimate(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t).(*lrdb.Store)

	// Test with a new organization that has no estimates
	orgID := uuid.New()
	dateint := int32(20250902)
	frequencyMs := int32(60000) // 1 minute
	instanceNum := int16(1)
	slotID := int32(0)
	slotCount := int32(1)
	rollupGroup := int64(12345)

	// Queue 10 work items with 50,000 records each
	now := time.Now()
	eligibleAt := now.Add(-10 * time.Minute)

	for i := 0; i < 10; i++ {
		segmentID := int64(100000 + i)
		err := store.MrqQueueWork(ctx, lrdb.MrqQueueWorkParams{
			OrganizationID: orgID,
			Dateint:        dateint,
			FrequencyMs:    frequencyMs,
			InstanceNum:    instanceNum,
			SlotID:         slotID,
			SlotCount:      slotCount,
			SegmentID:      segmentID,
			RecordCount:    50000,
			RollupGroup:    rollupGroup,
			Priority:       0,
			EligibleAt:     eligibleAt,
		})
		require.NoError(t, err)
	}

	// Test: Claim bundle
	workerID := int64(999)
	params := lrdb.BundleParams{
		WorkerID:      workerID,
		TargetRecords: 1000000, // This should be ignored
		OverFactor:    1.2,
		BatchLimit:    100,
		Grace:         time.Hour,
		DeferBase:     5 * time.Minute,
		MaxAttempts:   5,
	}

	result, err := store.ClaimRollupBundle(ctx, params)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should use the default from estimator (40,000)
	expectedDefault := int64(40000)
	assert.Equal(t, expectedDefault, result.EstimatedTarget,
		"Should use default target when no estimate exists")

	// With default of 40K and first item having 50K, should claim only 1
	assert.Equal(t, 1, len(result.Items),
		"Should claim only 1 item when it exceeds default target")
}
