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

package workqueue

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

func TestInqueueWorkflow(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	// Create test organization and work item
	orgID := uuid.New()
	bucket := "test-bucket"
	objectID := "test/object/path.json"
	telemetryType := "logs"
	workerID := int64(12345)

	// Put work in inqueue
	err := db.PutInqueueWork(ctx, lrdb.PutInqueueWorkParams{
		OrganizationID: orgID,
		CollectorName:  "default",
		Bucket:         bucket,
		ObjectID:       objectID,
		TelemetryType:  telemetryType,
		Priority:       1,
	})
	require.NoError(t, err)

	// Claim work item
	claimedWork, err := db.ClaimInqueueWork(ctx, lrdb.ClaimInqueueWorkParams{
		ClaimedBy:     workerID,
		TelemetryType: telemetryType,
	})
	require.NoError(t, err)
	assert.Equal(t, orgID, claimedWork.OrganizationID)
	assert.Equal(t, bucket, claimedWork.Bucket)
	assert.Equal(t, objectID, claimedWork.ObjectID)
	assert.Equal(t, telemetryType, claimedWork.TelemetryType)
	assert.Equal(t, workerID, claimedWork.ClaimedBy)

	// Try to claim same type of work again - should not get any since the one item is claimed
	_, err = db.ClaimInqueueWork(ctx, lrdb.ClaimInqueueWorkParams{
		ClaimedBy:     workerID + 1, // Different worker
		TelemetryType: telemetryType,
	})
	// This should return an error since no work is available
	assert.Error(t, err, "Should not find available work of same type")

	// Test work completion by deleting
	err = db.DeleteInqueueWork(ctx, lrdb.DeleteInqueueWorkParams{
		ID:        claimedWork.ID,
		ClaimedBy: workerID,
	})
	assert.NoError(t, err)

	// Try to claim work again - should fail since it was deleted
	_, err = db.ClaimInqueueWork(ctx, lrdb.ClaimInqueueWorkParams{
		ClaimedBy:     workerID,
		TelemetryType: telemetryType,
	})
	assert.Error(t, err, "Should not find work after deletion")
}

func TestInqueueJournalOperations(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	bucket := "test-journal-bucket"
	objectID := "test/journal/object.parquet"

	// Test journal upsert - should be new entry
	isNew, err := db.InqueueJournalUpsert(ctx, lrdb.InqueueJournalUpsertParams{
		OrganizationID: orgID,
		Bucket:         bucket,
		ObjectID:       objectID,
	})
	require.NoError(t, err)
	assert.True(t, isNew, "First upsert should be new entry")

	// Test journal upsert again - should not be new
	isNew, err = db.InqueueJournalUpsert(ctx, lrdb.InqueueJournalUpsertParams{
		OrganizationID: orgID,
		Bucket:         bucket,
		ObjectID:       objectID,
	})
	require.NoError(t, err)
	assert.False(t, isNew, "Second upsert should not be new entry")

	// Test journal deletion
	err = db.InqueueJournalDelete(ctx, lrdb.InqueueJournalDeleteParams{
		OrganizationID: orgID,
		Bucket:         bucket,
		ObjectID:       objectID,
	})
	assert.NoError(t, err)

	// Test upsert after deletion - should be new again
	isNew, err = db.InqueueJournalUpsert(ctx, lrdb.InqueueJournalUpsertParams{
		OrganizationID: orgID,
		Bucket:         bucket,
		ObjectID:       objectID,
	})
	require.NoError(t, err)
	assert.True(t, isNew, "Upsert after deletion should be new entry")
}

func TestInqueueSummary(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()

	// Add some work items of different types
	err := db.PutInqueueWork(ctx, lrdb.PutInqueueWorkParams{
		OrganizationID: orgID,
		CollectorName:  "default",
		Bucket:         "test-bucket",
		ObjectID:       "logs/test1.json",
		TelemetryType:  "logs",
		Priority:       1,
	})
	require.NoError(t, err)

	err = db.PutInqueueWork(ctx, lrdb.PutInqueueWorkParams{
		OrganizationID: orgID,
		CollectorName:  "default",
		Bucket:         "test-bucket",
		ObjectID:       "logs/test2.json",
		TelemetryType:  "logs",
		Priority:       1,
	})
	require.NoError(t, err)

	err = db.PutInqueueWork(ctx, lrdb.PutInqueueWorkParams{
		OrganizationID: orgID,
		CollectorName:  "default",
		Bucket:         "test-bucket",
		ObjectID:       "metrics/test1.parquet",
		TelemetryType:  "metrics",
		Priority:       1,
	})
	require.NoError(t, err)

	// Get summary
	summary, err := db.InqueueSummary(ctx)
	require.NoError(t, err)

	// Verify summary contains expected counts
	logCount := int64(0)
	metricCount := int64(0)
	for _, s := range summary {
		if s.TelemetryType == "logs" {
			logCount = s.Count
		} else if s.TelemetryType == "metrics" {
			metricCount = s.Count
		}
	}

	assert.Equal(t, int64(2), logCount, "Should have 2 log items")
	assert.Equal(t, int64(1), metricCount, "Should have 1 metric item")
}

func TestInqueueCleanup(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	telemetryType := "logs"
	workerID := int64(99999)

	// Add work item
	err := db.PutInqueueWork(ctx, lrdb.PutInqueueWorkParams{
		OrganizationID: orgID,
		CollectorName:  "default",
		Bucket:         "cleanup-test-bucket",
		ObjectID:       "cleanup/test.json",
		TelemetryType:  telemetryType,
		Priority:       1,
	})
	require.NoError(t, err)

	// Claim work
	claimedWork, err := db.ClaimInqueueWork(ctx, lrdb.ClaimInqueueWorkParams{
		ClaimedBy:     workerID,
		TelemetryType: telemetryType,
	})
	require.NoError(t, err)
	assert.Equal(t, workerID, claimedWork.ClaimedBy)

	// Run cleanup (this will reset items claimed more than 5 minutes ago)
	cutoffTime := time.Now().Add(-5 * time.Minute)
	_, err = db.CleanupInqueueWork(ctx, &cutoffTime)
	assert.NoError(t, err)

	// For recently claimed work, it should still be claimed
	// (Real cleanup only affects items older than 5 minutes)
}
