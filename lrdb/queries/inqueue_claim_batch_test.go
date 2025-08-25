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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/testhelpers"
)

func TestClaimInqueueWorkBatch(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	telemetryType := "logs"
	workerID := int64(12345)

	// Create multiple work items for the same organization and telemetry type
	workItems := []lrdb.PutInqueueWorkParams{
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "logs1.json.gz",
			TelemetryType:  telemetryType,
			Priority:       1,
			FileSize:       1024,
		},
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "logs2.json.gz",
			TelemetryType:  telemetryType,
			Priority:       1,
			FileSize:       2048,
		},
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "logs3.json.gz",
			TelemetryType:  telemetryType,
			Priority:       1,
			FileSize:       512,
		},
	}

	// Add work items to queue
	for _, item := range workItems {
		err := db.PutInqueueWork(ctx, item)
		require.NoError(t, err)
	}

	// Add work item for different organization - should not be claimed together
	differentOrgID := uuid.New()
	err := db.PutInqueueWork(ctx, lrdb.PutInqueueWorkParams{
		OrganizationID: differentOrgID,
		CollectorName:  "collector1",
		InstanceNum:    1,
		Bucket:         "test-bucket",
		ObjectID:       "logs_different_org.json.gz",
		TelemetryType:  telemetryType,
		Priority:       1,
		FileSize:       1024,
	})
	require.NoError(t, err)

	// Claim batch of work items
	claimedBatch, err := db.ClaimInqueueWorkBatch(ctx, lrdb.ClaimInqueueWorkBatchParams{
		TelemetryType: telemetryType,
		WorkerID:      workerID,
		MaxTotalSize:  1024 * 1024, // 1MB
		MinTotalSize:  0,           // No minimum
		MaxAgeSeconds: 30,          // 30 seconds
		BatchCount:    2,           // Limit to 2 items
	})
	require.NoError(t, err)

	// Should claim exactly 2 items from same organization
	assert.Len(t, claimedBatch, 2)

	// All claimed items should have same organization and telemetry type
	for _, item := range claimedBatch {
		assert.Equal(t, orgID, item.OrganizationID)
		assert.Equal(t, telemetryType, item.TelemetryType)
		assert.Equal(t, workerID, item.ClaimedBy)
		assert.Equal(t, int16(1), item.InstanceNum)
	}

	// Verify that total file size is tracked
	totalSize := int64(0)
	for _, item := range claimedBatch {
		totalSize += item.FileSize
	}
	assert.Greater(t, totalSize, int64(0), "Total file size should be greater than 0")

	// Try to claim more work - should get remaining item from same org
	claimedBatch2, err := db.ClaimInqueueWorkBatch(ctx, lrdb.ClaimInqueueWorkBatchParams{
		TelemetryType: telemetryType,
		WorkerID:      workerID + 1, // Different worker
		MaxTotalSize:  1024 * 1024,  // 1MB
		MinTotalSize:  0,            // No minimum
		MaxAgeSeconds: 30,           // 30 seconds
		BatchCount:    10,
	})
	require.NoError(t, err)

	// Should claim exactly 1 remaining item from same org
	assert.Len(t, claimedBatch2, 1)
	assert.Equal(t, orgID, claimedBatch2[0].OrganizationID)

	// Try to claim more work - should get item from different org
	claimedBatch3, err := db.ClaimInqueueWorkBatch(ctx, lrdb.ClaimInqueueWorkBatchParams{
		TelemetryType: telemetryType,
		WorkerID:      workerID + 2, // Different worker
		MaxTotalSize:  1024 * 1024,  // 1MB
		MinTotalSize:  0,            // No minimum
		MaxAgeSeconds: 30,           // 30 seconds
		BatchCount:    10,
	})
	require.NoError(t, err)

	// Should claim exactly 1 item from different org
	assert.Len(t, claimedBatch3, 1)
	assert.Equal(t, differentOrgID, claimedBatch3[0].OrganizationID)
}

func TestClaimInqueueWorkBatch_DifferentTelemetryTypes(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)

	// Create work items for different telemetry types
	workItems := []lrdb.PutInqueueWorkParams{
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "logs1.json.gz",
			TelemetryType:  "logs",
			Priority:       1,
			FileSize:       1024,
		},
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "metrics1.json.gz",
			TelemetryType:  "metrics",
			Priority:       1,
			FileSize:       2048,
		},
	}

	// Add work items to queue
	for _, item := range workItems {
		err := db.PutInqueueWork(ctx, item)
		require.NoError(t, err)
	}

	// Claim logs work
	logsBatch, err := db.ClaimInqueueWorkBatch(ctx, lrdb.ClaimInqueueWorkBatchParams{
		TelemetryType: "logs",
		WorkerID:      workerID,
		MaxTotalSize:  1024 * 1024, // 1MB
		MinTotalSize:  0,           // No minimum
		MaxAgeSeconds: 30,          // 30 seconds
		BatchCount:    10,
	})
	require.NoError(t, err)

	// Should claim exactly 1 logs item
	assert.Len(t, logsBatch, 1)
	assert.Equal(t, "logs", logsBatch[0].TelemetryType)

	// Claim metrics work
	metricsBatch, err := db.ClaimInqueueWorkBatch(ctx, lrdb.ClaimInqueueWorkBatchParams{
		TelemetryType: "metrics",
		WorkerID:      workerID + 1,
		MaxTotalSize:  1024 * 1024, // 1MB
		MinTotalSize:  0,           // No minimum
		MaxAgeSeconds: 30,          // 30 seconds
		BatchCount:    10,
	})
	require.NoError(t, err)

	// Should claim exactly 1 metrics item
	assert.Len(t, metricsBatch, 1)
	assert.Equal(t, "metrics", metricsBatch[0].TelemetryType)
}

func TestClaimInqueueWorkBatch_EmptyQueue(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	workerID := int64(12345)

	// Try to claim work from empty queue
	claimedBatch, err := db.ClaimInqueueWorkBatch(ctx, lrdb.ClaimInqueueWorkBatchParams{
		TelemetryType: "logs",
		WorkerID:      workerID,
		MaxTotalSize:  1024 * 1024, // 1MB
		MinTotalSize:  0,           // No minimum
		MaxAgeSeconds: 30,          // 30 seconds
		BatchCount:    10,
	})

	// Should succeed but return empty batch
	require.NoError(t, err)
	assert.Len(t, claimedBatch, 0)
}
