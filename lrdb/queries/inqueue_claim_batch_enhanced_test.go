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

// TestClaimInqueueWorkBatch_OversizedFile tests Rule 1: Oversized files should be claimed alone
func TestClaimInqueueWorkBatch_OversizedFile(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	telemetryType := "logs"
	workerID := int64(12345)

	// Create files where first file is oversized
	workItems := []lrdb.PutInqueueWorkParams{
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "huge_file.json.gz",
			TelemetryType:  telemetryType,
			Priority:       10,              // Highest priority
			FileSize:       2 * 1024 * 1024, // 2MB (oversized for 1MB limit)
		},
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "normal_file1.json.gz",
			TelemetryType:  telemetryType,
			Priority:       5,
			FileSize:       1024, // 1KB
		},
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "normal_file2.json.gz",
			TelemetryType:  telemetryType,
			Priority:       5,
			FileSize:       2048, // 2KB
		},
	}

	// Add work items to queue
	for _, item := range workItems {
		err := db.PutInqueueWork(ctx, item)
		require.NoError(t, err)
	}

	// Claim batch - should only get the oversized file
	claimedBatch, err := db.ClaimInqueueWorkBatch(ctx, lrdb.ClaimInqueueWorkBatchParams{
		TelemetryType: telemetryType,
		WorkerID:      workerID,
		MaxTotalSize:  1024 * 1024, // 1MB limit
		MinTotalSize:  512 * 1024,  // 512KB minimum for fresh files
		MaxAgeSeconds: 30,          // 30 seconds
		BatchCount:    10,          // Allow up to 10 files
	})
	require.NoError(t, err)

	// Should claim exactly 1 item (the oversized one)
	assert.Len(t, claimedBatch, 1)
	assert.Equal(t, "huge_file.json.gz", claimedBatch[0].ObjectID)
	assert.Equal(t, int64(2*1024*1024), claimedBatch[0].FileSize)
}

// TestClaimInqueueWorkBatch_TooOldFiles tests Rule 2: Old files should be processed eagerly
func TestClaimInqueueWorkBatch_TooOldFiles(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	telemetryType := "logs"
	workerID := int64(12345)

	// Create files with old timestamps
	workItems := []lrdb.PutInqueueWorkParams{
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "old_file1.json.gz",
			TelemetryType:  telemetryType,
			Priority:       10,
			FileSize:       1024, // 1KB
		},
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "old_file2.json.gz",
			TelemetryType:  telemetryType,
			Priority:       10,
			FileSize:       2048, // 2KB
		},
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "old_file3.json.gz",
			TelemetryType:  telemetryType,
			Priority:       10,
			FileSize:       3072, // 3KB
		},
	}

	// Add work items to queue
	for _, item := range workItems {
		err := db.PutInqueueWork(ctx, item)
		require.NoError(t, err)
	}

	// Use future timestamp for now_ts to make files appear old
	futureTime := time.Now().Add(60 * time.Second)
	claimedBatch, err := db.ClaimInqueueWorkBatch(ctx, lrdb.ClaimInqueueWorkBatchParams{
		TelemetryType: telemetryType,
		WorkerID:      workerID,
		NowTs:         &futureTime, // Use future time to make files appear old
		MaxTotalSize:  1024 * 1024, // 1MB limit
		MinTotalSize:  512 * 1024,  // 512KB minimum (ignored for old files)
		MaxAgeSeconds: 30,          // 30 seconds
		BatchCount:    10,          // Allow up to 10 files
	})
	require.NoError(t, err)

	// Should claim all files that fit within size limit (ignore minimum for old files)
	assert.Len(t, claimedBatch, 3)

	// Verify total size is within limit
	totalSize := int64(0)
	for _, item := range claimedBatch {
		totalSize += item.FileSize
	}
	assert.LessOrEqual(t, totalSize, int64(1024*1024))
}

// TestClaimInqueueWorkBatch_FreshFilesBelowMinimum tests Rule 3: Fresh files below minimum should wait
func TestClaimInqueueWorkBatch_FreshFilesBelowMinimum(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	telemetryType := "logs"
	workerID := int64(12345)

	// Create small fresh files that don't meet minimum
	workItems := []lrdb.PutInqueueWorkParams{
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "small_file1.json.gz",
			TelemetryType:  telemetryType,
			Priority:       10,
			FileSize:       1024, // 1KB
		},
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "small_file2.json.gz",
			TelemetryType:  telemetryType,
			Priority:       10,
			FileSize:       2048, // 2KB
		},
	}

	// Add work items to queue
	for _, item := range workItems {
		err := db.PutInqueueWork(ctx, item)
		require.NoError(t, err)
	}

	// Claim batch - should return nothing because total size < minimum
	claimedBatch, err := db.ClaimInqueueWorkBatch(ctx, lrdb.ClaimInqueueWorkBatchParams{
		TelemetryType: telemetryType,
		WorkerID:      workerID,
		MaxTotalSize:  1024 * 1024, // 1MB limit
		MinTotalSize:  512 * 1024,  // 512KB minimum
		MaxAgeSeconds: 30,          // 30 seconds
		BatchCount:    10,          // Allow up to 10 files
	})
	require.NoError(t, err)

	// Should claim nothing (wait for more files)
	assert.Len(t, claimedBatch, 0)
}

// TestClaimInqueueWorkBatch_FreshFilesAboveMinimum tests Rule 3: Fresh files above minimum should be claimed
func TestClaimInqueueWorkBatch_FreshFilesAboveMinimum(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	telemetryType := "logs"
	workerID := int64(12345)

	// Create files that together meet the minimum
	workItems := []lrdb.PutInqueueWorkParams{
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "medium_file1.json.gz",
			TelemetryType:  telemetryType,
			Priority:       10,
			FileSize:       400 * 1024, // 400KB
		},
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "medium_file2.json.gz",
			TelemetryType:  telemetryType,
			Priority:       10,
			FileSize:       300 * 1024, // 300KB
		},
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "small_file3.json.gz",
			TelemetryType:  telemetryType,
			Priority:       10,
			FileSize:       200 * 1024, // 200KB
		},
	}

	// Add work items to queue
	for _, item := range workItems {
		err := db.PutInqueueWork(ctx, item)
		require.NoError(t, err)
	}

	// Claim batch - should get files that fit within size limit
	claimedBatch, err := db.ClaimInqueueWorkBatch(ctx, lrdb.ClaimInqueueWorkBatchParams{
		TelemetryType: telemetryType,
		WorkerID:      workerID,
		MaxTotalSize:  1024 * 1024, // 1MB limit
		MinTotalSize:  512 * 1024,  // 512KB minimum
		MaxAgeSeconds: 30,          // 30 seconds
		BatchCount:    10,          // Allow up to 10 files
	})
	require.NoError(t, err)

	// Should claim all 3 files (400KB + 300KB + 200KB = 900KB, which is >= 512KB minimum and <= 1MB limit)
	assert.Len(t, claimedBatch, 3)

	// Verify total size meets minimum and stays within limit
	totalSize := int64(0)
	for _, item := range claimedBatch {
		totalSize += item.FileSize
	}
	assert.GreaterOrEqual(t, totalSize, int64(512*1024)) // >= minimum
	assert.LessOrEqual(t, totalSize, int64(1024*1024))   // <= limit
}

// TestClaimInqueueWorkBatch_BatchCountLimit tests that batch_count parameter limits number of files
func TestClaimInqueueWorkBatch_BatchCountLimit(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	telemetryType := "logs"
	workerID := int64(12345)

	// Create many small files
	workItems := make([]lrdb.PutInqueueWorkParams, 10)
	for i := 0; i < 10; i++ {
		workItems[i] = lrdb.PutInqueueWorkParams{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       fmt.Sprintf("file_%d.json.gz", i),
			TelemetryType:  telemetryType,
			Priority:       10,
			FileSize:       1024, // 1KB each
		}
	}

	// Add work items to queue
	for _, item := range workItems {
		err := db.PutInqueueWork(ctx, item)
		require.NoError(t, err)
	}

	// Use future timestamp to trigger Rule 2 (eager processing)
	futureTime := time.Now().Add(60 * time.Second)
	claimedBatch, err := db.ClaimInqueueWorkBatch(ctx, lrdb.ClaimInqueueWorkBatchParams{
		TelemetryType: telemetryType,
		WorkerID:      workerID,
		NowTs:         &futureTime, // Make files appear old
		MaxTotalSize:  1024 * 1024, // 1MB limit (plenty of room)
		MinTotalSize:  0,           // No minimum
		MaxAgeSeconds: 30,          // 30 seconds
		BatchCount:    3,           // Limit to 3 files
	})
	require.NoError(t, err)

	// Should claim exactly 3 items despite having more available
	assert.Len(t, claimedBatch, 3)
}

// TestClaimInqueueWorkBatch_PriorityOrdering tests that files are claimed in priority order
func TestClaimInqueueWorkBatch_PriorityOrdering(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	telemetryType := "logs"
	workerID := int64(12345)

	// Create files with different priorities
	workItems := []lrdb.PutInqueueWorkParams{
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "low_priority.json.gz",
			TelemetryType:  telemetryType,
			Priority:       1, // Low priority
			FileSize:       1024,
		},
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "high_priority.json.gz",
			TelemetryType:  telemetryType,
			Priority:       10, // High priority
			FileSize:       1024,
		},
		{
			OrganizationID: orgID,
			CollectorName:  "collector1",
			InstanceNum:    1,
			Bucket:         "test-bucket",
			ObjectID:       "medium_priority.json.gz",
			TelemetryType:  telemetryType,
			Priority:       5, // Medium priority
			FileSize:       1024,
		},
	}

	// Add work items to queue
	for _, item := range workItems {
		err := db.PutInqueueWork(ctx, item)
		require.NoError(t, err)
	}

	// Use future timestamp to trigger Rule 2 (eager processing)
	futureTime := time.Now().Add(60 * time.Second)
	claimedBatch, err := db.ClaimInqueueWorkBatch(ctx, lrdb.ClaimInqueueWorkBatchParams{
		TelemetryType: telemetryType,
		WorkerID:      workerID,
		NowTs:         &futureTime, // Make files appear old
		MaxTotalSize:  1024 * 1024, // 1MB limit
		MinTotalSize:  0,           // No minimum
		MaxAgeSeconds: 30,          // 30 seconds
		BatchCount:    10,          // Allow all files
	})
	require.NoError(t, err)

	// Should claim all 3 items in priority order (highest first)
	require.Len(t, claimedBatch, 3)
	assert.Equal(t, "high_priority.json.gz", claimedBatch[0].ObjectID)
	assert.Equal(t, int32(10), claimedBatch[0].Priority)
	assert.Equal(t, "medium_priority.json.gz", claimedBatch[1].ObjectID)
	assert.Equal(t, int32(5), claimedBatch[1].Priority)
	assert.Equal(t, "low_priority.json.gz", claimedBatch[2].ObjectID)
	assert.Equal(t, int32(1), claimedBatch[2].Priority)
}
