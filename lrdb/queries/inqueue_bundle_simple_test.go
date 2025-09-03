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

func TestClaimInqueueBundle_SimpleBatch(t *testing.T) {
	ctx := context.Background()
	pool := testhelpers.SetupTestLRDB(t)
	store := lrdb.NewStore(pool)
	t.Cleanup(func() {
		store.Close()
	})

	// Clean up any existing test data
	_, err := pool.Exec(ctx, "DELETE FROM inqueue WHERE bucket = 'test-simple-batch'")
	require.NoError(t, err)

	// Create a single org/instance group with 5 small files
	orgID := uuid.New()
	instanceNum := int16(1)
	signal := "metrics"
	fileSize := int64(10000) // 10KB each
	numFiles := 5

	var insertedIDs []uuid.UUID
	for i := 0; i < numFiles; i++ {
		fileID := uuid.New()
		insertedIDs = append(insertedIDs, fileID)

		// Insert files with old queue_ts to ensure they're eligible
		_, err := pool.Exec(ctx, `
			INSERT INTO inqueue (id, organization_id, collector_name, instance_num,
				bucket, object_id, signal, file_size, queue_ts, eligible_at, priority)
			VALUES ($1, $2, 'test-collector', $3, 'test-simple-batch', $4,
				$5, $6, NOW() - INTERVAL '2 minutes', NOW(), 10)`,
			fileID, orgID, instanceNum, fmt.Sprintf("file_%d.json", i), signal, fileSize)
		require.NoError(t, err)
	}

	// Claim the batch with high limits - should get all 5 files
	result, err := store.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
		WorkerID:      1000,
		Signal:        signal,
		TargetSize:    10000,           // Just 10KB target
		MaxSize:       1000000,         // 1MB max - plenty of room for all 5 files (50KB total)
		GracePeriod:   1 * time.Minute, // Files are 2 minutes old
		DeferInterval: 10 * time.Second,
		MaxAttempts:   1,   // Just need one attempt
		MaxCandidates: 100, // Fetch plenty of candidates
	})
	require.NoError(t, err)

	// CRITICAL: We should get ALL 5 files from the same group
	require.Len(t, result.Items, numFiles,
		fmt.Sprintf("Should claim all %d files from the same group (got %d)", numFiles, len(result.Items)))

	// Verify all items are from the same org/instance
	for _, item := range result.Items {
		assert.Equal(t, orgID, item.OrganizationID)
		assert.Equal(t, instanceNum, item.InstanceNum)
		assert.Equal(t, signal, item.Signal)
		assert.Equal(t, fileSize, item.FileSize)
	}

	// Calculate total size
	totalSize := int64(0)
	for _, item := range result.Items {
		totalSize += item.FileSize
	}
	assert.Equal(t, int64(numFiles)*fileSize, totalSize,
		fmt.Sprintf("Total size should be %d bytes", int64(numFiles)*fileSize))

	// Cleanup
	for _, id := range insertedIDs {
		_, _ = pool.Exec(ctx, "DELETE FROM inqueue WHERE id = $1", id)
	}
}
