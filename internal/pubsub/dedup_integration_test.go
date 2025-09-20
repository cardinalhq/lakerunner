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

package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// setupIntegrationTest creates a test database connection and returns cleanup function
func setupIntegrationTest(t *testing.T) (*lrdb.Store, func()) {
	ctx := context.Background()

	// Use the existing LRDB connection helper with warning on migration mismatches
	// This is suitable for integration tests
	store, err := lrdb.LRDBStoreForAdmin(ctx)
	require.NoError(t, err)

	// Clean up any existing test data
	_, err = store.Pool().Exec(ctx, "DELETE FROM pubsub_message_history")
	require.NoError(t, err)

	cleanup := func() {
		store.Pool().Close()
	}

	return store, cleanup
}

func TestDeduplicationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Set up test database
	store, cleanup := setupIntegrationTest(t)
	defer cleanup()

	// Create deduplicator
	dedup := NewDeduplicator(store)

	bucket := "test-bucket"
	objectID := "logs-raw/2023/01/01/file1.gz"
	source := "test"

	// First time should process
	shouldProcess, err := dedup.CheckAndRecord(ctx, bucket, objectID, source)
	require.NoError(t, err)
	require.True(t, shouldProcess, "First occurrence should be processed")

	// Second time should be duplicate
	shouldProcess, err = dedup.CheckAndRecord(ctx, bucket, objectID, source)
	require.NoError(t, err)
	require.False(t, shouldProcess, "Second occurrence should be duplicate")

	// Verify record was created in database
	count, err := store.PubSubMessageHistoryCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), count, "Should have exactly one record in database")
}

func TestDeduplicationWithoutCache(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Set up test database
	store, cleanup := setupIntegrationTest(t)
	defer cleanup()

	// Create deduplicator
	dedup := NewDeduplicator(store)

	bucket := "test-bucket"
	objectID := "logs-raw/2023/01/01/file1.gz"
	source := "test"

	// First time should process and add to DB
	shouldProcess, err := dedup.CheckAndRecord(ctx, bucket, objectID, source)
	require.NoError(t, err)
	require.True(t, shouldProcess)

	// Second time should be detected as duplicate via database
	shouldProcess, err = dedup.CheckAndRecord(ctx, bucket, objectID, source)
	require.NoError(t, err)
	require.False(t, shouldProcess)

	// Third time should also be detected as duplicate
	shouldProcess, err = dedup.CheckAndRecord(ctx, bucket, objectID, source)
	require.NoError(t, err)
	require.False(t, shouldProcess)
}

func TestDeduplicationCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Set up test database
	store, cleanup := setupIntegrationTest(t)
	defer cleanup()

	// Insert an old record using the new simplified structure
	_, err := store.PubSubMessageHistoryInsert(ctx, lrdb.PubSubMessageHistoryInsertParams{
		Bucket:   "test-bucket",
		ObjectID: "old-file.gz",
		Source:   "test",
	})
	require.NoError(t, err)

	// Manually update the timestamp to make it old
	_, err = store.Pool().Exec(ctx, `
		UPDATE pubsub_message_history
		SET received_at = $1
		WHERE bucket = $2 AND object_id = $3
	`, time.Now().Add(-25*time.Hour), "test-bucket", "old-file.gz")
	require.NoError(t, err)

	// Count records before cleanup
	totalBefore, err := store.PubSubMessageHistoryCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), totalBefore)

	// Run cleanup with 24-hour retention
	oldThreshold := time.Now().Add(-24 * time.Hour)
	_, err = store.PubSubMessageHistoryCleanup(ctx, lrdb.PubSubMessageHistoryCleanupParams{
		AgeThreshold: oldThreshold,
		BatchSize:    1000,
	})
	require.NoError(t, err)

	// Count records after cleanup
	totalAfter, err := store.PubSubMessageHistoryCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(0), totalAfter, "Old record should have been cleaned up")
}

func TestDeduplicationDifferentItems(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Set up test database
	store, cleanup := setupIntegrationTest(t)
	defer cleanup()

	// Create deduplicator
	dedup := NewDeduplicator(store)

	bucket := "test-bucket"
	objectID1 := "logs-raw/2023/01/01/file1.gz"
	objectID2 := "logs-raw/2023/01/01/file2.gz" // Different file
	source := "test"

	// Both items should process since they're different
	shouldProcess1, err := dedup.CheckAndRecord(ctx, bucket, objectID1, source)
	require.NoError(t, err)
	require.True(t, shouldProcess1)

	shouldProcess2, err := dedup.CheckAndRecord(ctx, bucket, objectID2, source)
	require.NoError(t, err)
	require.True(t, shouldProcess2)

	// Verify both records were created
	count, err := store.PubSubMessageHistoryCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(2), count, "Should have two different records")
}

func TestCheckAndRecord_NewMessage_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Set up test database
	store, cleanup := setupIntegrationTest(t)
	defer cleanup()

	// Create deduplicator
	dedup := NewDeduplicator(store)

	bucket := "test-bucket"
	objectID := "logs-raw/2023/01/01/file1.gz"
	source := "sqs"

	// First call should return true (new message)
	shouldProcess, err := dedup.CheckAndRecord(ctx, bucket, objectID, source)
	require.NoError(t, err)
	require.True(t, shouldProcess, "New message should be processed")

	// Verify record was created in database
	count, err := store.PubSubMessageHistoryCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), count, "Should have exactly one record")
}

func TestCheckAndRecord_DuplicateMessage_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Set up test database
	store, cleanup := setupIntegrationTest(t)
	defer cleanup()

	// Create deduplicator
	dedup := NewDeduplicator(store)

	bucket := "test-bucket"
	objectID := "logs-raw/2023/01/01/file1.gz"

	// First call should return true (new message)
	shouldProcess, err := dedup.CheckAndRecord(ctx, bucket, objectID, "gcp")
	require.NoError(t, err)
	require.True(t, shouldProcess, "New message should be processed")

	// Second call should return false (duplicate)
	shouldProcess, err = dedup.CheckAndRecord(ctx, bucket, objectID, "gcp")
	require.NoError(t, err)
	require.False(t, shouldProcess, "Duplicate message should not be processed")

	// Verify only one record exists
	count, err := store.PubSubMessageHistoryCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), count, "Should still have exactly one record")
}

func TestCheckAndRecord_DifferentSources_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Set up test database
	store, cleanup := setupIntegrationTest(t)
	defer cleanup()

	// Create deduplicator
	dedup := NewDeduplicator(store)

	bucket := "test-bucket"
	objectID := "logs-raw/2023/01/01/file1.gz"

	// Same message from different sources should both be processed
	shouldProcess1, err := dedup.CheckAndRecord(ctx, bucket, objectID, "sqs")
	require.NoError(t, err)
	require.True(t, shouldProcess1, "Message from SQS should be processed")

	shouldProcess2, err := dedup.CheckAndRecord(ctx, bucket, objectID, "gcp")
	require.NoError(t, err)
	require.True(t, shouldProcess2, "Same message from GCP should also be processed")

	shouldProcess3, err := dedup.CheckAndRecord(ctx, bucket, objectID, "azure")
	require.NoError(t, err)
	require.True(t, shouldProcess3, "Same message from Azure should also be processed")

	shouldProcess4, err := dedup.CheckAndRecord(ctx, bucket, objectID, "http")
	require.NoError(t, err)
	require.True(t, shouldProcess4, "Same message from HTTP should also be processed")

	// Verify four separate records exist (one per source)
	count, err := store.PubSubMessageHistoryCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(4), count, "Should have four records (one per source)")

	// Duplicates from same sources should be rejected
	shouldProcess, err := dedup.CheckAndRecord(ctx, bucket, objectID, "sqs")
	require.NoError(t, err)
	require.False(t, shouldProcess, "Duplicate SQS message should be rejected")

	shouldProcess, err = dedup.CheckAndRecord(ctx, bucket, objectID, "gcp")
	require.NoError(t, err)
	require.False(t, shouldProcess, "Duplicate GCP message should be rejected")

	// Count should remain the same
	count, err = store.PubSubMessageHistoryCount(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(4), count, "Should still have four records")
}

func TestCheckAndRecord_DatabaseConnection_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Set up test database
	store, cleanup := setupIntegrationTest(t)
	defer cleanup()

	// Close the database connection to simulate failure
	store.Pool().Close()

	// Create deduplicator with closed connection
	dedup := NewDeduplicator(store)

	bucket := "test-bucket"
	objectID := "logs-raw/2023/01/01/file1.gz"

	// Should fail closed (return false, error)
	shouldProcess, err := dedup.CheckAndRecord(ctx, bucket, objectID, "test")
	require.Error(t, err)
	require.False(t, shouldProcess, "Should fail closed when database is unavailable")
	require.Contains(t, err.Error(), "deduplication check failed")
}

func TestDeduplication_ErrorPropagation_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()

	// Set up test database
	store, cleanup := setupIntegrationTest(t)
	defer cleanup()

	// Create deduplicator with working database first
	deduplicator := NewDeduplicator(store)

	bucket := "test-bucket"
	objectID := "logs-raw/2023/01/01/file1.gz"

	// First verify normal operation works
	shouldProcess, err := deduplicator.CheckAndRecord(ctx, bucket, objectID, "test")
	require.NoError(t, err)
	require.True(t, shouldProcess, "First message should be processed")

	// Now close the database connection to simulate failure
	store.Pool().Close()

	// Create new objectID to test error propagation
	objectID2 := "logs-raw/2023/01/01/file2.gz" // Different file

	// Should return error when database is unavailable
	shouldProcess, err = deduplicator.CheckAndRecord(ctx, bucket, objectID2, "test")
	require.Error(t, err, "Should return error when database is unavailable")
	require.False(t, shouldProcess, "Should fail closed when database is unavailable")
	require.Contains(t, err.Error(), "deduplication check failed", "Error should indicate deduplication failure")

	// This verifies that the error handling change in kafka_handler.go will work correctly
	// because CheckAndRecord now properly returns errors that can be propagated
}
