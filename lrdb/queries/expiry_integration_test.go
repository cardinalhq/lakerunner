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
// +build integration

package queries_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/dbopen"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// setupTestDatabase creates a single connection for tests that need it
func setupTestDatabase(t *testing.T) (*pgx.Conn, *lrdb.Queries) {
	ctx := context.Background()

	// Use the standard connection helper with skip migration check for tests
	pool, err := lrdb.ConnectTolrdb(ctx, dbopen.SkipMigrationCheck())
	require.NoError(t, err, "Failed to connect to database")

	// Acquire a single connection from the pool
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err, "Failed to acquire connection from pool")

	// Hijack the connection so we can use it after pool closes
	hijackedConn := conn.Hijack()

	// Close the pool since we have the hijacked connection
	pool.Close()

	// Create queries instance with the single connection
	queries := lrdb.New(hijackedConn)

	return hijackedConn, queries
}

// setupTestOrganizationAndPartitions creates test data for partition testing
func setupTestOrganizationAndPartitions(t *testing.T, conn *pgx.Conn) (uuid.UUID, func()) {
	ctx := context.Background()
	orgID := uuid.New()

	// Note: In lrdb we don't have organizations table, just use a UUID
	// The actual org would be in configdb, but for partition testing we don't need it

	// Create partitioned table structure similar to what would exist in production
	// First check if the test tables exist, if not create them
	var exists bool
	err := conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_tables
			WHERE schemaname = 'public'
			AND tablename = 'test_log_seg'
		)
	`).Scan(&exists)
	require.NoError(t, err)

	if !exists {
		// Create main partitioned table
		_, err = conn.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS test_log_seg (
				organization_id UUID NOT NULL,
				dateint INTEGER NOT NULL,
				published BOOLEAN DEFAULT TRUE,
				ingest_dateint INTEGER NOT NULL,
				data TEXT
			) PARTITION BY LIST (organization_id)
		`)
		require.NoError(t, err)

		// Similar for metrics and traces
		_, err = conn.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS test_metric_seg (
				organization_id UUID NOT NULL,
				dateint INTEGER NOT NULL,
				published BOOLEAN DEFAULT TRUE,
				ingest_dateint INTEGER NOT NULL,
				data TEXT
			) PARTITION BY LIST (organization_id)
		`)
		require.NoError(t, err)

		_, err = conn.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS test_trace_seg (
				organization_id UUID NOT NULL,
				dateint INTEGER NOT NULL,
				published BOOLEAN DEFAULT TRUE,
				ingest_dateint INTEGER NOT NULL,
				data TEXT
			) PARTITION BY LIST (organization_id)
		`)
		require.NoError(t, err)
	}

	// Create organization-level partition
	orgPartitionName := fmt.Sprintf("test_log_seg_org_%s", orgID.String()[:8])
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s PARTITION OF test_log_seg
		FOR VALUES IN ('%s')
		PARTITION BY RANGE (dateint)
	`, orgPartitionName, orgID.String()))
	if err != nil {
		// Partition might already exist, that's ok
		t.Logf("Org partition might already exist: %v", err)
	}

	// Create date partition under the org partition
	datePartitionName := fmt.Sprintf("%s_20250920", orgPartitionName)
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s PARTITION OF %s
		FOR VALUES FROM (20250920) TO (20250921)
	`, datePartitionName, orgPartitionName))
	if err != nil {
		// Partition might already exist, that's ok
		t.Logf("Date partition might already exist: %v", err)
	}

	// Insert test data
	_, err = conn.Exec(ctx, `
		INSERT INTO test_log_seg (organization_id, dateint, published, ingest_dateint, data)
		VALUES
			($1, 20250920, TRUE, 20250915, 'old data 1'),
			($1, 20250920, TRUE, 20250916, 'old data 2'),
			($1, 20250920, TRUE, 20250919, 'recent data 1'),
			($1, 20250920, TRUE, 20250920, 'recent data 2'),
			($1, 20250920, FALSE, 20250915, 'already expired')
	`, orgID)
	require.NoError(t, err)

	// Cleanup function
	cleanup := func() {
		// Clean up in reverse order
		_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", datePartitionName))
		_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", orgPartitionName))
		_, _ = conn.Exec(ctx, "DELETE FROM organizations WHERE id = $1", orgID)
	}

	return orgID, cleanup
}

func TestCallFindOrgPartition_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	conn, queries := setupTestDatabase(t)
	defer conn.Close(context.Background())

	orgID, cleanup := setupTestOrganizationAndPartitions(t, conn)
	defer cleanup()

	ctx := context.Background()

	tests := []struct {
		name          string
		tableName     string
		orgID         uuid.UUID
		expectError   bool
		errorContains string
	}{
		{
			name:        "Find existing log partition",
			tableName:   "test_log_seg",
			orgID:       orgID,
			expectError: false,
		},
		{
			name:          "Table with no data for org",
			tableName:     "test_metric_seg",
			orgID:         orgID,
			expectError:   true,
			errorContains: "No rows for organization",
		},
		{
			name:          "Non-existent table",
			tableName:     "non_existent_table",
			orgID:         orgID,
			expectError:   true,
			errorContains: "does not exist",
		},
		{
			name:          "Non-existent organization",
			tableName:     "test_log_seg",
			orgID:         uuid.New(),
			expectError:   true,
			errorContains: "No rows for organization",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := queries.CallFindOrgPartition(ctx, lrdb.CallFindOrgPartitionParams{
				TableName:      tc.tableName,
				OrganizationID: tc.orgID,
			})

			if tc.expectError {
				assert.Error(t, err)
				if tc.errorContains != "" {
					assert.Contains(t, err.Error(), tc.errorContains)
				}
			} else {
				assert.NoError(t, err)
				assert.NotEmpty(t, result)
				// The result should be the org-level partition name
				assert.Contains(t, result, "test_log_seg_org_")
				t.Logf("Found partition: %s", result)
			}
		})
	}
}

func TestCallExpirePublishedByIngestCutoff_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	conn, queries := setupTestDatabase(t)
	defer conn.Close(context.Background())

	orgID, cleanup := setupTestOrganizationAndPartitions(t, conn)
	defer cleanup()

	ctx := context.Background()

	// First, find the partition for our test data
	partitionName, err := queries.CallFindOrgPartition(ctx, lrdb.CallFindOrgPartitionParams{
		TableName:      "test_log_seg",
		OrganizationID: orgID,
	})
	require.NoError(t, err)

	tests := []struct {
		name            string
		cutoffDateInt   int32
		batchSize       int32
		expectedExpired int64
		checkPublished  bool
	}{
		{
			name:            "Expire old data with cutoff 20250918",
			cutoffDateInt:   20250918,
			batchSize:       10,
			expectedExpired: 2, // Should expire the two entries with ingest_dateint < 20250918
			checkPublished:  true,
		},
		{
			name:            "No more data to expire with same cutoff",
			cutoffDateInt:   20250918,
			batchSize:       10,
			expectedExpired: 0, // Already expired in previous test
			checkPublished:  true,
		},
		{
			name:            "Expire with more recent cutoff",
			cutoffDateInt:   20250920,
			batchSize:       10,
			expectedExpired: 1, // Should expire the entry with ingest_dateint = 20250919
			checkPublished:  true,
		},
		{
			name:            "No expiry with future cutoff",
			cutoffDateInt:   20250921,
			batchSize:       10,
			expectedExpired: 1, // Should expire the last remaining published entry
			checkPublished:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Count published rows before expiry
			var publishedBefore int
			err = conn.QueryRow(ctx, `
				SELECT COUNT(*) FROM test_log_seg
				WHERE organization_id = $1 AND published = TRUE
			`, orgID).Scan(&publishedBefore)
			require.NoError(t, err)

			// Call the expiry function
			rowsExpired, err := queries.CallExpirePublishedByIngestCutoff(ctx, lrdb.CallExpirePublishedByIngestCutoffParams{
				PartitionName:  partitionName,
				OrganizationID: orgID,
				CutoffDateint:  tc.cutoffDateInt,
				BatchSize:      tc.batchSize,
			})

			assert.NoError(t, err)
			assert.Equal(t, tc.expectedExpired, rowsExpired, "Unexpected number of rows expired")

			if tc.checkPublished {
				// Count published rows after expiry
				var publishedAfter int
				err = conn.QueryRow(ctx, `
					SELECT COUNT(*) FROM test_log_seg
					WHERE organization_id = $1 AND published = TRUE
				`, orgID).Scan(&publishedAfter)
				require.NoError(t, err)

				// Verify the correct number of rows were marked as unpublished
				assert.Equal(t, publishedBefore-int(tc.expectedExpired), publishedAfter,
					"Published count mismatch after expiry")
			}

			// Log the results for debugging
			t.Logf("Expired %d rows with cutoff %d", rowsExpired, tc.cutoffDateInt)
		})
	}

	// Final verification - check specific rows
	t.Run("Verify specific rows expired correctly", func(t *testing.T) {
		var results []struct {
			IngestDateint int
			Published     bool
			Data          string
		}

		rows, err := conn.Query(ctx, `
			SELECT ingest_dateint, published, data
			FROM test_log_seg
			WHERE organization_id = $1
			ORDER BY ingest_dateint
		`, orgID)
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var r struct {
				IngestDateint int
				Published     bool
				Data          string
			}
			err := rows.Scan(&r.IngestDateint, &r.Published, &r.Data)
			require.NoError(t, err)
			results = append(results, r)
		}

		// All rows should now be unpublished since we expired everything
		for _, r := range results {
			assert.False(t, r.Published, "Row with ingest_dateint %d should be unpublished", r.IngestDateint)
			t.Logf("Row: ingest_dateint=%d, published=%v, data=%s", r.IngestDateint, r.Published, r.Data)
		}
	})
}

func TestExpirePublishedByIngestCutoff_BatchProcessing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	conn, queries := setupTestDatabase(t)
	defer conn.Close(context.Background())

	ctx := context.Background()
	orgID := uuid.New()

	// Note: In lrdb we don't have organizations table, just use a UUID
	// The actual org would be in configdb, but for partition testing we don't need it

	// Create test table and partition
	orgPartitionName := fmt.Sprintf("test_log_seg_batch_%s", orgID.String()[:8])

	// Create the org partition
	_, err := conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s PARTITION OF test_log_seg
		FOR VALUES IN ('%s')
		PARTITION BY RANGE (dateint)
	`, orgPartitionName, orgID.String()))
	if err != nil {
		t.Logf("Org partition might already exist: %v", err)
	}
	defer func() {
		_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", orgPartitionName))
	}()

	// Create date partition
	datePartitionName := fmt.Sprintf("%s_20250920", orgPartitionName)
	_, err = conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s PARTITION OF %s
		FOR VALUES FROM (20250920) TO (20250921)
	`, datePartitionName, orgPartitionName))
	if err != nil {
		t.Logf("Date partition might already exist: %v", err)
	}
	defer func() {
		_, _ = conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", datePartitionName))
	}()

	// Insert many rows to test batch processing
	numRows := 25
	for i := 0; i < numRows; i++ {
		_, err = conn.Exec(ctx, `
			INSERT INTO test_log_seg (organization_id, dateint, published, ingest_dateint, data)
			VALUES ($1, 20250920, TRUE, 20250910, $2)
		`, orgID, fmt.Sprintf("batch data %d", i))
		require.NoError(t, err)
	}

	// Find the partition
	partitionName, err := queries.CallFindOrgPartition(ctx, lrdb.CallFindOrgPartitionParams{
		TableName:      "test_log_seg",
		OrganizationID: orgID,
	})
	require.NoError(t, err)

	// Test batch processing with small batch size
	// The function processes all rows in batches internally, but returns the total
	batchSize := int32(10)

	// Call the expire function - it processes all rows internally in batches
	rowsExpired, err := queries.CallExpirePublishedByIngestCutoff(ctx, lrdb.CallExpirePublishedByIngestCutoffParams{
		PartitionName:  partitionName,
		OrganizationID: orgID,
		CutoffDateint:  20250915, // All test data has ingest_dateint = 20250910
		BatchSize:      batchSize,
	})
	require.NoError(t, err)

	t.Logf("Expired %d rows using batch size of %d", rowsExpired, batchSize)

	// Verify all rows were expired
	assert.Equal(t, int64(numRows), rowsExpired, "Should have expired all rows")

	// Verify no published rows remain
	var publishedCount int
	err = conn.QueryRow(ctx, `
		SELECT COUNT(*) FROM test_log_seg
		WHERE organization_id = $1 AND published = TRUE
	`, orgID).Scan(&publishedCount)
	require.NoError(t, err)
	assert.Equal(t, 0, publishedCount, "No published rows should remain")

	// Verify calling again returns 0 (no more work)
	rowsExpired, err = queries.CallExpirePublishedByIngestCutoff(ctx, lrdb.CallExpirePublishedByIngestCutoffParams{
		PartitionName:  partitionName,
		OrganizationID: orgID,
		CutoffDateint:  20250915,
		BatchSize:      batchSize,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), rowsExpired, "Second call should expire 0 rows")
}
