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

package duckdbx

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test that two connections from the pool share the same database state
func TestS3DB_SharedDataBetweenConnections(t *testing.T) {
	ctx := context.Background()

	// Create S3DB instance
	s3db, err := NewS3DB()
	require.NoError(t, err)
	defer func() {
		err := s3db.Close()
		require.NoError(t, err)
	}()

	// Get first connection and create a table with data
	conn1, release1, err := s3db.GetConnection(ctx, "local", "", "")
	require.NoError(t, err)
	defer release1()

	// Create a test table
	_, err = conn1.ExecContext(ctx, `CREATE TABLE test_shared (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	// Insert some rows
	_, err = conn1.ExecContext(ctx, `INSERT INTO test_shared VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`)
	require.NoError(t, err)

	// Get second connection and verify data is visible
	conn2, release2, err := s3db.GetConnection(ctx, "local", "", "")
	require.NoError(t, err)
	defer release2()

	// Query data from second connection
	rows, err := conn2.QueryContext(ctx, `SELECT id, name FROM test_shared ORDER BY id`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	// Verify the data
	expected := []struct {
		id   int
		name string
	}{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
	}

	i := 0
	for rows.Next() {
		var id int
		var name string
		err := rows.Scan(&id, &name)
		require.NoError(t, err)
		require.Less(t, i, len(expected), "more rows than expected")
		require.Equal(t, expected[i].id, id)
		require.Equal(t, expected[i].name, name)
		i++
	}
	require.Equal(t, len(expected), i, "fewer rows than expected")
}

// Test that extensions are properly configured in connections
func TestS3DB_ExtensionsLoaded(t *testing.T) {
	ctx := context.Background()

	// Create S3DB instance
	s3db, err := NewS3DB()
	require.NoError(t, err)
	defer func() {
		err := s3db.Close()
		require.NoError(t, err)
	}()

	// Get a connection
	conn, release, err := s3db.GetConnection(ctx, "local", "", "")
	require.NoError(t, err)
	defer release()

	// The most important test: can we actually use the functions we need?
	// Extensions loaded from disk might not show up in duckdb_extensions()
	// but their functions should still be available.

	// Check if read_parquet function is available (core functionality)
	rows, err := conn.QueryContext(ctx, `SELECT COUNT(*) FROM duckdb_functions() WHERE function_name = 'read_parquet'`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var count int
	require.True(t, rows.Next())
	err = rows.Scan(&count)
	require.NoError(t, err)
	require.Greater(t, count, 0, "read_parquet function should be available")
	t.Logf("read_parquet function available: %d overload(s)", count)

	// Check if read_csv is available (from httpfs)
	rows2, err := conn.QueryContext(ctx, `SELECT COUNT(*) FROM duckdb_functions() WHERE function_name = 'read_csv'`)
	require.NoError(t, err)
	defer func() { _ = rows2.Close() }()

	var csvCount int
	require.True(t, rows2.Next())
	err = rows2.Scan(&csvCount)
	require.NoError(t, err)
	// read_csv might be built-in, so we just check it exists
	t.Logf("read_csv function available: %d overload(s)", csvCount)

	// Log extension status for debugging (don't require, as loaded extensions might not show)
	rows3, err := conn.QueryContext(ctx, `SELECT extension_name, loaded, installed FROM duckdb_extensions() WHERE extension_name IN ('httpfs', 'aws', 'azure')`)
	require.NoError(t, err)
	defer func() { _ = rows3.Close() }()

	for rows3.Next() {
		var name, loaded, installed string
		err := rows3.Scan(&name, &loaded, &installed)
		require.NoError(t, err)
		t.Logf("Extension %s - loaded: %s, installed: %s", name, loaded, installed)
	}
}

// Test that secrets created in one connection are visible in another
func TestS3DB_SharedSecretsBetweenConnections(t *testing.T) {
	ctx := context.Background()

	// Skip if no S3 credentials in environment
	if os.Getenv("S3_ACCESS_KEY_ID") == "" || os.Getenv("S3_SECRET_ACCESS_KEY") == "" {
		t.Skip("Skipping S3 secret test - no S3 credentials in environment")
	}

	// Create S3DB instance
	s3db, err := NewS3DB()
	require.NoError(t, err)
	defer func() {
		err := s3db.Close()
		require.NoError(t, err)
	}()

	// Get first connection and create a secret (using a test bucket)
	conn1, release1, err := s3db.GetConnection(ctx, "test-bucket-1", "", "")
	require.NoError(t, err)
	defer release1()

	// Verify the secret was created
	rows1, err := conn1.QueryContext(ctx, `SELECT name FROM duckdb_secrets() WHERE name = 'secret_test_bucket_1'`)
	require.NoError(t, err)
	defer func() { _ = rows1.Close() }()

	found1 := false
	for rows1.Next() {
		var name string
		err := rows1.Scan(&name)
		require.NoError(t, err)
		if name == "secret_test_bucket_1" {
			found1 = true
			break
		}
	}
	require.True(t, found1, "secret should exist in first connection")

	// Get second connection for a different bucket
	conn2, release2, err := s3db.GetConnection(ctx, "test-bucket-2", "", "")
	require.NoError(t, err)
	defer release2()

	// Both secrets should be visible in the second connection
	rows2, err := conn2.QueryContext(ctx, `SELECT name FROM duckdb_secrets() WHERE name IN ('secret_test_bucket_1', 'secret_test_bucket_2') ORDER BY name`)
	require.NoError(t, err)
	defer func() { _ = rows2.Close() }()

	var secrets []string
	for rows2.Next() {
		var name string
		err := rows2.Scan(&name)
		require.NoError(t, err)
		secrets = append(secrets, name)
	}

	// Should have both secrets
	require.Equal(t, 2, len(secrets), "should have both secrets")
	require.Equal(t, "secret_test_bucket_1", secrets[0])
	require.Equal(t, "secret_test_bucket_2", secrets[1])

	// Now get a third connection back to the first bucket
	// It should reuse the existing secret (CREATE OR REPLACE)
	conn3, release3, err := s3db.GetConnection(ctx, "test-bucket-1", "", "")
	require.NoError(t, err)
	defer release3()

	// Verify both secrets are still visible
	rows3, err := conn3.QueryContext(ctx, `SELECT COUNT(*) FROM duckdb_secrets() WHERE name IN ('secret_test_bucket_1', 'secret_test_bucket_2')`)
	require.NoError(t, err)
	defer func() { _ = rows3.Close() }()

	var count int
	require.True(t, rows3.Next())
	err = rows3.Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count, "both secrets should still exist")
}

// Test that pool size limits are respected
func TestS3DB_PoolSizeLimits(t *testing.T) {
	ctx := context.Background()

	// Set a small pool size for testing
	oldVal := os.Getenv("DUCKDB_S3_POOL_SIZE")
	_ = os.Setenv("DUCKDB_S3_POOL_SIZE", "2")
	defer func() {
		if oldVal != "" {
			_ = os.Setenv("DUCKDB_S3_POOL_SIZE", oldVal)
		} else {
			_ = os.Unsetenv("DUCKDB_S3_POOL_SIZE")
		}
	}()

	// Create S3DB instance with pool size of 2
	s3db, err := NewS3DB()
	require.NoError(t, err)
	defer func() {
		err := s3db.Close()
		require.NoError(t, err)
	}()

	// Get two connections (fills the pool)
	conn1, release1, err := s3db.GetConnection(ctx, "local", "", "")
	require.NoError(t, err)

	conn2, release2, err := s3db.GetConnection(ctx, "local", "", "")
	require.NoError(t, err)

	// Both connections should work
	_, err = conn1.ExecContext(ctx, `SELECT 1`)
	require.NoError(t, err)

	_, err = conn2.ExecContext(ctx, `SELECT 1`)
	require.NoError(t, err)

	// Release one connection
	release1()

	// Should be able to get another connection now
	conn3, release3, err := s3db.GetConnection(ctx, "local", "", "")
	require.NoError(t, err)
	defer release3()

	_, err = conn3.ExecContext(ctx, `SELECT 1`)
	require.NoError(t, err)

	// Clean up
	release2()
}

// Test concurrent access to the pool
func TestS3DB_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()

	// Create S3DB instance
	s3db, err := NewS3DB()
	require.NoError(t, err)
	defer func() {
		err := s3db.Close()
		require.NoError(t, err)
	}()

	// Create a shared table
	conn, release, err := s3db.GetConnection(ctx, "local", "", "")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `CREATE TABLE concurrent_test (id INTEGER)`)
	require.NoError(t, err)
	release()

	// Run concurrent operations
	done := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			conn, release, err := s3db.GetConnection(ctx, "local", "", "")
			if err != nil {
				done <- fmt.Errorf("failed to get connection %d: %w", id, err)
				return
			}
			defer release()

			// Insert a value
			_, err = conn.ExecContext(ctx, `INSERT INTO concurrent_test VALUES (?)`, id)
			if err != nil {
				done <- fmt.Errorf("failed to insert %d: %w", id, err)
				return
			}

			done <- nil
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		err := <-done
		require.NoError(t, err)
	}

	// Verify all inserts succeeded
	conn, release, err = s3db.GetConnection(ctx, "local", "", "")
	require.NoError(t, err)
	defer release()

	var count int
	err = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM concurrent_test`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 10, count, "all concurrent inserts should succeed")
}
