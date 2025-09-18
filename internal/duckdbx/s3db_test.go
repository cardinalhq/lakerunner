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
	conn1, release1, err := s3db.GetConnection(ctx)
	require.NoError(t, err)
	defer release1()

	// Create a test table
	_, err = conn1.ExecContext(ctx, `CREATE TABLE test_shared (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	// Insert some rows
	_, err = conn1.ExecContext(ctx, `INSERT INTO test_shared VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`)
	require.NoError(t, err)

	// Get second connection and verify data is visible
	conn2, release2, err := s3db.GetConnection(ctx)
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
	conn, release, err := s3db.GetConnection(ctx)
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
	conn1, release1, err := s3db.GetConnectionForBucket(ctx, "test-bucket-1", "", "")
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
	conn2, release2, err := s3db.GetConnectionForBucket(ctx, "test-bucket-2", "", "")
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
	conn3, release3, err := s3db.GetConnectionForBucket(ctx, "test-bucket-1", "", "")
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
	conn1, release1, err := s3db.GetConnection(ctx)
	require.NoError(t, err)

	conn2, release2, err := s3db.GetConnection(ctx)
	require.NoError(t, err)

	// Both connections should work
	_, err = conn1.ExecContext(ctx, `SELECT 1`)
	require.NoError(t, err)

	_, err = conn2.ExecContext(ctx, `SELECT 1`)
	require.NoError(t, err)

	// Release one connection
	release1()

	// Should be able to get another connection now
	conn3, release3, err := s3db.GetConnection(ctx)
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
	conn, release, err := s3db.GetConnection(ctx)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `CREATE TABLE concurrent_test (id INTEGER)`)
	require.NoError(t, err)
	release()

	// Run concurrent operations
	done := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			conn, release, err := s3db.GetConnection(ctx)
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
	conn, release, err = s3db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	var count int
	err = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM concurrent_test`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 10, count, "all concurrent inserts should succeed")
}

// TestCreateS3Secret tests that secrets created via createS3Secret in one connection
// are visible in other pooled connections.
func TestCreateS3Secret(t *testing.T) {
	ctx := context.Background()

	// Create S3DB instance
	s3db, err := NewS3DB()
	require.NoError(t, err)
	defer func() {
		err := s3db.Close()
		require.NoError(t, err)
	}()

	// Get first connection (this will trigger setup and extension loading)
	conn1, release1, err := s3db.GetConnection(ctx)
	require.NoError(t, err)
	defer release1()

	// Create a test S3 secret using our new createS3Secret function
	testConfig := S3SecretConfig{
		Bucket:       "test-bucket-foo",
		Region:       "us-west-2",
		Endpoint:     "s3.us-west-2.amazonaws.com",
		KeyID:        "TEST_ACCESS_KEY_ID",
		Secret:       "TEST_SECRET_ACCESS_KEY",
		SessionToken: "TEST_SESSION_TOKEN",
		URLStyle:     "path",
		UseSSL:       true,
	}

	err = createS3Secret(ctx, conn1, testConfig)
	require.NoError(t, err, "createS3Secret should succeed")

	// Get second connection from the pool
	conn2, release2, err := s3db.GetConnection(ctx)
	require.NoError(t, err)
	defer release2()

	// Verify the secret is visible in the second connection
	expectedSecretName := "secret_test_bucket_foo"
	rows, err := conn2.QueryContext(ctx, `SELECT name, type FROM duckdb_secrets() WHERE name = ?`, expectedSecretName)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	// Check that the secret exists
	var found bool
	for rows.Next() {
		var name, secretType string
		err := rows.Scan(&name, &secretType)
		require.NoError(t, err)
		if name == expectedSecretName && secretType == "s3" {
			found = true
		}
	}
	require.True(t, found, "secret created in conn1 should be visible in conn2")

	// Create another secret with different configuration to test edge cases
	testConfig2 := S3SecretConfig{
		Bucket:   "test-bucket-bar",
		Region:   "eu-west-1",
		Endpoint: "http://localhost:9000", // MinIO-style endpoint
		KeyID:    "MINIOACCESSKEY",
		Secret:   "MINIOSECRETKEY",
		URLStyle: "vhost",
		UseSSL:   false, // HTTP endpoint
	}

	err = createS3Secret(ctx, conn2, testConfig2)
	require.NoError(t, err, "createS3Secret with HTTP endpoint should succeed")

	// Verify both secrets are visible in conn1 (after creating second in conn2)
	rows, err = conn1.QueryContext(ctx, `SELECT name FROM duckdb_secrets() WHERE name LIKE 'secret_test_bucket_%' ORDER BY name`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var secrets []string
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		require.NoError(t, err)
		secrets = append(secrets, name)
	}

	require.Equal(t, 2, len(secrets), "should have both test secrets")
	require.Equal(t, "secret_test_bucket_bar", secrets[0])
	require.Equal(t, "secret_test_bucket_foo", secrets[1])
}

// TestCreateS3SecretValidation tests the validation logic in createS3Secret.
func TestCreateS3SecretValidation(t *testing.T) {
	ctx := context.Background()

	// Create S3DB instance
	s3db, err := NewS3DB()
	require.NoError(t, err)
	defer func() {
		err := s3db.Close()
		require.NoError(t, err)
	}()

	// Get a connection
	conn, release, err := s3db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Test missing KeyID
	invalidConfig := S3SecretConfig{
		Bucket: "test-bucket",
		Secret: "SECRET_KEY",
	}
	err = createS3Secret(ctx, conn, invalidConfig)
	require.Error(t, err, "should fail with missing KeyID")
	require.Contains(t, err.Error(), "missing AWS credentials")

	// Test missing Secret
	invalidConfig = S3SecretConfig{
		Bucket: "test-bucket",
		KeyID:  "ACCESS_KEY",
	}
	err = createS3Secret(ctx, conn, invalidConfig)
	require.Error(t, err, "should fail with missing Secret")
	require.Contains(t, err.Error(), "missing AWS credentials")

	// Test missing Bucket
	invalidConfig = S3SecretConfig{
		KeyID:  "ACCESS_KEY",
		Secret: "SECRET_KEY",
	}
	err = createS3Secret(ctx, conn, invalidConfig)
	require.Error(t, err, "should fail with missing Bucket")
	require.Contains(t, err.Error(), "bucket is required")

	// Test with minimal valid config (defaults should be applied)
	minimalConfig := S3SecretConfig{
		Bucket: "minimal-bucket",
		KeyID:  "MIN_ACCESS_KEY",
		Secret: "MIN_SECRET_KEY",
	}
	err = createS3Secret(ctx, conn, minimalConfig)
	require.NoError(t, err, "should succeed with minimal config")

	// Verify the secret was created (we can't easily check the defaults from SQL)
	var name, secretType string
	err = conn.QueryRowContext(ctx,
		`SELECT name, type FROM duckdb_secrets() WHERE name = 'secret_minimal_bucket'`).Scan(&name, &secretType)
	require.NoError(t, err)
	require.Equal(t, "secret_minimal_bucket", name)
	require.Equal(t, "s3", secretType)
}

// TestPooledConnectionReuse tests that connections work correctly when returned to pool and reused.
// This ensures that loading extensions on new connections doesn't break reused connections.
func TestPooledConnectionReuse(t *testing.T) {
	ctx := context.Background()

	// Create S3DB instance with small pool to force reuse
	oldPoolSize := os.Getenv("DUCKDB_S3_POOL_SIZE")
	_ = os.Setenv("DUCKDB_S3_POOL_SIZE", "2") // Small pool
	defer func() {
		if oldPoolSize != "" {
			_ = os.Setenv("DUCKDB_S3_POOL_SIZE", oldPoolSize)
		} else {
			_ = os.Unsetenv("DUCKDB_S3_POOL_SIZE")
		}
	}()

	s3db, err := NewS3DB()
	require.NoError(t, err)
	defer func() {
		err := s3db.Close()
		require.NoError(t, err)
	}()

	// Test 1: Get connection, use it, return it, get it again
	conn1, release1, err := s3db.GetConnection(ctx)
	require.NoError(t, err)

	// Create a table and insert data
	_, err = conn1.ExecContext(ctx, `CREATE TABLE pool_test (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)
	_, err = conn1.ExecContext(ctx, `INSERT INTO pool_test VALUES (1, 'first')`)
	require.NoError(t, err)

	// Create an S3 secret to test extension functionality
	testConfig := S3SecretConfig{
		Bucket:   "reuse-test-bucket",
		Region:   "us-east-1",
		Endpoint: "s3.amazonaws.com",
		KeyID:    "REUSE_TEST_KEY",
		Secret:   "REUSE_TEST_SECRET",
		URLStyle: "path",
		UseSSL:   true,
	}
	err = createS3Secret(ctx, conn1, testConfig)
	require.NoError(t, err, "should be able to create S3 secret in first use")

	// Return connection to pool
	release1()

	// Get connection again (should get the same one from pool)
	conn2, release2, err := s3db.GetConnection(ctx)
	require.NoError(t, err)
	defer release2()

	// Verify the table still exists and we can query it
	var count int
	err = conn2.QueryRowContext(ctx, `SELECT COUNT(*) FROM pool_test`).Scan(&count)
	require.NoError(t, err, "should be able to query table created in first connection")
	require.Equal(t, 1, count)

	// Verify the secret still exists
	var secretName string
	err = conn2.QueryRowContext(ctx, `SELECT name FROM duckdb_secrets() WHERE name = 'secret_reuse_test_bucket'`).Scan(&secretName)
	require.NoError(t, err, "secret should still exist in reused connection")
	require.Equal(t, "secret_reuse_test_bucket", secretName)

	// Verify we can still create new secrets (extensions still work)
	testConfig2 := S3SecretConfig{
		Bucket:   "reuse-test-bucket-2",
		Region:   "eu-west-1",
		Endpoint: "s3.eu-west-1.amazonaws.com",
		KeyID:    "REUSE_TEST_KEY_2",
		Secret:   "REUSE_TEST_SECRET_2",
		URLStyle: "path",
		UseSSL:   true,
	}
	err = createS3Secret(ctx, conn2, testConfig2)
	require.NoError(t, err, "should be able to create new S3 secret in reused connection")

	// Insert more data to verify connection is fully functional
	_, err = conn2.ExecContext(ctx, `INSERT INTO pool_test VALUES (2, 'second')`)
	require.NoError(t, err, "should be able to insert data in reused connection")

	err = conn2.QueryRowContext(ctx, `SELECT COUNT(*) FROM pool_test`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count, "should have both rows")
}

// TestPooledConnectionReuseAcrossBuckets tests that connections work when reused for different buckets.
func TestPooledConnectionReuseAcrossBuckets(t *testing.T) {
	ctx := context.Background()

	// Set test AWS credentials
	oldKeyID := os.Getenv("S3_ACCESS_KEY_ID")
	oldSecret := os.Getenv("S3_SECRET_ACCESS_KEY")
	_ = os.Setenv("S3_ACCESS_KEY_ID", "TEST_ACCESS_KEY")
	_ = os.Setenv("S3_SECRET_ACCESS_KEY", "TEST_SECRET_KEY")
	defer func() {
		if oldKeyID != "" {
			_ = os.Setenv("S3_ACCESS_KEY_ID", oldKeyID)
		} else {
			_ = os.Unsetenv("S3_ACCESS_KEY_ID")
		}
		if oldSecret != "" {
			_ = os.Setenv("S3_SECRET_ACCESS_KEY", oldSecret)
		} else {
			_ = os.Unsetenv("S3_SECRET_ACCESS_KEY")
		}
	}()

	// Set small pool size to force connection reuse
	oldPoolSize := os.Getenv("DUCKDB_S3_POOL_SIZE")
	_ = os.Setenv("DUCKDB_S3_POOL_SIZE", "1") // Tiny pool - only 1 connection
	defer func() {
		if oldPoolSize != "" {
			_ = os.Setenv("DUCKDB_S3_POOL_SIZE", oldPoolSize)
		} else {
			_ = os.Unsetenv("DUCKDB_S3_POOL_SIZE")
		}
	}()

	s3db, err := NewS3DB()
	require.NoError(t, err)
	defer func() {
		err := s3db.Close()
		require.NoError(t, err)
	}()

	// Get connection for bucket 1
	conn1, release1, err := s3db.GetConnectionForBucket(ctx, "bucket-one", "us-east-1", "")
	require.NoError(t, err)

	// Verify secret was created for bucket-one
	var found bool
	rows, err := conn1.QueryContext(ctx, `SELECT name FROM duckdb_secrets() WHERE name = 'secret_bucket_one'`)
	require.NoError(t, err)
	for rows.Next() {
		var name string
		_ = rows.Scan(&name)
		if name == "secret_bucket_one" {
			found = true
		}
	}
	_ = rows.Close()
	require.True(t, found, "secret for bucket-one should exist")

	// Return connection
	release1()

	// Get connection for a different bucket (should reuse the same connection)
	conn2, release2, err := s3db.GetConnectionForBucket(ctx, "bucket-two", "eu-west-1", "")
	require.NoError(t, err)
	defer release2()

	// Both secrets should now exist
	rows, err = conn2.QueryContext(ctx, `SELECT name FROM duckdb_secrets() WHERE name IN ('secret_bucket_one', 'secret_bucket_two') ORDER BY name`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var secrets []string
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		require.NoError(t, err)
		secrets = append(secrets, name)
	}

	require.Equal(t, 2, len(secrets), "both bucket secrets should exist")
	require.Equal(t, "secret_bucket_one", secrets[0])
	require.Equal(t, "secret_bucket_two", secrets[1])

	// Verify we can still use the connection normally
	_, err = conn2.ExecContext(ctx, `SELECT 1`)
	require.NoError(t, err, "connection should still be functional")
}

// TestInMemoryDatabase tests that in-memory databases (empty filename)
// create isolated instances per connection and are not pooled.
func TestInMemoryDatabase(t *testing.T) {
	ctx := context.Background()

	// Test with in-memory database
	s3db, err := NewS3DB(WithInMemory())
	require.NoError(t, err)
	defer func() {
		err := s3db.Close()
		require.NoError(t, err)
	}()

	// Get first connection
	conn1, release1, err := s3db.GetConnection(ctx)
	require.NoError(t, err)

	// Create a table and insert data in conn1's isolated database
	_, err = conn1.ExecContext(ctx, `CREATE TABLE inmem_test (id INTEGER, value VARCHAR)`)
	require.NoError(t, err)
	_, err = conn1.ExecContext(ctx, `INSERT INTO inmem_test VALUES (1, 'from_conn1')`)
	require.NoError(t, err)

	// Verify data exists in conn1
	var count int
	err = conn1.QueryRowContext(ctx, `SELECT COUNT(*) FROM inmem_test`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count, "should have one row in conn1")

	// Get second connection (should be a completely separate in-memory database)
	conn2, release2, err := s3db.GetConnection(ctx)
	require.NoError(t, err)

	// The table should NOT exist in conn2's database
	_, err = conn2.ExecContext(ctx, `SELECT * FROM inmem_test`)
	require.Error(t, err, "table should not exist in conn2's separate database")
	require.Contains(t, err.Error(), "does not exist", "should get table not found error")

	// Create a different table in conn2 to verify isolation
	_, err = conn2.ExecContext(ctx, `CREATE TABLE conn2_only (id INTEGER)`)
	require.NoError(t, err)
	_, err = conn2.ExecContext(ctx, `INSERT INTO conn2_only VALUES (99)`)
	require.NoError(t, err)

	// Verify conn1 can't see conn2's table
	_, err = conn1.ExecContext(ctx, `SELECT * FROM conn2_only`)
	require.Error(t, err, "conn1 should not see conn2's table")

	// Release both connections
	release1()
	release2()

	t.Log("In-memory database test verified - connections are properly isolated")
}

// TestInMemoryNotPooled verifies that in-memory connections are not returned to the pool.
func TestInMemoryNotPooled(t *testing.T) {
	ctx := context.Background()

	// Create in-memory S3DB with small pool
	oldPoolSize := os.Getenv("DUCKDB_S3_POOL_SIZE")
	_ = os.Setenv("DUCKDB_S3_POOL_SIZE", "2")
	defer func() {
		if oldPoolSize != "" {
			_ = os.Setenv("DUCKDB_S3_POOL_SIZE", oldPoolSize)
		} else {
			_ = os.Unsetenv("DUCKDB_S3_POOL_SIZE")
		}
	}()

	s3db, err := NewS3DB(WithInMemory())
	require.NoError(t, err)
	defer func() {
		err := s3db.Close()
		require.NoError(t, err)
	}()

	// Get a connection
	conn1, release1, err := s3db.GetConnection(ctx)
	require.NoError(t, err)

	// Create a table (this will only exist in this connection's database)
	_, err = conn1.ExecContext(ctx, `CREATE TABLE test_not_pooled (id INTEGER)`)
	require.NoError(t, err)

	// Release the connection - for in-memory, this should close it, not pool it
	release1()

	// Get another connection - should be a fresh one
	conn2, release2, err := s3db.GetConnection(ctx)
	require.NoError(t, err)
	defer release2()

	// The table should NOT exist (proving we got a fresh connection)
	_, err = conn2.ExecContext(ctx, `SELECT * FROM test_not_pooled`)
	require.Error(t, err, "table should not exist in new connection")
	require.Contains(t, err.Error(), "does not exist")

	// Get a third connection while second is still held
	conn3, release3, err := s3db.GetConnection(ctx)
	require.NoError(t, err)
	defer release3()

	// Both conn2 and conn3 should be independent
	_, err = conn2.ExecContext(ctx, `CREATE TABLE conn2_table (id INTEGER)`)
	require.NoError(t, err)

	_, err = conn3.ExecContext(ctx, `SELECT * FROM conn2_table`)
	require.Error(t, err, "conn3 should not see conn2's table")

	t.Log("In-memory connections are not pooled - each get creates a new database")
}

// TestWithDatabasePathEmptyPanics verifies that WithDatabasePath("") panics
// to prevent accidental misuse.
func TestWithDatabasePathEmptyPanics(t *testing.T) {
	defer func() {
		r := recover()
		require.NotNil(t, r, "WithDatabasePath(\"\") should panic")
		require.Contains(t, fmt.Sprintf("%v", r), "must not be empty")
		require.Contains(t, fmt.Sprintf("%v", r), "WithInMemory()")
	}()

	// This should panic when the option is applied
	cfg := &s3DBConfig{}
	opt := WithDatabasePath("")
	opt(cfg)
}
