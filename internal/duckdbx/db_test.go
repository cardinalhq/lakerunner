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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Test that two connections from the pool share the same database state
func TestDB_SharedDataBetweenConnections(t *testing.T) {
	ctx := context.Background()

	// Create DB instance
	db, err := NewDB()
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	// Get first connection and create a table with data
	conn1, release1, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release1()

	// Create a test table
	_, err = conn1.ExecContext(ctx, `CREATE TABLE test_shared (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)

	// Insert some rows
	_, err = conn1.ExecContext(ctx, `INSERT INTO test_shared VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')`)
	require.NoError(t, err)

	// Get second connection and verify data is visible
	conn2, release2, err := db.GetConnection(ctx)
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

// Test that pool size limits are respected
func TestDB_PoolSizeLimits(t *testing.T) {
	ctx := context.Background()

	// Set a small pool size for testing
	oldVal := os.Getenv("DUCKDB_POOL_SIZE")
	_ = os.Setenv("DUCKDB_POOL_SIZE", "2")
	defer func() {
		if oldVal != "" {
			_ = os.Setenv("DUCKDB_POOL_SIZE", oldVal)
		} else {
			_ = os.Unsetenv("DUCKDB_POOL_SIZE")
		}
	}()

	// Create DB instance with pool size of 2
	db, err := NewDB()
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	// Get two connections (fills the pool)
	conn1, release1, err := db.GetConnection(ctx)
	require.NoError(t, err)

	conn2, release2, err := db.GetConnection(ctx)
	require.NoError(t, err)

	// Both connections should work
	_, err = conn1.ExecContext(ctx, `SELECT 1`)
	require.NoError(t, err)

	_, err = conn2.ExecContext(ctx, `SELECT 1`)
	require.NoError(t, err)

	// Release one connection
	release1()

	// Should be able to get another connection now
	conn3, release3, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release3()

	_, err = conn3.ExecContext(ctx, `SELECT 1`)
	require.NoError(t, err)

	// Clean up
	release2()
}

// Test concurrent access to the pool
func TestDB_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()

	// Create DB instance
	db, err := NewDB()
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	// Create a shared table
	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `CREATE TABLE concurrent_test (id INTEGER)`)
	require.NoError(t, err)
	release()

	// Run concurrent operations
	done := make(chan error, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			conn, release, err := db.GetConnection(ctx)
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
	conn, release, err = db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	var count int
	err = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM concurrent_test`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 10, count, "all concurrent inserts should succeed")
}

// TestPooledConnectionReuse tests that connections work correctly when returned to pool and reused.
func TestPooledConnectionReuse(t *testing.T) {
	ctx := context.Background()

	// Create DB instance with small pool to force reuse
	oldPoolSize := os.Getenv("DUCKDB_POOL_SIZE")
	_ = os.Setenv("DUCKDB_POOL_SIZE", "2") // Small pool
	defer func() {
		if oldPoolSize != "" {
			_ = os.Setenv("DUCKDB_POOL_SIZE", oldPoolSize)
		} else {
			_ = os.Unsetenv("DUCKDB_POOL_SIZE")
		}
	}()

	db, err := NewDB()
	require.NoError(t, err)
	defer func() {
		err := db.Close()
		require.NoError(t, err)
	}()

	// Test 1: Get connection, use it, return it, get it again
	conn1, release1, err := db.GetConnection(ctx)
	require.NoError(t, err)

	// Create a table and insert data
	_, err = conn1.ExecContext(ctx, `CREATE TABLE pool_test (id INTEGER, name VARCHAR)`)
	require.NoError(t, err)
	_, err = conn1.ExecContext(ctx, `INSERT INTO pool_test VALUES (1, 'first')`)
	require.NoError(t, err)

	// Return connection to pool
	release1()

	// Get connection again (should get the same one from pool)
	conn2, release2, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release2()

	// Verify the table still exists and we can query it
	var count int
	err = conn2.QueryRowContext(ctx, `SELECT COUNT(*) FROM pool_test`).Scan(&count)
	require.NoError(t, err, "should be able to query table created in first connection")
	require.Equal(t, 1, count)

	// Insert more data to verify connection is fully functional
	_, err = conn2.ExecContext(ctx, `INSERT INTO pool_test VALUES (2, 'second')`)
	require.NoError(t, err, "should be able to insert data in reused connection")

	err = conn2.QueryRowContext(ctx, `SELECT COUNT(*) FROM pool_test`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 2, count, "should have both rows")
}

// TestWithDatabasePathEmptyPanics verifies that WithDatabasePath("") panics
// to prevent accidental misuse.
func TestWithDatabasePathEmptyPanics(t *testing.T) {
	defer func() {
		r := recover()
		require.NotNil(t, r, "WithDatabasePath(\"\") should panic")
		require.Contains(t, fmt.Sprintf("%v", r), "must not be empty")
	}()

	// This should panic when the option is applied
	cfg := &dbConfig{}
	opt := WithDatabasePath("")
	opt(cfg)
}

// TestGetDatabasePath verifies that GetDatabasePath returns the correct path.
func TestGetDatabasePath(t *testing.T) {
	// Test with auto-generated temp path
	db1, err := NewDB()
	require.NoError(t, err)
	defer func() { _ = db1.Close() }()

	path1 := db1.GetDatabasePath()
	require.NotEmpty(t, path1)
	require.Contains(t, path1, "global.ddb")
	t.Logf("Auto-generated path: %s", path1)

	// Test with explicit path
	testPath := filepath.Join(t.TempDir(), "test.ddb")
	db2, err := NewDB(WithDatabasePath(testPath))
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	path2 := db2.GetDatabasePath()
	require.Equal(t, testPath, path2)
	t.Logf("Explicit path: %s", path2)
}

// TestCloseDoesNotDeleteUserProvidedPaths verifies that DB.Close() only removes
// internally-created temp directories and not user-provided paths.
func TestCloseDoesNotDeleteUserProvidedPaths(t *testing.T) {
	ctx := context.Background()

	// Test that user-provided paths are NOT deleted
	t.Run("UserProvidedPath", func(t *testing.T) {
		// Create a test directory with some files
		testDir := t.TempDir()
		dbPath := filepath.Join(testDir, "my.ddb")
		otherFile := filepath.Join(testDir, "important.txt")

		// Create an "important" file that should not be deleted
		err := os.WriteFile(otherFile, []byte("important data"), 0644)
		require.NoError(t, err)

		// Create DB with user-provided path
		db, err := NewDB(WithDatabasePath(dbPath))
		require.NoError(t, err)

		// Use it briefly to ensure it's working
		conn, release, err := db.GetConnection(ctx)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, `SELECT 1`)
		require.NoError(t, err)
		release()

		// Close the database
		err = db.Close()
		require.NoError(t, err)

		// Verify that the directory and files still exist
		_, err = os.Stat(testDir)
		require.NoError(t, err, "user-provided directory should still exist")

		_, err = os.Stat(otherFile)
		require.NoError(t, err, "other files in user-provided directory should still exist")

		// The database file itself should still exist
		_, err = os.Stat(dbPath)
		require.NoError(t, err, "database file should still exist")
	})

	// Test that internally-created temp directories ARE deleted
	t.Run("InternalTempPath", func(t *testing.T) {
		// Create DB without specifying a path (uses temp)
		db, err := NewDB()
		require.NoError(t, err)

		dbPath := db.GetDatabasePath()
		dbDir := filepath.Dir(dbPath)

		// Verify the temp directory exists
		_, err = os.Stat(dbDir)
		require.NoError(t, err, "temp directory should exist before close")

		// Use it briefly
		conn, release, err := db.GetConnection(ctx)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, `SELECT 1`)
		require.NoError(t, err)
		release()

		// Close the database
		err = db.Close()
		require.NoError(t, err)

		// Verify that the temp directory was deleted
		_, err = os.Stat(dbDir)
		require.Error(t, err, "temp directory should be deleted after close")
		require.True(t, os.IsNotExist(err), "temp directory should not exist")
	})
}
