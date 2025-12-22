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

package queryapi

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"
)

// TestDuckDBCoalesceMissingColumn tests if COALESCE can handle non-existent columns.
func TestDuckDBCoalesceMissingColumn(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Create table WITHOUT log_source column
	_, err = db.ExecContext(ctx, `
		CREATE TABLE test_table (
			id INTEGER,
			message VARCHAR
		)
	`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `INSERT INTO test_table VALUES (1, 'hello')`)
	require.NoError(t, err)

	// Test 1: Direct reference to missing column (should fail)
	t.Run("direct_reference", func(t *testing.T) {
		_, err := db.QueryContext(ctx, `SELECT log_source FROM test_table`)
		require.Error(t, err)
		t.Logf("Direct reference error: %v", err)
	})

	// Test 2: COALESCE with missing column (will this work?)
	t.Run("coalesce_missing", func(t *testing.T) {
		_, err := db.QueryContext(ctx, `SELECT COALESCE(log_source, '') FROM test_table`)
		if err != nil {
			t.Logf("COALESCE still fails: %v", err)
			require.Error(t, err, "COALESCE doesn't help with missing columns")
		} else {
			t.Logf("COALESCE works!")
		}
	})

	// Test 3: Try TRY_CAST or other DuckDB functions
	t.Run("try_cast", func(t *testing.T) {
		_, err := db.QueryContext(ctx, `SELECT TRY_CAST(log_source AS VARCHAR) FROM test_table`)
		if err != nil {
			t.Logf("TRY_CAST fails: %v", err)
		} else {
			t.Logf("TRY_CAST works!")
		}
	})

	// Test 4: Check if DuckDB has column existence check
	t.Run("column_exists", func(t *testing.T) {
		// Try various DuckDB system functions
		queries := []string{
			`SELECT * FROM test_table WHERE columns('log_source') IS NOT NULL`,
			`SELECT * FROM test_table WHERE column_exists('log_source')`,
		}
		for _, q := range queries {
			_, err := db.QueryContext(ctx, q)
			if err == nil {
				t.Logf("Query succeeded: %s", q)
				return
			}
			t.Logf("Query failed: %s - %v", q, err)
		}
	})
}
