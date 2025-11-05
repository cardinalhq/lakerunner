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
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLegacyORClauseBug reproduces the bug where adding an OR condition
// causes the query to return 0 results instead of the same or more results.
//
// Query 1 (works): resource.file.type CONTAINS "cloudxcommand"
// Query 2 (broken): resource.file.type CONTAINS "cloudxcommand" OR log.source CONTAINS "cloudxcommand"
//
// Query 2 should return the same or MORE results, but instead returns 0.
func TestLegacyORClauseBug(t *testing.T) {
	ctx := context.Background()

	// Create in-memory DuckDB database
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Create a logs table - NOTE: log_source field does NOT exist (mimics production)
	tableName := "test_logs"
	_, err = db.ExecContext(ctx, `CREATE TABLE `+tableName+` (
		chq_timestamp BIGINT,
		chq_fingerprint VARCHAR,
		log_message VARCHAR,
		resource_bucket_name VARCHAR,
		resource_file VARCHAR,
		resource_file_type VARCHAR
	)`)
	require.NoError(t, err)

	// Insert test data where resource_file_type contains "controller"
	testData := []map[string]any{
		{
			"chq_timestamp":        1761816631865,
			"chq_fingerprint":      "fp1",
			"log_message":          "Processing request",
			"resource_bucket_name": "avxit-dev-s3-use2-datalake",
			"resource_file":        "verint.com-abu-5sbgatfp7zf-1682612405.9400098_2025-10-30-180550_controller",
			"resource_file_type":   "controller",
		},
	}

	for _, data := range testData {
		_, err := IngestExemplarLogsJSONToDuckDB(ctx, db, tableName, data)
		require.NoError(t, err)
	}

	// Export to Parquet
	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "test_or_bug.parquet")
	_, err = db.ExecContext(ctx, `COPY `+tableName+` TO '`+parquetPath+`' (FORMAT PARQUET)`)
	require.NoError(t, err)
	t.Logf("Test data exported to: %s", parquetPath)

	// Query 1: Only resource.file.type in OR clause (THIS WORKS)
	query1 := BinaryClause{
		Op: "and",
		Clauses: []QueryClause{
			Filter{
				K:  "resource.bucket.name",
				V:  []string{"avxit-dev-s3-use2-datalake"},
				Op: "eq",
			},
			Filter{
				K:  "resource.file",
				V:  []string{"verint.com-abu-5sbgatfp7zf-1682612405.9400098_2025-10-30-180550_controller"},
				Op: "in",
			},
			BinaryClause{
				Op: "or",
				Clauses: []QueryClause{
					Filter{
						K:  "resource.file.type",
						V:  []string{"cloudxcommand"},
						Op: "contains",
					},
				},
			},
		},
	}

	leaf1 := &LegacyLeaf{Filter: query1}
	sql1 := leaf1.ToWorkerSQLWithLimit(100, "DESC", nil, nil)
	sql1 = strings.ReplaceAll(sql1, "{table}", tableName)
	sql1 = strings.ReplaceAll(sql1, "{start}", "0")
	sql1 = strings.ReplaceAll(sql1, "{end}", "9999999999999")

	t.Logf("Query 1 SQL:\n%s", sql1)

	rows1, err := db.QueryContext(ctx, sql1)
	require.NoError(t, err)
	defer rows1.Close()

	var count1 int
	for rows1.Next() {
		count1++
		// Just count rows, don't need to scan
		var dummy []any
		cols, _ := rows1.Columns()
		dummy = make([]any, len(cols))
		for i := range dummy {
			var v any
			dummy[i] = &v
		}
		rows1.Scan(dummy...)
	}
	require.NoError(t, rows1.Err())

	// Query 1 should return 0 rows because "controller" does NOT contain "cloudxcommand"
	assert.Equal(t, 0, count1, "Query 1 should return 0 rows (controller doesn't contain cloudxcommand)")

	// Query 2: Add log.source to OR clause (THIS SHOULD WORK BUT DOESN'T)
	query2 := BinaryClause{
		Op: "and",
		Clauses: []QueryClause{
			Filter{
				K:  "resource.bucket.name",
				V:  []string{"avxit-dev-s3-use2-datalake"},
				Op: "eq",
			},
			Filter{
				K:  "resource.file",
				V:  []string{"verint.com-abu-5sbgatfp7zf-1682612405.9400098_2025-10-30-180550_controller"},
				Op: "in",
			},
			BinaryClause{
				Op: "or",
				Clauses: []QueryClause{
					Filter{
						K:  "resource.file.type",
						V:  []string{"cloudxcommand"},
						Op: "contains",
					},
					Filter{
						K:  "log.source", // This field does NOT exist in schema
						V:  []string{"cloudxcommand"},
						Op: "contains",
					},
				},
			},
		},
	}

	// Now test with the FIX: get non-existent fields first
	leaf2 := &LegacyLeaf{Filter: query2}

	// Get schema from Parquet table
	nonExistentFields, err := leaf2.GetNonExistentFields(ctx, db, tableName)
	require.NoError(t, err)
	t.Logf("Non-existent fields: %v", nonExistentFields)

	// Generate SQL with non-existent fields passed in
	sql2 := leaf2.ToWorkerSQLWithLimit(100, "DESC", nil, nonExistentFields)
	sql2 = strings.ReplaceAll(sql2, "{table}", tableName)
	sql2 = strings.ReplaceAll(sql2, "{start}", "0")
	sql2 = strings.ReplaceAll(sql2, "{end}", "9999999999999")

	t.Logf("Query 2 SQL (WITH FIX):\n%s", sql2)

	// Verify SQL contains "false" for non-existent field
	assert.Contains(t, sql2, "false", "SQL should contain 'false' for non-existent log_source field")

	// Save SQL for inspection
	sqlPath := filepath.Join(tmpDir, "query2.sql")
	err = os.WriteFile(sqlPath, []byte(sql2), 0644)
	require.NoError(t, err)
	t.Logf("Query 2 SQL saved to: %s", sqlPath)

	// Execute the query - it should work now!
	rows2, err := db.QueryContext(ctx, sql2)
	require.NoError(t, err, "Query should succeed with the fix")
	defer rows2.Close()

	var count2 int
	for rows2.Next() {
		count2++
		var dummy []any
		cols, _ := rows2.Columns()
		dummy = make([]any, len(cols))
		for i := range dummy {
			var v any
			dummy[i] = &v
		}
		rows2.Scan(dummy...)
	}
	require.NoError(t, rows2.Err())

	// Query 2 should return the SAME results as Query 1 (0 rows)
	// The "false" condition for log_source gets optimized away by DuckDB
	t.Logf("Query 1 returned %d rows, Query 2 returned %d rows", count1, count2)
	assert.Equal(t, count1, count2, "Adding OR condition with non-existent field should not change results")
}
