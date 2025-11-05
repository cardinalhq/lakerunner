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
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLegacyORParquetDirectQuery tests if querying non-existent columns works
// when reading Parquet files directly (as opposed to querying tables).
func TestLegacyORParquetDirectQuery(t *testing.T) {
	ctx := context.Background()

	// Create in-memory DuckDB database
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Create a logs table - NOTE: log_source field does NOT exist
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

	// Insert test data
	testData := []map[string]any{
		{
			"chq_timestamp":        1761816631865,
			"chq_fingerprint":      "fp1",
			"log_message":          "Processing request",
			"resource_bucket_name": "test-bucket",
			"resource_file":        "test-file_controller",
			"resource_file_type":   "controller",
		},
	}

	for _, data := range testData {
		_, err := IngestExemplarLogsJSONToDuckDB(ctx, db, tableName, data)
		require.NoError(t, err)
	}

	// Export to Parquet
	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "test.parquet")
	_, err = db.ExecContext(ctx, `COPY `+tableName+` TO '`+parquetPath+`' (FORMAT PARQUET)`)
	require.NoError(t, err)

	// Build query with non-existent log_source field
	query := BinaryClause{
		Op: "and",
		Clauses: []QueryClause{
			Filter{K: "resource.bucket.name", V: []string{"test-bucket"}, Op: "eq"},
			BinaryClause{
				Op: "or",
				Clauses: []QueryClause{
					Filter{K: "resource.file.type", V: []string{"cloudxcommand"}, Op: "contains"},
					Filter{K: "log.source", V: []string{"cloudxcommand"}, Op: "contains"},
				},
			},
		},
	}

	leaf := &LegacyLeaf{Filter: query}
	sqlQuery := leaf.ToWorkerSQLWithLimit(100, "DESC", nil, nil)

	// Replace {table} with Parquet file path (simulating production behavior)
	sqlQuery = strings.ReplaceAll(sqlQuery, "{table}", "'"+parquetPath+"'")
	sqlQuery = strings.ReplaceAll(sqlQuery, "{start}", "0")
	sqlQuery = strings.ReplaceAll(sqlQuery, "{end}", "9999999999999")

	t.Logf("SQL Query:\n%s", sqlQuery)

	// Execute query against Parquet file directly
	rows, err := db.QueryContext(ctx, sqlQuery)
	if err != nil {
		t.Logf("Query failed with error: %v", err)
		// DuckDB fails when querying non-existent columns even in Parquet files
		assert.Contains(t, err.Error(), "log_source", "Error should mention the missing column")
		return
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}

	t.Logf("Query returned %d rows", count)
	assert.Equal(t, 0, count, "Should return 0 rows (controller doesn't contain cloudxcommand)")
}
