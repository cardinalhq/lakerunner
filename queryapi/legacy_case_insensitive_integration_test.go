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

// TestLegacyCaseInsensitiveCompoundOR tests that the legacy query path
// correctly handles case-insensitive matching in compound OR queries.
// This test validates the fix for case sensitivity issues to match Scala behavior.
func TestLegacyCaseInsensitiveCompoundOR(t *testing.T) {
	ctx := context.Background()

	// Create in-memory DuckDB database
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Create a logs table with the expected schema
	tableName := "test_logs"
	_, err = db.ExecContext(ctx, `CREATE TABLE `+tableName+` (
		chq_timestamp BIGINT,
		chq_fingerprint VARCHAR,
		chq_message VARCHAR,
		resource_bucket_name VARCHAR,
		resource_file VARCHAR,
		log_log_level VARCHAR,
		resource_file_type VARCHAR,
		log_source VARCHAR
	)`)
	require.NoError(t, err)

	// Insert test data with various cases to test case-insensitive matching
	// These rows should match our compound OR query
	testData := []map[string]any{
		{
			"chq_timestamp":        1761816631865,
			"chq_fingerprint":      "fp1",
			"chq_message":          "Processing testCommand request", // lowercase
			"resource_bucket_name": "test-bucket",
			"resource_file":        "test-file-001.log",
			"log_log_level":        "INFO",
			"resource_file_type":   "application",
			"log_source":           "app-server",
		},
		{
			"chq_timestamp":        1761816632000,
			"chq_fingerprint":      "fp2",
			"chq_message":          "Error in processing",
			"resource_bucket_name": "test-bucket",
			"resource_file":        "test-file-001.log",
			"log_log_level":        "TestCommand executed", // mixed case in different field
			"resource_file_type":   "application",
			"log_source":           "app-server",
		},
		{
			"chq_timestamp":        1761816633000,
			"chq_fingerprint":      "fp3",
			"chq_message":          "Normal operation",
			"resource_bucket_name": "test-bucket",
			"resource_file":        "test-file-001.log",
			"log_log_level":        "DEBUG",
			"resource_file_type":   "TESTCOMMAND output", // uppercase in different field
			"log_source":           "worker",
		},
		{
			"chq_timestamp":        1761816634000,
			"chq_fingerprint":      "fp4",
			"chq_message":          "Starting service",
			"resource_bucket_name": "test-bucket",
			"resource_file":        "test-file-001.log",
			"log_log_level":        "INFO",
			"resource_file_type":   "application",
			"log_source":           "TestCommand-logger", // camelCase in source
		},
		// This row should NOT match (different bucket and no testcommand)
		{
			"chq_timestamp":        1761816635000,
			"chq_fingerprint":      "fp5",
			"chq_message":          "Different message",
			"resource_bucket_name": "other-bucket",
			"resource_file":        "other-file.log",
			"log_log_level":        "WARN",
			"resource_file_type":   "batch",
			"log_source":           "scheduler",
		},
		// This row should NOT match (correct bucket but no testcommand in any OR field)
		{
			"chq_timestamp":        1761816636000,
			"chq_fingerprint":      "fp6",
			"chq_message":          "Regular operation",
			"resource_bucket_name": "test-bucket",
			"resource_file":        "test-file-001.log",
			"log_log_level":        "INFO",
			"resource_file_type":   "application",
			"log_source":           "app-server",
		},
	}

	// Insert all test data
	for _, data := range testData {
		_, err := IngestExemplarLogsJSONToDuckDB(ctx, db, tableName, data)
		require.NoError(t, err)
	}

	// Export to Parquet file for manual inspection if needed
	tmpDir := t.TempDir()
	parquetPath := filepath.Join(tmpDir, "test_case_insensitive.parquet")
	_, err = db.ExecContext(ctx, `COPY `+tableName+` TO '`+parquetPath+`' (FORMAT PARQUET)`)
	require.NoError(t, err)
	t.Logf("Test data exported to: %s", parquetPath)

	// Create the compound query matching the user's example
	// AND of:
	//   - resource.bucket.name = "test-bucket"
	//   - resource.file IN ["test-file-001.log"]
	//   - OR of:
	//       - _cardinalhq.message CONTAINS "testcommand"
	//       - log.log_level CONTAINS "testcommand"
	//       - resource.file.type CONTAINS "testcommand"
	//       - log.source CONTAINS "testcommand"
	filter := BinaryClause{
		Op: "and",
		Clauses: []QueryClause{
			Filter{
				K:  "resource.bucket.name",
				V:  []string{"test-bucket"},
				Op: "eq",
			},
			Filter{
				K:  "resource.file",
				V:  []string{"test-file-001.log"},
				Op: "in",
			},
			BinaryClause{
				Op: "or",
				Clauses: []QueryClause{
					Filter{
						K:  "_cardinalhq.message",
						V:  []string{"testcommand"},
						Op: "contains",
					},
					Filter{
						K:  "log.log_level",
						V:  []string{"testcommand"},
						Op: "contains",
					},
					Filter{
						K:  "resource.file.type",
						V:  []string{"testcommand"},
						Op: "contains",
					},
					Filter{
						K:  "log.source",
						V:  []string{"testcommand"},
						Op: "contains",
					},
				},
			},
		},
	}

	// Generate SQL using LegacyLeaf
	leaf := &LegacyLeaf{Filter: filter}
	sql := leaf.ToWorkerSQLWithLimit(100, "DESC", nil)

	// Replace placeholders
	sql = strings.ReplaceAll(sql, "{table}", tableName)
	sql = strings.ReplaceAll(sql, "{start}", "0")
	sql = strings.ReplaceAll(sql, "{end}", "9999999999999")

	t.Logf("Generated SQL:\n%s", sql)

	// Execute query
	rows, err := db.QueryContext(ctx, sql)
	require.NoError(t, err)
	defer rows.Close()

	// Collect results
	var results []map[string]any
	cols, err := rows.Columns()
	require.NoError(t, err)

	for rows.Next() {
		values := make([]any, len(cols))
		valuePtrs := make([]any, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		err := rows.Scan(valuePtrs...)
		require.NoError(t, err)

		row := make(map[string]any)
		for i, col := range cols {
			row[col] = values[i]
		}
		results = append(results, row)
	}
	require.NoError(t, rows.Err())

	// Verify results
	t.Logf("Found %d matching rows", len(results))

	// Should match exactly 4 rows (fp1, fp2, fp3, fp4)
	// - fp1: has "testCommand" (mixed case) in chq_message
	// - fp2: has "TestCommand" (title case) in log_log_level
	// - fp3: has "TESTCOMMAND" (uppercase) in resource_file_type
	// - fp4: has "TestCommand" (camel case) in log_source
	// Should NOT match fp5 (different bucket) or fp6 (no testcommand anywhere)
	assert.Equal(t, 4, len(results), "Should match exactly 4 rows with case-insensitive OR matching")

	// Verify the matched rows
	matchedFingerprints := make(map[string]bool)
	for _, row := range results {
		fp := row["chq_fingerprint"].(string)
		matchedFingerprints[fp] = true
		t.Logf("Matched row: %s", fp)
	}

	// Check that we got the expected fingerprints
	assert.True(t, matchedFingerprints["fp1"], "Should match fp1 (testCommand in message)")
	assert.True(t, matchedFingerprints["fp2"], "Should match fp2 (TestCommand in log_level)")
	assert.True(t, matchedFingerprints["fp3"], "Should match fp3 (TESTCOMMAND in file_type)")
	assert.True(t, matchedFingerprints["fp4"], "Should match fp4 (TestCommand in log_source)")
	assert.False(t, matchedFingerprints["fp5"], "Should NOT match fp5 (wrong bucket)")
	assert.False(t, matchedFingerprints["fp6"], "Should NOT match fp6 (no testcommand)")

	// Verify SQL contains case-insensitive regex patterns
	assert.Contains(t, sql, "REGEXP_MATCHES", "Should use REGEXP_MATCHES for contains")
	assert.Contains(t, sql, "'i'", "Should use case-insensitive flag")

	// Save SQL to file for inspection
	sqlPath := filepath.Join(tmpDir, "generated_query.sql")
	err = os.WriteFile(sqlPath, []byte(sql), 0644)
	require.NoError(t, err)
	t.Logf("Generated SQL saved to: %s", sqlPath)
}

// TestLegacyCaseInsensitiveRegex tests regex operator with case-insensitive matching
func TestLegacyCaseInsensitiveRegex(t *testing.T) {
	ctx := context.Background()

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	tableName := "test_regex"
	_, err = db.ExecContext(ctx, `CREATE TABLE `+tableName+` (
		chq_timestamp BIGINT,
		chq_fingerprint VARCHAR,
		log_level VARCHAR
	)`)
	require.NoError(t, err)

	// Insert test data with various cases
	testData := []map[string]any{
		{"chq_timestamp": 1000, "chq_fingerprint": "fp1", "log_level": "ERROR"},
		{"chq_timestamp": 2000, "chq_fingerprint": "fp2", "log_level": "error"},
		{"chq_timestamp": 3000, "chq_fingerprint": "fp3", "log_level": "Error"},
		{"chq_timestamp": 4000, "chq_fingerprint": "fp4", "log_level": "WARN"},
		{"chq_timestamp": 5000, "chq_fingerprint": "fp5", "log_level": "warn"},
		{"chq_timestamp": 6000, "chq_fingerprint": "fp6", "log_level": "INFO"},
	}

	for _, data := range testData {
		_, err := IngestExemplarLogsJSONToDuckDB(ctx, db, tableName, data)
		require.NoError(t, err)
	}

	// Query for ERROR or WARN with lowercase pattern (should match all cases)
	filter := Filter{
		K:  "log_level",
		V:  []string{"error|warn"},
		Op: "regex",
	}

	leaf := &LegacyLeaf{Filter: filter}
	sql := leaf.ToWorkerSQLWithLimit(100, "DESC", nil)
	sql = strings.ReplaceAll(sql, "{table}", tableName)
	sql = strings.ReplaceAll(sql, "{start}", "0")
	sql = strings.ReplaceAll(sql, "{end}", "9999999999999")

	t.Logf("Generated SQL:\n%s", sql)

	rows, err := db.QueryContext(ctx, sql)
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
		var ts int64
		var fp, level string
		err := rows.Scan(&ts, &fp, &level)
		require.NoError(t, err)
		t.Logf("Matched: %s = %s", fp, level)
	}

	// Should match all 5 error/warn rows regardless of case
	assert.Equal(t, 5, count, "Should match ERROR, error, Error, WARN, and warn")

	// Verify SQL uses case-insensitive regex
	assert.Contains(t, sql, "REGEXP_MATCHES")
	assert.Contains(t, sql, "'i'")
}
