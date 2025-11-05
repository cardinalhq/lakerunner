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
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLegacyCorrectedFieldNames tests the corrected field names for the OR clause.
// This test verifies that using the correct field names (log.message, log.level, resource.file.type)
// properly matches records, whereas the incorrect field names (_cardinalhq.message, log.log_level, log.source)
// would not match.
func TestLegacyCorrectedFieldNames(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "legacy-corrected-fields-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	parquetFile := filepath.Join(tmpDir, "test_data.parquet")

	// Create in-memory DuckDB database
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Create test table with correct schema matching production
	_, err = db.Exec(`
		CREATE TABLE test_logs (
			chq_fingerprint BIGINT,
			chq_timestamp BIGINT,
			log_level VARCHAR,
			log_message VARCHAR,
			resource_bucket_name VARCHAR,
			resource_file VARCHAR,
			resource_file_type VARCHAR
		)
	`)
	require.NoError(t, err)

	// Insert test data
	// Record 1: Has "testcommand" in log_message (should match)
	_, err = db.Exec(`
		INSERT INTO test_logs VALUES (
			1, 1000000, 'INFO',
			'Running testcommand for system check',
			'test-bucket', 'test-file_controller', 'controller'
		)
	`)
	require.NoError(t, err)

	// Record 2: Has "TESTCOMMAND" in log_message (should match - case insensitive)
	_, err = db.Exec(`
		INSERT INTO test_logs VALUES (
			2, 1000001, 'INFO',
			'Processing TESTCOMMAND operation',
			'test-bucket', 'test-file_controller', 'controller'
		)
	`)
	require.NoError(t, err)

	// Record 3: Has "testcommand" in resource_file_type (should match)
	_, err = db.Exec(`
		INSERT INTO test_logs VALUES (
			3, 1000002, 'INFO',
			'Some other log message',
			'test-bucket', 'test-file_testcommands', 'testcommands'
		)
	`)
	require.NoError(t, err)

	// Record 4: Has "TestCommand" in log_level (unusual but should match - case insensitive)
	_, err = db.Exec(`
		INSERT INTO test_logs VALUES (
			4, 1000003, 'TestCommand',
			'Another log message',
			'test-bucket', 'test-file_controller', 'controller'
		)
	`)
	require.NoError(t, err)

	// Record 5: Has NO "testcommand" anywhere (should NOT match)
	_, err = db.Exec(`
		INSERT INTO test_logs VALUES (
			5, 1000004, 'INFO',
			'Regular log message',
			'test-bucket', 'test-file_controller', 'controller'
		)
	`)
	require.NoError(t, err)

	// Record 6: Different bucket (should NOT match due to bucket filter)
	_, err = db.Exec(`
		INSERT INTO test_logs VALUES (
			6, 1000005, 'INFO',
			'Has testcommand in message',
			'different-bucket', 'test-file_controller', 'controller'
		)
	`)
	require.NoError(t, err)

	// Export to Parquet
	_, err = db.Exec(fmt.Sprintf("COPY test_logs TO '%s' (FORMAT PARQUET)", parquetFile))
	require.NoError(t, err)

	// Build the corrected query with proper field names
	correctedQuery := BinaryClause{
		Op: "and",
		Clauses: []QueryClause{
			// q1: Bucket filter
			Filter{
				K:  "resource.bucket.name",
				V:  []string{"test-bucket"},
				Op: "eq",
			},
			// q2: File filter
			Filter{
				K:  "resource.file",
				V:  []string{"test-file_controller"},
				Op: "in",
			},
			// qs3: OR clause with CORRECTED field names
			BinaryClause{
				Op: "or",
				Clauses: []QueryClause{
					Filter{K: "log.message", V: []string{"testcommand"}, Op: "contains"},        // CORRECTED
					Filter{K: "log.level", V: []string{"testcommand"}, Op: "contains"},          // CORRECTED
					Filter{K: "resource.file.type", V: []string{"testcommand"}, Op: "contains"}, // This was already correct
				},
			},
		},
	}

	leaf := &LegacyLeaf{Filter: correctedQuery}
	sql := leaf.ToWorkerSQLWithLimit(0, "", nil)

	// Verify SQL contains correct field names
	assert.Contains(t, sql, `"log_message"`, "SQL should reference log_message")
	assert.Contains(t, sql, `"log_level"`, "SQL should reference log_level")
	assert.Contains(t, sql, `"resource_file_type"`, "SQL should reference resource_file_type")

	// Verify SQL does NOT contain incorrect field names
	assert.NotContains(t, sql, `"chq_message"`, "SQL should NOT reference chq_message")
	assert.NotContains(t, sql, `"log_log_level"`, "SQL should NOT reference log_log_level")
	assert.NotContains(t, sql, `"log_source"`, "SQL should NOT reference log_source")

	// Verify case-insensitive matching
	assert.Contains(t, sql, "REGEXP_MATCHES", "Should use REGEXP_MATCHES")
	assert.Contains(t, sql, "'i'", "Should use case-insensitive flag")

	// Execute the query
	actualSQL := sql
	actualSQL = strings.ReplaceAll(actualSQL, "{table}", fmt.Sprintf("'%s'", parquetFile))
	actualSQL = strings.ReplaceAll(actualSQL, "{start}", "0")
	actualSQL = strings.ReplaceAll(actualSQL, "{end}", "2000000")

	t.Logf("Executing SQL:\n%s", actualSQL)

	rows, err := db.Query(actualSQL)
	require.NoError(t, err)
	defer rows.Close()

	// Collect results
	var results []int64
	for rows.Next() {
		var fp int64
		var ts int64
		var level, message, bucket, file, fileType string
		err := rows.Scan(&fp, &ts, &level, &message, &bucket, &file, &fileType)
		require.NoError(t, err)
		results = append(results, fp)
		t.Logf("Match: fp=%d, level=%s, message=%s, file_type=%s", fp, level, message, fileType)
	}
	require.NoError(t, rows.Err())

	// Verify results
	// Should match records 1, 2, and 4 (all have "testcommand" in log_message or log_level)
	// Record 3 has wrong file name (test-file_testcommands vs test-file_controller)
	// Record 5 has no "testcommand" anywhere
	// Record 6 has wrong bucket
	assert.ElementsMatch(t, []int64{1, 2, 4}, results, "Should match records 1, 2, and 4")
}

// TestLegacyIncorrectFieldNames verifies that truly incorrect field names (log.log_level, log.source)
// cause query failures. Note: _cardinalhq.message now has backward compatibility mapping to log_message.
func TestLegacyIncorrectFieldNames(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "legacy-incorrect-fields-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	parquetFile := filepath.Join(tmpDir, "test_data.parquet")

	// Create in-memory DuckDB database
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Create test table
	_, err = db.Exec(`
		CREATE TABLE test_logs (
			chq_fingerprint BIGINT,
			chq_timestamp BIGINT,
			log_level VARCHAR,
			log_message VARCHAR,
			resource_bucket_name VARCHAR,
			resource_file VARCHAR,
			resource_file_type VARCHAR
		)
	`)
	require.NoError(t, err)

	// Insert test data - same as corrected test
	_, err = db.Exec(`
		INSERT INTO test_logs VALUES
			(1, 1000000, 'INFO', 'Running testcommand for system check', 'test-bucket', 'test-file_controller', 'controller'),
			(2, 1000001, 'INFO', 'Processing TESTCOMMAND operation', 'test-bucket', 'test-file_controller', 'controller'),
			(3, 1000002, 'INFO', 'Some other log message', 'test-bucket', 'test-file_testcommands', 'testcommands')
	`)
	require.NoError(t, err)

	// Export to Parquet
	_, err = db.Exec(fmt.Sprintf("COPY test_logs TO '%s' (FORMAT PARQUET)", parquetFile))
	require.NoError(t, err)

	// Build query with INCORRECT field names
	// Note: _cardinalhq.message now maps to log_message (backward compat), so we test with truly wrong fields
	incorrectQuery := BinaryClause{
		Op: "and",
		Clauses: []QueryClause{
			Filter{K: "resource.bucket.name", V: []string{"test-bucket"}, Op: "eq"},
			Filter{K: "resource.file", V: []string{"test-file_controller"}, Op: "in"},
			BinaryClause{
				Op: "or",
				Clauses: []QueryClause{
					Filter{K: "log.log_level", V: []string{"testcommand"}, Op: "contains"},      // WRONG - double log
					Filter{K: "resource.file.type", V: []string{"testcommand"}, Op: "contains"}, // CORRECT
					Filter{K: "log.source", V: []string{"testcommand"}, Op: "contains"},         // WRONG - doesn't exist
				},
			},
		},
	}

	leaf := &LegacyLeaf{Filter: incorrectQuery}
	sql := leaf.ToWorkerSQLWithLimit(0, "", nil)

	// Verify SQL contains INCORRECT field names
	assert.Contains(t, sql, `"log_log_level"`, "Should generate log_log_level (which doesn't exist)")
	assert.Contains(t, sql, `"log_source"`, "Should generate log_source (which doesn't exist)")

	// Execute the query
	actualSQL := sql
	actualSQL = strings.ReplaceAll(actualSQL, "{table}", fmt.Sprintf("'%s'", parquetFile))
	actualSQL = strings.ReplaceAll(actualSQL, "{start}", "0")
	actualSQL = strings.ReplaceAll(actualSQL, "{end}", "2000000")

	t.Logf("Executing SQL with INCORRECT fields:\n%s", actualSQL)

	rows, err := db.Query(actualSQL)

	// DuckDB will throw a binding error because log_log_level and log_source don't exist
	// This demonstrates that incorrect field names cause the query to fail
	if err != nil {
		t.Logf("Query failed with binding error (as expected): %v", err)
		assert.Contains(t, err.Error(), "not found", "Should fail with column not found error")
		return
	}
	defer rows.Close()

	// If the query somehow succeeded, we'd expect 0 results because:
	// - log_log_level doesn't exist (would need IS NOT NULL check but fails before that)
	// - log_source doesn't exist (would need IS NOT NULL check but fails before that)
	// - resource_file_type exists but "controller" doesn't contain "testcommand"
	var results []int64
	for rows.Next() {
		var fp int64
		var ts int64
		var level, message, bucket, file, fileType string
		err := rows.Scan(&fp, &ts, &level, &message, &bucket, &file, &fileType)
		require.NoError(t, err)
		results = append(results, fp)
	}
	require.NoError(t, rows.Err())

	assert.Empty(t, results, "Should match 0 records even if query succeeded")
}

// TestLegacyBackwardCompatibilityForCardinalhqMessage tests that the old _cardinalhq.message
// field name is automatically converted to log_message for backward compatibility.
func TestLegacyBackwardCompatibilityForCardinalhqMessage(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "legacy-backward-compat-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	parquetFile := filepath.Join(tmpDir, "test_data.parquet")

	// Create in-memory DuckDB database
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	// Create test table
	_, err = db.Exec(`
		CREATE TABLE test_logs (
			chq_fingerprint BIGINT,
			chq_timestamp BIGINT,
			log_level VARCHAR,
			log_message VARCHAR,
			resource_bucket_name VARCHAR,
			resource_file VARCHAR,
			resource_file_type VARCHAR
		)
	`)
	require.NoError(t, err)

	// Insert test data with "testcommand" in log_message
	_, err = db.Exec(`
		INSERT INTO test_logs VALUES (
			1, 1000000, 'INFO',
			'Running testcommand for system check',
			'test-bucket', 'test-file', 'controller'
		)
	`)
	require.NoError(t, err)

	// Export to Parquet
	_, err = db.Exec(fmt.Sprintf("COPY test_logs TO '%s' (FORMAT PARQUET)", parquetFile))
	require.NoError(t, err)

	// Build query using OLD field name _cardinalhq.message (should map to log_message)
	oldQuery := BinaryClause{
		Op: "and",
		Clauses: []QueryClause{
			Filter{K: "resource.bucket.name", V: []string{"test-bucket"}, Op: "eq"},
			Filter{K: "_cardinalhq.message", V: []string{"testcommand"}, Op: "contains"}, // OLD field name
		},
	}

	leaf := &LegacyLeaf{Filter: oldQuery}
	sql := leaf.ToWorkerSQLWithLimit(0, "", nil)

	// Verify SQL uses log_message (not chq_message)
	assert.Contains(t, sql, `"log_message"`, "Should convert _cardinalhq.message to log_message")
	assert.NotContains(t, sql, `"chq_message"`, "Should NOT use chq_message")

	// Execute the query
	actualSQL := sql
	actualSQL = strings.ReplaceAll(actualSQL, "{table}", fmt.Sprintf("'%s'", parquetFile))
	actualSQL = strings.ReplaceAll(actualSQL, "{start}", "0")
	actualSQL = strings.ReplaceAll(actualSQL, "{end}", "2000000")

	t.Logf("Executing SQL with backward-compatible _cardinalhq.message:\n%s", actualSQL)

	rows, err := db.Query(actualSQL)
	require.NoError(t, err, "Query with _cardinalhq.message should succeed")
	defer rows.Close()

	// Should find the record
	var results []int64
	for rows.Next() {
		var fp int64
		var ts int64
		var level, message, bucket, file, fileType string
		err := rows.Scan(&fp, &ts, &level, &message, &bucket, &file, &fileType)
		require.NoError(t, err)
		results = append(results, fp)
		t.Logf("Match: fp=%d, message=%s", fp, message)
	}
	require.NoError(t, rows.Err())

	assert.Equal(t, []int64{1}, results, "Should match the record using backward-compatible field name")
}

// TestLegacyFieldMapping verifies the field name normalization logic.
func TestLegacyFieldMapping(t *testing.T) {
	tests := []struct {
		dottedName  string
		expectedCol string
		description string
	}{
		{"log.message", "log_message", "Standard log message field"},
		{"log.level", "log_level", "Standard log level field"},
		{"log.log_level", "log_log_level", "Incorrect double log prefix"},
		{"_cardinalhq.message", "log_message", "Legacy cardinalhq.message maps to log_message for backward compatibility"},
		{"_cardinalhq.level", "log_level", "Legacy cardinalhq.level maps to log_level for backward compatibility"},
		{"_cardinalhq.timestamp", "chq_timestamp", "Other cardinalhq fields still map to chq_ prefix"},
		{"_cardinalhq.fingerprint", "chq_fingerprint", "cardinalhq.fingerprint maps to chq_ prefix"},
		{"resource.file.type", "resource_file_type", "Resource file type"},
		{"log.source", "log_source", "Log source field"},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			actual := normalizeLabelName(tt.dottedName)
			assert.Equal(t, tt.expectedCol, actual, "Field name normalization for %s", tt.dottedName)
		})
	}
}
