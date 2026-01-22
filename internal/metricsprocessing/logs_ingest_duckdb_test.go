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

package metricsprocessing

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
)

func TestProcessLogIngestWithDuckDB(t *testing.T) {
	testFile := filepath.Join("..", "..", "testdata", "logs", "otel-logs.binpb.gz")

	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skipf("Test file does not exist: %s", testFile)
	}

	tmpDir := t.TempDir()
	ctx := context.Background()

	// Copy test file to temp location to simulate downloaded segment
	testData, err := os.ReadFile(testFile)
	require.NoError(t, err)

	segmentFile := filepath.Join(tmpDir, "segment.binpb.gz")
	err = os.WriteFile(segmentFile, testData, 0644)
	require.NoError(t, err)

	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001").String()

	result, err := processLogIngestWithDuckDB(ctx, []string{segmentFile}, orgID, tmpDir, "resource_service_name")
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should have at least one dateint partition
	assert.NotEmpty(t, result.DateintBins, "expected at least one dateint partition")
	assert.Greater(t, result.TotalRows, int64(0), "expected at least one row")

	// Verify each partition has valid data
	for dateint, bin := range result.DateintBins {
		assert.NotZero(t, dateint, "dateint should not be zero")
		assert.NotEmpty(t, bin.OutputFile, "output file should not be empty")
		assert.Greater(t, bin.RecordCount, int64(0), "record count should be greater than 0")
		assert.Greater(t, bin.FileSize, int64(0), "file size should be greater than 0")
		assert.NotNil(t, bin.Metadata, "metadata should not be nil")

		// Verify output file exists
		_, err := os.Stat(bin.OutputFile)
		require.NoError(t, err, "output file should exist")

		// Verify metadata
		md := bin.Metadata
		assert.Greater(t, md.StartTs, int64(0), "start timestamp should be positive")
		assert.GreaterOrEqual(t, md.EndTs, md.StartTs, "end timestamp should be >= start")
		assert.NotEmpty(t, md.Fingerprints, "fingerprints should not be empty")
	}
}

func TestProcessLogIngestWithDuckDB_MultipleFiles(t *testing.T) {
	testFile := filepath.Join("..", "..", "testdata", "logs", "otel-logs.binpb.gz")

	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skipf("Test file does not exist: %s", testFile)
	}

	tmpDir := t.TempDir()
	ctx := context.Background()

	// Copy test file to temp location twice to simulate multiple segments
	testData, err := os.ReadFile(testFile)
	require.NoError(t, err)

	segmentFile1 := filepath.Join(tmpDir, "segment1.binpb.gz")
	segmentFile2 := filepath.Join(tmpDir, "segment2.binpb.gz")
	err = os.WriteFile(segmentFile1, testData, 0644)
	require.NoError(t, err)
	err = os.WriteFile(segmentFile2, testData, 0644)
	require.NoError(t, err)

	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001").String()

	// Process both files together to test schema merging
	result, err := processLogIngestWithDuckDB(ctx, []string{segmentFile1, segmentFile2}, orgID, tmpDir, "resource_service_name")
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should have at least one dateint partition
	assert.NotEmpty(t, result.DateintBins, "expected at least one dateint partition")
	// With two identical files, we expect double the rows
	assert.Greater(t, result.TotalRows, int64(0), "expected at least one row")
}

func TestProcessLogIngestWithDuckDB_EmptyFiles(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001").String()

	_, err := processLogIngestWithDuckDB(ctx, []string{}, orgID, tmpDir, "resource_service_name")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no binpb files provided")
}

func TestGetDistinctLogDateintKeys(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Create a table with known timestamps
	// Day 1: 2025-01-01 = 1735689600000 ms (days since epoch: 20089)
	// Day 2: 2025-01-02 = 1735776000000 ms (days since epoch: 20090)
	_, err = conn.ExecContext(ctx, `
		CREATE TABLE logs_raw (
			chq_timestamp BIGINT
		)
	`)
	require.NoError(t, err)

	// Insert timestamps spanning two days
	_, err = conn.ExecContext(ctx, `
		INSERT INTO logs_raw VALUES
			(1735689600000),  -- 2025-01-01 00:00:00 UTC
			(1735700000000),  -- 2025-01-01 ~03:00:00 UTC
			(1735776000000),  -- 2025-01-02 00:00:00 UTC
			(1735800000000)   -- 2025-01-02 ~06:00:00 UTC
	`)
	require.NoError(t, err)

	keys, err := getDistinctLogDateintKeys(ctx, conn)
	require.NoError(t, err)

	assert.Len(t, keys, 2, "should have 2 distinct days")
	assert.Equal(t, int64(20089), keys[0], "first day key should be 20089")
	assert.Equal(t, int64(20090), keys[1], "second day key should be 20090")
}

func TestMergeSchemaAndInsert(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Create accumulation table with initial schema
	_, err = conn.ExecContext(ctx, `
		CREATE TABLE accum (
			id INTEGER,
			name VARCHAR
		)
	`)
	require.NoError(t, err)

	// Insert initial data
	_, err = conn.ExecContext(ctx, `INSERT INTO accum VALUES (1, 'first')`)
	require.NoError(t, err)

	// Create temp table with additional column
	_, err = conn.ExecContext(ctx, `
		CREATE TABLE temp_0 (
			id INTEGER,
			name VARCHAR,
			extra_col VARCHAR
		)
	`)
	require.NoError(t, err)

	// Insert data into temp table
	_, err = conn.ExecContext(ctx, `INSERT INTO temp_0 VALUES (2, 'second', 'extra_value')`)
	require.NoError(t, err)

	// Merge schema and insert
	err = mergeSchemaAndInsert(ctx, conn, "temp_0", "accum", 0)
	require.NoError(t, err)

	// Verify accum table has new column
	var count int64
	err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM accum").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count, "should have 2 rows after merge")

	// Verify the new column exists and has correct value
	var extraVal sql.NullString
	err = conn.QueryRowContext(ctx, "SELECT extra_col FROM accum WHERE id = 2").Scan(&extraVal)
	require.NoError(t, err)
	assert.True(t, extraVal.Valid)
	assert.Equal(t, "extra_value", extraVal.String)

	// Verify first row has NULL for new column
	err = conn.QueryRowContext(ctx, "SELECT extra_col FROM accum WHERE id = 1").Scan(&extraVal)
	require.NoError(t, err)
	assert.False(t, extraVal.Valid, "first row should have NULL for extra_col")

	// Verify temp table was dropped
	var exists int
	err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'temp_0'").Scan(&exists)
	require.NoError(t, err)
	assert.Equal(t, 0, exists, "temp table should be dropped")
}

func TestGetTempTableColumns(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Create a table with known columns
	_, err = conn.ExecContext(ctx, `
		CREATE TABLE test_cols (
			first_col INTEGER,
			second_col VARCHAR,
			third_col DOUBLE
		)
	`)
	require.NoError(t, err)

	cols, err := getTempTableColumns(ctx, conn, "test_cols", 0)
	require.NoError(t, err)

	assert.Len(t, cols, 3, "should have 3 columns")
	// Columns should be quoted
	assert.Equal(t, `"first_col"`, cols[0])
	assert.Equal(t, `"second_col"`, cols[1])
	assert.Equal(t, `"third_col"`, cols[2])
}
