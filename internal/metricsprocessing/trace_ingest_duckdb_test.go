// Copyright (C) 2025-2026 CardinalHQ, Inc
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

func TestProcessTraceIngestWithDuckDB(t *testing.T) {
	testFile := filepath.Join("..", "..", "testdata", "traces", "otel-traces.binpb.gz")

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

	result, err := processTraceIngestWithDuckDB(ctx, []string{segmentFile}, orgID, tmpDir)
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

func TestProcessTraceIngestWithDuckDB_EmptyFiles(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001").String()

	_, err := processTraceIngestWithDuckDB(ctx, []string{}, orgID, tmpDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no binpb files provided")
}

func TestGetDistinctTraceDateintKeys(t *testing.T) {
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
		CREATE TABLE traces_raw (
			chq_timestamp BIGINT
		)
	`)
	require.NoError(t, err)

	// Insert timestamps spanning two days
	_, err = conn.ExecContext(ctx, `
		INSERT INTO traces_raw VALUES
			(1735689600000),  -- 2025-01-01 00:00:00 UTC
			(1735700000000),  -- 2025-01-01 ~03:00:00 UTC
			(1735776000000),  -- 2025-01-02 00:00:00 UTC
			(1735800000000)   -- 2025-01-02 ~06:00:00 UTC
	`)
	require.NoError(t, err)

	keys, err := getDistinctTraceDateintKeys(ctx, conn)
	require.NoError(t, err)

	assert.Len(t, keys, 2, "should have 2 distinct days")
	assert.Equal(t, int64(20089), keys[0], "first day key should be 20089")
	assert.Equal(t, int64(20090), keys[1], "second day key should be 20090")
}

func TestGetTraceFingerprints(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Create a table with indexed columns
	_, err = conn.ExecContext(ctx, `
		CREATE TABLE traces_raw (
			chq_timestamp BIGINT,
			chq_telemetry_type VARCHAR,
			resource_service_name VARCHAR,
			span_trace_id VARCHAR
		)
	`)
	require.NoError(t, err)

	// Insert some trace data
	_, err = conn.ExecContext(ctx, `
		INSERT INTO traces_raw VALUES
			(1735689600000, 'traces', 'service-a', 'trace-1'),
			(1735689700000, 'traces', 'service-a', 'trace-2'),
			(1735689800000, 'traces', 'service-b', 'trace-3'),
			(1735689900000, 'traces', 'service-b', 'trace-1')
	`)
	require.NoError(t, err)

	startMs := int64(1735689600000)
	endMs := int64(1735700000000)
	schema := []string{"chq_timestamp", "chq_telemetry_type", "resource_service_name", "span_trace_id"}

	fingerprints, err := getTraceFingerprints(ctx, conn, startMs, endMs, schema)
	require.NoError(t, err)

	// Should have fingerprints computed from indexed fields
	// - exists fingerprints for each column
	// - value fingerprints for indexed columns (chq_telemetry_type, resource_service_name, span_trace_id)
	assert.NotEmpty(t, fingerprints, "should have fingerprints")

	// Verify fingerprints are sorted
	for i := 1; i < len(fingerprints); i++ {
		assert.LessOrEqual(t, fingerprints[i-1], fingerprints[i], "fingerprints should be sorted")
	}
}

func TestMergeTraceSchemaAndInsert(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Create accumulation table with initial schema
	_, err = conn.ExecContext(ctx, `
		CREATE TABLE traces_accum (
			trace_id VARCHAR,
			span_id VARCHAR,
			chq_timestamp BIGINT
		)
	`)
	require.NoError(t, err)

	// Insert initial data
	_, err = conn.ExecContext(ctx, `INSERT INTO traces_accum VALUES ('trace-1', 'span-1', 1000)`)
	require.NoError(t, err)

	// Create temp table with additional column
	_, err = conn.ExecContext(ctx, `
		CREATE TABLE traces_temp_0 (
			trace_id VARCHAR,
			span_id VARCHAR,
			chq_timestamp BIGINT,
			resource_service_name VARCHAR
		)
	`)
	require.NoError(t, err)

	// Insert data into temp table
	_, err = conn.ExecContext(ctx, `INSERT INTO traces_temp_0 VALUES ('trace-2', 'span-2', 2000, 'my-service')`)
	require.NoError(t, err)

	// Merge schema and insert
	err = mergeTraceSchemaAndInsert(ctx, conn, "traces_temp_0", "traces_accum", 0)
	require.NoError(t, err)

	// Verify accum table has new column and both rows
	var count int64
	err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM traces_accum").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count, "should have 2 rows after merge")

	// Verify the new column exists and has correct value for new row
	var serviceName sql.NullString
	err = conn.QueryRowContext(ctx, "SELECT resource_service_name FROM traces_accum WHERE trace_id = 'trace-2'").Scan(&serviceName)
	require.NoError(t, err)
	assert.True(t, serviceName.Valid)
	assert.Equal(t, "my-service", serviceName.String)

	// Verify first row has NULL for new column
	err = conn.QueryRowContext(ctx, "SELECT resource_service_name FROM traces_accum WHERE trace_id = 'trace-1'").Scan(&serviceName)
	require.NoError(t, err)
	assert.False(t, serviceName.Valid, "first row should have NULL for resource_service_name")

	// Verify temp table was dropped
	var exists int
	err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'traces_temp_0'").Scan(&exists)
	require.NoError(t, err)
	assert.Equal(t, 0, exists, "temp table should be dropped")
}

func TestMergeTraceSchemaAndInsert_MultipleNewColumns(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Create accumulation table with minimal schema
	_, err = conn.ExecContext(ctx, `
		CREATE TABLE traces_accum2 (
			trace_id VARCHAR
		)
	`)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `INSERT INTO traces_accum2 VALUES ('trace-1')`)
	require.NoError(t, err)

	// Create temp table with multiple additional columns
	_, err = conn.ExecContext(ctx, `
		CREATE TABLE traces_temp_1 (
			trace_id VARCHAR,
			span_id VARCHAR,
			resource_service_name VARCHAR,
			attr_http_method VARCHAR
		)
	`)
	require.NoError(t, err)

	_, err = conn.ExecContext(ctx, `INSERT INTO traces_temp_1 VALUES ('trace-2', 'span-1', 'service-a', 'GET')`)
	require.NoError(t, err)

	// Merge schema and insert
	err = mergeTraceSchemaAndInsert(ctx, conn, "traces_temp_1", "traces_accum2", 1)
	require.NoError(t, err)

	// Verify all columns were added
	var count int64
	err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM traces_accum2").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// Verify schema has all columns
	var colCount int
	err = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'traces_accum2'").Scan(&colCount)
	require.NoError(t, err)
	assert.Equal(t, 4, colCount, "should have 4 columns after merge")
}

func TestGetTraceTempTableColumns(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Create a table with known columns
	_, err = conn.ExecContext(ctx, `
		CREATE TABLE trace_test_cols (
			trace_id VARCHAR,
			span_id VARCHAR,
			chq_timestamp BIGINT,
			resource_service_name VARCHAR
		)
	`)
	require.NoError(t, err)

	cols, err := getTraceTempTableColumns(ctx, conn, "trace_test_cols", 0)
	require.NoError(t, err)

	assert.Len(t, cols, 4, "should have 4 columns")
	// Columns should be quoted
	assert.Equal(t, `"trace_id"`, cols[0])
	assert.Equal(t, `"span_id"`, cols[1])
	assert.Equal(t, `"chq_timestamp"`, cols[2])
	assert.Equal(t, `"resource_service_name"`, cols[3])
}

func TestGetTraceTempTableColumns_EmptyTable(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Create an empty table
	_, err = conn.ExecContext(ctx, `
		CREATE TABLE trace_empty_cols (
			id INTEGER
		)
	`)
	require.NoError(t, err)

	cols, err := getTraceTempTableColumns(ctx, conn, "trace_empty_cols", 0)
	require.NoError(t, err)

	assert.Len(t, cols, 1, "should have 1 column")
	assert.Equal(t, `"id"`, cols[0])
}
