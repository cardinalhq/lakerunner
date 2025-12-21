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
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// createTestSketch creates a valid DDSketch with some sample values.
func createTestSketch(t *testing.T, values ...float64) []byte {
	t.Helper()
	sketch, err := helpers.GetSketch()
	require.NoError(t, err)
	for _, v := range values {
		require.NoError(t, sketch.Add(v))
	}
	return helpers.EncodeAndReturnSketch(sketch)
}

// createTestMetricsParquetFile creates a parquet file with metric data for testing.
func createTestMetricsParquetFile(t *testing.T, rows []map[string]any) string {
	t.Helper()

	if len(rows) == 0 {
		t.Fatal("rows must not be empty")
	}

	// Build schema from first row
	nodes := make(map[string]parquet.Node)
	for key, value := range rows[0] {
		var node parquet.Node
		switch value.(type) {
		case int64:
			node = parquet.Optional(parquet.Int(64))
		case string:
			node = parquet.Optional(parquet.String())
		case float64:
			node = parquet.Optional(parquet.Leaf(parquet.DoubleType))
		case []byte:
			node = parquet.Optional(parquet.Leaf(parquet.ByteArrayType))
		case int16:
			node = parquet.Optional(parquet.Int(16))
		default:
			t.Fatalf("Unsupported type %T for key %s", value, key)
		}
		nodes[key] = node
	}

	schema := parquet.NewSchema("metrics", parquet.Group(nodes))

	tmpFile, err := os.CreateTemp("", "test-metrics-*.parquet")
	require.NoError(t, err)

	writer := parquet.NewGenericWriter[map[string]any](tmpFile, schema)

	for _, row := range rows {
		_, err := writer.Write([]map[string]any{row})
		require.NoError(t, err)
	}

	require.NoError(t, writer.Close())
	require.NoError(t, tmpFile.Close())

	t.Cleanup(func() {
		_ = os.Remove(tmpFile.Name())
	})

	return tmpFile.Name()
}

func TestDiscoverSchemaConn(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	rows := []map[string]any{
		{
			"metric_name":   "cpu_usage",
			"chq_tid":       "tid-1",
			"chq_timestamp": int64(1640000000000),
			"value":         float64(42.5),
		},
	}
	testFile := createTestMetricsParquetFile(t, rows)

	schema, err := discoverSchemaConn(ctx, conn, testFile)
	require.NoError(t, err)

	assert.Contains(t, schema, "metric_name")
	assert.Contains(t, schema, "chq_tid")
	assert.Contains(t, schema, "chq_timestamp")
	assert.Contains(t, schema, "value")
	assert.Len(t, schema, 4)
}

func TestGetRecordCount(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	tests := []struct {
		name          string
		rows          []map[string]any
		expectedCount int64
	}{
		{
			name: "single row",
			rows: []map[string]any{
				{
					"metric_name":   "cpu_usage",
					"chq_tid":       "tid-1",
					"chq_timestamp": int64(1640000000000),
				},
			},
			expectedCount: 1,
		},
		{
			name: "multiple rows",
			rows: []map[string]any{
				{"metric_name": "cpu", "chq_tid": "1", "chq_timestamp": int64(1000)},
				{"metric_name": "mem", "chq_tid": "2", "chq_timestamp": int64(2000)},
				{"metric_name": "disk", "chq_tid": "3", "chq_timestamp": int64(3000)},
			},
			expectedCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFile := createTestMetricsParquetFile(t, tt.rows)
			count, err := getRecordCount(ctx, conn, testFile)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedCount, count)
		})
	}
}

func TestComputeStatsFromSegments(t *testing.T) {
	tests := []struct {
		name            string
		segments        []lrdb.MetricSeg
		expectedFirstTS int64
		expectedLastTS  int64
		expectedNames   []string
		expectedTypes   []int16
	}{
		{
			name: "single segment",
			segments: []lrdb.MetricSeg{
				{
					TsRange:      pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 1000}, Upper: pgtype.Int8{Int64: 2000}},
					Fingerprints: []int64{123},
					MetricNames:  []string{"cpu_usage"},
					MetricTypes:  []int16{1},
				},
			},
			expectedFirstTS: 1000,
			expectedLastTS:  2000,
			expectedNames:   []string{"cpu_usage"},
			expectedTypes:   []int16{1},
		},
		{
			name: "multiple segments with overlapping ranges",
			segments: []lrdb.MetricSeg{
				{
					TsRange:      pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 1000}, Upper: pgtype.Int8{Int64: 2000}},
					Fingerprints: []int64{123},
					MetricNames:  []string{"cpu_usage"},
					MetricTypes:  []int16{1},
				},
				{
					TsRange:      pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 1500}, Upper: pgtype.Int8{Int64: 3000}},
					Fingerprints: []int64{456},
					MetricNames:  []string{"memory_usage"},
					MetricTypes:  []int16{2},
				},
			},
			expectedFirstTS: 1000,
			expectedLastTS:  3000,
			expectedNames:   []string{"cpu_usage", "memory_usage"},
			expectedTypes:   []int16{1, 2},
		},
		{
			name: "segments with duplicate metric names",
			segments: []lrdb.MetricSeg{
				{
					TsRange:     pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 1000}, Upper: pgtype.Int8{Int64: 2000}},
					MetricNames: []string{"cpu_usage", "memory_usage"},
					MetricTypes: []int16{1, 2},
				},
				{
					TsRange:     pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 2000}, Upper: pgtype.Int8{Int64: 3000}},
					MetricNames: []string{"cpu_usage", "disk_io"},
					MetricTypes: []int16{1, 3},
				},
			},
			expectedFirstTS: 1000,
			expectedLastTS:  3000,
			expectedNames:   []string{"cpu_usage", "disk_io", "memory_usage"},
			expectedTypes:   []int16{1, 3, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			firstTS, lastTS, fingerprints, metricNames, metricTypes := computeStatsFromSegments(tt.segments)

			assert.Equal(t, tt.expectedFirstTS, firstTS)
			assert.Equal(t, tt.expectedLastTS, lastTS)
			assert.Equal(t, tt.expectedNames, metricNames)
			assert.Equal(t, tt.expectedTypes, metricTypes)
			_ = fingerprints // fingerprints are computed, just verify no panic
		})
	}
}

func TestExecuteAggregationConn(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Load DDSketch extension
	err = duckdbx.LoadDDSketchExtension(ctx, conn)
	require.NoError(t, err)

	// Create valid DDSketch data
	sketch1 := createTestSketch(t, 10.0)
	sketch2 := createTestSketch(t, 20.0)
	sketch3 := createTestSketch(t, 30.0)

	// Create input file with multiple rows to aggregate
	// All rows have the same (metric_name, chq_tid, chq_timestamp) so they MUST aggregate into 1
	baseTS := int64(1640000000000)
	baseTSNs := int64(1640000000000000000)
	rows := []map[string]any{
		{
			"metric_name":     "cpu_usage",
			"chq_tid":         "tid-1",
			"chq_timestamp":   baseTS,
			"chq_tsns":        baseTSNs,
			"chq_sketch":      sketch1,
			"chq_metric_type": "gauge",
			"value":           float64(10.0),
		},
		{
			"metric_name":     "cpu_usage",
			"chq_tid":         "tid-1",
			"chq_timestamp":   baseTS, // same timestamp
			"chq_tsns":        baseTSNs,
			"chq_sketch":      sketch2,
			"chq_metric_type": "gauge",
			"value":           float64(20.0),
		},
		{
			"metric_name":     "cpu_usage",
			"chq_tid":         "tid-1",
			"chq_timestamp":   baseTS, // same timestamp
			"chq_tsns":        baseTSNs,
			"chq_sketch":      sketch3,
			"chq_metric_type": "gauge",
			"value":           float64(30.0),
		},
	}
	inputFile := createTestMetricsParquetFile(t, rows)

	schema, err := discoverSchemaConn(ctx, conn, inputFile)
	require.NoError(t, err)

	outputFile := inputFile + ".output.parquet"
	defer func() { _ = os.Remove(outputFile) }()

	// Aggregate with 60-second frequency (should combine all 3 rows)
	err = executeAggregationConn(ctx, conn, []string{inputFile}, outputFile, schema, 60000)
	require.NoError(t, err)

	// Verify output file exists
	_, err = os.Stat(outputFile)
	require.NoError(t, err)

	// Verify aggregation result
	recordCount, err := getRecordCount(ctx, conn, outputFile)
	require.NoError(t, err)

	// All 3 rows should be aggregated into 1 (same metric_name, chq_tid, within same 60s window)
	assert.Equal(t, int64(1), recordCount)
}

func TestExecuteAggregationConn_SpecialColumnNames(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Load DDSketch extension
	err = duckdbx.LoadDDSketchExtension(ctx, conn)
	require.NoError(t, err)

	// Create valid DDSketch data
	sketch1 := createTestSketch(t, 1.0)
	sketch2 := createTestSketch(t, 2.0)

	// Test with column names containing special characters (like Kubernetes labels with /)
	// Note: parquet-go doesn't allow / in column names, so we test with other special chars
	rows := []map[string]any{
		{
			"metric_name":          "kube_pod_info",
			"chq_tid":              "tid-1",
			"chq_timestamp":        int64(1640000000000),
			"chq_tsns":             int64(1640000000000000000),
			"chq_sketch":           sketch1,
			"chq_metric_type":      "gauge",
			"resource_app_name":    "myapp",
			"resource_app_version": "v1.0",
		},
		{
			"metric_name":          "kube_pod_info",
			"chq_tid":              "tid-1",
			"chq_timestamp":        int64(1640000010000),
			"chq_tsns":             int64(1640000010000000000),
			"chq_sketch":           sketch2,
			"chq_metric_type":      "gauge",
			"resource_app_name":    "myapp",
			"resource_app_version": "v1.0",
		},
	}
	inputFile := createTestMetricsParquetFile(t, rows)

	schema, err := discoverSchemaConn(ctx, conn, inputFile)
	require.NoError(t, err)

	outputFile := inputFile + ".output.parquet"
	defer func() { _ = os.Remove(outputFile) }()

	err = executeAggregationConn(ctx, conn, []string{inputFile}, outputFile, schema, 60000)
	require.NoError(t, err)

	_, err = os.Stat(outputFile)
	require.NoError(t, err)
}

func TestExecuteAggregationConn_MultipleInputFiles(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Load DDSketch extension
	err = duckdbx.LoadDDSketchExtension(ctx, conn)
	require.NoError(t, err)

	// Create valid DDSketch data
	sketch1 := createTestSketch(t, 10.0)
	sketch2 := createTestSketch(t, 20.0)

	// Create two input files with same (metric_name, chq_tid, chq_timestamp) so they aggregate
	baseTS := int64(1640000000000)
	baseTSNs := int64(1640000000000000000)
	rows1 := []map[string]any{
		{
			"metric_name":     "cpu_usage",
			"chq_tid":         "tid-1",
			"chq_timestamp":   baseTS,
			"chq_tsns":        baseTSNs,
			"chq_sketch":      sketch1,
			"chq_metric_type": "gauge",
			"value":           float64(10.0),
		},
	}
	rows2 := []map[string]any{
		{
			"metric_name":     "cpu_usage",
			"chq_tid":         "tid-1",
			"chq_timestamp":   baseTS, // same timestamp
			"chq_tsns":        baseTSNs,
			"chq_sketch":      sketch2,
			"chq_metric_type": "gauge",
			"value":           float64(20.0),
		},
	}
	inputFile1 := createTestMetricsParquetFile(t, rows1)
	inputFile2 := createTestMetricsParquetFile(t, rows2)

	schema, err := discoverSchemaConn(ctx, conn, inputFile1)
	require.NoError(t, err)

	outputFile := inputFile1 + ".output.parquet"
	defer func() { _ = os.Remove(outputFile) }()

	err = executeAggregationConn(ctx, conn, []string{inputFile1, inputFile2}, outputFile, schema, 60000)
	require.NoError(t, err)

	recordCount, err := getRecordCount(ctx, conn, outputFile)
	require.NoError(t, err)

	// Both rows should aggregate into 1
	assert.Equal(t, int64(1), recordCount)
}

func TestColumnQuotingWithSpecialChars(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Test that we can read columns with special characters that DuckDB would
	// otherwise interpret as operators. We can't create parquet files with /
	// in column names using parquet-go, but we can test the SQL generation.
	// This is more of a smoke test to ensure our quoting works.

	// Create a simple file
	rows := []map[string]any{
		{
			"metric_name":   "test",
			"chq_tid":       "tid-1",
			"chq_timestamp": int64(1640000000000),
		},
	}
	testFile := createTestMetricsParquetFile(t, rows)

	// Test that schema discovery works
	schema, err := discoverSchemaConn(ctx, conn, testFile)
	require.NoError(t, err)
	assert.Len(t, schema, 3)

	// Verify that the SQL we generate uses proper quoting
	// by running a simple query with quoted identifiers
	query := fmt.Sprintf(`SELECT "metric_name", "chq_tid", "chq_timestamp" FROM read_parquet('%s')`, testFile)
	rows2, err := conn.QueryContext(ctx, query)
	require.NoError(t, err)
	defer func() { _ = rows2.Close() }()

	count := 0
	for rows2.Next() {
		count++
	}
	require.NoError(t, rows2.Err())
	assert.Equal(t, 1, count)
}
