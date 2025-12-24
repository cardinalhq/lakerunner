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
	"strings"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// createTestLogsParquetFile creates a parquet file with log data for testing.
func createTestLogsParquetFile(t *testing.T, rows []map[string]any) string {
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
		default:
			t.Fatalf("Unsupported type %T for key %s", value, key)
		}
		nodes[key] = node
	}

	schema := parquet.NewSchema("logs", parquet.Group(nodes))

	tmpFile, err := os.CreateTemp("", "test-logs-*.parquet")
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

func TestBuildLogOrderByClause(t *testing.T) {
	tests := []struct {
		name        string
		schema      []string
		streamField string
		wantOrder   string
	}{
		{
			name:        "all columns present with stream field",
			schema:      []string{"resource_service_name", "resource_customer_domain", "chq_fingerprint", "chq_timestamp", "my_stream"},
			streamField: "my_stream",
			wantOrder:   `COALESCE(NULLIF("my_stream", ''), NULLIF("resource_customer_domain", ''), NULLIF("resource_service_name", ''), ''), "chq_fingerprint", "chq_timestamp"`,
		},
		{
			name:        "no stream field configured",
			schema:      []string{"resource_service_name", "resource_customer_domain", "chq_fingerprint", "chq_timestamp"},
			streamField: "",
			wantOrder:   `COALESCE(NULLIF("resource_customer_domain", ''), NULLIF("resource_service_name", ''), ''), "chq_fingerprint", "chq_timestamp"`,
		},
		{
			name:        "only service_name present",
			schema:      []string{"resource_service_name", "chq_fingerprint", "chq_timestamp"},
			streamField: "",
			wantOrder:   `NULLIF("resource_service_name", ''), "chq_fingerprint", "chq_timestamp"`,
		},
		{
			name:        "only timestamp present",
			schema:      []string{"chq_timestamp"},
			streamField: "",
			wantOrder:   `"chq_timestamp"`,
		},
		{
			name:        "stream field not in schema",
			schema:      []string{"resource_service_name", "chq_fingerprint", "chq_timestamp"},
			streamField: "missing_field",
			wantOrder:   `NULLIF("resource_service_name", ''), "chq_fingerprint", "chq_timestamp"`,
		},
		{
			name:        "empty schema",
			schema:      []string{},
			streamField: "",
			wantOrder:   "1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildLogOrderByClause(tt.schema, tt.streamField)
			assert.Equal(t, tt.wantOrder, got)
		})
	}
}

func TestBuildServiceIdentifierExpr(t *testing.T) {
	tests := []struct {
		name        string
		schema      []string
		streamField string
		want        string
	}{
		{
			name:        "all fields with stream field",
			schema:      []string{"my_stream", "resource_customer_domain", "resource_service_name"},
			streamField: "my_stream",
			want:        `COALESCE(NULLIF("my_stream", ''), NULLIF("resource_customer_domain", ''), NULLIF("resource_service_name", ''), '')`,
		},
		{
			name:        "only customer domain",
			schema:      []string{"resource_customer_domain"},
			streamField: "",
			want:        `NULLIF("resource_customer_domain", '')`,
		},
		{
			name:        "only service name",
			schema:      []string{"resource_service_name"},
			streamField: "",
			want:        `NULLIF("resource_service_name", '')`,
		},
		{
			name:        "no matching fields",
			schema:      []string{"other_field"},
			streamField: "",
			want:        "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schemaSet := mapset.NewSet[string]()
			for _, col := range tt.schema {
				schemaSet.Add(col)
			}
			got := buildServiceIdentifierExpr(schemaSet, tt.streamField)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestComputeLogStatsFromSegments(t *testing.T) {
	tests := []struct {
		name            string
		segments        []lrdb.LogSeg
		streamField     string
		expectedFirstTS int64
		expectedLastTS  int64
		expectedFPs     []int64
		expectedStreams []string
	}{
		{
			name:            "empty segments",
			segments:        []lrdb.LogSeg{},
			streamField:     "",
			expectedFirstTS: 0,
			expectedLastTS:  0,
			expectedFPs:     nil,
			expectedStreams: nil,
		},
		{
			name: "single segment",
			segments: []lrdb.LogSeg{
				{
					TsRange:      pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 1000}, Upper: pgtype.Int8{Int64: 2000}},
					Fingerprints: []int64{123, 456},
					StreamIds:    []string{"service-a"},
				},
			},
			streamField:     "",
			expectedFirstTS: 1000,
			expectedLastTS:  2000,
			expectedFPs:     []int64{123, 456},
			expectedStreams: []string{"service-a"},
		},
		{
			name: "multiple segments with overlapping ranges",
			segments: []lrdb.LogSeg{
				{
					TsRange:      pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 1000}, Upper: pgtype.Int8{Int64: 2000}},
					Fingerprints: []int64{123},
					StreamIds:    []string{"service-a"},
				},
				{
					TsRange:      pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 500}, Upper: pgtype.Int8{Int64: 3000}},
					Fingerprints: []int64{456},
					StreamIds:    []string{"service-b"},
				},
			},
			streamField:     "",
			expectedFirstTS: 500,
			expectedLastTS:  3000,
			expectedFPs:     []int64{123, 456},
			expectedStreams: []string{"service-a", "service-b"},
		},
		{
			name: "segments with duplicate values",
			segments: []lrdb.LogSeg{
				{
					TsRange:      pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 1000}, Upper: pgtype.Int8{Int64: 2000}},
					Fingerprints: []int64{123, 456},
					StreamIds:    []string{"service-a", "service-b"},
				},
				{
					TsRange:      pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 2000}, Upper: pgtype.Int8{Int64: 3000}},
					Fingerprints: []int64{123, 789}, // 123 is duplicate
					StreamIds:    []string{"service-a", "service-c"},
				},
			},
			streamField:     "",
			expectedFirstTS: 1000,
			expectedLastTS:  3000,
			expectedFPs:     []int64{123, 456, 789}, // deduplicated
			expectedStreams: []string{"service-a", "service-b", "service-c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := computeLogStatsFromSegments(tt.segments, tt.streamField)

			assert.Equal(t, tt.expectedFirstTS, stats.FirstTS)
			assert.Equal(t, tt.expectedLastTS, stats.LastTS)
			assert.ElementsMatch(t, tt.expectedFPs, stats.Fingerprints)
			assert.ElementsMatch(t, tt.expectedStreams, stats.StreamValues)
		})
	}
}

func TestComputeLogStatsFromSegments_StreamIdField(t *testing.T) {
	field1 := "resource_service_name"
	field2 := "resource_customer_domain"

	tests := []struct {
		name                string
		segments            []lrdb.LogSeg
		streamField         string
		expectedStreamIdFld *string
	}{
		{
			name: "takes first non-nil stream ID field",
			segments: []lrdb.LogSeg{
				{
					TsRange:       pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 1000}, Upper: pgtype.Int8{Int64: 2000}},
					StreamIDField: nil,
				},
				{
					TsRange:       pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 2000}, Upper: pgtype.Int8{Int64: 3000}},
					StreamIDField: &field1,
				},
				{
					TsRange:       pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 3000}, Upper: pgtype.Int8{Int64: 4000}},
					StreamIDField: &field2, // ignored, first wins
				},
			},
			streamField:         "",
			expectedStreamIdFld: &field1,
		},
		{
			name: "uses streamField when no segment has it",
			segments: []lrdb.LogSeg{
				{
					TsRange:       pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 1000}, Upper: pgtype.Int8{Int64: 2000}},
					StreamIDField: nil,
				},
			},
			streamField:         "my_stream",
			expectedStreamIdFld: stringPtr("my_stream"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := computeLogStatsFromSegments(tt.segments, tt.streamField)
			if tt.expectedStreamIdFld == nil {
				assert.Nil(t, stats.StreamIdField)
			} else {
				require.NotNil(t, stats.StreamIdField)
				assert.Equal(t, *tt.expectedStreamIdFld, *stats.StreamIdField)
			}
		})
	}
}

func stringPtr(s string) *string {
	return &s
}

func TestExecuteLogCompaction(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Create input log file
	rows := []map[string]any{
		{
			"chq_timestamp":         int64(3000),
			"chq_fingerprint":       int64(100),
			"resource_service_name": "service-b",
			"log_level":             "info",
			"log_body":              "message 3",
		},
		{
			"chq_timestamp":         int64(1000),
			"chq_fingerprint":       int64(100),
			"resource_service_name": "service-a",
			"log_level":             "error",
			"log_body":              "message 1",
		},
		{
			"chq_timestamp":         int64(2000),
			"chq_fingerprint":       int64(200),
			"resource_service_name": "service-a",
			"log_level":             "warn",
			"log_body":              "message 2",
		},
	}
	inputFile := createTestLogsParquetFile(t, rows)

	outputFile := inputFile + ".output.parquet"
	defer func() { _ = os.Remove(outputFile) }()

	err = executeLogCompaction(ctx, conn, []string{inputFile}, outputFile, "")
	require.NoError(t, err)

	// Verify output file exists
	_, err = os.Stat(outputFile)
	require.NoError(t, err)

	// Verify record count
	recordCount, err := getRecordCount(ctx, conn, outputFile)
	require.NoError(t, err)
	assert.Equal(t, int64(3), recordCount)

	// Verify sort order by reading back
	escapedFile := strings.ReplaceAll(outputFile, "'", "''")
	query := fmt.Sprintf(`SELECT resource_service_name, chq_fingerprint, chq_timestamp FROM read_parquet('%s')`, escapedFile)
	resultRows, err := conn.QueryContext(ctx, query)
	require.NoError(t, err)
	defer func() { _ = resultRows.Close() }()

	var results []struct {
		serviceName string
		fingerprint int64
		timestamp   int64
	}
	for resultRows.Next() {
		var r struct {
			serviceName string
			fingerprint int64
			timestamp   int64
		}
		err := resultRows.Scan(&r.serviceName, &r.fingerprint, &r.timestamp)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, resultRows.Err())

	// Expected order: service-a fp=100 ts=1000, service-a fp=200 ts=2000, service-b fp=100 ts=3000
	require.Len(t, results, 3)
	assert.Equal(t, "service-a", results[0].serviceName)
	assert.Equal(t, int64(100), results[0].fingerprint)
	assert.Equal(t, int64(1000), results[0].timestamp)

	assert.Equal(t, "service-a", results[1].serviceName)
	assert.Equal(t, int64(200), results[1].fingerprint)
	assert.Equal(t, int64(2000), results[1].timestamp)

	assert.Equal(t, "service-b", results[2].serviceName)
	assert.Equal(t, int64(100), results[2].fingerprint)
	assert.Equal(t, int64(3000), results[2].timestamp)
}

func TestExecuteLogCompaction_MultipleFiles(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Create two input files with different columns (simulating schema evolution)
	rows1 := []map[string]any{
		{
			"chq_timestamp":         int64(1000),
			"chq_fingerprint":       int64(100),
			"resource_service_name": "service-a",
			"log_level":             "info",
		},
	}
	rows2 := []map[string]any{
		{
			"chq_timestamp":            int64(2000),
			"chq_fingerprint":          int64(200),
			"resource_customer_domain": "domain-a",
			"log_level":                "error",
		},
	}
	inputFile1 := createTestLogsParquetFile(t, rows1)
	inputFile2 := createTestLogsParquetFile(t, rows2)

	outputFile := inputFile1 + ".output.parquet"
	defer func() { _ = os.Remove(outputFile) }()

	// union_by_name should merge the schemas
	err = executeLogCompaction(ctx, conn, []string{inputFile1, inputFile2}, outputFile, "")
	require.NoError(t, err)

	recordCount, err := getRecordCount(ctx, conn, outputFile)
	require.NoError(t, err)
	assert.Equal(t, int64(2), recordCount)
}

func TestExtractAggCounts(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Create input log file
	rows := []map[string]any{
		{
			"chq_timestamp":         int64(10000), // bucket 10000
			"log_level":             "info",
			"resource_service_name": "service-a",
		},
		{
			"chq_timestamp":         int64(10500), // bucket 10000
			"log_level":             "info",
			"resource_service_name": "service-a",
		},
		{
			"chq_timestamp":         int64(20000), // bucket 20000
			"log_level":             "error",
			"resource_service_name": "service-a",
		},
		{
			"chq_timestamp":         int64(10000), // bucket 10000
			"log_level":             "info",
			"resource_service_name": "service-b",
		},
	}
	inputFile := createTestLogsParquetFile(t, rows)

	// Load into DuckDB table
	escapedFile := strings.ReplaceAll(inputFile, "'", "''")
	createSQL := fmt.Sprintf("CREATE TABLE logs_input AS SELECT * FROM read_parquet('%s')", escapedFile)
	_, err = conn.ExecContext(ctx, createSQL)
	require.NoError(t, err)

	aggCounts, err := extractAggCounts(ctx, conn, "")
	require.NoError(t, err)

	// Expected:
	// bucket=10000, level=info, service=service-a: count=2
	// bucket=20000, level=error, service=service-a: count=1
	// bucket=10000, level=info, service=service-b: count=1
	assert.Len(t, aggCounts, 3)

	key1 := factories.LogAggKey{
		TimestampBucket:  10000,
		LogLevel:         "info",
		StreamFieldName:  "resource_service_name",
		StreamFieldValue: "service-a",
	}
	assert.Equal(t, int64(2), aggCounts[key1])

	key2 := factories.LogAggKey{
		TimestampBucket:  20000,
		LogLevel:         "error",
		StreamFieldName:  "resource_service_name",
		StreamFieldValue: "service-a",
	}
	assert.Equal(t, int64(1), aggCounts[key2])

	key3 := factories.LogAggKey{
		TimestampBucket:  10000,
		LogLevel:         "info",
		StreamFieldName:  "resource_service_name",
		StreamFieldValue: "service-b",
	}
	assert.Equal(t, int64(1), aggCounts[key3])
}

func TestExtractAggCounts_WithStreamField(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Create input log file with custom stream field
	rows := []map[string]any{
		{
			"chq_timestamp":         int64(10000),
			"log_level":             "info",
			"my_custom_stream":      "stream-x",
			"resource_service_name": "service-a", // should be ignored
		},
		{
			"chq_timestamp":         int64(10000),
			"log_level":             "info",
			"my_custom_stream":      "stream-y",
			"resource_service_name": "service-a",
		},
	}
	inputFile := createTestLogsParquetFile(t, rows)

	// Load into DuckDB table
	escapedFile := strings.ReplaceAll(inputFile, "'", "''")
	createSQL := fmt.Sprintf("CREATE TABLE logs_input AS SELECT * FROM read_parquet('%s')", escapedFile)
	_, err = conn.ExecContext(ctx, createSQL)
	require.NoError(t, err)

	aggCounts, err := extractAggCounts(ctx, conn, "my_custom_stream")
	require.NoError(t, err)

	assert.Len(t, aggCounts, 2)

	// Verify the custom stream field is used
	for key := range aggCounts {
		assert.Equal(t, "my_custom_stream", key.StreamFieldName)
	}
}
