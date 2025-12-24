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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// createTestTracesParquetFile creates a parquet file with trace data for testing.
func createTestTracesParquetFile(t *testing.T, rows []map[string]any) string {
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

	schema := parquet.NewSchema("traces", parquet.Group(nodes))

	tmpFile, err := os.CreateTemp("", "test-traces-*.parquet")
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

func TestBuildTraceOrderByClause(t *testing.T) {
	tests := []struct {
		name      string
		schema    []string
		wantOrder string
	}{
		{
			name:      "all columns present",
			schema:    []string{"trace_id", "chq_timestamp", "span_id"},
			wantOrder: `"trace_id", "chq_timestamp"`,
		},
		{
			name:      "only trace_id",
			schema:    []string{"trace_id", "span_id"},
			wantOrder: `"trace_id"`,
		},
		{
			name:      "only timestamp",
			schema:    []string{"chq_timestamp", "span_id"},
			wantOrder: `"chq_timestamp"`,
		},
		{
			name:      "neither column present",
			schema:    []string{"span_id", "other_field"},
			wantOrder: "1",
		},
		{
			name:      "empty schema",
			schema:    []string{},
			wantOrder: "1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildTraceOrderByClause(tt.schema)
			assert.Equal(t, tt.wantOrder, got)
		})
	}
}

func TestComputeTraceStatsFromSegments(t *testing.T) {
	tests := []struct {
		name            string
		segments        []lrdb.TraceSeg
		expectedFirstTS int64
		expectedLastTS  int64
		expectedFPs     []int64
	}{
		{
			name:            "empty segments",
			segments:        []lrdb.TraceSeg{},
			expectedFirstTS: 0,
			expectedLastTS:  0,
			expectedFPs:     nil,
		},
		{
			name: "single segment",
			segments: []lrdb.TraceSeg{
				{
					TsRange:      pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 1000}, Upper: pgtype.Int8{Int64: 2000}},
					Fingerprints: []int64{123, 456},
				},
			},
			expectedFirstTS: 1000,
			expectedLastTS:  2000,
			expectedFPs:     []int64{123, 456},
		},
		{
			name: "multiple segments with overlapping ranges",
			segments: []lrdb.TraceSeg{
				{
					TsRange:      pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 1000}, Upper: pgtype.Int8{Int64: 2000}},
					Fingerprints: []int64{123},
				},
				{
					TsRange:      pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 500}, Upper: pgtype.Int8{Int64: 3000}},
					Fingerprints: []int64{456},
				},
			},
			expectedFirstTS: 500,
			expectedLastTS:  3000,
			expectedFPs:     []int64{123, 456},
		},
		{
			name: "segments with duplicate fingerprints",
			segments: []lrdb.TraceSeg{
				{
					TsRange:      pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 1000}, Upper: pgtype.Int8{Int64: 2000}},
					Fingerprints: []int64{123, 456},
				},
				{
					TsRange:      pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 2000}, Upper: pgtype.Int8{Int64: 3000}},
					Fingerprints: []int64{123, 789}, // 123 is duplicate
				},
			},
			expectedFirstTS: 1000,
			expectedLastTS:  3000,
			expectedFPs:     []int64{123, 456, 789}, // deduplicated
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := computeTraceStatsFromSegments(tt.segments)

			assert.Equal(t, tt.expectedFirstTS, stats.FirstTS)
			assert.Equal(t, tt.expectedLastTS, stats.LastTS)
			assert.ElementsMatch(t, tt.expectedFPs, stats.Fingerprints)
		})
	}
}

func TestComputeTraceStatsFromSegments_LabelNameMap(t *testing.T) {
	tests := []struct {
		name               string
		segments           []lrdb.TraceSeg
		expectedLabelNames []string
	}{
		{
			name: "single segment with label names",
			segments: []lrdb.TraceSeg{
				{
					TsRange:      pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 1000}, Upper: pgtype.Int8{Int64: 2000}},
					LabelNameMap: mustMarshalJSON(map[string]string{"label_a": "label_a", "label_b": "label_b"}),
				},
			},
			expectedLabelNames: []string{"label_a", "label_b"},
		},
		{
			name: "multiple segments merge label names",
			segments: []lrdb.TraceSeg{
				{
					TsRange:      pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 1000}, Upper: pgtype.Int8{Int64: 2000}},
					LabelNameMap: mustMarshalJSON(map[string]string{"label_a": "label_a"}),
				},
				{
					TsRange:      pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 2000}, Upper: pgtype.Int8{Int64: 3000}},
					LabelNameMap: mustMarshalJSON(map[string]string{"label_a": "label_a", "label_c": "label_c"}),
				},
			},
			expectedLabelNames: []string{"label_a", "label_c"},
		},
		{
			name: "segment with nil label name map",
			segments: []lrdb.TraceSeg{
				{
					TsRange:      pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 1000}, Upper: pgtype.Int8{Int64: 2000}},
					LabelNameMap: nil,
				},
			},
			expectedLabelNames: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := computeTraceStatsFromSegments(tt.segments)

			if tt.expectedLabelNames == nil {
				assert.Nil(t, stats.LabelNameMap)
			} else {
				var gotMap map[string]string
				err := json.Unmarshal(stats.LabelNameMap, &gotMap)
				require.NoError(t, err)

				var gotNames []string
				for k := range gotMap {
					gotNames = append(gotNames, k)
				}
				assert.ElementsMatch(t, tt.expectedLabelNames, gotNames)
			}
		})
	}
}

func mustMarshalJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func TestExecuteTraceCompaction(t *testing.T) {
	ctx := context.Background()

	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	// Create input trace file with unsorted data
	rows := []map[string]any{
		{
			"trace_id":      "trace-c",
			"chq_timestamp": int64(3000),
			"span_id":       "span-3",
		},
		{
			"trace_id":      "trace-a",
			"chq_timestamp": int64(1000),
			"span_id":       "span-1",
		},
		{
			"trace_id":      "trace-a",
			"chq_timestamp": int64(2000),
			"span_id":       "span-2",
		},
		{
			"trace_id":      "trace-b",
			"chq_timestamp": int64(1500),
			"span_id":       "span-4",
		},
	}
	inputFile := createTestTracesParquetFile(t, rows)

	outputFile := inputFile + ".output.parquet"
	defer func() { _ = os.Remove(outputFile) }()

	err = executeTraceCompaction(ctx, conn, []string{inputFile}, outputFile)
	require.NoError(t, err)

	// Verify output file exists
	_, err = os.Stat(outputFile)
	require.NoError(t, err)

	// Verify record count
	recordCount, err := getRecordCount(ctx, conn, outputFile)
	require.NoError(t, err)
	assert.Equal(t, int64(4), recordCount)

	// Verify sort order by reading back
	escapedFile := strings.ReplaceAll(outputFile, "'", "''")
	query := fmt.Sprintf(`SELECT trace_id, chq_timestamp FROM read_parquet('%s')`, escapedFile)
	resultRows, err := conn.QueryContext(ctx, query)
	require.NoError(t, err)
	defer func() { _ = resultRows.Close() }()

	var results []struct {
		traceID   string
		timestamp int64
	}
	for resultRows.Next() {
		var r struct {
			traceID   string
			timestamp int64
		}
		err := resultRows.Scan(&r.traceID, &r.timestamp)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.NoError(t, resultRows.Err())

	// Expected order: trace-a ts=1000, trace-a ts=2000, trace-b ts=1500, trace-c ts=3000
	require.Len(t, results, 4)
	assert.Equal(t, "trace-a", results[0].traceID)
	assert.Equal(t, int64(1000), results[0].timestamp)

	assert.Equal(t, "trace-a", results[1].traceID)
	assert.Equal(t, int64(2000), results[1].timestamp)

	assert.Equal(t, "trace-b", results[2].traceID)
	assert.Equal(t, int64(1500), results[2].timestamp)

	assert.Equal(t, "trace-c", results[3].traceID)
	assert.Equal(t, int64(3000), results[3].timestamp)
}

func TestExecuteTraceCompaction_MultipleFiles(t *testing.T) {
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
			"trace_id":      "trace-a",
			"chq_timestamp": int64(1000),
			"span_id":       "span-1",
		},
	}
	rows2 := []map[string]any{
		{
			"trace_id":      "trace-b",
			"chq_timestamp": int64(2000),
			"span_id":       "span-2",
			"extra_column":  "extra_value",
		},
	}
	inputFile1 := createTestTracesParquetFile(t, rows1)
	inputFile2 := createTestTracesParquetFile(t, rows2)

	outputFile := inputFile1 + ".output.parquet"
	defer func() { _ = os.Remove(outputFile) }()

	// union_by_name should merge the schemas
	err = executeTraceCompaction(ctx, conn, []string{inputFile1, inputFile2}, outputFile)
	require.NoError(t, err)

	recordCount, err := getRecordCount(ctx, conn, outputFile)
	require.NoError(t, err)
	assert.Equal(t, int64(2), recordCount)
}
