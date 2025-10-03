//go:build experimental

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

package experimental

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// createTestMetricParquetFile creates a temporary metric Parquet file for testing
func createTestMetricParquetFile(t *testing.T, rowCount int, withSketch bool) string {
	t.Helper()

	rows := make([]map[string]any, rowCount)
	for i := range rows {
		row := map[string]any{
			"chq_timestamp":    int64(1000000 + i),
			"chq_collector_id": fmt.Sprintf("collector-%d", i),
			"metric_name":      "test_metric",
			"chq_rollup_sum":   float64(i * 100),
		}

		if withSketch {
			// Add sketch data as bytes
			row["chq_sketch"] = []byte(fmt.Sprintf("sketch-data-%d", i))
		}

		rows[i] = row
	}

	// Build schema
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

	schema := parquet.NewSchema("metrics", parquet.Group(nodes))

	// Create temporary file
	tmpFile, err := os.CreateTemp("", "test-metrics-*.parquet")
	require.NoError(t, err)

	writer := parquet.NewGenericWriter[map[string]any](tmpFile, schema)

	for _, row := range rows {
		_, err := writer.Write([]map[string]any{row})
		require.NoError(t, err, "Failed to write row")
	}

	err = writer.Close()
	require.NoError(t, err, "Failed to close writer")

	err = tmpFile.Close()
	require.NoError(t, err, "Failed to close file")

	// Register cleanup
	t.Cleanup(func() {
		_ = os.Remove(tmpFile.Name())
	})

	return tmpFile.Name()
}

func countDuckDBParquetRows(t *testing.T, paths []string) int {
	reader, err := NewDuckDBParquetRawReader(context.TODO(), paths, 10)
	require.NoError(t, err)
	defer reader.Close()

	rows := 0
	for {
		batch, err := reader.Next(context.TODO())
		if batch != nil {
			rows += batch.Len()
			pipeline.ReturnBatch(batch)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}

	require.Equal(t, int64(rows), reader.TotalRowsReturned())
	return rows
}

// TestDuckDBParquetRawReaderWithRealFile verifies streaming of a parquet file using DuckDB.
func TestDuckDBParquetRawReaderWithRealFile(t *testing.T) {
	path := createTestMetricParquetFile(t, 100, false)
	rows := countDuckDBParquetRows(t, []string{path})
	require.Greater(t, rows, 0)
}

// TestDuckDBParquetRawReaderMultipleFiles verifies streaming multiple parquet files.
func TestDuckDBParquetRawReaderMultipleFiles(t *testing.T) {
	path1 := createTestMetricParquetFile(t, 100, false)
	path2 := createTestMetricParquetFile(t, 150, false)
	paths := []string{path1, path2}
	rows := countDuckDBParquetRows(t, paths)
	require.Equal(t, 250, rows, "should read all rows from both files")
}

// TestDuckDBParquetRawReaderCopiesSketch verifies sketch values aren't reused across rows.
func TestDuckDBParquetRawReaderCopiesSketch(t *testing.T) {
	path := createTestMetricParquetFile(t, 100, true)
	reader, err := NewDuckDBParquetRawReader(context.TODO(), []string{path}, 1)
	require.NoError(t, err)
	defer reader.Close()

	batch, err := reader.Next(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 1, batch.Len())
	row := pipeline.CopyRow(batch.Get(0))
	pipeline.ReturnBatch(batch)

	sketch, ok := row[wkk.RowKeySketch].([]byte)
	require.True(t, ok)
	snapshot := append([]byte(nil), sketch...)

	// Reading the next batch should not mutate the previously copied sketch
	batch2, err := reader.Next(context.TODO())
	if batch2 != nil {
		pipeline.ReturnBatch(batch2)
	}

	require.True(t, bytes.Equal(sketch, snapshot))
}
