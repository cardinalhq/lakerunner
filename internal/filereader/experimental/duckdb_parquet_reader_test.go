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
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

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
	path := "../../testdata/metrics/compact-test-0001/tbl_299476441865651503.parquet"
	rows := countDuckDBParquetRows(t, []string{path})
	require.Greater(t, rows, 0)
}

// TestDuckDBParquetRawReaderMultipleFiles verifies streaming multiple parquet files.
func TestDuckDBParquetRawReaderMultipleFiles(t *testing.T) {
	paths := []string{
		"../../testdata/metrics/compact-test-0001/tbl_299476441865651503.parquet",
		"../../testdata/metrics/compact-test-0001/tbl_299476446630380847.parquet",
	}
	rows := countDuckDBParquetRows(t, paths)
	require.Greater(t, rows, 0)
}

// TestDuckDBParquetRawReaderCopiesSketch verifies sketch values aren't reused across rows.
func TestDuckDBParquetRawReaderCopiesSketch(t *testing.T) {
	path := "../../testdata/metrics/compact-test-0001/tbl_299476441865651503.parquet"
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
	if errors.Is(err, io.EOF) {
		t.Skip("not enough rows to verify sketch copy")
	}

	require.True(t, bytes.Equal(sketch, snapshot))
}
