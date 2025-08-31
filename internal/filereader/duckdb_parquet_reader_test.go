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

package filereader

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
)

func countDuckDBParquetRows(t *testing.T, paths []string) int {
	reader, err := NewDuckDBParquetRawReader(paths, 10)
	require.NoError(t, err)
	defer reader.Close()

	rows := 0
	for {
		batch, err := reader.Next()
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
