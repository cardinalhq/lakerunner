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

package queryworker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/stretchr/testify/require"
)

func TestDDBSink_IngestParquetBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Step 1: Discover local .parquet files
	parquetDir := "./testdata/db"
	var parquetPaths []string
	err := filepath.Walk(parquetDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(info.Name(), ".parquet") {
			parquetPaths = append(parquetPaths, fmt.Sprintf("./%s", path))
		}
		return nil
	})
	require.NoError(t, err, "failed to walk ./testdata/db")
	require.NotEmpty(t, parquetPaths, "no parquet files found under ./testdata/db")

	// Step 2: Assign dummy segment IDs
	segmentIDs := make([]int64, len(parquetPaths))
	for i := range segmentIDs {
		segmentIDs[i] = int64(1000 + i) // dummy segment IDs
	}

	// Step 3: Create a shared S3DB pool for testing
	s3Pool, err := duckdbx.NewS3DB()
	require.NoError(t, err, "failed to create S3DB pool")
	defer func() { _ = s3Pool.Close() }()

	// Step 4: Create fresh DDBSink (in-memory or file-backed)
	sink, err := NewDDBSink("metrics", ctx, s3Pool)
	require.NoError(t, err, "failed to create DDBSink")
	defer func() { _ = sink.Close() }()

	// Step 5: Ingest parquet files
	err = sink.IngestParquetBatch(ctx, parquetPaths, segmentIDs)
	require.NoError(t, err, "failed to ingest parquet batch")

	// Step 6: Validate row count
	rowCount := sink.RowCount()
	t.Logf("Ingested %d rows", rowCount)
	require.Greater(t, rowCount, int64(0), "no rows ingested")

	// Step 6: Optionally query segment_id column to check distinct values
	conn, release, err := s3Pool.GetConnection(ctx, "local", "", "")
	require.NoError(t, err, "get connection failed")
	defer release()

	rows, err := conn.QueryContext(ctx, `SELECT DISTINCT segment_id FROM metrics_cached`)
	require.NoError(t, err, "query segment_id failed")
	defer func() { _ = rows.Close() }()

	var seenIDs []int64
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		seenIDs = append(seenIDs, id)
	}
	require.Equal(t, len(seenIDs), len(parquetPaths), "no segment_id values found")
	t.Logf("Seen segment_ids: %v", seenIDs)
}
