package queryworker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDDBSink_IngestParquetBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Step 1: Discover local .parquet files
	parquetDir := "./db"
	var parquetPaths []string
	err := filepath.Walk(parquetDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(info.Name(), ".parquet") {
			parquetPaths = append(parquetPaths, fmt.Sprintf("./%s", path))
		}
		return nil
	})
	require.NoError(t, err, "failed to walk ./db")
	require.NotEmpty(t, parquetPaths, "no parquet files found under ./db")

	// Step 2: Assign dummy segment IDs
	segmentIDs := make([]int64, len(parquetPaths))
	for i := range segmentIDs {
		segmentIDs[i] = int64(1000 + i) // dummy segment IDs
	}

	// Step 3: Create fresh DDBSink (in-memory or file-backed)
	sink, err := NewDDBSink(ctx)
	require.NoError(t, err, "failed to create DDBSink")
	defer sink.Close()

	// Step 4: Ingest parquet files
	err = sink.IngestParquetBatch(ctx, parquetPaths, segmentIDs)
	require.NoError(t, err, "failed to ingest parquet batch")

	// Step 5: Validate row count
	rowCount := sink.RowCount()
	t.Logf("Ingested %d rows", rowCount)
	require.Greater(t, rowCount, int64(0), "no rows ingested")

	// Step 6: Optionally query segment_id column to check distinct values
	db := sink.db
	rows, conn, err := db.QueryContext(ctx, `SELECT DISTINCT segment_id FROM cached`)
	require.NoError(t, err, "query segment_id failed")
	defer rows.Close()
	defer conn.Close()

	var seenIDs []int64
	for rows.Next() {
		var id int64
		require.NoError(t, rows.Scan(&id))
		seenIDs = append(seenIDs, id)
	}
	require.Equal(t, len(seenIDs), len(parquetPaths), "no segment_id values found")
	t.Logf("Seen segment_ids: %v", seenIDs)
}
