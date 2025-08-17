package worker

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func TestIngestParquetDir_CountMatches_Batch(t *testing.T) {
	// find parquet files
	paths, err := filepath.Glob("./db/*.parquet")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(paths) == 0 {
		t.Skip("no parquet files in ./db; skipping")
	}

	// derive deterministic segment IDs (one per file)
	segmentIDs := make([]string, len(paths))
	for i, p := range paths {
		base := filepath.Base(p)
		segmentIDs[i] = strings.TrimSuffix(base, filepath.Ext(base))
	}

	// fresh duckdb in a temp dir
	ctx := context.Background()
	td := t.TempDir()
	dbPath := filepath.Join(td, "cache.duckdb")
	table := "cache"

	// create sink
	sink, err := New(ctx, dbPath, table)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	defer sink.Close()

	// open a separate read connection to the same DB for counting
	readDB, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatalf("open read db: %v", err)
	}
	defer readDB.Close()

	// helper to count rows in a parquet file
	countParquet := func(p string) (int64, error) {
		row := readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)`, p)
		var n int64
		if err := row.Scan(&n); err != nil {
			return 0, err
		}
		return n, nil
	}

	// compute expected total rows across all files
	var expected int64
	for _, p := range paths {
		n, err := countParquet(p)
		if err != nil {
			t.Fatalf("COUNT(*) for %s: %v", p, err)
		}
		expected += n
	}

	// ingest all files in a single batch (now requires segmentIDs)
	if err := sink.IngestParquetBatch(ctx, paths, segmentIDs); err != nil {
		t.Fatalf("batch ingest: %v", err)
	}

	// final count from sink table
	var got int64
	if err := readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&got); err != nil {
		t.Fatalf("final COUNT(*): %v", err)
	}
	if got != expected {
		t.Fatalf("row count mismatch: got=%d expected=%d", got, expected)
	}

	// optional: ensure segment_id was populated
	var nullSeg int64
	if err := readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table+` WHERE segment_id IS NULL`).Scan(&nullSeg); err != nil {
		t.Fatalf("segment_id NULL check: %v", err)
	}
	if nullSeg != 0 {
		t.Fatalf("segment_id has %d NULL rows", nullSeg)
	}
}
