package worker

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeParquet creates a tiny parquet file with rows (i from 0..rows-1)
// and a BIGINT column named exactly "_cardinalhq.timestamp".
func writeParquet(t *testing.T, path string, rows int) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb for parquet gen: %v", err)
	}
	defer db.Close()

	create := fmt.Sprintf(`
CREATE OR REPLACE TABLE _gen AS
SELECT (1700000000000 + i)::BIGINT AS "_cardinalhq.timestamp",
       i::INTEGER AS v
FROM range(0, %d) AS t(i);`, rows)
	if _, err := db.Exec(create); err != nil {
		t.Fatalf("create gen table: %v", err)
	}

	out := fmt.Sprintf(`COPY _gen TO '%s' (FORMAT PARQUET);`, strings.ReplaceAll(path, "'", "''"))
	if _, err := db.Exec(out); err != nil {
		t.Fatalf("copy parquet: %v", err)
	}
	_, _ = db.Exec(`DROP TABLE IF EXISTS _gen;`)
}

// copyFile copies src → dst, creating parent dirs.
func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(dst), err)
	}
	in, err := os.Open(src)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		t.Fatalf("create dst: %v", err)
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		t.Fatalf("copy: %v", err)
	}
	if err := out.Close(); err != nil {
		t.Fatalf("close dst: %v", err)
	}
}

func TestQuery_StreamFromLocalS3_AndIngest(t *testing.T) {
	ctx := context.Background()

	// 1) Prepare a "local S3" directory with some parquet files.
	// If ./db already has your listed files, we can use those;
	// Otherwise, create a small set so the test is hermetic.
	s3dir := "./db"
	_ = os.MkdirAll(s3dir, 0o755)

	// If no files exist, generate a few.
	glob := filepath.Join(s3dir, "tbl_*.parquet")
	paths, _ := filepath.Glob(glob)
	if len(paths) == 0 {
		// Generate 3 files (3,2,4 rows)
		want := []struct {
			filename string
			rows     int
		}{
			{"tbl_297569037997572372.parquet", 3},
			{"tbl_297569038148567316.parquet", 2},
			{"tbl_297569038215676180.parquet", 4},
		}
		for _, w := range want {
			writeParquet(t, filepath.Join(s3dir, w.filename), w.rows)
		}
		paths, _ = filepath.Glob(glob)
	}

	if len(paths) < 2 {
		t.Skip("need at least 2 parquet files in ./db to run this test")
	}

	// 2) Create a fresh DuckDB sink + CacheManager (no-op downloader).
	td := t.TempDir()
	dbPath := filepath.Join(td, "cache.duckdb")
	table := "cache"

	sink, err := New(ctx, dbPath, table)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close() })

	// Downloader is nil here; we pass already-local files to the ingest loop.
	w := NewCacheManager(sink, 1<<60, nil, time.Hour)
	t.Cleanup(func() { w.Close() })

	// 3) Compute expected rowcount by summing counts over the chosen files.
	readDB, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatalf("open read db: %v", err)
	}
	t.Cleanup(func() { _ = readDB.Close() })

	countParquet := func(p string) int64 {
		row := readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM read_parquet(?, union_by_name=true)`, p)
		var n int64
		if err := row.Scan(&n); err != nil {
			t.Fatalf("count parquet %s: %v", p, err)
		}
		return n
	}

	var expected int64
	for _, p := range paths {
		expected += countParquet(p)
	}
	if expected == 0 {
		t.Fatalf("expected > 0 total rows from local S3, got 0")
	}

	// 4) Stream directly from "S3" (really: local files) using streamFromS3.
	// Use a simple SQL that works for both S3 and cached table.
	userSQL := `SELECT COUNT(*) FROM {table}`

	type one struct{ N int64 }
	mapper := func(r *sql.Rows) (one, error) {
		var n int64
		if err := r.Scan(&n); err != nil {
			return one{}, err
		}
		return one{N: n}, nil
	}

	// Small glob size to exercise batching/fan-out
	s3Channels, err := streamFromS3(ctx, w, paths /* use local paths as URIs */, 3, userSQL, mapper)
	if err != nil {
		t.Fatalf("streamFromS3: %v", err)
	}

	// Collect counts from each S3 batch and sum them.
	var s3Total int64
	for _, ch := range s3Channels {
		for v := range ch {
			s3Total += v.N
		}
	}
	if s3Total != expected {
		t.Fatalf("S3 stream total mismatch: got %d want %d", s3Total, expected)
	}

	// 5) Enqueue the same segments for async ingest (simulate Query’s warm-cache step).
	// IMPORTANT: don't let ingestLoop delete your source ./db files — copy them to temp.
	tempCopyDir := filepath.Join(td, "staging")
	_ = os.MkdirAll(tempCopyDir, 0o755)

	var tempPaths []string
	var ids []int64
	for _, src := range paths {
		id, derr := deriveSegmentIDFromPath(src)
		if derr != nil {
			t.Fatalf("derive id from %s: %v", src, derr)
		}
		dst := filepath.Join(tempCopyDir, filepath.Base(src))
		copyFile(t, src, dst)
		tempPaths = append(tempPaths, dst)
		ids = append(ids, id)
	}
	w.enqueueIngest(tempPaths, ids)

	// 6) Wait until the cache contains the expected number of rows (with timeout).
	deadline := time.Now().Add(8 * time.Second)
	for {
		var got int64
		if err := readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&got); err != nil {
			t.Fatalf("count from cache table: %v", err)
		}
		if got == expected {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for ingest: have %d want %d", got, expected)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// 7) Stream the same SQL from the local cache (covers the cached path).
	cachedOuts := streamCached(ctx, w, ids, userSQL, mapper)
	var cachedTotal int64
	for _, ch := range cachedOuts {
		for v := range ch {
			cachedTotal += v.N
		}
	}
	if cachedTotal != expected {
		t.Fatalf("cached stream total mismatch: got %d want %d", cachedTotal, expected)
	}
}
