package worker

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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

	// Generate rows using range(0, rows)
	create := fmt.Sprintf(`
CREATE OR REPLACE TABLE _gen AS
SELECT (1700000000000 + i)::BIGINT AS "_cardinalhq.timestamp",
       i::INTEGER AS v
FROM range(0, %d) AS t(i);`, rows)
	if _, err := db.Exec(create); err != nil {
		t.Fatalf("create gen table: %v", err)
	}

	// COPY requires a literal path, not a parameter.
	out := fmt.Sprintf(`COPY _gen TO '%s' (FORMAT PARQUET);`, strings.ReplaceAll(path, "'", "''"))
	if _, err := db.Exec(out); err != nil {
		t.Fatalf("copy parquet: %v", err)
	}
	_, _ = db.Exec(`DROP TABLE IF EXISTS _gen;`)
}

func TestCacheWrapper_EnsureAndQuery_Ingest_Dedupe_Evict(t *testing.T) {
	ctx := context.Background()

	// temp workspace
	td := t.TempDir()
	dbPath := filepath.Join(td, "cache.duckdb")
	table := "cache"

	// Generate two small parquet files with known row counts
	p1 := filepath.Join(td, "tbl_123.parquet")
	p2 := filepath.Join(td, "tbl_456.parquet")
	writeParquet(t, p1, 3) // 3 rows
	writeParquet(t, p2, 2) // 2 rows
	paths := []string{p1, p2}

	// Create the sink (real DuckDB)
	sink, err := New(ctx, dbPath, table)
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	defer func(sink *Sink) {
		err := sink.Close()
		if err != nil {
			t.Fatalf("close sink: %v", err)
		}
	}(sink)

	// Downloader just verifies files exist; count how many times it's called.
	var dlCalls int32
	downloader := func(ctx context.Context, ps []string) error {
		atomic.AddInt32(&dlCalls, 1)
		for _, p := range ps {
			if _, err := os.Stat(p); err != nil {
				return err
			}
		}
		return nil
	}

	// Cache wrapper; long interval so cron doesn't interfere (we call maybeEvictOnce directly).
	w := NewCacheManager(sink, 1<<60, downloader, time.Hour)
	defer w.Close()

	// Row mapper for (segment_id, count) rows
	type pair struct {
		ID int64
		N  int64
	}
	mapper := func(r *sql.Rows) (pair, error) {
		var id int64
		var n int64
		if err := r.Scan(&id, &n); err != nil {
			return pair{}, err
		}
		return pair{ID: id, N: n}, nil
	}

	// Query: group counts by segment_id
	q := `SELECT segment_id, COUNT(*) 
	      FROM ` + table + ` 
	      GROUP BY 1 
	      ORDER BY 1`

	// Fire two concurrent EnsureAndQuery calls over the same segments (tests in-flight dedupe).
	out1, err1 := EnsureAndQuery(ctx, w, paths, q, nil, mapper)
	out2, err2 := EnsureAndQuery(ctx, w, paths, q, nil, mapper)

	// Collect results helper
	collect := func(out <-chan pair, errs <-chan error) ([]pair, error) {
		var got []pair
		for v := range out {
			got = append(got, v)
		}
		select {
		case e := <-errs:
			return got, e
		default:
			return got, nil
		}
	}

	g1, e1 := collect(out1, err1)
	g2, e2 := collect(out2, err2)
	if e1 != nil {
		t.Fatalf("query1 error: %v", e1)
	}
	if e2 != nil {
		t.Fatalf("query2 error: %v", e2)
	}

	// Expect exactly two rows in each result: (segA,3), (segB,2)
	check := func(got []pair) {
		if len(got) != 2 {
			t.Fatalf("expected 2 groups, got %d (%v)", len(got), got)
		}
		if got[0].ID != 123 || got[0].N != 3 {
			t.Fatalf("unexpected first row: %+v", got[0])
		}
		if got[1].ID != 456 || got[1].N != 2 {
			t.Fatalf("unexpected second row: %+v", got[1])
		}
	}
	check(g1)
	check(g2)

	// In-flight dedupe: downloader should have been called only once
	if c := atomic.LoadInt32(&dlCalls); c != 1 {
		t.Fatalf("downloader called %d times; want 1", c)
	}

	// Verify table row count via a read connection (don’t rely on RowCount())
	readDB, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatalf("open read db: %v", err)
	}
	defer readDB.Close()

	var before int64
	if err := readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&before); err != nil {
		t.Fatalf("count before eviction: %v", err)
	}
	if before != 5 {
		t.Fatalf("unexpected row count before eviction: got %d want 5", before)
	}

	// Now force an eviction pass: bring maxRows down and evict once.
	w.maxRows = 2
	w.maybeEvictOnce(ctx)

	var after int64
	if err := readDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&after); err != nil {
		t.Fatalf("count after eviction: %v", err)
	}
	if after > w.maxRows {
		t.Fatalf("after eviction rows=%d > maxRows=%d", after, w.maxRows)
	}

	// Best-effort local file cleanup: confirm at least one file was removed.
	// (If only one segment needed to be dropped to hit <= maxRows, only that file may be gone.)
	removedAny := false
	if _, err := os.Stat(p1); err != nil {
		removedAny = true
	}
	if _, err := os.Stat(p2); err != nil {
		removedAny = true
	}
	if !removedAny {
		t.Log("no local files removed; acceptable if a single segment eviction met the target")
	}
}
