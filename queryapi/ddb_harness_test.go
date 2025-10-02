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

package queryapi

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func TestIngestExemplarLogsJSONToDuckDB_Smoke(t *testing.T) {
	ctx := context.Background()

	// 1) Load exemplar JSON
	b, err := os.ReadFile("testdata/exemplar.json")
	if err != nil {
		t.Fatalf("read exemplar: %v", err)
	}

	// 2) Open in-memory DuckDB and run ingest.
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const table = "logs_exemplar"

	n, err := IngestExemplarLogsJSONToDuckDB(ctx, db, table, string(b))
	if err != nil {
		t.Fatalf("ingest exemplar: %v", err)
	}
	if n <= 0 {
		t.Fatalf("expected >0 rows inserted, got %d", n)
	}

	// 3) Verify table exists and row count matches.
	var cnt int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quoteIdent(table)).Scan(&cnt); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if cnt < int64(n) {
		t.Fatalf("row count mismatch: table=%d, inserted=%d", cnt, n)
	}

	// 4) Ensure the anchor timestamp column exists.
	hasTS, err := duckHasColumn(ctx, db, table, "chq_timestamp")
	if err != nil {
		t.Fatalf("duckHasColumn: %v", err)
	}
	if !hasTS {
		t.Fatalf("missing required column chq_timestamp")
	}

	// 5) Re-ingest same exemplar; should not error on ALTER, and row count should increase.
	n2, err := IngestExemplarLogsJSONToDuckDB(ctx, db, table, string(b))
	if err != nil {
		t.Fatalf("re-ingest exemplar: %v", err)
	}
	if n2 <= 0 {
		t.Fatalf("expected >0 rows on re-ingest, got %d", n2)
	}
	var cnt2 int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quoteIdent(table)).Scan(&cnt2); err != nil {
		t.Fatalf("count rows (2): %v", err)
	}
	if cnt2 < cnt+int64(n2) {
		t.Fatalf("row count did not increase as expected: before=%d after=%d n2=%d", cnt, cnt2, n2)
	}

	// 6) Optional: ensure table has >1 column (ALTERs happened)
	ok, err := duckHasAnyNonTSColumn(ctx, db, table)
	if err != nil {
		t.Fatalf("duckHasAnyNonTSColumn: %v", err)
	}
	if !ok {
		t.Fatalf("expected at least one non-timestamp column after ingest")
	}
}

func TestValidateLogQLAgainstExemplar_AggregateRateCounter(t *testing.T) {
	t.Skip("TODO: update test to use new field names (chq_* prefix)")
	ctx := context.Background()

	b, err := os.ReadFile("testdata/exemplar1.json")
	if err != nil {
		t.Fatalf("read exemplar: %v", err)
	}

	// 2) Open in-memory DuckDB so we can keep full control in the test.
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const table = "logs_agg_unit"

	// 3) Aggregate LogQL:
	// - Extract 3-digit code (e.g., 401) from the log body
	// - unwrap it as numeric
	// - compute per-second rate of the window sums
	// - sum(...) to force aggregate path
	//
	// This matches the earlier style you use for unwrap+rate_counter.
	q := `sum(rate({resource_service_name="lakerunner-ingest-logs"} | regexp "(?P<code>[0-9]{3})" | unwrap code [10s]))`

	res, err := ValidateLogQLAgainstExemplar(
		ctx,
		q,
		string(b),
		WithDB(db),
		WithTable(table),
		WithAggStep(10*time.Second),
	)
	if err != nil {
		t.Fatalf("ValidateLogQLAgainstExemplar: %v", err)
	}

	if res == nil {
		t.Fatalf("nil result")
	}
	if !res.IsAggregate {
		t.Fatalf("expected aggregate path (sum(...)), got non-aggregate")
	}
	if res.InsertedRows <= 0 {
		t.Fatalf("expected >0 inserted rows, got %d", res.InsertedRows)
	}
	if len(res.Rows) == 0 {
		t.Fatalf("expected worker SQL to return rows")
	}
	if res.WorkerSQL == "" {
		t.Fatalf("empty WorkerSQL")
	}
	if strings.Contains(res.WorkerSQL, "{table}") ||
		strings.Contains(res.WorkerSQL, "{start}") ||
		strings.Contains(res.WorkerSQL, "{end}") {
		t.Fatalf("unresolved placeholders in WorkerSQL:\n%s", res.WorkerSQL)
	}
	if res.StartMillis <= 0 || res.EndMillis <= 0 || res.EndMillis < res.StartMillis {
		t.Fatalf("bad [start,end] in result: start=%d end=%d", res.StartMillis, res.EndMillis)
	}

	lcSQL := strings.ToLower(res.WorkerSQL)
	if !strings.Contains(lcSQL, "regexp_extract") || !strings.Contains(lcSQL, "__unwrap_value") {
		t.Fatalf("expected regexp+unwrap pipeline in worker SQL, got:\n%s", res.WorkerSQL)
	}

	t.Logf("worker SQL:\n%s", res.WorkerSQL)
}

func duckHasColumn(ctx context.Context, db *sql.DB, table, col string) (bool, error) {
	// Use function form; only scan 'name' to avoid bool/int scanning issues.
	q := `SELECT name FROM pragma_table_info(` + quoteStringLit(table) + `);`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false, err
		}
		if name == col {
			return true, nil
		}
	}
	return false, rows.Err()
}

func duckHasAnyNonTSColumn(ctx context.Context, db *sql.DB, table string) (bool, error) {
	q := `SELECT name FROM pragma_table_info(` + quoteStringLit(table) + `);`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()

	hasOther := false
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false, err
		}
		if name != "chq_timestamp" {
			hasOther = true
			break
		}
	}
	return hasOther, rows.Err()
}
