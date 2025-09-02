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

package promql

import (
	"database/sql"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func openDuckDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		slog.Error("open duckdb", "error", err)
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec failed: %v\nsql:\n%s", err, q)
	}
}

type rowmap map[string]any

func queryAll(t *testing.T, db *sql.DB, q string) []rowmap {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("query failed: %v\nsql:\n%s", err, q)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns failed: %v", err)
	}

	var out []rowmap
	for rows.Next() {
		raw := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		m := make(rowmap, len(cols))
		for i, c := range cols {
			m[c] = raw[i]
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	return out
}

func getInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case uint64:
		return int64(x)
	case []byte:
		var y int64
		_, _ = fmt.Sscan(string(x), &y)
		return y
	case string:
		var y int64
		_, _ = fmt.Sscan(x, &y)
		return y
	default:
		return 0
	}
}

func getFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int64:
		return float64(x)
	case int32:
		return float64(x)
	case int:
		return float64(x)
	case []byte:
		var y float64
		_, _ = fmt.Sscan(string(x), &y)
		return y
	case string:
		var y float64
		_, _ = fmt.Sscan(x, &y)
		return y
	default:
		return math.NaN()
	}
}

func replaceTableMetrics(sql string) string {
	// Simple passthrough subquery keeps the builder's aliases intact.
	base := `(SELECT * FROM metrics) AS _t`
	return strings.ReplaceAll(sql, "{table}", base)
}

func replaceStartEnd(sql string, start, end int64) string {
	s := strings.ReplaceAll(sql, "{start}", fmt.Sprintf("%d", start))
	s = strings.ReplaceAll(s, "{end}", fmt.Sprintf("%d", end))
	return s
}

// Sets up a minimal metrics table used by step-agg tests.
// Columns: timestamp/name + rollup_* + optional "pod" label for group-by tests.
func createMetricsTable(t *testing.T, db *sql.DB, withPod bool) {
	t.Helper()
	mustDropTable(db, "metrics")
	stmt := `CREATE TABLE metrics(
		"_cardinalhq.timestamp" BIGINT,
		"_cardinalhq.name"     TEXT,
		rollup_sum    DOUBLE,
		rollup_count  BIGINT,
		rollup_min    DOUBLE,
		rollup_max    DOUBLE` +
		func() string {
			if withPod {
				return `,
		"pod"          TEXT`
			}
			return ``
		}() + `
	);`
	mustExec(t, db, stmt)
}

func mustDropTable(db *sql.DB, name string) {
	_, _ = db.Exec(`DROP TABLE IF EXISTS ` + name)
}

// --- Tests -------------------------------------------------------------------

// Worker returns raw per-bucket sum (no windowing). Gaps remain gaps.
func TestBuildStepAgg_Sum_NoGroup_GappySeries(t *testing.T) {
	db := openDuckDB(t)
	createMetricsTable(t, db, false)

	mustExec(t, db, `INSERT INTO metrics VALUES
	 (    0, 'm', 1.0, 1, 1.0, 1.0),
	 (10000, 'm', 2.0, 1, 2.0, 2.0),
	 (40000, 'm', 4.0, 1, 4.0, 4.0)`)

	be := &BaseExpr{
		Metric:   "m",
		FuncName: "sum_over_time",
		Range:    "30s", // ignored by worker now; API applies window later
	}
	sql := be.ToWorkerSQL(10 * time.Second)
	if sql == "" {
		t.Fatal("empty SQL from ToWorkerSQL")
	}
	// Sanity: metric WHERE + sentinel present
	if !strings.Contains(sql, `"_cardinalhq.name" = 'm'`) || !strings.Contains(sql, "AND true") {
		t.Fatalf("expected metric filter + AND true, got:\n%s", sql)
	}
	sql = replaceStartEnd(replaceTableMetrics(sql), 0, 50000)

	rows := queryAll(t, db, sql)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows (no densification), got %d\nsql:\n%s", len(rows), sql)
	}

	got := make(map[int64]float64, 3)
	for _, r := range rows {
		got[getInt64(r["bucket_ts"])] = getFloat(r["sum"])
	}
	want := map[int64]float64{
		0:     1.0,
		10000: 2.0,
		40000: 4.0,
	}
	for ts, w := range want {
		if g := got[ts]; math.Abs(g-w) > 1e-9 {
			t.Fatalf("ts=%d sum got=%v want=%v", ts, g, w)
		}
	}
}

// Group-by isolation should hold; still raw per-bucket sums.
func TestBuildStepAgg_Sum_GroupBy_Isolation(t *testing.T) {
	db := openDuckDB(t)
	createMetricsTable(t, db, true)

	// a@0:2, a@10:3, a@40:5; b@10:10
	mustExec(t, db, `INSERT INTO metrics VALUES
	 (    0, 'm', 2.0, 1, 2.0, 2.0, 'a'),
	 (10000, 'm', 3.0, 1, 3.0, 3.0, 'a'),
	 (10000, 'm',10.0, 1,10.0,10.0, 'b'),
	 (40000, 'm', 5.0, 1, 5.0, 5.0, 'a')`)

	be := &BaseExpr{
		Metric:   "m",
		FuncName: "sum_over_time",
		Range:    "30s", // worker ignores windowing
		GroupBy:  []string{"pod"},
	}
	sql := replaceStartEnd(replaceTableMetrics(be.ToWorkerSQL(10*time.Second)), 0, 50000)

	rows := queryAll(t, db, sql)
	// Existing buckets only: a@0, a@10, a@40, b@10
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d\nsql:\n%s", len(rows), sql)
	}

	type key struct {
		pod string
		ts  int64
	}
	got := map[key]float64{}
	for _, r := range rows {
		got[key{pod: asString(r["pod"]), ts: getInt64(r["bucket_ts"])}] = getFloat(r["sum"])
	}

	want := map[key]float64{
		{"a", 0}:     2,
		{"a", 10000}: 3,
		{"a", 40000}: 5,
		{"b", 10000}: 10,
	}
	for k, v := range want {
		if g, ok := got[k]; !ok {
			t.Fatalf("missing row pod=%s ts=%d", k.pod, k.ts)
		} else if math.Abs(g-v) > 1e-9 {
			t.Fatalf("pod=%s ts=%d sum got=%v want=%v", k.pod, k.ts, g, v)
		}
	}
}

// Verify we use SUM(rollup_count), not COUNT(*), per bucket.
func TestBuildStepAgg_Avg_UsesSumOfRollupCount_PerBucket(t *testing.T) {
	db := openDuckDB(t)
	createMetricsTable(t, db, false)

	// Two distinct buckets:
	// ts=0:     sum=1, count=5
	// ts=10000: sum=2, count=7
	mustExec(t, db, `INSERT INTO metrics VALUES
	 (    0, 'm', 1.0, 5, 1.0, 1.0),
	 (10000, 'm', 2.0, 7, 2.0, 2.0)`)

	be := &BaseExpr{
		Metric:   "m",
		FuncName: "avg_over_time",
		Range:    "30s", // worker returns raw per-bucket sum & count
	}
	sql := replaceStartEnd(replaceTableMetrics(be.ToWorkerSQL(10*time.Second)), 0, 20000)
	rows := queryAll(t, db, sql)

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d\nsql:\n%s", len(rows), sql)
	}
	// Check ts=10000 count = 7 (SUM(rollup_count)), not COUNT(*)
	var found bool
	for _, r := range rows {
		if getInt64(r["bucket_ts"]) == 10000 {
			found = true
			if c := getFloat(r["count"]); math.Abs(c-7.0) > 1e-9 {
				t.Fatalf("count@10000 got=%v want=7", c)
			}
			if s := getFloat(r["sum"]); math.Abs(s-2.0) > 1e-9 {
				t.Fatalf("sum@10000 got=%v want=2", s)
			}
		}
	}
	if !found {
		t.Fatalf("no row for ts=10000\nsql:\n%s", sql)
	}
}

// Max is per-bucket now (no window carry).
func TestBuildStepAgg_Max_NoGroup(t *testing.T) {
	db := openDuckDB(t)
	createMetricsTable(t, db, false)

	mustExec(t, db, `INSERT INTO metrics VALUES
	 (    0, 'm', 0.0, 1, 1.0,  1.0),
	 (10000, 'm', 0.0, 1, 9.0,  9.0),
	 (40000, 'm', 0.0, 1, 3.0,  3.0)`)

	be := &BaseExpr{
		Metric:   "m",
		FuncName: "max_over_time",
		Range:    "30s", // ignored by worker; API windows later
	}
	sql := replaceStartEnd(replaceTableMetrics(be.ToWorkerSQL(10*time.Second)), 0, 50000)
	rows := queryAll(t, db, sql)

	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d\nsql:\n%s", len(rows), sql)
	}

	got := map[int64]float64{}
	for _, r := range rows {
		got[getInt64(r["bucket_ts"])] = getFloat(r["max"])
	}
	// Per-bucket maxima
	if math.Abs(got[0]-1.0) > 1e-9 || math.Abs(got[10000]-9.0) > 1e-9 || math.Abs(got[40000]-3.0) > 1e-9 {
		t.Fatalf("max series wrong: got=%v", got)
	}
}

// Metric filter must exclude other series; still raw per-bucket sums.
func TestBuildStepAgg_RespectsMetricFilter_IgnoresOtherMetrics(t *testing.T) {
	db := openDuckDB(t)
	createMetricsTable(t, db, false)

	mustExec(t, db, `INSERT INTO metrics VALUES
	 (0, 'm',     2.0, 1, 2.0, 2.0),
	 (0, 'other', 9.9, 9, 9.9, 9.9),
	 (10000, 'm', 3.0, 1, 3.0, 3.0)`)

	be := &BaseExpr{
		Metric:   "m",
		FuncName: "sum_over_time",
		Range:    "20s",
	}
	sql := replaceStartEnd(replaceTableMetrics(be.ToWorkerSQL(10*time.Second)), 0, 20000)
	if !strings.Contains(sql, `"_cardinalhq.name" = 'm'`) {
		t.Fatalf("missing metric WHERE in SQL:\n%s", sql)
	}
	rows := queryAll(t, db, sql)

	// Should have ts=0 and 10000 only, sums 2 and 3 (no window accumulation).
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d\nsql:\n%s", len(rows), sql)
	}
	got := make(map[int64]float64, 2)
	for _, r := range rows {
		got[getInt64(r["bucket_ts"])] = getFloat(r["sum"])
	}
	if math.Abs(got[0]-2.0) > 1e-9 || math.Abs(got[10000]-3.0) > 1e-9 {
		t.Fatalf("wrong sums: got=%v", got)
	}
}

// --- small helpers ---

func asString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}
