//go:build !race

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

// Sets up a minimal metrics table used by buildWindowed tests.
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

func TestBuildWindowed_SumOverTime_NoGroup_GappySeries(t *testing.T) {
	db := openDuckDB(t)
	createMetricsTable(t, db, false)

	mustExec(t, db, `INSERT INTO metrics VALUES
	 (    0, 'm', 1.0, 1, 1.0, 1.0),
	 (10000, 'm', 2.0, 1, 2.0, 2.0),
	 (40000, 'm', 4.0, 1, 4.0, 4.0)`)

	be := &BaseExpr{
		Metric:   "m",
		FuncName: "sum_over_time",
		Range:    "30s",
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

	type pair struct {
		ts  int64
		sum float64
	}
	var got []pair
	for _, r := range rows {
		got = append(got, pair{
			ts:  getInt64(r["bucket_ts"]),
			sum: getFloat(r["sum"]),
		})
	}

	// Expected (RANGE 30s inclusive of boundary):
	// ts=0     -> [0..0]                = 1
	// ts=10000 -> [ -..10000]           = 1+2 = 3
	// ts=40000 -> [10000..40000]        = 2+4 = 6   (no 20s/30s rows; still correct)
	want := map[int64]float64{
		0:     1.0,
		10000: 3.0,
		40000: 6.0,
	}
	for _, p := range got {
		if want[p.ts] == 0 && p.ts != 0 {
			t.Fatalf("unexpected ts=%d present", p.ts)
		}
		if math.Abs(want[p.ts]-p.sum) > 1e-9 {
			t.Fatalf("ts=%d sum got=%v want=%v", p.ts, p.sum, want[p.ts])
		}
	}
}

func TestBuildWindowed_SumOverTime_GroupBy_PartitionIsolation(t *testing.T) {
	db := openDuckDB(t)
	createMetricsTable(t, db, true)

	// Interleave groups at shared steps; ensure windows don't bleed.
	// a@0:2, a@10:3, a@40:5; b@10:10
	mustExec(t, db, `INSERT INTO metrics VALUES
	 (    0, 'm', 2.0, 1, 2.0, 2.0, 'a'),
	 (10000, 'm', 3.0, 1, 3.0, 3.0, 'a'),
	 (10000, 'm',10.0, 1,10.0,10.0, 'b'),
	 (40000, 'm', 5.0, 1, 5.0, 5.0, 'a')`)

	be := &BaseExpr{
		Metric:   "m",
		FuncName: "sum_over_time",
		Range:    "30s",
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

	// For pod a:
	//  0  -> 2
	//  10 -> 2+3 = 5
	//  40 -> 3+5 = 8
	// For pod b:
	//  10 -> 10
	want := map[key]float64{
		{"a", 0}:     2,
		{"a", 10000}: 5,
		{"a", 40000}: 8,
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

func TestBuildWindowed_AvgOverTime_UsesSumOfRollupCount(t *testing.T) {
	db := openDuckDB(t)
	createMetricsTable(t, db, false)

	// Two buckets in range at ts=10s:
	// sum: 1 + 2 = 3
	// cnt: 5 + 7 = 12  (verifies SUM(rollup_count), not COUNT(*))
	mustExec(t, db, `INSERT INTO metrics VALUES
	 (    0, 'm', 1.0, 5, 1.0, 1.0),
	 (10000, 'm', 2.0, 7, 2.0, 2.0)`)

	be := &BaseExpr{
		Metric:   "m",
		FuncName: "avg_over_time",
		Range:    "30s",
	}
	sql := replaceStartEnd(replaceTableMetrics(be.ToWorkerSQL(10*time.Second)), 0, 20000)
	rows := queryAll(t, db, sql)

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d\nsql:\n%s", len(rows), sql)
	}
	// Check ts=10000 count = 12
	var found bool
	for _, r := range rows {
		if getInt64(r["bucket_ts"]) == 10000 {
			found = true
			if c := getFloat(r["count"]); math.Abs(c-12.0) > 1e-9 {
				t.Fatalf("count@10000 got=%v want=12", c)
			}
			// (Optional) avg = sum/count = 3/12; that divide happens upstream of worker.
		}
	}
	if !found {
		t.Fatalf("no row for ts=10000\nsql:\n%s", sql)
	}
}

func TestBuildWindowed_MaxOverTime_NoGroup(t *testing.T) {
	db := openDuckDB(t)
	createMetricsTable(t, db, false)

	// Max over window should respect RANGE across buckets.
	mustExec(t, db, `INSERT INTO metrics VALUES
	 (    0, 'm', 0.0, 1, 1.0,  1.0),
	 (10000, 'm', 0.0, 1, 9.0,  9.0),
	 (40000, 'm', 0.0, 1, 3.0,  3.0)`)

	be := &BaseExpr{
		Metric:   "m",
		FuncName: "max_over_time",
		Range:    "30s",
	}
	sql := replaceStartEnd(replaceTableMetrics(be.ToWorkerSQL(10*time.Second)), 0, 50000)
	rows := queryAll(t, db, sql)

	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d\nsql:\n%s", len(rows), sql)
	}

	type pair struct {
		ts  int64
		max float64
	}
	got := map[int64]float64{}
	for _, r := range rows {
		got[getInt64(r["bucket_ts"])] = getFloat(r["max"])
	}

	// ts=0     -> max(1)      = 1
	// ts=10000 -> max(1,9)    = 9
	// ts=40000 -> max(9,3)    = 9  (10000 still within 30s)
	if math.Abs(got[0]-1.0) > 1e-9 || math.Abs(got[10000]-9.0) > 1e-9 || math.Abs(got[40000]-9.0) > 1e-9 {
		t.Fatalf("max series wrong: got=%v", got)
	}
}

func TestBuildWindowed_RespectsMetricFilter_IgnoresOtherMetrics(t *testing.T) {
	db := openDuckDB(t)
	createMetricsTable(t, db, false)

	// Mix 'm' and 'other'; query should only see 'm'.
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

	// Should have ts=0 and 10000 only, sums 2 and 5.
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d\nsql:\n%s", len(rows), sql)
	}
	type pair struct {
		ts  int64
		sum float64
	}
	got := make(map[int64]float64, 2)
	for _, r := range rows {
		got[getInt64(r["bucket_ts"])] = getFloat(r["sum"])
	}
	if math.Abs(got[0]-2.0) > 1e-9 || math.Abs(got[10000]-5.0) > 1e-9 {
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
