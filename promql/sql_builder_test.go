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

	"github.com/cardinalhq/lakerunner/logql"
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
	defer func() { _ = rows.Close() }()

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
		// Try Stringer interface for types like DuckDB Decimal
		if s, ok := v.(fmt.Stringer); ok {
			var y float64
			_, _ = fmt.Sscan(s.String(), &y)
			return y
		}
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
// Columns: timestamp/name + chq_rollup_* + optional "pod" label for group-by tests.
func createMetricsTable(t *testing.T, db *sql.DB, withPod bool) {
	t.Helper()
	mustDropTable(db, "metrics")
	stmt := `CREATE TABLE metrics(
		"chq_timestamp" BIGINT,
		"metric_name"     TEXT,
		chq_rollup_sum    DOUBLE,
		chq_rollup_count  BIGINT,
		chq_rollup_min    DOUBLE,
		chq_rollup_max    DOUBLE` +
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
	if !strings.Contains(sql, `"metric_name" = 'm'`) || !strings.Contains(sql, "AND true") {
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

// Verify we use SUM(chq_rollup_count), not COUNT(*), per bucket.
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
	// Check ts=10000 count = 7 (SUM(chq_rollup_count)), not COUNT(*)
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
	if !strings.Contains(sql, `"metric_name" = 'm'`) {
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

func TestToWorkerSQLForTagValues(t *testing.T) {
	db := openDuckDB(t)
	createMetricsTable(t, db, true)

	// Insert test data with different tag values
	mustExec(t, db, `INSERT INTO metrics VALUES
	 (    0, 'cpu_usage', 1.0, 1, 1.0, 1.0, 'pod1'),
	 ( 1000, 'cpu_usage', 2.0, 1, 2.0, 2.0, 'pod2'),
	 ( 2000, 'cpu_usage', 3.0, 1, 3.0, 3.0, 'pod1'),
	 ( 3000, 'memory_usage', 4.0, 1, 4.0, 4.0, 'pod3'),
	 ( 4000, 'cpu_usage', 5.0, 1, 5.0, 5.0, 'pod2')`)

	// Test basic tag values query
	be := &BaseExpr{
		Metric: "cpu_usage",
	}
	sql := replaceStartEnd(replaceTableMetrics(be.ToWorkerSQLForTagValues(10*time.Second, "pod")), 0, 5000)

	rows := queryAll(t, db, sql)

	// Should return distinct pod values for cpu_usage metric
	if len(rows) != 2 {
		t.Fatalf("expected 2 distinct pod values, got %d\nsql:\n%s", len(rows), sql)
	}

	got := make(map[string]bool)
	for _, r := range rows {
		got[asString(r["tag_value"])] = true
	}

	expected := map[string]bool{"pod1": true, "pod2": true}
	for k, v := range expected {
		if !got[k] {
			t.Fatalf("missing expected pod value: %s", k)
		}
		if !v {
			t.Fatalf("unexpected pod value: %s", k)
		}
	}
}

func TestToWorkerSQLForTagValues_WithMatchers(t *testing.T) {
	db := openDuckDB(t)
	// Create a custom table with both pod and region columns
	mustDropTable(db, "metrics")
	mustExec(t, db, `CREATE TABLE metrics(
		"chq_timestamp" BIGINT,
		"metric_name"     TEXT,
		chq_rollup_sum    DOUBLE,
		chq_rollup_count  BIGINT,
		chq_rollup_min    DOUBLE,
		chq_rollup_max    DOUBLE,
		"pod"         TEXT,
		"region"      TEXT
	)`)

	// Insert test data with different tag values
	mustExec(t, db, `INSERT INTO metrics VALUES
	 (    0, 'cpu_usage', 1.0, 1, 1.0, 1.0, 'pod1', 'us-east-1'),
	 ( 1000, 'cpu_usage', 2.0, 1, 2.0, 2.0, 'pod2', 'us-west-1'),
	 ( 2000, 'cpu_usage', 3.0, 1, 3.0, 3.0, 'pod1', 'us-east-1'),
	 ( 3000, 'memory_usage', 4.0, 1, 4.0, 4.0, 'pod3', 'us-central-1'),
	 ( 4000, 'cpu_usage', 5.0, 1, 5.0, 5.0, 'pod2', 'us-west-1')`)

	// Test with matchers
	be := &BaseExpr{
		Metric: "cpu_usage",
		Matchers: []LabelMatch{
			{Label: "region", Op: MatchEq, Value: "us-east-1"},
		},
	}
	sql := replaceStartEnd(replaceTableMetrics(be.ToWorkerSQLForTagValues(10*time.Second, "pod")), 0, 5000)

	rows := queryAll(t, db, sql)

	// Should return only pod1 (the one with us-east-1 region)
	if len(rows) != 1 {
		t.Fatalf("expected 1 distinct pod value, got %d\nsql:\n%s", len(rows), sql)
	}

	if asString(rows[0]["tag_value"]) != "pod1" {
		t.Fatalf("expected pod1, got %s", asString(rows[0]["tag_value"]))
	}
}

// --- Log aggregation optimization tests ---

// createLogsTable creates a table that mimics log segment parquet structure
func createLogsTable(t *testing.T, db *sql.DB) {
	t.Helper()
	mustDropTable(db, "logs")
	stmt := `CREATE TABLE logs(
		"chq_timestamp" BIGINT,
		"log_message" TEXT,
		"log_level" TEXT,
		"resource_service_name" TEXT,
		"chq_fingerprint" TEXT
	);`
	mustExec(t, db, stmt)
}

func replaceTableLogs(sql string) string {
	return strings.ReplaceAll(sql, "{table}", `(SELECT * FROM logs) AS _t`)
}

// TestBuildSimpleLogAggSQL_CountByLogLevel verifies the optimized flat SQL path
// generates correct SQL for simple count_over_time queries.
func TestBuildSimpleLogAggSQL_CountByLogLevel(t *testing.T) {
	db := openDuckDB(t)
	createLogsTable(t, db)

	// Insert test data: 3 INFO, 2 ERROR at t=0; 1 INFO, 3 ERROR at t=10000
	mustExec(t, db, `INSERT INTO logs VALUES
	 (    0, 'msg1', 'INFO',  'svc1', 'fp1'),
	 (    0, 'msg2', 'INFO',  'svc1', 'fp2'),
	 (    0, 'msg3', 'INFO',  'svc1', 'fp3'),
	 (    0, 'msg4', 'ERROR', 'svc1', 'fp4'),
	 (    0, 'msg5', 'ERROR', 'svc1', 'fp5'),
	 (10000, 'msg6', 'INFO',  'svc1', 'fp6'),
	 (10000, 'msg7', 'ERROR', 'svc1', 'fp7'),
	 (10000, 'msg8', 'ERROR', 'svc1', 'fp8'),
	 (10000, 'msg9', 'ERROR', 'svc1', 'fp9')`)

	// Create a BaseExpr with a simple LogLeaf (no parsers/filters)
	be := &BaseExpr{
		Metric:   SynthLogCount,
		FuncName: "count_over_time",
		GroupBy:  []string{"log_level"},
		LogLeaf: &logql.LogLeaf{
			ID:       "test1",
			Matchers: []logql.LabelMatch{{Label: "resource_service_name", Op: logql.MatchEq, Value: "svc1"}},
			OutBy:    []string{"log_level"},
		},
	}

	// Verify the leaf is detected as simple
	if !be.LogLeaf.IsSimpleAggregation() {
		t.Fatal("expected LogLeaf to be detected as simple aggregation")
	}

	sql := be.ToWorkerSQL(10 * time.Second)
	if sql == "" {
		t.Fatal("empty SQL from ToWorkerSQL")
	}

	// Verify it's using the simple path (no CTEs)
	if strings.Contains(sql, "WITH") {
		t.Fatalf("expected flat SQL without CTEs for simple aggregation, got:\n%s", sql)
	}

	// Verify it does NOT contain SELECT * (column projection optimization)
	if strings.Contains(sql, "SELECT *") {
		t.Fatalf("expected column-specific SELECT for simple aggregation, got:\n%s", sql)
	}

	// Verify GROUP BY is present
	if !strings.Contains(sql, "GROUP BY") {
		t.Fatalf("expected GROUP BY in SQL, got:\n%s", sql)
	}

	sql = replaceStartEnd(replaceTableLogs(sql), 0, 20000)
	rows := queryAll(t, db, sql)

	// Should have 4 rows: INFO@0, ERROR@0, INFO@10000, ERROR@10000
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d\nsql:\n%s", len(rows), sql)
	}

	// Verify counts
	type key struct {
		level string
		ts    int64
	}
	got := map[key]int64{}
	for _, r := range rows {
		got[key{level: asString(r["log_level"]), ts: getInt64(r["bucket_ts"])}] = getInt64(r["count"])
	}

	want := map[key]int64{
		{"INFO", 0}:      3,
		{"ERROR", 0}:     2,
		{"INFO", 10000}:  1,
		{"ERROR", 10000}: 3,
	}
	for k, v := range want {
		if g, ok := got[k]; !ok {
			t.Fatalf("missing row level=%s ts=%d", k.level, k.ts)
		} else if g != v {
			t.Fatalf("level=%s ts=%d count got=%d want=%d", k.level, k.ts, g, v)
		}
	}
}

// TestBuildComplexLogAggSQL_WithParser verifies that queries with parsers
// fall back to the CTE-based SQL path.
func TestBuildComplexLogAggSQL_WithParser(t *testing.T) {
	be := &BaseExpr{
		Metric:   SynthLogCount,
		FuncName: "count_over_time",
		GroupBy:  []string{"log_level"},
		LogLeaf: &logql.LogLeaf{
			ID:       "test2",
			Matchers: []logql.LabelMatch{{Label: "resource_service_name", Op: logql.MatchEq, Value: "svc1"}},
			OutBy:    []string{"log_level"},
			Parsers:  []logql.ParserStage{{Type: "json"}}, // Has a parser
		},
	}

	// Verify the leaf is NOT detected as simple
	if be.LogLeaf.IsSimpleAggregation() {
		t.Fatal("expected LogLeaf with parser to NOT be detected as simple aggregation")
	}

	sql := be.ToWorkerSQL(10 * time.Second)
	if sql == "" {
		t.Fatal("empty SQL from ToWorkerSQL")
	}

	// Verify it's using the complex path (has CTEs)
	if !strings.Contains(sql, "WITH") {
		t.Fatalf("expected CTE-based SQL for complex aggregation, got:\n%s", sql)
	}
}

// TestBuildSimpleLogAggSQL_NoMatchers verifies SQL works with no matchers
func TestBuildSimpleLogAggSQL_NoMatchers(t *testing.T) {
	db := openDuckDB(t)
	createLogsTable(t, db)

	mustExec(t, db, `INSERT INTO logs VALUES
	 (0, 'msg1', 'INFO', 'svc1', 'fp1'),
	 (0, 'msg2', 'ERROR', 'svc1', 'fp2')`)

	be := &BaseExpr{
		Metric:   SynthLogCount,
		FuncName: "count_over_time",
		GroupBy:  []string{"log_level"},
		LogLeaf: &logql.LogLeaf{
			ID:    "test3",
			OutBy: []string{"log_level"},
			// No matchers
		},
	}

	sql := be.ToWorkerSQL(10 * time.Second)
	if sql == "" {
		t.Fatal("empty SQL from ToWorkerSQL")
	}

	// Should still use simple path
	if strings.Contains(sql, "WITH") {
		t.Fatalf("expected flat SQL for simple aggregation, got:\n%s", sql)
	}

	sql = replaceStartEnd(replaceTableLogs(sql), 0, 10000)
	rows := queryAll(t, db, sql)

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d\nsql:\n%s", len(rows), sql)
	}
}

// --- Agg file SQL tests ---

func TestCanUseAggFile(t *testing.T) {
	tests := []struct {
		name          string
		aggFields     []string
		groupBy       []string
		matcherFields []string
		expected      bool
	}{
		{
			name:          "empty aggFields returns false",
			aggFields:     []string{},
			groupBy:       []string{"log_level"},
			matcherFields: []string{},
			expected:      false,
		},
		{
			name:          "nil aggFields returns false",
			aggFields:     nil,
			groupBy:       []string{"log_level"},
			matcherFields: []string{},
			expected:      false,
		},
		{
			name:          "empty groupBy with aggFields returns true",
			aggFields:     []string{"log_level", "resource_customer_domain"},
			groupBy:       []string{},
			matcherFields: []string{},
			expected:      true,
		},
		{
			name:          "exact match returns true",
			aggFields:     []string{"log_level", "resource_customer_domain"},
			groupBy:       []string{"log_level", "resource_customer_domain"},
			matcherFields: []string{},
			expected:      true,
		},
		{
			name:          "subset match returns true",
			aggFields:     []string{"log_level", "resource_customer_domain"},
			groupBy:       []string{"log_level"},
			matcherFields: []string{},
			expected:      true,
		},
		{
			name:          "groupBy has field not in aggFields returns false",
			aggFields:     []string{"log_level", "resource_customer_domain"},
			groupBy:       []string{"log_level", "other_field"},
			matcherFields: []string{},
			expected:      false,
		},
		{
			name:          "groupBy superset of aggFields returns false",
			aggFields:     []string{"log_level"},
			groupBy:       []string{"log_level", "resource_customer_domain"},
			matcherFields: []string{},
			expected:      false,
		},
		{
			name:          "dotted field names normalized",
			aggFields:     []string{"resource.customer.domain"},
			groupBy:       []string{"resource_customer_domain"},
			matcherFields: []string{},
			expected:      true,
		},
		{
			name:          "matcher field in aggFields returns true",
			aggFields:     []string{"log_level", "resource_service_name"},
			groupBy:       []string{"log_level"},
			matcherFields: []string{"resource_service_name"},
			expected:      true,
		},
		{
			name:          "matcher field not in aggFields returns false",
			aggFields:     []string{"log_level", "resource_customer_domain"},
			groupBy:       []string{"log_level"},
			matcherFields: []string{"resource_service_name"},
			expected:      false,
		},
		{
			name:          "multiple matcher fields all in aggFields returns true",
			aggFields:     []string{"log_level", "resource_service_name", "resource_customer_domain"},
			groupBy:       []string{"log_level"},
			matcherFields: []string{"resource_service_name", "resource_customer_domain"},
			expected:      true,
		},
		{
			name:          "one matcher field missing from aggFields returns false",
			aggFields:     []string{"log_level", "resource_service_name"},
			groupBy:       []string{"log_level"},
			matcherFields: []string{"resource_service_name", "other_field"},
			expected:      false,
		},
		{
			name:          "matcher field with dotted name normalized",
			aggFields:     []string{"log_level", "resource.service.name"},
			groupBy:       []string{"log_level"},
			matcherFields: []string{"resource_service_name"},
			expected:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CanUseAggFile(tt.aggFields, tt.groupBy, tt.matcherFields)
			if result != tt.expected {
				t.Fatalf("CanUseAggFile(%v, %v, %v) = %v, want %v",
					tt.aggFields, tt.groupBy, tt.matcherFields, result, tt.expected)
			}
		})
	}
}

// createAggTable creates a table that mimics the agg_ parquet file structure.
// The stream field column uses the actual field name (e.g., "resource_service_name")
// rather than a generic "stream_id" column.
func createAggTable(t *testing.T, db *sql.DB, streamFieldName string) {
	t.Helper()
	mustDropTable(db, "agg_logs")
	if streamFieldName == "" {
		streamFieldName = "resource_customer_domain"
	}
	// Note: "count" is a reserved word but works in DuckDB when quoted
	stmt := fmt.Sprintf(`CREATE TABLE agg_logs(
		bucket_ts BIGINT,
		log_level TEXT,
		"%s" TEXT,
		frequency BIGINT,
		"count" BIGINT
	);`, streamFieldName)
	mustExec(t, db, stmt)
}

func replaceTableAgg(sql string) string {
	return strings.ReplaceAll(sql, "{table}", `(SELECT * FROM agg_logs) AS _t`)
}

func TestBuildAggFileSQL_ReaggregateToLargerStep(t *testing.T) {
	db := openDuckDB(t)
	createAggTable(t, db, "resource_customer_domain")

	// Insert 10s bucket data (AggFrequency = 10000ms)
	// Six 10s buckets: 0, 10000, 20000, 30000, 40000, 50000
	// We'll aggregate to 60s step
	mustExec(t, db, `INSERT INTO agg_logs VALUES
	 (    0, 'INFO',  'example.com', 10000, 10),
	 (10000, 'INFO',  'example.com', 10000, 20),
	 (20000, 'INFO',  'example.com', 10000, 30),
	 (30000, 'INFO',  'example.com', 10000, 40),
	 (40000, 'INFO',  'example.com', 10000, 50),
	 (50000, 'INFO',  'example.com', 10000, 60),
	 (    0, 'ERROR', 'example.com', 10000, 5),
	 (30000, 'ERROR', 'example.com', 10000, 15)`)

	be := &BaseExpr{
		GroupBy: []string{"log_level"},
	}

	sql := BuildAggFileSQL(be, 60*time.Second)
	if sql == "" {
		t.Fatal("empty SQL from BuildAggFileSQL")
	}

	// Verify no CTEs
	if strings.Contains(sql, "WITH") {
		t.Fatalf("expected flat SQL, got:\n%s", sql)
	}

	sql = replaceStartEnd(replaceTableAgg(sql), 0, 60000)
	rows := queryAll(t, db, sql)

	// Should have 2 rows: INFO and ERROR both in bucket 0 (0-59999 → 0)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d\nsql:\n%s", len(rows), sql)
	}

	// Verify counts are summed correctly
	type key struct {
		level string
		ts    int64
	}
	got := map[key]float64{}
	for _, r := range rows {
		got[key{level: asString(r["log_level"]), ts: getInt64(r["bucket_ts"])}] = getFloat(r["count"])
	}

	// All 10s buckets (0-50000) fall into the 0 bucket with 60s step
	// INFO: 10+20+30+40+50+60 = 210
	// ERROR: 5+15 = 20
	want := map[key]float64{
		{"INFO", 0}:  210,
		{"ERROR", 0}: 20,
	}
	for k, v := range want {
		if g, ok := got[k]; !ok {
			t.Fatalf("missing row level=%s ts=%d", k.level, k.ts)
		} else if g != v {
			t.Fatalf("level=%s ts=%d count got=%f want=%f", k.level, k.ts, g, v)
		}
	}
}

func TestBuildAggFileSQL_NoGroupBy(t *testing.T) {
	db := openDuckDB(t)
	createAggTable(t, db, "resource_customer_domain")

	mustExec(t, db, `INSERT INTO agg_logs VALUES
	 (    0, 'INFO',  'example.com', 10000, 10),
	 (    0, 'ERROR', 'example.com', 10000, 5),
	 (10000, 'INFO',  'example.com', 10000, 20)`)

	be := &BaseExpr{
		GroupBy: []string{}, // No GROUP BY
	}

	sql := BuildAggFileSQL(be, 10*time.Second)
	sql = replaceStartEnd(replaceTableAgg(sql), 0, 20000)
	rows := queryAll(t, db, sql)

	// Two time buckets, each summed across all levels
	// bucket 0: 10 + 5 = 15
	// bucket 10000: 20
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d\nsql:\n%s", len(rows), sql)
	}

	got := map[int64]float64{}
	for _, r := range rows {
		got[getInt64(r["bucket_ts"])] = getFloat(r["count"])
	}

	if got[0] != 15 {
		t.Fatalf("bucket 0 count got=%f want=15", got[0])
	}
	if got[10000] != 20 {
		t.Fatalf("bucket 10000 count got=%f want=20", got[10000])
	}
}

func TestBuildAggFileSQL_GroupByStreamField(t *testing.T) {
	db := openDuckDB(t)
	createAggTable(t, db, "resource_service_name")

	mustExec(t, db, `INSERT INTO agg_logs VALUES
	 (    0, 'INFO',  'service-a', 10000, 10),
	 (    0, 'INFO',  'service-b', 10000, 20),
	 (10000, 'INFO',  'service-a', 10000, 30),
	 (10000, 'INFO',  'service-b', 10000, 40)`)

	be := &BaseExpr{
		GroupBy: []string{"resource_service_name"},
	}

	sql := BuildAggFileSQL(be, 10*time.Second)
	sql = replaceStartEnd(replaceTableAgg(sql), 0, 20000)
	rows := queryAll(t, db, sql)

	// Should have 4 rows: 2 services × 2 time buckets
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d\nsql:\n%s", len(rows), sql)
	}

	type key struct {
		service string
		ts      int64
	}
	got := map[key]float64{}
	for _, r := range rows {
		got[key{service: asString(r["resource_service_name"]), ts: getInt64(r["bucket_ts"])}] = getFloat(r["count"])
	}

	want := map[key]float64{
		{"service-a", 0}:     10,
		{"service-b", 0}:     20,
		{"service-a", 10000}: 30,
		{"service-b", 10000}: 40,
	}
	for k, v := range want {
		if g, ok := got[k]; !ok {
			t.Fatalf("missing row service=%s ts=%d", k.service, k.ts)
		} else if g != v {
			t.Fatalf("service=%s ts=%d count got=%f want=%f", k.service, k.ts, g, v)
		}
	}
}

func TestBuildAggFileSQL_WithMatchers(t *testing.T) {
	db := openDuckDB(t)
	createAggTable(t, db, "resource_service_name")

	// Insert test data for multiple services
	mustExec(t, db, `INSERT INTO agg_logs VALUES
	 (    0, 'INFO',  'service-a', 10000, 10),
	 (    0, 'INFO',  'service-b', 10000, 20),
	 (    0, 'ERROR', 'service-a', 10000, 5),
	 (10000, 'INFO',  'service-a', 10000, 30),
	 (10000, 'INFO',  'service-b', 10000, 40),
	 (10000, 'ERROR', 'service-b', 10000, 15)`)

	// Query with matcher filtering to service-a only
	be := &BaseExpr{
		GroupBy: []string{"log_level"},
		LogLeaf: &logql.LogLeaf{
			ID: "test-matcher",
			Matchers: []logql.LabelMatch{
				{Label: "resource_service_name", Op: logql.MatchEq, Value: "service-a"},
			},
			OutBy: []string{"log_level"},
		},
	}

	sql := BuildAggFileSQL(be, 10*time.Second)
	if sql == "" {
		t.Fatal("empty SQL from BuildAggFileSQL")
	}

	// Verify the WHERE clause contains the matcher
	if !strings.Contains(sql, `"resource_service_name" = 'service-a'`) {
		t.Fatalf("expected matcher in WHERE clause, got:\n%s", sql)
	}

	sql = replaceStartEnd(replaceTableAgg(sql), 0, 20000)
	rows := queryAll(t, db, sql)

	// Should have 3 rows for service-a only: INFO@0, ERROR@0, INFO@10000
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d\nsql:\n%s", len(rows), sql)
	}

	type key struct {
		level string
		ts    int64
	}
	got := map[key]float64{}
	for _, r := range rows {
		got[key{level: asString(r["log_level"]), ts: getInt64(r["bucket_ts"])}] = getFloat(r["count"])
	}

	// Only service-a counts
	want := map[key]float64{
		{"INFO", 0}:     10,
		{"ERROR", 0}:    5,
		{"INFO", 10000}: 30,
	}
	for k, v := range want {
		if g, ok := got[k]; !ok {
			t.Fatalf("missing row level=%s ts=%d", k.level, k.ts)
		} else if g != v {
			t.Fatalf("level=%s ts=%d count got=%f want=%f", k.level, k.ts, g, v)
		}
	}
}

func TestBuildAggFileSQL_WithNegativeMatcher(t *testing.T) {
	db := openDuckDB(t)
	createAggTable(t, db, "resource_service_name")

	mustExec(t, db, `INSERT INTO agg_logs VALUES
	 (    0, 'INFO',  'service-a', 10000, 10),
	 (    0, 'INFO',  'service-b', 10000, 20),
	 (    0, 'INFO',  'service-c', 10000, 30)`)

	// Query with negative matcher excluding service-a
	be := &BaseExpr{
		GroupBy: []string{"resource_service_name"},
		LogLeaf: &logql.LogLeaf{
			ID: "test-negative-matcher",
			Matchers: []logql.LabelMatch{
				{Label: "resource_service_name", Op: logql.MatchNe, Value: "service-a"},
			},
			OutBy: []string{"resource_service_name"},
		},
	}

	sql := BuildAggFileSQL(be, 10*time.Second)

	// Verify the WHERE clause contains the negative matcher
	if !strings.Contains(sql, `"resource_service_name" <> 'service-a'`) {
		t.Fatalf("expected negative matcher in WHERE clause, got:\n%s", sql)
	}

	sql = replaceStartEnd(replaceTableAgg(sql), 0, 10000)
	rows := queryAll(t, db, sql)

	// Should have 2 rows: service-b and service-c
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d\nsql:\n%s", len(rows), sql)
	}

	got := map[string]float64{}
	for _, r := range rows {
		got[asString(r["resource_service_name"])] = getFloat(r["count"])
	}

	if got["service-b"] != 20 {
		t.Fatalf("service-b count got=%f want=20", got["service-b"])
	}
	if got["service-c"] != 30 {
		t.Fatalf("service-c count got=%f want=30", got["service-c"])
	}
	if _, exists := got["service-a"]; exists {
		t.Fatal("service-a should have been excluded by negative matcher")
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
