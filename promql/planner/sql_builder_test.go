package planner

import (
	"github.com/cardinalhq/lakerunner/promql"
	"testing"
	"time"
)

func TestSQLBuilder_SumRate(t *testing.T) {
	q := `sum(rate(http_request_duration_seconds_bucket[5m]))`

	// Parse PromQL → our AST
	root, err := promql.FromPromQL(q)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	// Plan it
	res, err := Compile(root)
	if err != nil {
		t.Fatalf("compile error: %v", err)
	}

	// Expect exactly one leaf (the underlying selector for the rate())
	if len(res.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(res.Leaves))
	}
	leaf := res.Leaves[0]

	// Build SQL for a 10s step
	step := 10 * time.Second
	sql := leaf.ToWorkerSQL(step)

	// Expected: bucketed SELECT with SUM(value) as sum, metric filter, time predicate, GROUP BY bucket expr, ORDER BY bucket_ts
	want := `WITH buckets AS (SELECT range AS bucket_ts FROM range({start}, {end}, 10000)), step_aggr AS (SELECT ("_cardinalhq.timestamp" - ("_cardinalhq.timestamp" % 10000)) AS bucket_ts, SUM(value) AS step_sum FROM {table} WHERE metric = 'http_request_duration_seconds_bucket' AND "_cardinalhq.timestamp" >= {start} AND "_cardinalhq.timestamp" < {end} GROUP BY bucket_ts) SELECT bucket_ts, SUM(w_step_sum) OVER ( ORDER BY bucket_ts ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS sum FROM (SELECT bucket_ts, COALESCE(sa.step_sum, 0) AS w_step_sum FROM buckets b LEFT JOIN step_aggr sa USING (bucket_ts))  ORDER BY bucket_ts ASC`

	if sql != want {
		t.Fatalf("unexpected SQL.\n got: %s\nwant: %s", sql, want)
	}
}

func TestSQLBuilder_SumRate_ByJob(t *testing.T) {
	q := `sum by (job) (rate(http_request_duration_seconds_bucket[5m]))`

	root, err := promql.FromPromQL(q)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	res, err := Compile(root)
	if err != nil {
		t.Fatalf("compile error: %v", err)
	}
	if len(res.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(res.Leaves))
	}
	leaf := res.Leaves[0]

	step := 10 * time.Second
	sql := leaf.ToWorkerSQL(step)

	want := `WITH buckets AS (SELECT range AS bucket_ts FROM range({start}, {end}, 10000)), ` +
		`groups AS (SELECT DISTINCT job FROM {table} WHERE metric = 'http_request_duration_seconds_bucket' AND "_cardinalhq.timestamp" >= {start} AND "_cardinalhq.timestamp" < {end}), ` +
		`grid AS (SELECT bucket_ts, job FROM buckets CROSS JOIN groups), ` +
		`step_aggr AS (SELECT ("_cardinalhq.timestamp" - ("_cardinalhq.timestamp" % 10000)) AS bucket_ts, SUM(value) AS step_sum, job FROM {table} WHERE metric = 'http_request_duration_seconds_bucket' AND "_cardinalhq.timestamp" >= {start} AND "_cardinalhq.timestamp" < {end} GROUP BY bucket_ts, job) ` +
		`SELECT bucket_ts, job, SUM(w_step_sum) OVER (PARTITION BY job ORDER BY bucket_ts ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS sum ` +
		`FROM (SELECT bucket_ts, job, COALESCE(sa.step_sum, 0) AS w_step_sum FROM grid g LEFT JOIN step_aggr sa USING (bucket_ts, job))  ` +
		`ORDER BY bucket_ts ASC`

	if sql != want {
		t.Fatalf("unexpected SQL.\n got: %s\nwant: %s", sql, want)
	}
}
