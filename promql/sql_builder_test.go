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
	"testing"
	"time"
)

func TestSQLBuilder_SumRate(t *testing.T) {
	q := `sum(rate(http_request_duration_seconds_bucket[5m]))`

	// Parse PromQL → our AST
	root, err := FromPromQL(q)
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

	// Matches buildWindowed() no-group-by path:
	//   - WITH buckets, step_aggr
	//   - inner SELECT that joins buckets to step_aggr (COALESCE to 0)
	//   - WHERE EXISTS (SELECT 1 FROM step_aggr) guard
	//   - ORDER BY bucket_ts ASC
	want := `WITH grid AS (SELECT CAST(range AS BIGINT) AS step_idx FROM range(CAST(({start} - ({start} % 10000)) AS BIGINT), CAST((({end}-1) - (({end}-1) % 10000) + 10000) AS BIGINT), 10000)), ` +
		`step_aggr AS (SELECT (CAST("_cardinalhq.timestamp" AS BIGINT) - (CAST("_cardinalhq.timestamp" AS BIGINT) % 10000)) AS step_idx, SUM(rollup_sum) AS step_sum FROM {table} ` +
		`WHERE "_cardinalhq.name" = 'http_request_duration_seconds_bucket' AND "_cardinalhq.timestamp" >= {start} AND "_cardinalhq.timestamp" < {end} GROUP BY step_idx) ` +
		`SELECT bucket_ts, CAST(SUM(w_step_sum) OVER ( ORDER BY bucket_ts ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS DOUBLE) AS sum ` +
		`FROM (SELECT CAST(g.step_idx AS BIGINT) AS bucket_ts, COALESCE(sa.step_sum, 0) AS w_step_sum FROM grid g LEFT JOIN step_aggr sa ON g.step_idx = sa.step_idx) ` +
		`ORDER BY bucket_ts ASC`

	if sql != want {
		t.Fatalf("unexpected SQL.\n got: %s\nwant: %s", sql, want)
	}
}

func TestSQLBuilder_SumRate_ByJob(t *testing.T) {
	q := `sum by (job) (rate(http_request_duration_seconds_bucket[5m]))`

	root, err := FromPromQL(q)
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

	// Matches buildWindowed() with GROUP BY "job":
	//   - WITH buckets, groups, grid, step_aggr
	//   - inner SELECT that joins grid to step_aggr (COALESCE to 0)
	//   - PARTITION BY job in the window
	//   - ORDER BY bucket_ts ASC
	want := `WITH grid AS (SELECT CAST(range AS BIGINT) AS step_idx FROM range(CAST(({start} - ({start} % 10000)) AS BIGINT), CAST((({end}-1) - (({end}-1) % 10000) + 10000) AS BIGINT), 10000)), ` +
		`step_aggr AS (SELECT (CAST("_cardinalhq.timestamp" AS BIGINT) - (CAST("_cardinalhq.timestamp" AS BIGINT) % 10000)) AS step_idx, SUM(rollup_sum) AS step_sum FROM {table} ` +
		`WHERE "_cardinalhq.name" = 'http_request_duration_seconds_bucket' AND "_cardinalhq.timestamp" >= {start} AND "_cardinalhq.timestamp" < {end} GROUP BY step_idx) ` +
		`SELECT bucket_ts, CAST(SUM(w_step_sum) OVER ( ORDER BY bucket_ts ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS DOUBLE) AS sum ` +
		`FROM (SELECT CAST(g.step_idx AS BIGINT) AS bucket_ts, COALESCE(sa.step_sum, 0) AS w_step_sum FROM grid g LEFT JOIN step_aggr sa ON g.step_idx = sa.step_idx) ` +
		`ORDER BY bucket_ts ASC`

	if sql != want {
		t.Fatalf("unexpected SQL.\n got: %s\nwant: %s", sql, want)
	}
}
