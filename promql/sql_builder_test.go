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
	"strings"
	"testing"
	"time"
)

func TestBuildRawSimple_NoGroup(t *testing.T) {
	be := &BaseExpr{
		ID:     "leaf-raw",
		Metric: "http_requests_total",
		// no GroupBy, no Range
	}
	sql := buildRawSimple(be, []proj{
		{"MIN(rollup_min)", "min"},
		{"MAX(rollup_max)", "max"},
		{"SUM(rollup_sum)", "sum"},
		{"COUNT(rollup_count)", "count"},
	}, 10*time.Second)

	mustContain(t, sql, `SELECT ("_cardinalhq.timestamp" - ("_cardinalhq.timestamp" % 10000)) AS bucket_ts`)
	mustContain(t, sql, `AND "_cardinalhq.timestamp" >= {start} AND "_cardinalhq.timestamp" < {end}`)
	mustContain(t, sql, `"_cardinalhq.name" = 'http_requests_total'`)
	mustContain(t, sql, `GROUP BY bucket_ts`)
	mustContain(t, sql, `ORDER BY bucket_ts ASC`)
}

func TestBuildStepOnly_NoGroup(t *testing.T) {
	be := &BaseExpr{
		ID:     "leaf-step",
		Metric: "bytes_total",
	}
	sql := buildStepOnly(be, []proj{
		{"SUM(rollup_sum)", "sum"},
		{"COUNT(rollup_count)", "count"},
	}, 10*time.Second)

	// CTE with aligned range
	mustContain(t, sql, `WITH buckets AS (SELECT range AS bucket_ts FROM range(({start} - ({start} % 10000)), (({end} - 1) - (({end} - 1) % 10000) + 10000), 10000))`)
	// step_aggr grouping by bucket_ts and time filter present
	mustContain(t, sql, `step_aggr AS (SELECT ("_cardinalhq.timestamp" - ("_cardinalhq.timestamp" % 10000)) AS bucket_ts`)
	mustContain(t, sql, `FROM {table} WHERE "_cardinalhq.name" = 'bytes_total' AND "_cardinalhq.timestamp" >= {start} AND "_cardinalhq.timestamp" < {end} GROUP BY bucket_ts`)
	// LEFT JOIN and COALESCE zeros
	mustContain(t, sql, `LEFT JOIN step_aggr sa USING (bucket_ts)`)
	mustContain(t, sql, `COALESCE(sa.step_sum, 0) AS sum`)
	mustContain(t, sql, `COALESCE(sa.step_count, 0) AS count`)
	mustContain(t, sql, `ORDER BY bucket_ts ASC`)
}

func TestBuildWindowed_SumCount_NoRange(t *testing.T) {
	be := &BaseExpr{
		ID:     "leaf-win",
		Metric: "events_total",
		// Range may be empty; k-1 will be 0; we still want densification semantics
	}
	sql := buildWindowed(be, need{sum: true, count: true}, 10*time.Second)

	// grid CTE with aligned bounds (BIGINT) and step 10s
	mustContain(t, sql, `grid AS (SELECT CAST(range AS BIGINT) AS step_idx FROM range(CAST(({start} - ({start} % 10000)) AS BIGINT), CAST((({end}-1) - (({end}-1) % 10000) + 10000) AS BIGINT), 10000))`)
	// step_aggr buckets by same modulo and time predicate WITH placeholders
	mustContain(t, sql, `step_aggr AS (SELECT (CAST("_cardinalhq.timestamp" AS BIGINT) - (CAST("_cardinalhq.timestamp" AS BIGINT) % 10000)) AS step_idx`)
	mustContain(t, sql, `FROM {table} WHERE "_cardinalhq.name" = 'events_total' AND "_cardinalhq.timestamp" >= {start} AND "_cardinalhq.timestamp" < {end} GROUP BY step_idx`)
	// base join on explicit ON and COALESCE to 0
	mustContain(t, sql, `FROM grid g LEFT JOIN step_aggr sa ON g.step_idx = sa.step_idx`)
	mustContain(t, sql, `COALESCE(sa.step_sum, 0) AS w_step_sum`)
	mustContain(t, sql, `COALESCE(sa.step_count, 0) AS w_step_count`)
	// window SUM(...) OVER ( ORDER BY bucket_ts ROWS BETWEEN 0 PRECEDING AND CURRENT ROW)
	mustContain(t, sql, `CAST(SUM(w_step_sum) OVER ( ORDER BY bucket_ts ROWS BETWEEN 0 PRECEDING AND CURRENT ROW) AS DOUBLE) AS sum`)
	mustContain(t, sql, `CAST(SUM(w_step_count) OVER ( ORDER BY bucket_ts ROWS BETWEEN 0 PRECEDING AND CURRENT ROW) AS DOUBLE) AS count`)
	mustContain(t, sql, `ORDER BY bucket_ts ASC`)
}

func TestToWorkerSQL_Raw_NoRange_UsesRawSimple(t *testing.T) {
	be := &BaseExpr{
		ID:       "leaf-dispatch",
		Metric:   "latency_ms",
		FuncName: "", // raw/instant
		Range:    "", // no range -> RawSimple
	}
	sql := be.ToWorkerSQL(15 * time.Second)

	// Should look like the raw simple query (no CTE grid / step_aggr)
	mustContain(t, sql, `SELECT ("_cardinalhq.timestamp" - ("_cardinalhq.timestamp" % 15000)) AS bucket_ts`)
	mustContain(t, sql, `GROUP BY bucket_ts`)
	mustNotContain(t, sql, `WITH grid AS`)
	mustNotContain(t, sql, `step_aggr AS`)
}

func TestBuildDDS_Basic(t *testing.T) {
	be := &BaseExpr{
		ID:       "leaf-dds",
		Metric:   "p95_latency",
		FuncName: "", // function ignored for DDS path, WantDDS drives it
		GroupBy:  []string{"service", "region"},
		WantDDS:  true,
	}
	sql := buildDDS(be, 5*time.Second)

	// bucket projection + group columns + sketch
	mustContain(t, sql, `SELECT ("_cardinalhq.timestamp" - ("_cardinalhq.timestamp" % 5000)) AS bucket_ts, service, region, sketch`)
	// aligned, end-exclusive bounds in WHERE
	mustContain(t, sql, `"_cardinalhq.timestamp"`)
	// allow small formatting variations: check the key structure instead
	mustContain(t, sql, `"_cardinalhq.timestamp"`)
	mustContain(t, sql, `"_cardinalhq.timestamp" >= ({start} - ({start} % 5000))`)
	mustContain(t, sql, `"_cardinalhq.timestamp" < (({end} - 1) - (({end} - 1) % 5000) + 5000)`)
	mustContain(t, sql, `ORDER BY bucket_ts ASC`)
}

func TestToWorkerSQL_DDS_Dispatch(t *testing.T) {
	be := &BaseExpr{
		ID:      "leaf-dds-dispatch",
		Metric:  "latency_histogram",
		WantDDS: true,
	}
	sql := be.ToWorkerSQL(10 * time.Second)
	// Should be the DDS path (no GROUP BY clause, no SUM/COUNT/COALESCE)
	mustContain(t, sql, `SELECT ("_cardinalhq.timestamp" - ("_cardinalhq.timestamp" % 10000)) AS bucket_ts`)
	mustContain(t, sql, `sketch`)
	mustNotContain(t, sql, `SUM(rollup_sum)`)
	mustNotContain(t, sql, `COUNT(rollup_count)`)
	mustNotContain(t, sql, `COALESCE(`)
	mustNotContain(t, sql, `WITH grid AS`)
}

func mustContain(t *testing.T, s, sub string) {
	t.Helper()
	if !strings.Contains(s, sub) {
		t.Fatalf("expected SQL to contain:\n%s\n\nSQL was:\n%s", sub, s)
	}
}

func mustNotContain(t *testing.T, s, sub string) {
	t.Helper()
	if strings.Contains(s, sub) {
		t.Fatalf("expected SQL NOT to contain:\n%s\n\nSQL was:\n%s", sub, s)
	}
}
