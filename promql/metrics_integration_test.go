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
	"math"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func replaceWorkerPlaceholders(sql string, start, end int64) string {
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = strings.ReplaceAll(sql, "{start}", fmt.Sprintf("%d", start))
	sql = strings.ReplaceAll(sql, "{end}", fmt.Sprintf("%d", end))
	return sql
}

func i64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case []byte:
		var n int64
		_, _ = fmt.Sscan(string(x), &n)
		return n
	default:
		var n int64
		_, _ = fmt.Sscan(fmt.Sprintf("%v", x), &n)
		return n
	}
}

func s(v any) string {
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

// Make a minimal “metrics” table that the prom SQL builder expects.
// We intentionally call it `logs` so the same placeholder helper works.
func createTempTable(t *testing.T, db *sql.DB) {
	mustExec(t, db, `
CREATE TABLE logs(
  "chq_timestamp"   BIGINT,
  "chq_name"        TEXT,
  "resource_service_name"   TEXT,
  "resource_k8s_pod_name"   TEXT,
  instance                  TEXT,
  chq_rollup_sum                DOUBLE,
  chq_rollup_count              BIGINT,
  chq_rollup_min                DOUBLE,
  chq_rollup_max                DOUBLE
);`)
}

func TestProm_CountPods_Global_TwoWorkers_Eval(t *testing.T) {
	// --- Worker 1: only api-7f ------------------------------------------------
	db1 := openDuckDB(t)
	createTempTable(t, db1)
	mustExec(t, db1, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket 0
(10*1000,  'req_total', 'api-gateway', 'api-7f', 'a', 1, 10, 1, 1),
(20*1000,  'req_total', 'api-gateway', 'api-7f', 'b', 1, 10, 1, 1),
-- bucket 1
(70*1000,  'req_total', 'api-gateway', 'api-7f', 'a', 1, 10, 1, 1),
(80*1000,  'req_total', 'api-gateway', 'api-7f', 'b', 1, 10, 1, 1);
`)

	// --- Worker 2: only api-9x ------------------------------------------------
	db2 := openDuckDB(t)
	createTempTable(t, db2)
	mustExec(t, db2, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket 0
(40*1000,  'req_total', 'api-gateway', 'api-9x', 'a', 1, 10, 1, 1),
-- bucket 1
(100*1000, 'req_total', 'api-gateway', 'api-9x', 'a', 1, 10, 1, 1);
`)

	// Expression: count( count by (pod) ( rate({__name__="req_total"}[5m]) ) )
	// Plan will have a single leaf for the inner rate(...) and the query-api does
	// the outer count at Eval-time.
	q := `count( count by ("resource_k8s_pod_name") ( rate({__name__="req_total"}[5m]) ) )`

	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatalf("parse promql: %v", err)
	}
	plan, err := Compile(expr)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(plan.Leaves))
	}
	leaf := plan.Leaves[0]

	// Build the worker SQL once.
	step := time.Minute
	workerSQL := replaceWorkerPlaceholders(leaf.ToWorkerSQL(step), 0, 120*1000)

	// Run the leaf SQL on both workers.
	rows1 := queryAll(t, db1, workerSQL)
	rows2 := queryAll(t, db2, workerSQL)

	// Group rows per bucket across workers → []SketchInput per bucket.
	type bucket = int64
	perBucket := map[bucket][]SketchInput{}

	addRows := func(rows []rowmap) {
		for _, r := range rows {
			b := i64(r["bucket_ts"])
			pod := s(r[`resource.k8s.pod.name`])
			if pod == "" {
				pod = s(r[`resource_k8s_pod_name`])
			}
			var cnt float64
			if v, ok := r["count"]; ok && v != nil {
				cnt = float64(i64(v))
			}
			if cnt == 0 {
				continue
			}
			si := SketchInput{
				ExprID:         leaf.ID,
				OrganizationID: "org-test",
				Timestamp:      b,
				Frequency:      int64(step / time.Second),
				SketchTags: SketchTags{
					Tags: map[string]any{
						"resource_k8s_pod_name": pod,
					},
					SketchType: SketchMAP,
					Agg: map[string]float64{
						"count": cnt,
					},
				},
			}
			perBucket[b] = append(perBucket[b], si)
		}
	}
	addRows(rows1)
	addRows(rows2)

	// Sort buckets for deterministic evaluation order.
	var buckets []int64
	for b := range perBucket {
		buckets = append(buckets, b)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })

	// For each bucket, feed a SketchGroup into plan.Root.Eval and validate results.
	type key struct{ bucket int64 }
	got := map[key]int64{}

	for _, b := range buckets {
		sg := SketchGroup{
			Timestamp: b,
			Group: map[string][]SketchInput{
				leaf.ID: perBucket[b], // all worker inputs for this leaf+bucket
			},
		}

		out := plan.Root.Eval(sg, step)

		// For a global count(...) (no grouping retained), we expect exactly one result at this bucket.
		if len(out) != 1 {
			t.Fatalf("expected exactly 1 EvalResult at bucket %d, got %d; out=%v", b, len(out), out)
		}
		for _, er := range out {
			if er.Timestamp != b {
				t.Fatalf("EvalResult timestamp mismatch: got %d want %d", er.Timestamp, b)
			}
			// Extract numeric value; tolerate several shapes (float64, stringified, etc.).
			val := er.Value.Num
			got[key{b}] = int64(val + 0.5) // round to nearest int
		}
	}

	// Expect 2 pods in each bucket.
	exp := map[key]int64{
		{0}:     2,
		{60000}: 2,
	}
	for k, want := range exp {
		if got[k] != want {
			t.Fatalf("unexpected global pod count for %v: got=%v want=%v\nworkerSQL=\n%s\nrows1=%v\nrows2=%v",
				k, got[k], want, workerSQL, rows1, rows2)
		}
	}
}

func TestProm_CountPods_Global_TwoWorkers_SamePod_Dedup_Eval(t *testing.T) {
	// --- Worker 1: api-7f -----------------------------------------------------
	db1 := openDuckDB(t)
	createTempTable(t, db1)
	mustExec(t, db1, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket 0
(10*1000,  'req_total', 'api-gateway', 'api-7f', 'a', 1, 10, 1, 1),
(20*1000,  'req_total', 'api-gateway', 'api-7f', 'b', 1, 10, 1, 1),
-- bucket 1
(70*1000,  'req_total', 'api-gateway', 'api-7f', 'a', 1, 10, 1, 1);
`)

	// --- Worker 2: the SAME pod 'api-7f' in the SAME buckets ------------------
	db2 := openDuckDB(t)
	createTempTable(t, db2)
	mustExec(t, db2, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket 0
(15*1000,  'req_total', 'api-gateway', 'api-7f', 'c', 1, 10, 1, 1),
-- bucket 1
(80*1000,  'req_total', 'api-gateway', 'api-7f', 'd', 1, 10, 1, 1);
`)

	// PromQL: global count of pods (no "by")
	// Note: inner "count by (pod)(rate(...))" collapses to one row per (bucket,pod).
	// With duplicates across workers, naive global count (row-count) would be 2,
	// but semantically we want DISTINCT pods → 1.
	q := `count( count by ("resource_k8s_pod_name") ( rate({__name__="req_total"}[5m]) ) )`

	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatalf("parse promql: %v", err)
	}
	plan, err := Compile(expr)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(plan.Leaves))
	}
	leaf := plan.Leaves[0]

	step := time.Minute
	workerSQL := replaceWorkerPlaceholders(leaf.ToWorkerSQL(step), 0, 120*1000)

	// Run leaf on both workers
	rows1 := queryAll(t, db1, workerSQL)
	rows2 := queryAll(t, db2, workerSQL)

	// Build per-bucket inputs; keep only non-zero COUNT rows (skip densified empties).
	type bucket = int64
	perBucket := map[bucket][]SketchInput{}

	addRows := func(rows []rowmap) {
		for _, r := range rows {
			b := i64(r["bucket_ts"])
			pod := s(r[`resource_k8s_pod_name`])
			cnt := int64(0)
			if v, ok := r["count"]; ok && v != nil {
				cnt = i64(v)
			}
			if cnt == 0 {
				continue
			}
			perBucket[b] = append(perBucket[b], SketchInput{
				ExprID:         leaf.ID,
				OrganizationID: "org-test",
				Timestamp:      b,
				Frequency:      int64(step / time.Second),
				SketchTags: SketchTags{
					Tags:       map[string]any{"resource_k8s_pod_name": pod},
					SketchType: SketchMAP,
					Agg:        map[string]float64{"count": float64(cnt)},
				},
			})
		}
	}
	addRows(rows1)
	addRows(rows2)

	// Deterministic order
	var buckets []int64
	for b := range perBucket {
		buckets = append(buckets, b)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })

	// Evaluate per bucket, expect 1 (distinct pod) each
	type key struct{ bucket int64 }
	got := map[key]int64{}
	for _, b := range buckets {
		out := plan.Root.Eval(SketchGroup{
			Timestamp: b,
			Group:     map[string][]SketchInput{leaf.ID: perBucket[b]},
		}, step)

		if len(out) != 1 {
			t.Fatalf("expected exactly 1 EvalResult at bucket %d, got %d; out=%v", b, len(out), out)
		}
		for _, er := range out {
			if er.Timestamp != b {
				t.Fatalf("EvalResult timestamp mismatch: got %d want %d", er.Timestamp, b)
			}
			got[key{b}] = int64(er.Value.Num + 0.5)
		}
	}

	// Expect 1 pod per bucket (dedup across workers)
	exp := map[key]int64{
		{0}:     1,
		{60000}: 1,
	}
	for k, want := range exp {
		if got[k] != want {
			t.Fatalf("unexpected global pod count for %v: got=%v want=%v\nworkerSQL=\n%s\nrows1=%v\nrows2=%v",
				k, got[k], want, workerSQL, rows1, rows2)
		}
	}
}

func TestProm_CountByPod_Rate_FastPath_TwoWorkers_Eval(t *testing.T) {
	// --- Worker 1: api-7f only ------------------------------------------------
	db1 := openDuckDB(t)
	createTempTable(t, db1)
	mustExec(t, db1, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
(10*1000,  'req_total', 'api-gateway', 'api-7f', 'a', 1, 10, 1, 1),
(20*1000,  'req_total', 'api-gateway', 'api-7f', 'b', 1, 10, 1, 1),
(70*1000,  'req_total', 'api-gateway', 'api-7f', 'a', 1, 10, 1, 1),
(80*1000,  'req_total', 'api-gateway', 'api-7f', 'b', 1, 10, 1, 1),
(90*1000,  'req_total', 'api-gateway', 'api-7f', 'c', 1, 10, 1, 1);
`)

	// --- Worker 2: api-9x only ------------------------------------------------
	db2 := openDuckDB(t)
	createTempTable(t, db2)
	mustExec(t, db2, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
(40*1000,  'req_total', 'api-gateway', 'api-9x', 'a', 1, 10, 1, 1),
(100*1000, 'req_total', 'api-gateway', 'api-9x', 'a', 1, 10, 1, 1);
`)

	// We deliberately use SUM of inner COUNT so the parent consumes the leaf fast-path
	// numeric counts correctly (rather than row-counting again).
	q := `sum by ("resource_k8s_pod_name")(
           count by ("resource_k8s_pod_name") ( rate({__name__="req_total"}[5m]) )
         )`

	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatalf("parse promql: %v", err)
	}
	plan, err := Compile(expr)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(plan.Leaves))
	}
	leaf := plan.Leaves[0]

	step := time.Minute
	workerSQL := replaceWorkerPlaceholders(leaf.ToWorkerSQL(step), 0, 120*1000)

	rows1 := queryAll(t, db1, workerSQL)
	rows2 := queryAll(t, db2, workerSQL)

	type bucket = int64
	perBucket := map[bucket][]SketchInput{}

	addRows := func(rows []rowmap) {
		for _, r := range rows {
			b := i64(r["bucket_ts"])
			pod := s(r[`resource_k8s_pod_name`])
			cnt := float64(i64(r["count"]))
			if cnt == 0 { // skip densified empties
				continue
			}
			perBucket[b] = append(perBucket[b], SketchInput{
				ExprID:         leaf.ID,
				OrganizationID: "org-test",
				Timestamp:      b,
				Frequency:      int64(step / time.Second),
				SketchTags: SketchTags{
					Tags:       map[string]any{"resource_k8s_pod_name": pod},
					SketchType: SketchMAP,
					Agg:        map[string]float64{"count": cnt},
				},
			})
		}
	}
	addRows(rows1)
	addRows(rows2)

	// Deterministic order
	var buckets []int64
	for b := range perBucket {
		buckets = append(buckets, b)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })

	type key struct {
		bucket int64
		pod    string
	}
	got := map[key]int64{}

	for _, b := range buckets {
		out := plan.Root.Eval(SketchGroup{
			Timestamp: b,
			Group:     map[string][]SketchInput{leaf.ID: perBucket[b]},
		}, step)

		// Expect one result per (bucket,pod)
		for _, er := range out {
			pod := s(er.Tags[`resource_k8s_pod_name`])
			got[key{b, pod}] = int64(er.Value.Num + 0.5)
		}
	}

	exp := map[key]int64{
		{0, "api-7f"}:     2,
		{0, "api-9x"}:     1,
		{60000, "api-7f"}: 3,
		{60000, "api-9x"}: 1,
	}
	for k, want := range exp {
		if got[k] != want {
			t.Fatalf("unexpected count for %v: got=%v want=%v\nrows1=%v\nrows2=%v\nsql=\n%s", k, got[k], want, rows1, rows2, workerSQL)
		}
	}
}

func TestProm_CountPodsPerService_TwoWorkers_Eval(t *testing.T) {
	// --- Worker 1: api-gateway rows (api-7f, api-9x) --------------------------
	db1 := openDuckDB(t)
	createTempTable(t, db1)
	mustExec(t, db1, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket 0 (api-gateway)
(10*1000,  'req_total', 'api-gateway', 'api-7f',  'a', 1, 10, 1, 1),
(20*1000,  'req_total', 'api-gateway', 'api-7f',  'b', 1, 10, 1, 1),
(40*1000,  'req_total', 'api-gateway', 'api-9x',  'a', 1, 10, 1, 1),
-- bucket 1 (api-gateway)
(70*1000,  'req_total', 'api-gateway', 'api-7f',  'a', 1, 10, 1, 1),
(80*1000,  'req_total', 'api-gateway', 'api-7f',  'c', 1, 10, 1, 1),
(100*1000, 'req_total', 'api-gateway', 'api-9x',  'a', 1, 10, 1, 1);
`)

	// --- Worker 2: auth rows (auth-1a, auth-2b) -------------------------------
	db2 := openDuckDB(t)
	createTempTable(t, db2)
	mustExec(t, db2, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket 0 (auth)
(50*1000,  'req_total', 'auth',        'auth-1a', 'a', 1, 10, 1, 1),
-- bucket 1 (auth)
(110*1000, 'req_total', 'auth',        'auth-1a', 'b', 1, 10, 1, 1),
(115*1000, 'req_total', 'auth',        'auth-2b', 'a', 1, 10, 1, 1);
`)

	// PromQL: count by (service) of distinct pods per service:
	//   count by (service)(
	//     count by (pod, service)( rate(req_total[5m]) )
	//   )
	q := `count by ("resource_service_name") (
            count by ("resource_k8s_pod_name","resource_service_name") (
              rate({__name__="req_total"}[5m])
            )
          )`

	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatalf("parse promql: %v", err)
	}
	plan, err := Compile(expr)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(plan.Leaves))
	}
	leaf := plan.Leaves[0]

	step := time.Minute
	workerSQL := replaceWorkerPlaceholders(leaf.ToWorkerSQL(step), 0, 120*1000)

	// Run leaf SQL on both workers.
	rows1 := queryAll(t, db1, workerSQL)
	rows2 := queryAll(t, db2, workerSQL)
	if len(rows1) == 0 && len(rows2) == 0 {
		t.Fatalf("no rows returned; sql=\n%s", workerSQL)
	}

	// Merge rows per bucket → []SketchInput per bucket for this leaf.
	type bucket = int64
	perBucket := map[bucket][]SketchInput{}

	addRows := func(rows []rowmap) {
		for _, r := range rows {
			b := i64(r["bucket_ts"])

			svc := s(r[`resource_service_name`])
			pod := s(r[`resource_k8s_pod_name`])

			// Count fast-path emits a numeric "count"; ignore densified zeroes.
			cnt := float64(i64(r["count"]))
			if cnt == 0 {
				continue
			}

			perBucket[b] = append(perBucket[b], SketchInput{
				ExprID:         leaf.ID,
				OrganizationID: "org-test",
				Timestamp:      b,
				Frequency:      int64(step / time.Second),
				SketchTags: SketchTags{
					Tags: map[string]any{
						"resource_k8s_pod_name": pod,
						"resource_service_name": svc,
					},
					SketchType: SketchMAP,
					Agg:        map[string]float64{"count": cnt},
				},
			})
		}
	}
	addRows(rows1)
	addRows(rows2)

	// Deterministic bucket order.
	var buckets []int64
	for b := range perBucket {
		buckets = append(buckets, b)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })

	// Evaluate per bucket and collect "pods per service".
	type key struct {
		bucket  int64
		service string
	}
	got := map[key]int64{}

	for _, b := range buckets {
		out := plan.Root.Eval(SketchGroup{
			Timestamp: b,
			Group:     map[string][]SketchInput{leaf.ID: perBucket[b]},
		}, step)

		// out is keyed by "service" (top-level count by(service))
		for _, er := range out {
			svc := s(er.Tags[`resource_service_name`])
			got[key{b, svc}] = int64(er.Value.Num + 0.5) // round
		}
	}

	const (
		b0 = int64(0)
		b1 = int64(60000)
	)
	exp := map[key]int64{
		{b0, "api-gateway"}: 2,
		{b0, "auth"}:        1,
		{b1, "api-gateway"}: 2,
		{b1, "auth"}:        2,
	}

	for k, want := range exp {
		if got[k] != want {
			t.Fatalf("unexpected pod-count for %v: got=%v want=%v\nworkerSQL=\n%s\nrows1=%v\nrows2=%v",
				k, got[k], want, workerSQL, rows1, rows2)
		}
	}
}

func TestProm_CountPodsPerService_FromSumByPodService_TwoWorkers(t *testing.T) {
	// --- Worker 1: api-gateway rows (api-7f, api-9x) --------------------------
	db1 := openDuckDB(t)
	createTempTable(t, db1)
	mustExec(t, db1, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket 0 (api-gateway)
(10*1000,  'req_total', 'api-gateway', 'api-7f',   'a', 1, 10, 1, 1),
(20*1000,  'req_total', 'api-gateway', 'api-7f',   'b', 1, 10, 1, 1),
(40*1000,  'req_total', 'api-gateway', 'api-9x',   'a', 1, 10, 1, 1),
-- bucket 1 (api-gateway)
(70*1000,  'req_total', 'api-gateway', 'api-7f',   'a', 1, 10, 1, 1),
(80*1000,  'req_total', 'api-gateway', 'api-7f',   'c', 1, 10, 1, 1),
(100*1000, 'req_total', 'api-gateway', 'api-9x',   'a', 1, 10, 1, 1);
`)

	// --- Worker 2: auth rows (auth-1a, auth-2b) -------------------------------
	db2 := openDuckDB(t)
	createTempTable(t, db2)
	mustExec(t, db2, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket 0 (auth)
(50*1000,  'req_total', 'auth',        'auth-1a',  'a', 1, 10, 1, 1),
-- bucket 1 (auth)
(110*1000, 'req_total', 'auth',        'auth-1a',  'b', 1, 10, 1, 1),
(115*1000, 'req_total', 'auth',        'auth-2b',  'a', 1, 10, 1, 1);
`)

	// Worker query: sum by (pod,service)(rate(...)).
	q := `sum by ("resource_k8s_pod_name","resource_service_name")(
            rate({__name__="req_total"}[5m])
          )`

	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatalf("parse promql: %v", err)
	}
	plan, err := Compile(expr)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(plan.Leaves))
	}
	leaf := plan.Leaves[0]

	// Execute worker SQL on both workers for 1m steps over [0, 120s).
	sql := replaceWorkerPlaceholders(leaf.ToWorkerSQL(time.Minute), 0, 120*1000)
	rows1 := queryAll(t, db1, sql)
	rows2 := queryAll(t, db2, sql)
	if len(rows1) == 0 && len(rows2) == 0 {
		t.Fatalf("no rows returned; sql=\n%s", sql)
	}

	// Merge worker outputs and emulate parent: for each (bucket, service),
	// count distinct pods where the worker produced a (non-zero) sample.
	type key struct {
		bucket  int64
		service string
	}
	seenPods := map[key]map[string]struct{}{}

	add := func(rows []rowmap) {
		for _, r := range rows {
			b := i64(r["bucket_ts"])

			svc := s(r[`resource_service_name`])
			pod := s(r[`resource_k8s_pod_name`])

			// If worker emits sum=0 (densified), ignore it. (This builder path generally
			// doesn’t densify, but guard stays in place for parity with the 1-worker test.)
			if v, ok := r["sum"]; ok && v != nil && i64(v) == 0 {
				continue
			}

			k := key{bucket: b, service: svc}
			if _, ok := seenPods[k]; !ok {
				seenPods[k] = map[string]struct{}{}
			}
			seenPods[k][pod] = struct{}{}
		}
	}
	add(rows1)
	add(rows2)

	got := map[key]int64{}
	for k, pods := range seenPods {
		got[k] = int64(len(pods))
	}

	const (
		b0 = int64(0)
		b1 = int64(60000)
	)
	exp := map[key]int64{
		{b0, "api-gateway"}: 2,
		{b0, "auth"}:        1,
		{b1, "api-gateway"}: 2,
		{b1, "auth"}:        2,
	}
	for k, want := range exp {
		if got[k] != want {
			t.Fatalf("unexpected pod-count for %v: got=%v want=%v\nsql=\n%s\nrows1=%v\nrows2=%v",
				k, got[k], want, sql, rows1, rows2)
		}
	}
}

func TestProm_Rate_1m_RangeCoverage_TwoWorkers_Eval(t *testing.T) {
	// --- Worker 1: api-7f -----------------------------------------------------
	db1 := openDuckDB(t)
	createTempTable(t, db1)
	mustExec(t, db1, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket [0s..30s)
(10*1000,  'req_total', 'api-gateway', 'api-7f', 'a', 1, 1, 1, 1),
(20*1000,  'req_total', 'api-gateway', 'api-7f', 'b', 1, 1, 1, 1),
-- bucket [60s..90s)
(70*1000,  'req_total', 'api-gateway', 'api-7f', 'a', 1, 1, 1, 1),
(80*1000,  'req_total', 'api-gateway', 'api-7f', 'b', 1, 1, 1, 1),
-- bucket [90s..120s)
(90*1000,  'req_total', 'api-gateway', 'api-7f', 'c', 1, 1, 1, 1);
`)

	// --- Worker 2: api-9x -----------------------------------------------------
	db2 := openDuckDB(t)
	createTempTable(t, db2)
	mustExec(t, db2, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket [30s..60s)
(40*1000,  'req_total', 'api-gateway', 'api-9x', 'a', 1, 1, 1, 1),
-- bucket [90s..120s)
(100*1000, 'req_total', 'api-gateway', 'api-9x', 'a', 1, 1, 1, 1);
`)

	// PromQL: rate over 1m, parent sums by pod (so we keep pod identity).
	q := `sum by ("resource_k8s_pod_name")(rate({__name__="req_total"}[1m]))`

	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatalf("parse promql: %v", err)
	}
	plan, err := Compile(expr)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(plan.Leaves))
	}
	leaf := plan.Leaves[0]

	step := 30 * time.Second // 30s step so second bucket fully covers 1m window
	workerSQL := replaceWorkerPlaceholders(leaf.ToWorkerSQL(step), 0, 120*1000)

	// Run on both workers
	rows1 := queryAll(t, db1, workerSQL)
	rows2 := queryAll(t, db2, workerSQL)

	// Build per-bucket SketchInputs from SUM (skip densified zeros).
	type bucket = int64
	perBucket := map[bucket][]SketchInput{}

	addRows := func(rows []rowmap) {
		for _, r := range rows {
			b := i64(r["bucket_ts"])
			pod := s(r[`resource_k8s_pod_name`])
			sum := int64(0)
			if v, ok := r["sum"]; ok && v != nil {
				sum = i64(v)
			}
			if sum == 0 {
				continue // ignore densified empty buckets
			}
			perBucket[b] = append(perBucket[b], SketchInput{
				ExprID:         leaf.ID,
				OrganizationID: "org-test",
				Timestamp:      b,
				Frequency:      int64(step / time.Second),
				SketchTags: SketchTags{
					Tags:       map[string]any{"resource_k8s_pod_name": pod},
					SketchType: SketchMAP,
					Agg:        map[string]float64{"sum": float64(sum)},
				},
			})
		}
	}
	addRows(rows1)
	addRows(rows2)

	// Deterministic order
	var buckets []int64
	for b := range perBucket {
		buckets = append(buckets, b)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })

	// Evaluate per bucket.
	// Expect: bucket 0 → no results (window not covered), later buckets produce rates.
	type key struct {
		bucket int64
		pod    string
	}
	gotTimes60 := map[key]int64{} // rate * 60s → last-1m event count

	for _, b := range buckets {
		out := plan.Root.Eval(SketchGroup{
			Timestamp: b,
			Group:     map[string][]SketchInput{leaf.ID: perBucket[b]},
		}, step)

		switch b {
		case 0:
			// First bucket has only 30s of coverage (< 1m) → NaN filtered → no output.
			if len(out) != 0 {
				t.Fatalf("expected no results at bucket %d (not enough coverage), got %d; out=%v", b, len(out), out)
			}
			continue
		}

		// For covered buckets, we should have results per pod. Capture rate*60 (count per last 1m).
		for _, er := range out {
			pod := s(er.Tags[`resource_k8s_pod_name`])
			gotTimes60[key{b, pod}] = int64(er.Value.Num*60.0 + 0.5) // nearest int
		}
	}

	exp := map[key]int64{
		{60000, "api-7f"}: 0, // window (0s,60s]
		{90000, "api-7f"}: 3, // window (30s,90s]
	}
	for k, want := range exp {
		if gotTimes60[k] != want {
			t.Fatalf("unexpected rate*60 at %v: got=%v want=%v\nrows1=%v\nrows2=%v\nsql=\n%s", k, gotTimes60[k], want, rows1, rows2, workerSQL)
		}
	}
}

func TestProm_SumOfCount_vs_CountOfCount_TwoWorkers_Eval(t *testing.T) {
	// --- Worker 1: api-7f with 2 series in bucket0, 3 series in bucket1 ------
	db1 := openDuckDB(t)
	createTempTable(t, db1)
	mustExec(t, db1, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket 0
(10*1000,  'req_total', 'api-gateway', 'api-7f', 'a', 1, 10, 1, 1),
(20*1000,  'req_total', 'api-gateway', 'api-7f', 'b', 1, 10, 1, 1),
-- bucket 1
(70*1000,  'req_total', 'api-gateway', 'api-7f', 'a', 1, 10, 1, 1),
(80*1000,  'req_total', 'api-gateway', 'api-7f', 'b', 1, 10, 1, 1),
(90*1000,  'req_total', 'api-gateway', 'api-7f', 'c', 1, 10, 1, 1);
`)

	// --- Worker 2: api-9x with 1 series per bucket ----------------------------
	db2 := openDuckDB(t)
	createTempTable(t, db2)
	mustExec(t, db2, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket 0
(40*1000,  'req_total', 'api-gateway', 'api-9x', 'a', 1, 10, 1, 1),
-- bucket 1
(100*1000, 'req_total', 'api-gateway', 'api-9x', 'a', 1, 10, 1, 1);
`)

	step := time.Minute

	// Helper to run a query end-to-end with two workers and return per-(bucket,pod) numeric results.
	run := func(q string) map[[2]any]float64 {
		expr, err := FromPromQL(q)
		if err != nil {
			t.Fatalf("parse promql: %v", err)
		}
		plan, err := Compile(expr)
		if err != nil {
			t.Fatalf("compile: %v", err)
		}
		if len(plan.Leaves) != 1 {
			t.Fatalf("expected 1 leaf, got %d", len(plan.Leaves))
		}
		leaf := plan.Leaves[0]

		workerSQL := replaceWorkerPlaceholders(leaf.ToWorkerSQL(step), 0, 120*1000)
		rows1 := queryAll(t, db1, workerSQL)
		rows2 := queryAll(t, db2, workerSQL)

		// Build per-bucket inputs from the leaf output. We expect the leaf to be
		// in the COUNT fast-path so it emits one row per (bucket,pod) with a "count" value.
		type bucket = int64
		perBucket := map[bucket][]SketchInput{}
		addRows := func(rows []rowmap) {
			for _, r := range rows {
				b := i64(r["bucket_ts"])
				pod := s(r[`resource_k8s_pod_name`])
				cnt := int64(0)
				if v, ok := r["count"]; ok && v != nil {
					cnt = i64(v)
				}
				if cnt == 0 { // skip densified empties
					continue
				}
				perBucket[b] = append(perBucket[b], SketchInput{
					ExprID:         leaf.ID,
					OrganizationID: "org-test",
					Timestamp:      b,
					Frequency:      int64(step / time.Second),
					SketchTags: SketchTags{
						Tags:       map[string]any{"resource_k8s_pod_name": pod},
						SketchType: SketchMAP,
						Agg:        map[string]float64{"count": float64(cnt)},
					},
				})
			}
		}
		addRows(rows1)
		addRows(rows2)

		// Deterministic order
		var buckets []int64
		for b := range perBucket {
			buckets = append(buckets, b)
		}
		sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })

		got := map[[2]any]float64{} // key = [bucketMs, pod], value = scalar
		for _, b := range buckets {
			out := plan.Root.Eval(SketchGroup{
				Timestamp: b,
				Group:     map[string][]SketchInput{leaf.ID: perBucket[b]},
			}, step)
			for _, er := range out {
				pod := s(er.Tags[`resource_k8s_pod_name`])
				got[[2]any{b, pod}] = er.Value.Num
			}
		}
		return got
	}

	// 1) sum by (pod)( count by (pod)( rate(...) ) )  => numeric counts per (bucket,pod)
	sumOfCountQ := `sum by ("resource_k8s_pod_name")(
                      count by ("resource_k8s_pod_name")( rate({__name__="req_total"}[5m]) )
                    )`
	sumRes := run(sumOfCountQ)

	// Expect numeric counts: api-7f: 2 then 3; api-9x: 1 then 1.
	expSum := map[[2]any]float64{
		{int64(0), "api-7f"}:     2,
		{int64(0), "api-9x"}:     1,
		{int64(60000), "api-7f"}: 3,
		{int64(60000), "api-9x"}: 1,
	}
	for k, want := range expSum {
		got := sumRes[k]
		if int64(got+0.5) != int64(want) {
			t.Fatalf("sum-of-count mismatch at %v: got=%v want=%v", k, got, want)
		}
	}

	// 2) count by (pod)( count by (pod)( rate(...) ) )  => 1 per pod per bucket
	countOfCountQ := `count by ("resource_k8s_pod_name")(
                        count by ("resource_k8s_pod_name")( rate({__name__="req_total"}[5m]) )
                      )`
	countRes := run(countOfCountQ)

	expCount := map[[2]any]float64{
		{int64(0), "api-7f"}:     1,
		{int64(0), "api-9x"}:     1,
		{int64(60000), "api-7f"}: 1,
		{int64(60000), "api-9x"}: 1,
	}
	for k, want := range expCount {
		got := countRes[k]
		if int64(got+0.5) != int64(want) {
			t.Fatalf("count-of-count mismatch at %v: got=%v want=%v", k, got, want)
		}
	}
}

func TestProm_CountByPod_InstantVector(t *testing.T) {
	// --- Worker 1 -------------------------------------------------------------
	db1 := openDuckDB(t)
	createTempTable(t, db1)
	mustExec(t, db1, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket 0
(10*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-7f', 'a', 1, 1, 1, 1),
(20*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-7f', 'b', 1, 1, 1, 1),
-- bucket 1
(70*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-7f', 'a', 1, 1, 1, 1),
(80*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-7f', 'b', 1, 1, 1, 1),
(90*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-7f', 'c', 1, 1, 1, 1);
`)

	// --- Worker 2 -------------------------------------------------------------
	db2 := openDuckDB(t)
	createTempTable(t, db2)
	mustExec(t, db2, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket 0
(40*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-9x', 'a', 1, 1, 1, 1),
-- bucket 1
(100*1000, 'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-9x', 'a', 1, 1, 1, 1);
`)

	step := time.Minute
	q := `count by ("resource_k8s_pod_name") ({__name__="k8s.container.cpu_limit_utilization"})`

	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatalf("parse promql: %v", err)
	}
	plan, err := Compile(expr)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(plan.Leaves))
	}
	leaf := plan.Leaves[0]

	workerSQL := replaceWorkerPlaceholders(leaf.ToWorkerSQL(step), 0, 120*1000)
	rows1 := queryAll(t, db1, workerSQL)
	rows2 := queryAll(t, db2, workerSQL)
	if len(rows1) == 0 && len(rows2) == 0 {
		t.Fatalf("no rows from either worker; sql=\n%s", workerSQL)
	}

	// Build coordinator inputs: pass through the leaf's numeric COUNT.
	type bucket = int64
	perBucket := map[bucket][]SketchInput{}
	addRows := func(rows []rowmap) {
		for _, r := range rows {
			b := i64(r["bucket_ts"])
			pod := s(r[`resource_k8s_pod_name`])
			cnt := i64(r["count"]) // leaf step_count
			if cnt == 0 {
				// skip densified empties
				continue
			}
			perBucket[b] = append(perBucket[b], SketchInput{
				ExprID:         leaf.ID,
				OrganizationID: "org-test",
				Timestamp:      b,
				Frequency:      int64(step / time.Second),
				SketchTags: SketchTags{
					Tags:       map[string]any{"resource_k8s_pod_name": pod},
					SketchType: SketchMAP,
					Agg:        map[string]float64{"count": float64(cnt)}, // <-- key fix
				},
			})
		}
	}
	addRows(rows1)
	addRows(rows2)

	// Deterministic order.
	var buckets []int64
	for b := range perBucket {
		buckets = append(buckets, b)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })

	got := map[[2]any]float64{}
	for _, b := range buckets {
		out := plan.Root.Eval(SketchGroup{
			Timestamp: b,
			Group:     map[string][]SketchInput{leaf.ID: perBucket[b]},
		}, step)
		for _, er := range out {
			pod := s(er.Tags[`resource_k8s_pod_name`])
			got[[2]any{b, pod}] = er.Value.Num
		}
	}

	// Expect the leaf's counts per (bucket,pod).
	exp := map[[2]any]float64{
		{int64(0), "api-7f"}:     2,
		{int64(0), "api-9x"}:     1,
		{int64(60000), "api-7f"}: 3,
		{int64(60000), "api-9x"}: 1,
	}
	for k, want := range exp {
		if int64(got[k]+0.5) != int64(want) {
			t.Fatalf("count-by-pod mismatch at %v: got=%v want=%v\nrows1=%v\nrows2=%v\nsql=\n%s",
				k, got[k], want, rows1, rows2, workerSQL)
		}
	}
}

func TestProm_CountOfPods_InstantVector(t *testing.T) {
	// --- Worker 1 -------------------------------------------------------------
	db1 := openDuckDB(t)
	createTempTable(t, db1)
	mustExec(t, db1, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket 0
(10*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-7f', 'a', 1, 1, 1, 1),
(20*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-7f', 'b', 1, 1, 1, 1),
-- bucket 1
(70*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-7f', 'a', 1, 1, 1, 1),
(80*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-7f', 'b', 1, 1, 1, 1),
(90*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-7f', 'c', 1, 1, 1, 1);
`)

	// --- Worker 2 -------------------------------------------------------------
	db2 := openDuckDB(t)
	createTempTable(t, db2)
	mustExec(t, db2, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket 0
(40*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-9x', 'a', 1, 1, 1, 1),
-- bucket 1
(100*1000, 'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-9x', 'a', 1, 1, 1, 1);
`)

	step := time.Minute
	q := `count(count by ("resource_k8s_pod_name") ({__name__="k8s.container.cpu_limit_utilization"}))`

	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatalf("parse promql: %v", err)
	}
	plan, err := Compile(expr)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(plan.Leaves))
	}
	leaf := plan.Leaves[0]

	workerSQL := replaceWorkerPlaceholders(leaf.ToWorkerSQL(step), 0, 120*1000)
	rows1 := queryAll(t, db1, workerSQL)
	rows2 := queryAll(t, db2, workerSQL)
	if len(rows1) == 0 && len(rows2) == 0 {
		t.Fatalf("no rows from either worker; sql=\n%s", workerSQL)
	}

	// Build coordinator inputs: one SketchInput per (bucket,pod) with the leaf's step_count.
	type bucket = int64
	perBucket := map[bucket][]SketchInput{}
	addRows := func(rows []rowmap) {
		for _, r := range rows {
			b := i64(r["bucket_ts"])
			pod := s(r[`resource_k8s_pod_name`])
			cnt := int64(0)
			if v, ok := r["count"]; ok && v != nil {
				cnt = i64(v)
			}
			if cnt == 0 { // skip densified empties
				continue
			}
			perBucket[b] = append(perBucket[b], SketchInput{
				ExprID:         leaf.ID,
				OrganizationID: "org-test",
				Timestamp:      b,
				Frequency:      int64(step / time.Second),
				SketchTags: SketchTags{
					Tags:       map[string]any{"resource_k8s_pod_name": pod},
					SketchType: SketchMAP,
					Agg:        map[string]float64{"count": float64(cnt)},
				},
			})
		}
	}
	addRows(rows1)
	addRows(rows2)

	// Deterministic order of buckets.
	var buckets []int64
	for b := range perBucket {
		buckets = append(buckets, b)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })

	// Evaluate the full plan per bucket.
	got := map[int64]float64{} // single series per bucket (outer count has no "by")
	for _, b := range buckets {
		out := plan.Root.Eval(SketchGroup{
			Timestamp: b,
			Group:     map[string][]SketchInput{leaf.ID: perBucket[b]},
		}, step)

		if len(out) != 1 {
			t.Fatalf("expected 1 series at bucket %d, got %d; out=%v\nrows1=%v\nrows2=%v\nsql=\n%s",
				b, len(out), out, rows1, rows2, workerSQL)
		}
		got[b] = out["default"].Value.Num
	}

	// We expect “number of pods present” per bucket: 2 in both buckets.
	exp := map[int64]float64{
		0:     2,
		60000: 2,
	}
	for b, want := range exp {
		if int64(got[b]+0.5) != int64(want) {
			t.Fatalf("count(count by pod) mismatch at bucket %d: got=%v want=%v\nrows1=%v\nrows2=%v\nsql=\n%s",
				b, got[b], want, rows1, rows2, workerSQL)
		}
	}
}

func TestProm_Max_GT_Scalar_Integration(t *testing.T) {
	// --- Worker 1 -------------------------------------------------------------
	db1 := openDuckDB(t)
	createTempTable(t, db1)
	mustExec(t, db1, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket 0 (all <= 0.9)
(10*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-7f', 'a', 0.80, 1, 0.80, 0.80),
(20*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-7f', 'b', 0.70, 1, 0.70, 0.70),
-- bucket 1 (includes > 0.9)
(70*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-7f', 'a', 0.95, 1, 0.95, 0.95),
(80*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-7f', 'b', 0.65, 1, 0.65, 0.65);
`)

	// --- Worker 2 -------------------------------------------------------------
	db2 := openDuckDB(t)
	createTempTable(t, db2)
	mustExec(t, db2, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket 0 (still <= 0.9 globally)
(40*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-9x', 'a', 0.85, 1, 0.85, 0.85),
-- bucket 1 (also > 0.9)
(100*1000, 'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-9x', 'a', 0.91, 1, 0.91, 0.91);
`)

	step := time.Minute
	q := `max({__name__="k8s.container.cpu_limit_utilization"}) > bool 0.9`

	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatalf("parse promql: %v", err)
	}
	plan, err := Compile(expr)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(plan.Leaves))
	}
	leaf := plan.Leaves[0]

	workerSQL := replaceWorkerPlaceholders(leaf.ToWorkerSQL(step), 0, 120*1000)
	rows1 := queryAll(t, db1, workerSQL)
	rows2 := queryAll(t, db2, workerSQL)
	if len(rows1) == 0 && len(rows2) == 0 {
		t.Fatalf("no rows from either worker; sql=\n%s", workerSQL)
	}

	// Build coordinator inputs: we need the per-bucket "max" from each worker.
	type bucket = int64
	perBucket := map[bucket][]SketchInput{}
	addRows := func(rows []rowmap) {
		for _, r := range rows {
			b := i64(r["bucket_ts"])

			// leaf SQL (instant, no group-by) returns one row per bucket with columns:
			// sum, count, min, max. We care about "max" here.
			mv := f64(r["max"])

			perBucket[b] = append(perBucket[b], SketchInput{
				ExprID:         leaf.ID,
				OrganizationID: "org-test",
				Timestamp:      b,
				Frequency:      int64(step / time.Second),
				SketchTags: SketchTags{
					Tags:       map[string]any{}, // no grouping for this query
					SketchType: SketchMAP,
					Agg:        map[string]float64{"max": mv},
				},
			})
		}
	}
	addRows(rows1)
	addRows(rows2)

	// Deterministic bucket order.
	var buckets []int64
	for b := range perBucket {
		buckets = append(buckets, b)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })

	// Evaluate the full plan per bucket.
	got := map[int64]float64{} // single series per bucket (comparison yields scalar series)
	for _, b := range buckets {
		out := plan.Root.Eval(SketchGroup{
			Timestamp: b,
			Group:     map[string][]SketchInput{leaf.ID: perBucket[b]},
		}, step)

		if len(out) != 1 {
			t.Fatalf("expected 1 series at bucket %d, got %d; out=%v\nrows1=%v\nrows2=%v\nsql=\n%s",
				b, len(out), out, rows1, rows2, workerSQL)
		}
		// Convention: scalar/no-label series in tests is keyed by "default".
		got[b] = out["default"].Value.Num
	}

	// Expect false (0) for bucket 0 (all <= 0.9) and true (1) for bucket 1 (some > 0.9).
	exp := map[int64]float64{
		0:     0,
		60000: 1,
	}
	for b, want := range exp {
		if int64(got[b]+0.5) != int64(want) {
			t.Fatalf("max(...) > 0.9 mismatch at bucket %d: got=%v want=%v\nrows1=%v\nrows2=%v\nsql=\n%s",
				b, got[b], want, rows1, rows2, workerSQL)
		}
	}
}

func TestProm_Max_GT_Scalar_Filter_Integration(t *testing.T) {
	// --- Worker 1 -------------------------------------------------------------
	db1 := openDuckDB(t)
	createTempTable(t, db1)
	mustExec(t, db1, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket 0 (all <= 0.9)
(10*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-7f', 'a', 0.80, 1, 0.80, 0.80),
(20*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-7f', 'b', 0.70, 1, 0.70, 0.70),
-- bucket 1 (includes > 0.9)
(70*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-7f', 'a', 0.95, 1, 0.95, 0.95),
(80*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-7f', 'b', 0.65, 1, 0.65, 0.65);
`)

	// --- Worker 2 -------------------------------------------------------------
	db2 := openDuckDB(t)
	createTempTable(t, db2)
	mustExec(t, db2, `
INSERT INTO logs("chq_timestamp","chq_name","resource_service_name","resource_k8s_pod_name",instance,chq_rollup_sum,chq_rollup_count,chq_rollup_min,chq_rollup_max) VALUES
-- bucket 0 (still <= 0.9 globally)
(40*1000,  'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-9x', 'a', 0.85, 1, 0.85, 0.85),
-- bucket 1 (also > 0.9)
(100*1000, 'k8s.container.cpu_limit_utilization', 'api-gateway', 'api-9x', 'a', 0.91, 1, 0.91, 0.91);
`)

	step := time.Minute
	q := `max({__name__="k8s.container.cpu_limit_utilization"}) > 0.9` // no 'bool' → filtering semantics

	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatalf("parse promql: %v", err)
	}
	plan, err := Compile(expr)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(plan.Leaves))
	}
	leaf := plan.Leaves[0]

	workerSQL := replaceWorkerPlaceholders(leaf.ToWorkerSQL(step), 0, 120*1000)
	rows1 := queryAll(t, db1, workerSQL)
	rows2 := queryAll(t, db2, workerSQL)
	if len(rows1) == 0 && len(rows2) == 0 {
		t.Fatalf("no rows from either worker; sql=\n%s", workerSQL)
	}

	// Build coordinator inputs: use the per-bucket "max" from each worker.
	type bucket = int64
	perBucket := map[bucket][]SketchInput{}
	addRows := func(rows []rowmap) {
		for _, r := range rows {
			b := i64(r["bucket_ts"])
			mv := f64(r["max"]) // we only care about max for this test

			perBucket[b] = append(perBucket[b], SketchInput{
				ExprID:         leaf.ID,
				OrganizationID: "org-test",
				Timestamp:      b,
				Frequency:      int64(step / time.Second),
				SketchTags: SketchTags{
					Tags:       map[string]any{}, // no grouping
					SketchType: SketchMAP,
					Agg:        map[string]float64{"max": mv},
				},
			})
		}
	}
	addRows(rows1)
	addRows(rows2)

	// Deterministic order of buckets.
	var buckets []int64
	for b := range perBucket {
		buckets = append(buckets, b)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i] < buckets[j] })

	// Evaluate per bucket.
	got := map[int64]float64{}
	for _, b := range buckets {
		out := plan.Root.Eval(SketchGroup{
			Timestamp: b,
			Group:     map[string][]SketchInput{leaf.ID: perBucket[b]},
		}, step)

		switch b {
		case 0:
			// Filtering semantics: condition false → series dropped
			if len(out) != 0 {
				t.Fatalf("expected 0 series at bucket %d (filter), got %d; out=%v\nrows1=%v\nrows2=%v\nsql=\n%s",
					b, len(out), out, rows1, rows2, workerSQL)
			}
		case 60000:
			// Condition true → keep original LHS value (global max = 0.95)
			if len(out) != 1 {
				t.Fatalf("expected 1 series at bucket %d, got %d; out=%v\nrows1=%v\nrows2=%v\nsql=\n%s",
					b, len(out), out, rows1, rows2, workerSQL)
			}
			got[b] = out["default"].Value.Num
		}
	}

	// Validate kept value equals the LHS max (0.95).
	if v := got[60000]; math.Abs(v-0.95) > 1e-9 {
		t.Fatalf("value at bucket 60000 mismatch: got=%v want=0.95\n", v)
	}
}

func f64(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int64:
		return float64(x)
	case int32:
		return float64(x)
	case int16:
		return float64(x)
	case int8:
		return float64(x)
	case int:
		return float64(x)
	case uint64:
		return float64(x)
	case uint32:
		return float64(x)
	case uint16:
		return float64(x)
	case uint8:
		return float64(x)
	case []byte:
		if n, err := strconv.ParseFloat(string(x), 64); err == nil {
			return n
		}
		return 0
	default:
		if n, err := strconv.ParseFloat(fmt.Sprintf("%v", x), 64); err == nil {
			return n
		}
		return 0
	}
}
