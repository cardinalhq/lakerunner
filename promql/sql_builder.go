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
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/common/model"
)

var supportedFuncs = map[string]bool{
	"sum_over_time":      true,
	"avg_over_time":      true,
	"min_over_time":      true,
	"max_over_time":      true,
	"rate":               true,
	"irate":              true, // same SQL as rate; API can do last-two-samples nuance later if needed
	"increase":           true,
	"quantile_over_time": false, // needs DDS
	"histogram_quantile": false, // needs DDS
	"":                   true,  // raw/instant (we still bucket for step)
}

// ToWorkerSQL builds a per-step, per-group SQL (DuckDB).
// - For *_over_time / rate / increase: densify to one row per step, then window (ROWS K-1 PRECEDING).
// - For raw/instant (no range): you could use buildRawSimple; for your test you’re driving everything through buildWindowed.
func (be *BaseExpr) ToWorkerSQL(step time.Duration) string {
	// Sketch-required paths → worker should return sketches
	if be.WantDDS {
		return buildDDS(be, step)
	}
	// If func not supported and it's not a topk/bottomk child, skip SQL
	if !supportedFuncs[be.FuncName] && !(be.WantTopK || be.WantBottomK) {
		return ""
	}

	// COUNT fast-path: COUNT-by-group with no identity collapse → simple raw bucketing
	if be.WantCount && equalStringSets(be.CountOnBy, be.GroupBy) {
		return buildStepOnly(be, []proj{{"COUNT(*)", "count"}}, step)
	}
	// Identity collapse (distinct series counting) → HLL/sketch path
	if be.WantCount && !equalStringSets(be.CountOnBy, be.GroupBy) {
		return buildCountHLLHash(be, step)
	}

	switch be.FuncName {
	// Sliding-window functions (need range + densify)
	case "sum_over_time":
		return buildWindowed(be, need{sum: true}, step)
	case "avg_over_time":
		return buildWindowed(be, need{sum: true, count: true}, step)
	case "min_over_time":
		return buildWindowed(be, need{min: true}, step)
	case "max_over_time":
		return buildWindowed(be, need{max: true}, step)
	case "rate", "irate":
		// rate = sum_over_time / range_seconds → push SUM window; parent divides by range.
		return buildWindowed(be, need{sum: true}, step)
	case "increase":
		return buildIncreaseWindowed(be, step)

	// Raw/instant — for your current experiment you’re also using the windowed path.
	case "":
		if be.Range == "" {
			return buildRawSimple(be, []proj{
				{"MIN(rollup_min)", "min"},
				{"MAX(rollup_max)", "max"},
				{"SUM(rollup_sum)", "sum"},
				{"COUNT(rollup_count)", "count"},
			}, step)
		}
		return buildWindowed(be, need{sum: true, count: true}, step)

	default:
		return ""
	}
}

// buildDDS: bucket timestamps, project group-by labels + sketch
func buildDDS(be *BaseExpr, step time.Duration) string {
	stepMs := step.Milliseconds()
	bucket := fmt.Sprintf("(\"_cardinalhq.timestamp\" - (\"_cardinalhq.timestamp\" %% %d))", stepMs)

	// Use aligned, end-exclusive time like the numeric paths
	alignedStart := fmt.Sprintf("({start} - ({start} %% %d))", stepMs)
	alignedEndEx := fmt.Sprintf("(({end} - 1) - (({end} - 1) %% %d) + %d)", stepMs, stepMs)

	base := whereFor(be)
	timeWhere := func() string {
		tc := fmt.Sprintf("\"_cardinalhq.timestamp\" >= %s AND \"_cardinalhq.timestamp\" < %s", alignedStart, alignedEndEx)
		if base == "" {
			return " WHERE " + tc
		}
		return base + " AND " + tc
	}()

	cols := []string{bucket + " AS bucket_ts"}
	if len(be.GroupBy) > 0 {
		cols = append(cols, strings.Join(be.GroupBy, ", "))
	}
	cols = append(cols, "sketch")

	// One row per stored sample; we’ll merge per (bucket_ts, groupkey) in Go.
	sql := "SELECT " + strings.Join(cols, ", ") +
		" FROM {table}" + timeWhere +
		" ORDER BY bucket_ts ASC"
	return sql
}

type proj struct{ expr, alias string }

type need struct {
	sum, count, min, max bool
}

const (
	timePredicate = "\"_cardinalhq.timestamp\" >= {start} AND \"_cardinalhq.timestamp\" < {end}"
)

// --- (optional) Raw bucketing (no densify) -----------------------------------

// buildRawSimple: bucket by ms and aggregate real rows only.
// Kept here if you want to switch raw/instant back to simple GROUP BY later.
func buildRawSimple(be *BaseExpr, projs []proj, step time.Duration) string {
	stepMs := step.Milliseconds()
	bucket := fmt.Sprintf("(\"_cardinalhq.timestamp\" - (\"_cardinalhq.timestamp\" %% %d))", stepMs)

	where := withTime(whereFor(be))

	cols := make([]string, 0, 1+len(projs)+len(be.GroupBy))
	cols = append(cols, bucket+" AS bucket_ts")
	for _, p := range projs {
		cols = append(cols, fmt.Sprintf("%s AS %s", p.expr, p.alias))
	}
	if len(be.GroupBy) > 0 {
		cols = append(cols, strings.Join(be.GroupBy, ", "))
	}

	sql := "SELECT " + strings.Join(cols, ", ") +
		" FROM {table}" + where +
		groupByClause(be.GroupBy, "bucket_ts") +
		" ORDER BY bucket_ts ASC"
	return sql
}

// buildCountHLLHash: per (bucket_ts, GroupBy..., identity) emit one row with an id_hash.
// Worker merges rows into one HLL per (bucket_ts, GroupBy...).
func buildCountHLLHash(be *BaseExpr, step time.Duration) string {
	stepMs := step.Milliseconds()
	bucket := fmt.Sprintf("(\"_cardinalhq.timestamp\" - (\"_cardinalhq.timestamp\" %% %d))", stepMs)

	// Align [start, end) to step, as elsewhere
	alignedStart := fmt.Sprintf("({start} - ({start} %% %d))", stepMs)
	alignedEndEx := fmt.Sprintf("(({end} - 1) - (({end} - 1) %% %d) + %d)", stepMs, stepMs)

	base := whereFor(be)
	timeWhere := func() string {
		tc := fmt.Sprintf("\"_cardinalhq.timestamp\" >= %s AND \"_cardinalhq.timestamp\" < %s", alignedStart, alignedEndEx)
		if base == "" {
			return " WHERE " + tc
		}
		return base + " AND " + tc
	}()

	// Build a stable hash over CountOnBy labels.
	// Use a sentinel for NULLs so NULL != "" (Prom labels never include 0x00).
	// DuckDB’s hash(...) returns BIGINT; we can cast to unsigned in Go.
	var idExprParts []string
	for _, c := range be.CountOnBy {
		idExprParts = append(idExprParts, fmt.Sprintf("COALESCE(%s, '\x00')", c))
	}
	idHashExpr := "hash(" + strings.Join(idExprParts, ", ") + ")"

	// SELECT bucket_ts, <GroupBy...>, id_hash
	sel := []string{bucket + " AS bucket_ts"}
	if len(be.GroupBy) > 0 {
		sel = append(sel, strings.Join(be.GroupBy, ", "))
	}
	sel = append(sel, idHashExpr+" AS id_hash")

	// GROUP BY bucket_ts, GroupBy..., id_hash -> one row per identity per bucket
	gb := []string{"bucket_ts"}
	if len(be.GroupBy) > 0 {
		gb = append(gb, be.GroupBy...)
	}
	gb = append(gb, "id_hash")

	sql := "SELECT " + strings.Join(sel, ", ") +
		" FROM {table}" + timeWhere +
		" GROUP BY " + strings.Join(gb, ", ") +
		" ORDER BY bucket_ts ASC"
	return sql
}

// --- Densified step aggregation (no window) ----------------------------------

// buildStepOnly: densify to one row per step (+ per group), then left-join step aggregates.
// COALESCE turns missing buckets into zeros so holes appear as 0s.
func buildStepOnly(be *BaseExpr, projs []proj, step time.Duration) string {
	stepMs := step.Milliseconds()
	bucketExpr := fmt.Sprintf("(\"_cardinalhq.timestamp\" - (\"_cardinalhq.timestamp\" %% %d))", stepMs)

	where := withTime(whereFor(be))

	// Densify buckets for the whole query span:
	// start_aligned = floor(start/step)*step
	// end_exclusive = floor((end-1)/step)*step + step
	alignedStart := fmt.Sprintf("({start} - ({start} %% %d))", stepMs)
	alignedEndExclusive := fmt.Sprintf("(({end} - 1) - (({end} - 1) %% %d) + %d)", stepMs, stepMs)
	buckets := fmt.Sprintf("buckets AS (SELECT range AS bucket_ts FROM range(%s, %s, %d))",
		alignedStart, alignedEndExclusive, stepMs)

	// Optional groups grid (if grouping).
	var groupsCTE, gridCTE, gridFrom string
	if len(be.GroupBy) > 0 {
		groupsCTE = "groups AS (SELECT DISTINCT " + strings.Join(be.GroupBy, ", ") + " FROM {table}" + where + ")"
		gridCTE = "grid AS (SELECT bucket_ts, " + strings.Join(be.GroupBy, ", ") + " FROM buckets CROSS JOIN groups)"
		gridFrom = "grid g"
	} else {
		gridFrom = "buckets b"
	}

	// Step aggregates per bucket (+ group).
	stepCols := []string{bucketExpr + " AS bucket_ts"}
	needSum, needCount := false, false
	for _, p := range projs {
		switch p.alias {
		case "sum":
			needSum = true
		case "count":
			needCount = true
		}
	}
	if needSum {
		stepCols = append(stepCols, "SUM(rollup_sum) AS step_sum")
	}
	if needCount {
		stepCols = append(stepCols, "COUNT(rollup_count) AS step_count")
	}
	if len(be.GroupBy) > 0 {
		stepCols = append(stepCols, strings.Join(be.GroupBy, ", "))
	}
	stepAgg := "step_aggr AS (SELECT " + strings.Join(stepCols, ", ") +
		" FROM {table}" + where +
		groupByClause(be.GroupBy, "bucket_ts") + ")"

	// Final select: densified grid LEFT JOIN step_aggr; COALESCE to 0 for gaps.
	var outCols []string
	outCols = append(outCols, "bucket_ts")
	if len(be.GroupBy) > 0 {
		outCols = append(outCols, strings.Join(be.GroupBy, ", "))
	}
	for _, p := range projs {
		switch p.alias {
		case "sum":
			outCols = append(outCols, "COALESCE(sa.step_sum, 0) AS sum")
		case "count":
			outCols = append(outCols, "COALESCE(sa.step_count, 0) AS count")
		default:
			outCols = append(outCols, fmt.Sprintf("%s AS %s", p.expr, p.alias))
		}
	}

	var joinKeys string
	if len(be.GroupBy) > 0 {
		joinKeys = "USING (bucket_ts, " + strings.Join(be.GroupBy, ", ") + ")"
	} else {
		joinKeys = "USING (bucket_ts)"
	}

	withs := []string{buckets}
	if groupsCTE != "" {
		withs = append(withs, groupsCTE, gridCTE)
	}
	withs = append(withs, stepAgg)

	sql := "WITH " + strings.Join(withs, ", ") +
		" SELECT " + strings.Join(outCols, ", ") +
		" FROM " + gridFrom +
		" LEFT JOIN step_aggr sa " + joinKeys +
		" ORDER BY bucket_ts ASC"

	return sql
}

// PromQL-like increase with sample-aware extrapolation.
// PromQL-like increase with sample-aware extrapolation.
func buildIncreaseWindowed(be *BaseExpr, step time.Duration) string {
	stepMs := step.Milliseconds()
	timeWhere := withTime(whereFor(be))
	bucketExpr := fmt.Sprintf(
		"(CAST(\"_cardinalhq.timestamp\" AS BIGINT) - (CAST(\"_cardinalhq.timestamp\" AS BIGINT) %% %d))",
		stepMs,
	)

	// Quote group-by keys
	var gbq []string
	for _, f := range be.GroupBy {
		gbq = append(gbq, fmt.Sprintf("\"%s\"", f))
	}

	// 1) Per-bucket aggregates, but keep sample-aware stats.
	stepCols := []string{
		bucketExpr + " AS bucket_ts",
		"SUM(rollup_sum) AS step_delta",                                   // counter deltas (post-reset, non-negative)
		"SUM(COALESCE(rollup_count, 1)) AS step_count",                    // number of raw samples in bucket
		"MIN(CAST(\"_cardinalhq.timestamp\" AS BIGINT)) AS step_first_ts", // first raw sample ts in bucket
		"MAX(CAST(\"_cardinalhq.timestamp\" AS BIGINT)) AS step_last_ts",  // last raw sample ts in bucket
	}
	if len(gbq) > 0 {
		stepCols = append(stepCols, strings.Join(gbq, ", "))
	}
	groupBy := []string{"bucket_ts"}
	if len(gbq) > 0 {
		groupBy = append(groupBy, gbq...)
	}

	stepAgg := "step_aggr AS (SELECT " + strings.Join(stepCols, ", ") +
		" FROM {table}" + timeWhere +
		" GROUP BY " + strings.Join(groupBy, ", ") + ")"

	rangeMs := rangeMsFromRange(be.Range)
	var part string
	if len(gbq) > 0 {
		part = "PARTITION BY " + strings.Join(gbq, ", ") + " "
	}
	win := fmt.Sprintf("OVER (%sORDER BY bucket_ts RANGE BETWEEN %d PRECEDING AND CURRENT ROW)", part, rangeMs)

	// 2) Window stats based on sample-aware fields.
	winCTE := "win AS (" +
		"SELECT bucket_ts" +
		func() string {
			if len(gbq) == 0 {
				return ""
			}
			return ", " + strings.Join(gbq, ", ")
		}() +
		", SUM(step_delta) " + win + " AS delta_sum" +
		", SUM(step_count) " + win + " AS sample_count" +
		", MIN(step_first_ts) " + win + " AS first_obs_ts" +
		", MAX(step_last_ts) " + win + " AS last_obs_ts" +
		" FROM step_aggr)"

	// 3) Extrapolation using those stats (½ avg-interval caps), capped by range.
	out := []string{"bucket_ts"}
	if len(gbq) > 0 {
		out = append(out, strings.Join(gbq, ", "))
	}
	out = append(out,
		"CAST(last_obs_ts - first_obs_ts AS DOUBLE) AS sampled_interval",
		"CASE WHEN sample_count > 1 THEN CAST(last_obs_ts - first_obs_ts AS DOUBLE) / (sample_count - 1) END AS avg_interval",
		fmt.Sprintf("GREATEST(0, CAST(first_obs_ts - (bucket_ts - %d) AS DOUBLE)) AS gap_start", rangeMs),
		"GREATEST(0, CAST(bucket_ts - last_obs_ts AS DOUBLE)) AS gap_end",
		"LEAST(gap_start, COALESCE(avg_interval, 0)/2.0) AS ex_start",
		"LEAST(gap_end,   COALESCE(avg_interval, 0)/2.0) AS ex_end",
		"(sampled_interval + ex_start + ex_end) AS effective_interval",
		fmt.Sprintf("LEAST(effective_interval, %d) AS effective_capped", rangeMs),
		"CASE WHEN sample_count >= 2 AND sampled_interval > 0 THEN effective_capped / sampled_interval END AS factor",
		"CASE WHEN sample_count >= 2 AND sampled_interval > 0 THEN delta_sum * factor END AS sum",
	)

	return "WITH " + stepAgg + ", " + winCTE +
		" SELECT " + strings.Join(out, ", ") +
		" FROM win ORDER BY bucket_ts ASC"
}

func buildWindowed(be *BaseExpr, need need, step time.Duration) string {
	stepMs := step.Milliseconds()
	timeWhere := withTime(whereFor(be))

	// Bucket every sample to the aligned step boundary
	bucketExpr := fmt.Sprintf(
		"(CAST(\"_cardinalhq.timestamp\" AS BIGINT) - (CAST(\"_cardinalhq.timestamp\" AS BIGINT) %% %d))",
		stepMs,
	)

	// Quote group-by keys once, reuse everywhere
	var gbq []string
	if len(be.GroupBy) > 0 {
		gbq = make([]string, 0, len(be.GroupBy))
		for _, f := range be.GroupBy {
			gbq = append(gbq, fmt.Sprintf("\"%s\"", f))
		}
	}

	// Build step_aggr SELECT (per-bucket, per-group aggregates)
	stepCols := []string{bucketExpr + " AS bucket_ts"}
	if need.sum {
		stepCols = append(stepCols, "SUM(rollup_sum) AS step_sum")
	}
	if need.count {
		stepCols = append(stepCols, "SUM(COALESCE(rollup_count, 0)) AS step_count")
	}
	// Only compute min/max if requested; they represent true sample min/max within the bucket
	if need.min {
		stepCols = append(stepCols, "MIN(rollup_min) AS step_min")
	}
	if need.max {
		stepCols = append(stepCols, "MAX(rollup_max) AS step_max")
	}
	if len(gbq) > 0 {
		stepCols = append(stepCols, strings.Join(gbq, ", "))
	}

	groupByParts := []string{"bucket_ts"}
	if len(gbq) > 0 {
		groupByParts = append(groupByParts, gbq...)
	}

	stepAgg := "step_aggr AS (SELECT " + strings.Join(stepCols, ", ") +
		" FROM {table}" + timeWhere +
		" GROUP BY " + strings.Join(groupByParts, ", ") + ")"

	// Window spec: RANGE = time-based frame; fall back to a degenerate frame for instant queries
	var frame string
	rangeMs := rangeMsFromRange(be.Range)
	if rangeMs > 0 {
		// Inclusive time window: include rows where (current_ts - row_ts) <= rangeMs
		frame = fmt.Sprintf("RANGE BETWEEN %d PRECEDING AND CURRENT ROW", rangeMs)
	} else {
		// No range → behave like CURRENT ROW only
		frame = "ROWS BETWEEN 0 PRECEDING AND CURRENT ROW"
	}

	var partition string
	if len(gbq) > 0 {
		partition = "PARTITION BY " + strings.Join(gbq, ", ") + " "
	}

	window := "OVER (" + partition + "ORDER BY bucket_ts " + frame + ")"

	// Build output projection with time-windowed aggregates
	out := []string{"bucket_ts"}
	if len(gbq) > 0 {
		out = append(out, strings.Join(gbq, ", "))
	}
	if need.sum {
		out = append(out, "CAST(SUM(step_sum) "+window+" AS DOUBLE) AS sum")
	}
	if need.count {
		out = append(out, "CAST(SUM(step_count) "+window+" AS DOUBLE) AS count")
	}
	if need.min {
		out = append(out, "CAST(MIN(step_min) "+window+" AS DOUBLE) AS min")
	}
	if need.max {
		out = append(out, "CAST(MAX(step_max) "+window+" AS DOUBLE) AS max")
	}

	sql := "WITH " + stepAgg +
		" SELECT " + strings.Join(out, ", ") +
		" FROM step_aggr" +
		" ORDER BY bucket_ts ASC"
	return sql
}

func rangeMsFromRange(rangeStr string) int64 {
	if rangeStr == "" {
		return 0
	}
	d, err := model.ParseDuration(rangeStr)
	if err != nil {
		return 0
	}
	return time.Duration(d).Milliseconds()
}

// --- Helpers ---------------------------------------------------------------
// --- Helpers ----------------------------------------------------------------

func withTime(where string) string {
	if where == "" {
		return " WHERE " + timePredicate
	}
	return where + " AND " + timePredicate
}

func groupByClause(by []string, first string) string {
	parts := []string{first}
	if len(by) > 0 {
		parts = append(parts, by...)
	}
	return " GROUP BY " + strings.Join(parts, ", ")
}

func rowsPreceding(rangeStr string, step time.Duration) int {
	// Default to a single-row window if no range (CURRENT ROW only)
	if rangeStr == "" {
		return 0
	}
	dur, err := model.ParseDuration(rangeStr)
	if err != nil {
		return 0
	}
	rangeMs := time.Duration(dur).Milliseconds()
	stepMs := step.Milliseconds()
	if stepMs <= 0 {
		return 0
	}
	k := int64(math.Ceil(float64(rangeMs) / float64(stepMs)))
	if k <= 0 {
		return 0
	}
	return int(k - 1) // ROWS <k-1> PRECEDING + CURRENT covers k rows
}

func equalStringSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	as := append([]string(nil), a...)
	bs := append([]string(nil), b...)
	sort.Strings(as)
	sort.Strings(bs)
	for i := range as {
		if as[i] != bs[i] {
			return false
		}
	}
	return true
}

func whereFor(be *BaseExpr) string {
	var parts []string
	if be.Metric != "" {
		parts = append(parts, fmt.Sprintf("\"_cardinalhq.name\" = %s", sqlLit(be.Metric)))
	}
	for _, m := range be.Matchers {
		switch m.Op {
		case MatchEq:
			parts = append(parts, fmt.Sprintf("%s = %s", m.Label, sqlLit(m.Value)))
		case MatchNe:
			parts = append(parts, fmt.Sprintf("%s <> %s", m.Label, sqlLit(m.Value)))
		case MatchRe:
			parts = append(parts, fmt.Sprintf("%s ~ %s", m.Label, sqlLit(m.Value)))
		case MatchNre:
			parts = append(parts, fmt.Sprintf("%s !~ %s", m.Label, sqlLit(m.Value)))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(parts, " AND ") + " AND true"
}

func sqlLit(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
