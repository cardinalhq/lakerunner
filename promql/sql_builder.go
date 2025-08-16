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
		be.WantCount = false
		return buildStepOnly(be, []proj{{"COUNT(*)", "count"}}, step)
	}
	// Identity collapse (distinct series counting) → HLL/sketch path
	if be.WantCount && !equalStringSets(be.CountOnBy, be.GroupBy) {
		return ""
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
		// increase = sum_over_time over counter deltas → same windowed SUM, no divide.
		return buildWindowed(be, need{sum: true}, step)

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

func buildWindowed(be *BaseExpr, need need, step time.Duration) string {
	stepMs := step.Milliseconds()

	// Aligned grid bounds
	alignedStart := fmt.Sprintf("CAST(({start} - ({start} %% %d)) AS BIGINT)", stepMs)
	alignedEndEx := fmt.Sprintf("CAST((({end}-1) - (({end}-1) %% %d) + %d) AS BIGINT)", stepMs, stepMs)

	timeWhere := withTime(whereFor(be))

	// grid (no groups here; add back if needed)
	grid := fmt.Sprintf(
		"grid AS (SELECT CAST(range AS BIGINT) AS step_idx FROM range(%s, %s, %d))",
		alignedStart, alignedEndEx, stepMs,
	)

	// step_aggr: same modulo bucketing, force BIGINT
	bucketExpr := fmt.Sprintf(
		"(CAST(\"_cardinalhq.timestamp\" AS BIGINT) - (CAST(\"_cardinalhq.timestamp\" AS BIGINT) %% %d))",
		stepMs,
	)
	stepCols := []string{bucketExpr + " AS step_idx"}
	if need.sum {
		stepCols = append(stepCols, "SUM(rollup_sum) AS step_sum")
	}
	if need.count {
		stepCols = append(stepCols, "SUM(COALESCE(rollup_count, 0)) AS step_count")
	}

	stepAgg := "step_aggr AS (SELECT " + strings.Join(stepCols, ", ") +
		" FROM {table}" + timeWhere + " GROUP BY step_idx)"

	// base (explicit ON join)
	base := "SELECT " +
		"CAST(g.step_idx AS BIGINT) AS bucket_ts" +
		func() string {
			var cols []string
			if need.sum {
				cols = append(cols, ", COALESCE(sa.step_sum, 0) AS w_step_sum")
			}
			if need.count {
				cols = append(cols, ", COALESCE(sa.step_count, 0) AS w_step_count")
			}
			if need.min {
				cols = append(cols, ", COALESCE(MIN(rollup_min), 0) AS w_step_min")
			}
			if need.max {
				cols = append(cols, ", COALESCE(MAX(rollup_max), 0) AS w_step_max")
			}
			return strings.Join(cols, "")
		}() +
		" FROM grid g LEFT JOIN step_aggr sa ON g.step_idx = sa.step_idx"

	// window
	kMinus1 := rowsPreceding(be.Range, step)
	order := " ORDER BY bucket_ts"
	var outCols []string
	outCols = append(outCols, "bucket_ts")
	if need.sum {
		outCols = append(outCols,
			fmt.Sprintf("CAST(SUM(w_step_sum) OVER (%s ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS DOUBLE) AS sum", order, kMinus1))
	}
	if need.count {
		outCols = append(outCols,
			fmt.Sprintf("CAST(SUM(w_step_count) OVER (%s ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS DOUBLE) AS count", order, kMinus1))
	}

	if need.min {
		outCols = append(outCols,
			fmt.Sprintf("CAST(MIN(w_step_min) OVER (%s ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS DOUBLE) AS min", order, kMinus1))
	}
	if need.max {
		outCols = append(outCols,
			fmt.Sprintf("CAST(MAX(w_step_max) OVER (%s ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS DOUBLE) AS max", order, kMinus1))
	}

	sql := "WITH " + grid + ", " + stepAgg +
		" SELECT " + strings.Join(outCols, ", ") +
		" FROM (" + base + ")" +
		" ORDER BY bucket_ts ASC"
	return sql
}

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
	return " WHERE " + strings.Join(parts, " AND ")
}

func sqlLit(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
