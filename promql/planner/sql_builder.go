package planner

import (
	"fmt"
	"github.com/cardinalhq/lakerunner/promql"
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
// We always bucket to `step`; for *_over_time/rate/increase we apply a ROWS
// window of K-1 PRECEDING, where K = ceil(range/step). Output is 1 row per step bucket.
func (be *BaseExpr) ToWorkerSQL(step time.Duration) string {
	// Sketch-required paths → worker should return sketches
	if be.WantDDS {
		return ""
	}
	// If func not supported and it's not a topk/bottomk child, skip SQL
	if !supportedFuncs[be.FuncName] && !(be.WantTopK || be.WantBottomK) {
		return ""
	}

	// COUNT fast-path: COUNT-by-group with no identity collapse → pure SQL
	if be.WantCount && equalStringSets(be.CountOnBy, be.GroupBy) {
		return buildStepOnly(be, []proj{{"COUNT(*)", "count"}}, step)
	}
	// Identity collapse (distinct series counting) → HLL path
	if be.WantCount && !equalStringSets(be.CountOnBy, be.GroupBy) {
		return ""
	}

	switch be.FuncName {
	// Sliding-window functions (need range)
	case "sum_over_time":
		return buildWindowed(be, need{sum: true}, step)
	case "avg_over_time":
		return buildWindowed(be, need{sum: true, count: true}, step)
	case "min_over_time":
		return buildWindowed(be, need{min: true}, step)
	case "max_over_time":
		return buildWindowed(be, need{max: true}, step)
	case "rate", "irate":
		// rate = sum_over_time / range_seconds. We push sum window; API can divide by range.
		return buildWindowed(be, need{sum: true}, step)
	case "increase":
		// increase = sum_over_time (over counter deltas). Same windowed SUM; API uses as-is.
		return buildWindowed(be, need{sum: true}, step)

	// Raw/instant—just step bucket aggregates (no sliding window)
	case "":
		return buildStepOnly(be, []proj{
			{"SUM(value)", "sum"},
			{"COUNT(value)", "count"},
		}, step)

	default:
		return ""
	}
}

type proj struct{ expr, alias string }

type need struct {
	sum, count, min, max bool
}

const (
	timePredicate = "\"_cardinalhq.timestamp\" >= {start} AND \"_cardinalhq.timestamp\" < {end}"
)

// --- Builders ---------------------------------------------------------------

// buildStepOnly: densify to one row per step (+ per group), join step aggregates.
func buildStepOnly(be *BaseExpr, projs []proj, step time.Duration) string {
	stepMs := step.Milliseconds()
	bucketExpr := fmt.Sprintf("(\"_cardinalhq.timestamp\" - (\"_cardinalhq.timestamp\" %% %d))", stepMs)

	where := whereFor(be)
	timeWhere := withTime(where)

	// CTE: buckets (one row per step)
	buckets := fmt.Sprintf("buckets AS (SELECT range AS bucket_ts FROM range({start}, {end}, %d))", stepMs)

	var groupsCTE, gridCTE, gridFrom string
	if len(be.GroupBy) > 0 {
		// Distinct groups present in the time range
		groupsCTE = "groups AS (SELECT DISTINCT " + strings.Join(be.GroupBy, ", ") + " FROM {table}" + timeWhere + ")"
		// Cross join to produce full grid of (bucket, group)
		gridCTE = "grid AS (SELECT bucket_ts, " + strings.Join(be.GroupBy, ", ") + " FROM buckets CROSS JOIN groups)"
		gridFrom = "grid g"
	} else {
		gridFrom = "buckets b"
	}

	// Step aggregates
	stepCols := []string{bucketExpr + " AS bucket_ts"}
	// Only sum/count are used by step-only currently
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
		stepCols = append(stepCols, "SUM(value) AS step_sum")
	}
	if needCount {
		stepCols = append(stepCols, "COUNT(value) AS step_count")
	}
	if len(be.GroupBy) > 0 {
		stepCols = append(stepCols, strings.Join(be.GroupBy, ", "))
	}
	stepAgg := "step_aggr AS (SELECT " + strings.Join(stepCols, ", ") +
		" FROM {table}" + timeWhere +
		groupByClause(be.GroupBy, "bucket_ts") + ")"

	// Final select: join grid with step aggregates; COALESCE sums/counts to 0
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
			// (not expected in step-only path)
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

// buildWindowed: densify grid then window over step aggregates.
func buildWindowed(be *BaseExpr, need need, step time.Duration) string {
	stepMs := step.Milliseconds()
	bucketExpr := fmt.Sprintf("(\"_cardinalhq.timestamp\" - (\"_cardinalhq.timestamp\" %% %d))", stepMs)

	where := whereFor(be)
	timeWhere := withTime(where)

	// CTE: buckets
	buckets := fmt.Sprintf("buckets AS (SELECT range AS bucket_ts FROM range({start}, {end}, %d))", stepMs)

	// Optional groups/grid CTEs
	var groupsCTE, gridCTE, gridFrom string
	if len(be.GroupBy) > 0 {
		groupsCTE = "groups AS (SELECT DISTINCT " + strings.Join(be.GroupBy, ", ") + " FROM {table}" + timeWhere + ")"
		gridCTE = "grid AS (SELECT bucket_ts, " + strings.Join(be.GroupBy, ", ") + " FROM buckets CROSS JOIN groups)"
		gridFrom = "grid g"
	} else {
		gridFrom = "buckets b"
	}

	// Step aggregates (per step + group)
	stepCols := []string{bucketExpr + " AS bucket_ts"}
	if need.sum {
		stepCols = append(stepCols, "SUM(value) AS step_sum")
	}
	if need.count {
		stepCols = append(stepCols, "COUNT(value) AS step_count")
	}
	if need.min {
		stepCols = append(stepCols, "MIN(value) AS step_min")
	}
	if need.max {
		stepCols = append(stepCols, "MAX(value) AS step_max")
	}
	if len(be.GroupBy) > 0 {
		stepCols = append(stepCols, strings.Join(be.GroupBy, ", "))
	}
	stepAgg := "step_aggr AS (SELECT " + strings.Join(stepCols, ", ") +
		" FROM {table}" + timeWhere +
		groupByClause(be.GroupBy, "bucket_ts") + ")"

	// Window frame size
	kMinus1 := rowsPreceding(be.Range, step)

	// Final select: join grid with step_aggr; COALESCE sum/count to 0 before windowing
	var baseCols []string
	baseCols = append(baseCols, "bucket_ts")
	if len(be.GroupBy) > 0 {
		baseCols = append(baseCols, strings.Join(be.GroupBy, ", "))
	}
	if need.sum {
		baseCols = append(baseCols, "COALESCE(sa.step_sum, 0) AS w_step_sum")
	}
	if need.count {
		baseCols = append(baseCols, "COALESCE(sa.step_count, 0) AS w_step_count")
	}
	if need.min {
		// min/max: leave NULLs; window MIN/MAX of NULLs stays NULL until a value appears
		baseCols = append(baseCols, "sa.step_min AS w_step_min")
	}
	if need.max {
		baseCols = append(baseCols, "sa.step_max AS w_step_max")
	}

	var part string
	if len(be.GroupBy) > 0 {
		part = "PARTITION BY " + strings.Join(be.GroupBy, ", ")
	}

	order := " ORDER BY bucket_ts"
	var outCols []string
	outCols = append(outCols, "bucket_ts")
	if len(be.GroupBy) > 0 {
		outCols = append(outCols, strings.Join(be.GroupBy, ", "))
	}
	if need.sum {
		outCols = append(outCols,
			fmt.Sprintf("SUM(w_step_sum) OVER (%s%s ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS sum", part, order, kMinus1))
	}
	if need.count {
		outCols = append(outCols,
			fmt.Sprintf("SUM(w_step_count) OVER (%s%s ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS count", part, order, kMinus1))
	}
	if need.min {
		outCols = append(outCols,
			fmt.Sprintf("MIN(w_step_min) OVER (%s%s ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS min", part, order, kMinus1))
	}
	if need.max {
		outCols = append(outCols,
			fmt.Sprintf("MAX(w_step_max) OVER (%s%s ROWS BETWEEN %d PRECEDING AND CURRENT ROW) AS max", part, order, kMinus1))
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
		" FROM (SELECT " + strings.Join(baseCols, ", ") + " FROM " + gridFrom +
		" LEFT JOIN step_aggr sa " + joinKeys + ") " +
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
		parts = append(parts, fmt.Sprintf("metric = %s", sqlLit(be.Metric)))
	}
	for _, m := range be.Matchers {
		switch m.Op {
		case promql.MatchEq:
			parts = append(parts, fmt.Sprintf("%s = %s", m.Label, sqlLit(m.Value)))
		case promql.MatchNe:
			parts = append(parts, fmt.Sprintf("%s <> %s", m.Label, sqlLit(m.Value)))
		case promql.MatchRe:
			parts = append(parts, fmt.Sprintf("%s ~ %s", m.Label, sqlLit(m.Value)))
		case promql.MatchNre:
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
