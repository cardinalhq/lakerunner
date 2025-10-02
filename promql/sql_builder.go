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
	"sort"
	"strings"
	"time"

	"github.com/prometheus/common/model"
)

// normalizeFieldName converts field names from dotted format to underscore format
// e.g., "resource.k8s.namespace.name" -> "resource_k8s_namespace_name"
// This matches the actual column names in Parquet files
func normalizeFieldName(field string) string {
	// Special case: _cardinalhq fields already use underscores
	if strings.HasPrefix(field, "_cardinalhq") {
		return field
	}
	// Convert dots to underscores for other fields
	return strings.ReplaceAll(field, ".", "_")
}

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
	"count_over_time":    true,
	"last_over_time":     true,
	"":                   true, // raw/instant (we still bucket for step)
}

// ToWorkerSQL builds a per-step, per-group SQL (DuckDB).
func (be *BaseExpr) ToWorkerSQL(step time.Duration) string {
	// Sketch-required paths → worker should return sketches
	if be.WantDDS {
		return buildDDS(be, step)
	}
	// If func not supported and it's not a topk/bottomk child, skip SQL
	if !supportedFuncs[be.FuncName] && !(be.WantTopK || be.WantBottomK) {
		return ""
	}

	// Synthetic log metric → build from log leaf
	if be.isSyntheticLogMetric() && be.LogLeaf.ID != "" {
		return buildFromLogLeaf(be, step)
	}

	// COUNT path (no HLL): densify steps and count rows per group.
	// This yields one row per bucket per child-identity group (be.GroupBy).
	// The API/parent agg can then "count the series" per keep-set (e.g. service).
	if be.WantCount && equalStringSets(be.CountOnBy, be.GroupBy) {
		// COUNT(chq_rollup_count) avoids counting rows that have no data in the bucket.
		return buildCountOnly(be, []proj{{"COUNT(chq_rollup_count)", "count"}}, step)
	}

	switch be.FuncName {
	case "sum_over_time":
		return buildStepAggNoWindow(be, need{sum: true}, step)
	case "count_over_time":
		return buildStepAggNoWindow(be, need{sum: true}, step)
	case "avg_over_time":
		return buildStepAggNoWindow(be, need{sum: true, count: true}, step)
	case "min_over_time":
		return buildStepAggNoWindow(be, need{min: true}, step)
	case "max_over_time":
		return buildStepAggNoWindow(be, need{max: true}, step)
	case "rate", "irate":
		return buildStepAggNoWindow(be, need{sum: true}, step)
	case "increase":
		return buildStepAggNoWindow(be, need{sum: true}, step)
	case "":
		if be.Range == "" {
			return buildStepAggNoWindow(be, need{sum: true, min: true, max: true, count: true}, step)
		}
		return buildStepAggNoWindow(be, need{sum: true, count: true, min: true, max: true}, step)
	default:
		return ""
	}
}

func buildFromLogLeaf(be *BaseExpr, step time.Duration) string {
	stepMs := step.Milliseconds()
	tsCol := "\"chq_timestamp\""
	bodyCol := "\"chq_message\""

	pipelineSQL := strings.TrimSpace(be.LogLeaf.ToWorkerSQL(0, "", nil))

	bucketExpr := fmt.Sprintf(
		"(CAST(%s AS BIGINT) - (CAST(%s AS BIGINT) %% %d))",
		tsCol, tsCol, stepMs,
	)

	cols := []string{bucketExpr + " AS bucket_ts"}
	if len(be.GroupBy) > 0 {
		qbys := make([]string, 0, len(be.GroupBy))
		for _, g := range be.GroupBy {
			// Normalize field name from dots to underscores
			fieldName := normalizeFieldName(g)
			qbys = append(qbys, fmt.Sprintf("\"%s\"", fieldName))
		}
		// keep as one joined fragment so downstream code that expects a single element still works
		cols = append(cols, strings.Join(qbys, ", "))
	}

	gb := []string{"bucket_ts"}
	if len(be.GroupBy) > 0 {
		for _, g := range be.GroupBy {
			// Normalize field name from dots to underscores
			fieldName := normalizeFieldName(g)
			gb = append(gb, fmt.Sprintf("\"%s\"", fieldName))
		}
	}

	aggregationToFetch := "sum"
	if be.WantCount || be.FuncName == "count_over_time" {
		aggregationToFetch = "count"
	}

	switch be.Metric {
	case SynthLogUnwrap:
		switch strings.ToLower(be.FuncName) {
		case "min_over_time":
			cols = append(cols, "MIN(__unwrap_value) AS min")
		case "max_over_time":
			cols = append(cols, "MAX(__unwrap_value) AS max")
		case "sum_over_time":
			cols = append(cols, "SUM(__unwrap_value) AS sum")
		case "count_over_time":
			cols = append(cols, "COUNT(__unwrap_value) AS count")
		case "avg_over_time":
			cols = append(cols, "SUM(__unwrap_value) AS sum", "COUNT(__unwrap_value) AS count")
		case "rate", "irate", "increase":
			cols = append(cols, "SUM(__unwrap_value) AS sum")
		default:
			// Safe default: provide both so callers can derive avg, etc.
			cols = append(cols, "SUM(__unwrap_value) AS sum", "COUNT(__unwrap_value) AS count")
		}

		sql := "WITH _leaf AS (" + pipelineSQL + ")" +
			" SELECT " + strings.Join(cols, ", ") +
			" FROM _leaf" +
			" WHERE " + timePredicate + " AND __unwrap_value IS NOT NULL" +
			" GROUP BY " + strings.Join(gb, ", ") +
			" ORDER BY bucket_ts ASC"
		return sql

	case SynthLogBytes:
		weight := fmt.Sprintf("length(COALESCE(%s, ''))", bodyCol)
		cols = append(cols, "SUM("+weight+") AS "+aggregationToFetch)

	default:
		if aggregationToFetch == "count" {
			cols = append(cols, "COUNT(*) AS count")
		} else {
			cols = append(cols, "SUM(1) AS sum")
		}
	}

	sql := "WITH _leaf AS (" + pipelineSQL + ")" +
		" SELECT " + strings.Join(cols, ", ") +
		" FROM _leaf" +
		" WHERE " + timePredicate +
		" GROUP BY " + strings.Join(gb, ", ") +
		" ORDER BY bucket_ts ASC"

	return sql
}

func (be *BaseExpr) ToWorkerSQLForTagValues(step time.Duration, tagName string) string {
	// Build WHERE clause with metric name and matchers
	where := withTime(whereFor(be))

	// Add filter to ensure the tag column exists and is not null
	tagFilter := fmt.Sprintf(" AND \"%s\" IS NOT NULL", tagName)
	where += tagFilter

	// Build the SQL query to get distinct tag values
	sql := "SELECT DISTINCT \"" + tagName + "\" AS tag_value" +
		" FROM {table}" + where +
		" ORDER BY tag_value ASC"

	return sql
}

func buildStepAggNoWindow(be *BaseExpr, need need, step time.Duration) string {
	stepMs := step.Milliseconds()
	where := withTime(whereFor(be))

	bucketExpr := fmt.Sprintf(
		"(CAST(\"chq_timestamp\" AS BIGINT) - (CAST(\"chq_timestamp\" AS BIGINT) %% %d))",
		stepMs,
	)

	// Quote group-by once
	var gbq []string
	if len(be.GroupBy) > 0 {
		gbq = make([]string, 0, len(be.GroupBy))
		for _, f := range be.GroupBy {
			// Normalize field name from dots to underscores
			fieldName := normalizeFieldName(f)
			gbq = append(gbq, fmt.Sprintf("\"%s\"", fieldName))
		}
	}

	// Canonical aliases so MAP paths (and tests) are stable: sum,count,min,max
	cols := []string{bucketExpr + " AS bucket_ts"}
	if need.sum {
		cols = append(cols, "SUM(chq_rollup_sum) AS sum")
	}
	if need.count {
		cols = append(cols, "SUM(COALESCE(chq_rollup_count, 0)) AS count")
	}
	if need.min {
		cols = append(cols, "MIN(chq_rollup_min) AS min")
	}
	if need.max {
		cols = append(cols, "MAX(chq_rollup_max) AS max")
	}
	if len(gbq) > 0 {
		cols = append(cols, strings.Join(gbq, ", "))
	}

	gb := []string{"bucket_ts"}
	if len(gbq) > 0 {
		gb = append(gb, gbq...)
	}

	sql := "SELECT " + strings.Join(cols, ", ") +
		" FROM {table}" + where +
		" GROUP BY " + strings.Join(gb, ", ") +
		" ORDER BY bucket_ts ASC"
	return sql
}

// buildDDS: bucket timestamps, project group-by labels + sketch
func buildDDS(be *BaseExpr, step time.Duration) string {
	stepMs := step.Milliseconds()
	bucket := fmt.Sprintf("(\"chq_timestamp\" - (\"chq_timestamp\" %% %d))", stepMs)

	// Use aligned, end-exclusive time like the numeric paths
	alignedStart := fmt.Sprintf("({start} - ({start} %% %d))", stepMs)
	alignedEndEx := fmt.Sprintf("(({end} - 1) - (({end} - 1) %% %d) + %d)", stepMs, stepMs)

	base := whereFor(be)
	timeWhere := func() string {
		tc := fmt.Sprintf("\"chq_timestamp\" >= %s AND \"chq_timestamp\" < %s", alignedStart, alignedEndEx)
		if base == "" {
			return " WHERE " + tc
		}
		return base + " AND " + tc
	}()

	cols := []string{bucket + " AS bucket_ts"}
	if len(be.GroupBy) > 0 {
		// Normalize field names from dots to underscores
		normalizedGroups := make([]string, len(be.GroupBy))
		for i, g := range be.GroupBy {
			normalizedGroups[i] = normalizeFieldName(g)
		}
		cols = append(cols, strings.Join(normalizedGroups, ", "))
	}
	cols = append(cols, "chq_sketch")

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
	timePredicate = "\"chq_timestamp\" >= {start} AND \"chq_timestamp\" < {end}"
)

func buildCountOnly(be *BaseExpr, projs []proj, step time.Duration) string {
	stepMs := step.Milliseconds()
	bucketExpr := fmt.Sprintf("(\"chq_timestamp\" - (\"chq_timestamp\" %% %d))", stepMs)

	where := withTime(whereFor(be))

	// Align [start, end) to step boundaries.
	alignedStart := fmt.Sprintf("({start} - ({start} %% %d))", stepMs)
	alignedEndExclusive := fmt.Sprintf("(({end} - 1) - (({end} - 1) %% %d) + %d)", stepMs, stepMs)

	// Buckets CTE: dense time grid.
	buckets := fmt.Sprintf(
		"buckets AS (SELECT range AS bucket_ts FROM range(%s, %s, %d))",
		alignedStart, alignedEndExclusive, stepMs,
	)

	// Single source CTE so we only have ONE `{table}` to replace.
	// Apply all filters (incl. time) here and reuse below.
	source := "src AS (SELECT * FROM {table}" + where + ")"

	quote := func(id string) string { return fmt.Sprintf("\"%s\"", id) }
	var gbq []string
	for _, g := range be.GroupBy {
		// Normalize field name from dots to underscores
		fieldName := normalizeFieldName(g)
		gbq = append(gbq, quote(fieldName))
	}

	// Optional groups/grid CTEs for densification by group keys.
	var groupsCTE, gridCTE, gridFrom string
	if len(gbq) > 0 {
		groupsCTE = "groups AS (SELECT DISTINCT " + strings.Join(gbq, ", ") + " FROM src)"
		gridCTE = "grid AS (SELECT bucket_ts, " + strings.Join(gbq, ", ") + " FROM buckets CROSS JOIN groups)"
		gridFrom = "grid g"
	} else {
		gridFrom = "buckets b"
	}

	// Step aggregates per (bucket + group)
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
		stepCols = append(stepCols, "SUM(chq_rollup_sum) AS step_sum")
	}
	if needCount {
		stepCols = append(stepCols, "COUNT(chq_rollup_count) AS step_count")
	}
	if len(gbq) > 0 {
		stepCols = append(stepCols, strings.Join(gbq, ", "))
	}

	gb := []string{"bucket_ts"}
	if len(gbq) > 0 {
		gb = append(gb, gbq...)
	}
	stepAgg := "step_aggr AS (SELECT " + strings.Join(stepCols, ", ") +
		" FROM src GROUP BY " + strings.Join(gb, ", ") + ")"

	// Final projection from the dense grid LEFT JOIN step_aggr
	var outCols []string
	if len(gbq) == 0 {
		outCols = append(outCols, "b.bucket_ts")
	} else {
		outCols = append(outCols, "g.bucket_ts")
		for _, c := range gbq {
			outCols = append(outCols, "g."+c)
		}
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

	joinOn := func() string {
		var conds []string
		if len(gbq) == 0 {
			conds = []string{"b.bucket_ts = sa.bucket_ts"}
		} else {
			conds = []string{"g.bucket_ts = sa.bucket_ts"}
			for _, c := range gbq {
				conds = append(conds, "g."+c+" = sa."+c)
			}
		}
		return strings.Join(conds, " AND ")
	}()

	withs := []string{buckets, source}
	if groupsCTE != "" {
		withs = append(withs, groupsCTE, gridCTE)
	}
	withs = append(withs, stepAgg)

	orderBy := "b.bucket_ts ASC"
	if len(gbq) > 0 {
		orderBy = "g.bucket_ts ASC"
	}

	sql := "WITH " + strings.Join(withs, ", ") +
		" SELECT " + strings.Join(outCols, ", ") +
		" FROM " + gridFrom +
		" LEFT JOIN step_aggr sa ON " + joinOn +
		" ORDER BY " + orderBy

	return sql
}

func RangeMsFromRange(rangeStr string) int64 {
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
func withTime(where string) string {
	if where == "" {
		return " WHERE " + timePredicate
	}
	return where + " AND " + timePredicate
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

	// For synthetic log metrics, we do NOT filter by metric name at all.
	if be.Metric != "" && !be.isSyntheticLogMetric() {
		parts = append(parts, fmt.Sprintf("\"chq_name\" = %s", sqlLit(be.Metric)))
	}

	for _, m := range be.Matchers {
		if m.Label == LeafMatcher {
			continue
		}
		// Normalize field name from dots to underscores
		fieldName := normalizeFieldName(m.Label)
		switch m.Op {
		case MatchEq:
			parts = append(parts, fmt.Sprintf("\"%s\" = %s", fieldName, sqlLit(m.Value)))
		case MatchNe:
			parts = append(parts, fmt.Sprintf("\"%s\" <> %s", fieldName, sqlLit(m.Value)))
		case MatchRe:
			parts = append(parts, fmt.Sprintf("\"%s\" ~ %s", fieldName, sqlLit(m.Value)))
		case MatchNre:
			parts = append(parts, fmt.Sprintf("\"%s\" !~ %s", fieldName, sqlLit(m.Value)))
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
