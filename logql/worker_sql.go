// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package logql

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

func (be *LogLeaf) ToWorkerSQL(limit int, order string, fields []string) string {
	const baseRel = "{table}"
	const bodyCol = "\"log_message\""
	const tsCol = "\"chq_timestamp\""

	// 1) Prepare sets: group keys, parser-created, feature flags
	groupKeys := dedupeStrings(be.OutBy)
	parserCreated, hasJSON, hasLogFmt := analyzeParsers(be)

	// If json/logfmt appear, only mark group keys as parser-created when the key:
	//  - is not a base column, and
	//  - is not referenced by a selector matcher.
	if hasJSON || hasLogFmt {
		matcherSet := map[string]struct{}{}
		for _, m := range be.Matchers {
			matcherSet[m.Label] = struct{}{}
		}
		for _, k := range groupKeys {
			if _, isMatcher := matcherSet[k]; isMatcher {
				continue
			}
			qk := quoteIdent(k)
			if !isBaseCol(qk) {
				parserCreated[k] = struct{}{}
			}
		}
	}

	// 2) Build CTE pipeline
	pb := newPipelineBuilder()

	// s0: always SELECT * from the base relation
	pb.push([]string{"*"}, baseRel, nil)

	// s0+: normalize fingerprint type to string once up-front so downstream filters/clients are stable
	pb.push([]string{
		pb.top() + `.* REPLACE(CAST("chq_fingerprint" AS VARCHAR) AS "chq_fingerprint")`,
	}, pb.top(), nil)

	// s1: time window sentinel so segment filters can be spliced
	timePred := fmt.Sprintf(
		"CAST(%s AS BIGINT) >= {start} AND CAST(%s AS BIGINT) < {end}",
		tsCol, tsCol,
	)
	pb.push([]string{pb.top() + ".*"}, pb.top(), []string{"1=1", "true", timePred})

	// --- Split line filters into pre- vs post-parser using ParserIdx (nil => pre)
	preLF, postLF := partitionLineFilters(be.LineFilters)

	// 3) Apply selector & PRE-parser line filters
	emitSelectorAndPreLineFilters(be, &pb, bodyCol, preLF)

	// 4) Collect future-created labels (from label_format), unwrap needs
	futureCreated := collectFutureCreated(be)
	unwrapNeeded := collectUnwrapNeeds(be)

	// 5) Emit parsers left→right, pushing label filters as soon as labels exist
	remainingLbl := append([]LabelFilter(nil), be.LabelFilters...)
	emitParsersWithPostLineFilters(
		be, &pb, bodyCol,
		groupKeys, futureCreated, unwrapNeeded,
		&remainingLbl, &postLF,
		fields, parserCreated,
	)

	// 6) Any remaining label filters (base columns) → apply at the end
	if len(remainingLbl) > 0 {
		if where := buildLabelFilterWhere(remainingLbl, nil); len(where) > 0 {
			pb.push([]string{pb.top() + ".*"}, pb.top(), where)
		}
	}

	// 7) Final SELECT (exemplars → ORDER/LIMIT only when no range agg)
	return finalizeSelect(&pb, tsCol, be.RangeAggOp == "", order, limit)
}

/* ---------------- helpers ---------------- */

type pipelineBuilder struct {
	layers  []struct{ name, sql string }
	layerIx int
}

func newPipelineBuilder() pipelineBuilder  { return pipelineBuilder{} }
func (p *pipelineBuilder) mk(i int) string { return fmt.Sprintf("s%d", i) }
func (p *pipelineBuilder) top() string     { return p.mk(p.layerIx - 1) }

func (p *pipelineBuilder) push(selectList []string, from string, whereConds []string) {
	alias := p.mk(p.layerIx)
	p.layerIx++
	if len(selectList) == 0 {
		selectList = []string{"*"}
	}
	sl := strings.Join(selectList, ", ")
	var where string
	if len(whereConds) > 0 {
		where = " WHERE " + strings.Join(whereConds, " AND ")
	}
	sql := fmt.Sprintf("%s AS (\n  SELECT %s\n  FROM %s%s\n)", alias, sl, from, where)
	p.layers = append(p.layers, struct{ name, sql string }{name: alias, sql: sql})
}

func finalizeSelect(p *pipelineBuilder, tsCol string, wantOrder bool, order string, limit int) string {
	var sb strings.Builder
	sb.WriteString("WITH\n")
	for i, l := range p.layers {
		sb.WriteString("  ")
		sb.WriteString(l.sql)
		if i != len(p.layers)-1 {
			sb.WriteString(",\n")
		} else {
			sb.WriteString("\n")
		}
	}

	sb.WriteString("SELECT * FROM ")
	sb.WriteString(p.top())

	if wantOrder {
		dir := strings.ToUpper(strings.TrimSpace(order))
		if dir != "ASC" && dir != "DESC" {
			dir = "DESC"
		}
		sb.WriteString(" ORDER BY ")
		sb.WriteString(tsCol)
		sb.WriteByte(' ')
		sb.WriteString(dir)

		if limit > 0 {
			sb.WriteString(fmt.Sprintf(" LIMIT %d", limit))
		}
	}
	return sb.String()
}

/* --- analysis helpers --- */

func analyzeParsers(be *LogLeaf) (parserCreated map[string]struct{}, hasJSON, hasLogFmt bool) {
	parserCreated = make(map[string]struct{})
	for _, p := range be.Parsers {
		switch strings.ToLower(p.Type) {
		case "label_format", "label-format", "labelformat":
			if len(p.LabelFormats) > 0 {
				for _, lf := range p.LabelFormats {
					parserCreated[lf.Out] = struct{}{}
				}
			} else {
				for out := range p.Params {
					parserCreated[out] = struct{}{}
				}
			}
		case "regexp":
			// only named captures are considered "created" at this step
			for _, name := range regexCaptureNames(p.Params["pattern"]) {
				if !strings.HasPrefix(name, "__var_") {
					parserCreated[name] = struct{}{}
				}
			}
		case "json":
			hasJSON = true
		case "logfmt":
			hasLogFmt = true
		}
	}
	return
}

func collectTemplateDeps(tmpl string) []string {
	deps := map[string]struct{}{}
	_, _ = buildLabelFormatExprTemplate(tmpl, func(name string) string {
		// Don't convert dots here - preserve original names for JSON paths
		deps[name] = struct{}{}
		return "0"
	})
	out := make([]string, 0, len(deps))
	for k := range deps {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

/* --- line filters: pre vs post --- */

// Requires LineFilter to carry:
//
//	AfterParser bool          // true iff it syntactically appears after a parser
//	ParserIdx   *int          // index of parser it follows; nil => pre-parser
func partitionLineFilters(all []LineFilter) (pre []LineFilter, post []LineFilter) {
	for _, lf := range all {
		if lf.ParserIdx == nil {
			pre = append(pre, lf)
		} else {
			post = append(post, lf)
		}
	}
	return
}

func emitSelectorAndPreLineFilters(be *LogLeaf, pb *pipelineBuilder, bodyCol string, preLF []LineFilter) {
	// selector matchers as label filters
	if len(be.Matchers) > 0 {
		var mLfs []LabelFilter
		for _, m := range be.Matchers {
			mLfs = append(mLfs, LabelFilter{Label: m.Label, Op: m.Op, Value: m.Value})
		}
		if where := buildLabelFilterWhere(mLfs, nil); len(where) > 0 {
			pb.push([]string{pb.top() + ".*"}, pb.top(), where)
		}
	}
	// pre-parser line filters only
	if len(preLF) > 0 {
		if where := buildLineFilterWhere(preLF, bodyCol); len(where) > 0 {
			pb.push([]string{pb.top() + ".*"}, pb.top(), where)
		}
	}
}

// Apply the subset of post line-filters that should run *right after* parser `i`.
func applyLineFiltersAt(pb *pipelineBuilder, bodyCol string, remaining *[]LineFilter, parserIdx int) {
	if remaining == nil || len(*remaining) == 0 {
		return
	}
	var now, later []LineFilter
	for _, lf := range *remaining {
		if lf.ParserIdx != nil && *lf.ParserIdx == parserIdx {
			now = append(now, lf)
		} else {
			later = append(later, lf)
		}
	}
	if len(now) > 0 {
		if where := buildLineFilterWhere(now, bodyCol); len(where) > 0 {
			pb.push([]string{pb.top() + ".*"}, pb.top(), where)
		}
	}
	*remaining = later
}

/* --- future-created & unwrap needs --- */

func collectFutureCreated(be *LogLeaf) map[string]struct{} {
	future := make(map[string]struct{})
	for _, p := range be.Parsers {
		switch strings.ToLower(p.Type) {
		case "label_format", "label-format", "labelformat":
			if len(p.LabelFormats) > 0 {
				for _, lf := range p.LabelFormats {
					future[lf.Out] = struct{}{}
				}
			} else {
				for k := range p.Params {
					future[k] = struct{}{}
				}
			}
		}
	}
	return future
}

func collectUnwrapNeeds(be *LogLeaf) map[string]struct{} {
	needs := make(map[string]struct{})
	for _, p := range be.Parsers {
		if strings.ToLower(p.Type) == "unwrap" {
			if f := strings.TrimSpace(p.Params["field"]); f != "" {
				needs[f] = struct{}{}
			}
		}
	}
	return needs
}

/* --- parser emission (with post line-filters) --- */

func emitParsersWithPostLineFilters(
	be *LogLeaf,
	pb *pipelineBuilder,
	bodyCol string,
	groupKeys []string,
	futureCreated map[string]struct{},
	unwrapNeeded map[string]struct{},
	remainingLbl *[]LabelFilter, // label filters
	remainingLine *[]LineFilter, // post line filters
	fields []string,
	parserCreated map[string]struct{},
) {
	// Track which logical columns currently exist (unquoted names).
	present := make(map[string]struct{})
	addPresent := func(names ...string) {
		for _, n := range names {
			if n != "" {
				present[n] = struct{}{}
			}
		}
	}

	// base cols
	addPresent("log_message", "chq_timestamp", "chq_id", "log_level", "chq_fingerprint")
	// matchers
	for _, m := range be.Matchers {
		addPresent(m.Label)
	}
	// non-parser group-by keys
	for _, k := range groupKeys {
		if _, created := parserCreated[k]; !created {
			addPresent(k)
		}
	}
	// non-parser fields
	for _, f := range fields {
		if _, created := parserCreated[f]; !created {
			addPresent(f)
		}
	}

	uniqLabels := func(lfs []LabelFilter) []string {
		set := map[string]struct{}{}
		for _, lf := range lfs {
			set[lf.Label] = struct{}{}
		}
		out := make([]string, 0, len(set))
		for k := range set {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	excludeFuture := func(keys []string) []string {
		out := make([]string, 0, len(keys))
		for _, k := range keys {
			if _, later := futureCreated[k]; !later {
				out = append(out, k)
			}
		}
		return out
	}
	mkSet := func(keys []string) map[string]struct{} {
		m := make(map[string]struct{}, len(keys))
		for _, k := range keys {
			m[k] = struct{}{}
		}
		return m
	}

	// index-based loop so we can look ahead
	for i := range be.Parsers {
		p := be.Parsers[i]

		switch strings.ToLower(p.Type) {

		case "regexp":
			patNorm := p.Params["pattern"]

			// 2) Make unnamed groups non-capturing so named groups get contiguous 1..N indices.
			pat := namedOnlyPattern(patNorm)

			// Everything below stays the same, but uses `pat` for both analysis and SQL:
			order := regexCaptureOrder(pat)
			if len(order) == 0 {
				pb.push([]string{pb.top() + ".*"}, pb.top(), nil)
				if len(p.Filters) > 0 {
					*remainingLbl = append(*remainingLbl, p.Filters...)
				}
				applyLineFiltersAt(pb, bodyCol, remainingLine, i)
				break
			}

			nameToIdx := map[string]int{}
			var named []string
			for idx, n := range order {
				if strings.HasPrefix(n, "__g") {
					continue
				}
				if _, seen := nameToIdx[n]; !seen {
					nameToIdx[n] = idx + 1
					named = append(named, n)
				}
			}

			if len(named) == 0 {
				pb.push([]string{pb.top() + ".*"}, pb.top(), nil)
				if len(p.Filters) > 0 {
					*remainingLbl = append(*remainingLbl, p.Filters...)
				}
				applyLineFiltersAt(pb, bodyCol, remainingLine, i)
				break
			}

			var replacePairs, addCols []string
			star := pb.top() + ".*"
			created := make(map[string]struct{}, len(named))

			for _, n := range named {
				idx := nameToIdx[n]
				expr := fmt.Sprintf(
					"regexp_extract(%s, %s, %d) AS %s",
					bodyCol, sqlQuote(pat), idx, quoteIdent(n),
				)
				if _, ok := present[n]; ok {
					replacePairs = append(replacePairs, expr)
				} else {
					addCols = append(addCols, expr)
				}
				created[n] = struct{}{}
			}

			sel := []string{}
			if len(replacePairs) > 0 {
				sel = append(sel, star+" REPLACE("+strings.Join(replacePairs, ", ")+")")
			} else {
				sel = append(sel, star)
			}
			sel = append(sel, addCols...)
			pb.push(sel, pb.top(), nil)

			for n := range created {
				addPresent(n)
			}
			allFilters := append(append([]LabelFilter{}, *remainingLbl...), p.Filters...)
			now, later := partitionByNames(allFilters, created)
			if len(now) > 0 {
				if where := buildLabelFilterWhere(now, nil); len(where) > 0 {
					pb.push([]string{pb.top() + ".*"}, pb.top(), where)
				}
			}
			*remainingLbl = later
			applyLineFiltersAt(pb, bodyCol, remainingLine, i)

		case "json":
			// Keys needed by filters + group-by + unwrap.
			baseFilters := append(append([]LabelFilter{}, *remainingLbl...), p.Filters...)
			needKeys := uniqLabels(baseFilters)
			needKeys = append(needKeys, groupKeys...)
			for f := range unwrapNeeded {
				needKeys = append(needKeys, f)
			}

			// LOOK-AHEAD: include deps referenced by later line_format / label_format
			for j := i + 1; j < len(be.Parsers); j++ {
				pj := be.Parsers[j]
				switch strings.ToLower(pj.Type) {
				case "line_format", "line-format", "lineformat":
					if t := strings.TrimSpace(pj.Params["template"]); t != "" {
						for _, d := range collectTemplateDeps(t) {
							if !isBaseCol(quoteIdent(d)) {
								needKeys = append(needKeys, d)
							}
						}
					}
				case "label_format", "label-format", "labelformat":
					for _, tmpl := range pj.Params {
						if strings.TrimSpace(tmpl) == "" {
							continue
						}
						for _, d := range collectTemplateDeps(tmpl) {
							if !isBaseCol(quoteIdent(d)) {
								needKeys = append(needKeys, d)
							}
						}
					}
				}
			}

			// Exclude future-created (label_format) outputs and dedupe
			needKeys = dedupeStrings(excludeFuture(needKeys))

			// Filter: JSON should ONLY create non-base labels that truly need creation.
			filtered := make([]string, 0, len(needKeys))
			for _, k := range needKeys {
				if isBaseCol(quoteIdent(k)) {
					continue
				}
				if _, need := unwrapNeeded[k]; need {
					filtered = append(filtered, k)
					continue
				}
				if _, pc := parserCreated[k]; pc {
					filtered = append(filtered, k)
					continue
				}
				if _, already := present[k]; !already {
					filtered = append(filtered, k)
				}
			}
			needKeys = dedupeStrings(filtered)

			var replacePairs []string
			var addCols []string
			star := pb.top() + ".*"

			created := make(map[string]struct{})

			// explicit mappings first
			if len(p.Params) > 0 {
				keys := sortedKeys(p.Params)
				for _, out := range keys {
					rawPath := strings.TrimSpace(p.Params[out])
					if rawPath == "" {
						continue
					}
					path := jsonPathFromMapping(rawPath)
					expr := fmt.Sprintf("json_extract_string(%s, %s) AS %s",
						bodyCol, sqlQuote(path), quoteIdent(out),
					)
					if _, ok := present[out]; ok {
						replacePairs = append(replacePairs, expr)
					} else {
						addCols = append(addCols, expr)
					}
					created[out] = struct{}{}
				}
			}

			// auto-projected keys (filtered)
			for _, k := range needKeys {
				if _, ok := created[k]; ok {
					continue
				}
				// Don't extract base columns from JSON - they should already exist
				if strings.HasPrefix(k, "resource_") || strings.HasPrefix(k, "attr_") ||
					strings.HasPrefix(k, "scope_") || strings.HasPrefix(k, "chq_") ||
					strings.HasPrefix(k, "metric_") {
					continue
				}
				path := jsonPathForKey(k)
				if strings.Contains(k, ".") {
					path = jsonPathFromMapping(k)
				}
				// Convert dots to underscores for column name
				colName := strings.ReplaceAll(k, ".", "_")
				expr := fmt.Sprintf("json_extract_string(%s, %s) AS %s",
					bodyCol, sqlQuote(path), quoteIdent(colName),
				)
				if _, ok := present[colName]; ok {
					replacePairs = append(replacePairs, expr)
				} else {
					addCols = append(addCols, expr)
				}
				created[colName] = struct{}{}
			}

			sel := []string{}
			if len(replacePairs) > 0 {
				sel = append(sel, star+" REPLACE("+strings.Join(replacePairs, ", ")+")")
			} else {
				sel = append(sel, star)
			}
			if len(addCols) > 0 {
				sel = append(sel, addCols...)
			} else if len(created) == 0 {
				// Fallback: explicit __json when nothing is extracted
				sel = append(sel, fmt.Sprintf("to_json(%s) AS __json", bodyCol))
			}
			pb.push(sel, pb.top(), nil)

			// Apply any label filters whose columns now exist (global remaining + stage filters)
			for k := range created {
				addPresent(k)
			}
			allFilters := append(append([]LabelFilter{}, *remainingLbl...), p.Filters...)
			now, later := partitionByNames(allFilters, created)
			if len(now) > 0 {
				where := buildLabelFilterWhere(now, nil)
				if len(where) > 0 {
					pb.push([]string{pb.top() + ".*"}, pb.top(), where)
				}
			}
			*remainingLbl = later

			// Post line-filters after this parser
			applyLineFiltersAt(pb, bodyCol, remainingLine, i)

		case "logfmt":
			// Same logic as JSON for deciding which keys we need.
			baseFilters := append(append([]LabelFilter{}, *remainingLbl...), p.Filters...)
			needKeys := uniqLabels(baseFilters)
			needKeys = append(needKeys, groupKeys...)
			for f := range unwrapNeeded {
				needKeys = append(needKeys, f)
			}

			// LOOK-AHEAD for later templates
			for j := i + 1; j < len(be.Parsers); j++ {
				pj := be.Parsers[j]
				switch strings.ToLower(pj.Type) {
				case "line_format", "line-format", "lineformat":
					if t := strings.TrimSpace(pj.Params["template"]); t != "" {
						for _, d := range collectTemplateDeps(t) {
							if !isBaseCol(quoteIdent(d)) {
								needKeys = append(needKeys, d)
							}
						}
					}
				case "label_format", "label-format", "labelformat":
					for _, tmpl := range pj.Params {
						if strings.TrimSpace(tmpl) == "" {
							continue
						}
						for _, d := range collectTemplateDeps(tmpl) {
							if !isBaseCol(quoteIdent(d)) {
								needKeys = append(needKeys, d)
							}
						}
					}
				}
			}

			needKeys = dedupeStrings(excludeFuture(needKeys))

			// Filter like JSON: never auto-project base; only create if needed.
			filtered := make([]string, 0, len(needKeys))
			for _, k := range needKeys {
				if isBaseCol(quoteIdent(k)) {
					continue
				}
				if _, need := unwrapNeeded[k]; need {
					filtered = append(filtered, k)
					continue
				}
				if _, pc := parserCreated[k]; pc {
					filtered = append(filtered, k)
					continue
				}
				if _, already := present[k]; !already {
					filtered = append(filtered, k)
				}
			}
			needKeys = dedupeStrings(filtered)

			var replacePairs []string
			var addCols []string
			star := pb.top() + ".*"

			for _, k := range needKeys {
				reKey := fmt.Sprintf(`(?:^|\s)%s=([^\s]+)`, regexp.QuoteMeta(k))
				expr := fmt.Sprintf(
					"regexp_extract(%s, %s, 1) AS %s",
					bodyCol, sqlQuote(reKey), quoteIdent(k),
				)
				if _, ok := present[k]; ok {
					replacePairs = append(replacePairs, expr)
				} else {
					addCols = append(addCols, expr)
				}
			}

			var sel []string
			if len(replacePairs) > 0 {
				sel = append(sel, star+" REPLACE("+strings.Join(replacePairs, ", ")+")")
			} else {
				sel = append(sel, star)
			}
			if len(addCols) > 0 {
				sel = append(sel, addCols...)
			}
			pb.push(sel, pb.top(), nil)

			if len(needKeys) > 0 {
				for _, k := range needKeys {
					addPresent(k)
				}
				created := mkSet(needKeys)
				allFilters := append(append([]LabelFilter{}, *remainingLbl...), p.Filters...)
				now, later := partitionByNames(allFilters, created)
				if len(now) > 0 {
					where := buildLabelFilterWhere(now, nil)
					if len(where) > 0 {
						pb.push([]string{pb.top() + ".*"}, pb.top(), where)
					}
				}
				*remainingLbl = later
			} else if len(p.Filters) > 0 {
				*remainingLbl = append(*remainingLbl, p.Filters...)
			}

			// Post line-filters after this parser
			applyLineFiltersAt(pb, bodyCol, remainingLine, i)

		case "label_format", "label-format", "labelformat":
			// Compute label_format outputs; REPLACE if present else ADD
			var replacePairs []string
			var addCols []string
			star := pb.top() + ".*"

			created := make(map[string]struct{})

			if len(p.LabelFormats) > 0 {
				for _, lf := range p.LabelFormats {
					expr := fmt.Sprintf("(%s) AS %s", lf.SQL, quoteIdent(lf.Out))
					if _, ok := present[lf.Out]; ok {
						replacePairs = append(replacePairs, expr)
					} else {
						addCols = append(addCols, expr)
					}
					created[lf.Out] = struct{}{}
				}
			} else {
				keys := sortedKeys(p.Params)
				for _, out := range keys {
					exprSQL, err := buildLabelFormatExprTemplate(p.Params[out], func(s string) string {
						// Convert dots to underscores for field names
						normalized := strings.ReplaceAll(s, ".", "_")
						return quoteIdent(normalized)
					})
					if err != nil {
						exprSQL = "''"
					}
					expr := fmt.Sprintf("(%s) AS %s", exprSQL, quoteIdent(out))
					if _, ok := present[out]; ok {
						replacePairs = append(replacePairs, expr)
					} else {
						addCols = append(addCols, expr)
					}
					created[out] = struct{}{}
				}
			}

			sel := []string{}
			if len(replacePairs) > 0 {
				sel = append(sel, star+" REPLACE("+strings.Join(replacePairs, ", ")+")")
			} else {
				sel = append(sel, star)
			}
			sel = append(sel, addCols...)
			pb.push(sel, pb.top(), nil)

			for out := range created {
				addPresent(out)
			}

			// Filters: apply those that now exist (label filters)
			allFilters := append(append([]LabelFilter{}, *remainingLbl...), p.Filters...)
			now, later := partitionByNames(allFilters, created)
			if len(now) > 0 {
				where := buildLabelFilterWhere(now, nil)
				if len(where) > 0 {
					pb.push([]string{pb.top() + ".*"}, pb.top(), where)
				}
			}
			*remainingLbl = later

			// Post line-filters after this parser
			applyLineFiltersAt(pb, bodyCol, remainingLine, i)

		case "line_format":
			// Build a SQL expr from the Go-template-like string
			tmpl := strings.TrimSpace(p.Params["template"])
			if tmpl == "" {
				// No template → no-op pass-through, but still carry stage-level label filters
				pb.push([]string{pb.top() + ".*"}, pb.top(), nil)
				if len(p.Filters) > 0 {
					*remainingLbl = append(*remainingLbl, p.Filters...)
				}
				// Even with a no-op, apply post line-filters after this stage if any
				applyLineFiltersAt(pb, bodyCol, remainingLine, i)
				break
			}

			expr, err := buildLabelFormatExprTemplate(
				tmpl,
				func(col string) string {
					// Convert dots to underscores for field names
					normalized := strings.ReplaceAll(col, ".", "_")
					return quoteIdent(normalized)
				},
			)
			if err != nil {
				expr = "''"
			}

			// Replace the message column with the formatted result.
			sel := []string{
				pb.top() + `.* EXCLUDE "` + `log_message` + `"`,
				"(" + expr + `) AS "` + `log_message` + `"`,
			}
			pb.push(sel, pb.top(), nil)

			// message still present
			addPresent("log_message")

			// line_format doesn't create new labels; any stage label filters are carried
			if len(p.Filters) > 0 {
				*remainingLbl = append(*remainingLbl, p.Filters...)
			}

			// CRITICAL: apply the line filters that appear *after* this line_format
			applyLineFiltersAt(pb, bodyCol, remainingLine, i)

		case "unwrap":
			// Compute __unwrap_value (DOUBLE) based on previously created label
			field := strings.TrimSpace(p.Params["field"])
			fn := strings.ToLower(strings.TrimSpace(p.Params["func"]))
			if field == "" {
				pb.push([]string{pb.top() + ".*"}, pb.top(), nil)
				applyLineFiltersAt(pb, bodyCol, remainingLine, i)
				break
			}
			col := quoteIdent(field)
			var core string
			switch fn {
			case "duration":
				core = unwrapDurationExpr(col)
			case "bytes":
				core = unwrapBytesExpr(col)
			default:
				core = fmt.Sprintf("try_cast(%s AS DOUBLE)", col)
			}
			star := pb.top() + ".*"
			sel := []string{}
			if _, ok := present["__unwrap_value"]; ok {
				sel = append(sel, star+" REPLACE("+core+" AS __unwrap_value)")
			} else {
				sel = append(sel, star, core+" AS __unwrap_value")
			}
			pb.push(sel, pb.top(), nil)

			addPresent("__unwrap_value", field)

			created := map[string]struct{}{"__unwrap_value": {}, field: {}}
			allFilters := append(append([]LabelFilter{}, *remainingLbl...), p.Filters...)
			now, later := partitionByNames(allFilters, created)
			if len(now) > 0 {
				where := buildLabelFilterWhere(now, nil)
				if len(where) > 0 {
					pb.push([]string{pb.top() + ".*"}, pb.top(), where)
				}
			}
			*remainingLbl = later

			// Post line-filters after this parser
			applyLineFiltersAt(pb, bodyCol, remainingLine, i)

		default:
			// Unknown stage: still allow stage-level label filters to be carried forward.
			pb.push([]string{pb.top() + ".*"}, pb.top(), nil)
			if len(p.Filters) > 0 {
				*remainingLbl = append(*remainingLbl, p.Filters...)
			}
			// And still respect any post line-filters directed at this index
			applyLineFiltersAt(pb, bodyCol, remainingLine, i)
		}
	}
}

/* --- tiny utils --- */

func dedupeStrings(ss []string) []string {
	m := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		if s != "" {
			m[s] = struct{}{}
		}
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// jsonPathFromMapping converts a user-supplied mapping like "a.b.c" into "$.a.b.c".
// - If the string already starts with "$", it's assumed to be a JSONPath and returned as-is.
// - Dotted segments are treated as nested members; non-simple segments are quoted as members.
func jsonPathFromMapping(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "$"
	}
	if strings.HasPrefix(s, "$") {
		return s
	}
	parts := strings.Split(s, ".")
	var b strings.Builder
	b.WriteString("$")
	for _, seg := range parts {
		if seg == "" {
			continue
		}
		if simpleIdentRe.MatchString(seg) {
			b.WriteString(".")
			b.WriteString(seg)
			continue
		}
		// quote non-simple segments as members: $."weird.seg"
		esc := strings.ReplaceAll(seg, `"`, `\"`)
		b.WriteString(`."`)
		b.WriteString(esc)
		b.WriteString(`"`)
	}
	return b.String()
}

func isBaseCol(q string) bool {
	// quoted, so prefixes look like: "\"resource_", "\"log_", "\"attr_", "\"chq_"
	if strings.HasPrefix(q, "\"resource_") || strings.HasPrefix(q, "\"log_") || strings.HasPrefix(q, "\"attr_") || strings.HasPrefix(q, "\"chq_") {
		return true
	}
	return false
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

/* --- regex helpers --- */

// regexCaptureOrder returns the *positional* list of all capturing groups,
// in order of appearance. Unnamed groups are given placeholders "__g1", "__g2", ...
// Named groups (?P<name>...) return "name".
func regexCaptureOrder(pat string) []string {
	if pat == "" {
		return nil
	}
	var (
		out        []string
		inClass    bool // inside [...]
		escape     bool // previous char was '\'
		i          = 0
		groupIndex = 0
		runes      = []rune(pat)
		runesLen   = len(runes)
		nextIs     = func(s string) bool {
			n := len([]rune(s))
			if i+n > runesLen {
				return false
			}
			return string(runes[i:i+n]) == s
		}
	)
	for i = 0; i < runesLen; i++ {
		ch := runes[i]

		if escape {
			escape = false
			continue
		}
		if ch == '\\' {
			escape = true
			continue
		}
		if ch == '[' && !inClass {
			inClass = true
			continue
		}
		if ch == ']' && inClass {
			inClass = false
			continue
		}
		if inClass {
			continue
		}

		if ch == '(' {
			// look-ahead for "(?..."
			if i+1 < runesLen && runes[i+1] == '?' {
				// named capture? (?P<name>...)
				if nextIs("(?P<") {
					// parse name until '>'
					j := i + len("(?P<")
					nameStart := j
					for j < runesLen && runes[j] != '>' {
						j++
					}
					if j < runesLen && runes[j] == '>' {
						name := string(runes[nameStart:j])
						groupIndex++
						out = append(out, name)
					}
					continue
				}
				// other (?...) constructs are *non-capturing*
				continue
			}
			// a plain capturing group
			groupIndex++
			out = append(out, fmt.Sprintf("__g%d", groupIndex))
			continue
		}
	}
	return out
}

// regexCaptureNames returns only the *named* groups found.
var reNamedGroup = regexp.MustCompile(`\(\?P<([A-Za-z_][A-Za-z0-9_]*)>`)

func regexCaptureNames(pat string) []string {
	if pat == "" {
		return nil
	}
	m := reNamedGroup.FindAllStringSubmatch(pat, -1)
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	seen := map[string]struct{}{}
	for _, g := range m {
		if len(g) < 2 {
			continue
		}
		k := g[1]
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	return out
}

// namedOnlyPattern rewrites a regex so that *unnamed* capturing groups
// become non-capturing groups. It preserves escapes and character classes.
func namedOnlyPattern(pat string) string {
	if pat == "" {
		return pat
	}
	runes := []rune(pat)
	var b strings.Builder
	inClass := false
	escape := false
	for i := 0; i < len(runes); i++ {
		ch := runes[i]

		if escape {
			b.WriteRune(ch)
			escape = false
			continue
		}
		if ch == '\\' {
			b.WriteRune(ch)
			escape = true
			continue
		}
		if ch == '[' {
			inClass = true
			b.WriteRune(ch)
			continue
		}
		if ch == ']' && inClass {
			inClass = false
			b.WriteRune(ch)
			continue
		}
		if inClass {
			b.WriteRune(ch)
			continue
		}

		if ch == '(' {
			// If this is "(?..." then it's already non-capturing or special (?:, ?=, ?!, ?P<...)
			if i+1 < len(runes) && runes[i+1] == '?' {
				b.WriteRune('(')
				continue
			}
			// Plain capturing group -> make it non-capturing
			b.WriteString("(?:")
			continue
		}

		b.WriteRune(ch)
	}
	return b.String()
}
