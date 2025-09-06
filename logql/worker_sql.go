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

package logql

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Keep your existing imports/utilities (quoteIdent, buildLabelFilterWhere, etc.)

func (be *LogLeaf) ToWorkerSQL(limit int, order string) string {
	const baseRel = "{table}"
	const bodyCol = "\"_cardinalhq.message\""
	const tsCol = "\"_cardinalhq.timestamp\""

	// 1) Prepare sets: group keys, parser-created, feature flags
	groupKeys := dedupeStrings(be.OutBy)
	parserCreated, hasJSON, hasLogFmt := analyzeParsers(be)

	// If json/logfmt is present, treat all non-base groupKeys as parser-created
	if hasJSON || hasLogFmt {
		for _, k := range groupKeys {
			qk := quoteIdent(k)
			if !isBaseCol(qk) {
				parserCreated[k] = struct{}{}
			}
		}
	}

	// 2) Build CTE pipeline
	pb := newPipelineBuilder()

	// s0: minimal base projection (message, timestamp, exemplar defaults) + matchers + non-parser groupKeys
	s0Need := computeS0Need(be, groupKeys, parserCreated)
	pb.push(selectListFromSet(s0Need), baseRel, nil)

	// s1: time window sentinel so segment filters can be spliced
	timePred := fmt.Sprintf("CAST(%s AS BIGINT) >= {start} AND CAST(%s AS BIGINT) < {end}", tsCol, tsCol)
	pb.push([]string{pb.top() + ".*"}, pb.top(), []string{"1=1", "true", timePred})

	// 3) Apply selector & line filters before parsers
	emitSelectorAndLineFilters(be, &pb, bodyCol)

	// 4) Collect future-created labels (from label_format), unwrap needs
	futureCreated := collectFutureCreated(be)
	unwrapNeeded := collectUnwrapNeeds(be)

	// 5) Emit parsers left→right, pushing label filters as soon as labels exist
	remainingLF := append([]LabelFilter(nil), be.LabelFilters...)
	emitParsers(be, &pb, bodyCol, groupKeys, futureCreated, unwrapNeeded, &remainingLF)

	// 6) Any remaining label filters (base columns) → apply at the end
	if len(remainingLF) > 0 {
		where := buildLabelFilterWhere(remainingLF, nil)
		if len(where) > 0 {
			pb.push([]string{pb.top() + ".*"}, pb.top(), where)
		}
	}

	// 7) Final SELECT (exemplars → ORDER/LIMIT only when no range agg)
	return finalizeSelect(&pb, tsCol, be.RangeAggOp == "", order)
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

func finalizeSelect(p *pipelineBuilder, tsCol string, wantOrder bool, order string) string {
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

func computeS0Need(be *LogLeaf, groupKeys []string, parserCreated map[string]struct{}) map[string]struct{} {
	// base columns always in s0
	need := map[string]struct{}{
		"\"_cardinalhq.message\"":     {},
		"\"_cardinalhq.timestamp\"":   {},
		"\"_cardinalhq.id\"":          {},
		"\"_cardinalhq.level\"":       {},
		"\"_cardinalhq.fingerprint\"": {},
	}
	// matchers must exist before parsers
	for _, m := range be.Matchers {
		need[quoteIdent(m.Label)] = struct{}{}
	}
	// only non-parser groupKeys go into s0
	for _, k := range groupKeys {
		if _, created := parserCreated[k]; !created {
			need[quoteIdent(k)] = struct{}{}
		}
	}
	return need
}

func selectListFromSet(s map[string]struct{}) []string {
	out := make([]string, 0, len(s))
	for k := range s {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func emitSelectorAndLineFilters(be *LogLeaf, pb *pipelineBuilder, bodyCol string) {
	// selector matchers
	if len(be.Matchers) > 0 {
		var mLfs []LabelFilter
		for _, m := range be.Matchers {
			mLfs = append(mLfs, LabelFilter{Label: m.Label, Op: m.Op, Value: m.Value})
		}
		if where := buildLabelFilterWhere(mLfs, nil); len(where) > 0 {
			pb.push([]string{pb.top() + ".*"}, pb.top(), where)
		}
	}
	// line filters
	if where := buildLineFilterWhere(be.LineFilters, bodyCol); len(where) > 0 {
		pb.push([]string{pb.top() + ".*"}, pb.top(), where)
	}
}

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

/* --- parser emission --- */

func emitParsers(
	be *LogLeaf,
	pb *pipelineBuilder,
	bodyCol string,
	groupKeys []string,
	futureCreated map[string]struct{},
	unwrapNeeded map[string]struct{},
	remainingLF *[]LabelFilter,
) {
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

	for _, p := range be.Parsers {
		switch strings.ToLower(p.Type) {

		case "regexp":
			pat := p.Params["pattern"]
			names := regexCaptureNames(pat)
			sel := []string{pb.top() + ".*"}
			if len(names) > 0 {
				quoted := make([]string, 0, len(names))
				for _, name := range names {
					quoted = append(quoted, fmt.Sprintf("'%s'", name))
				}
				sel = append(sel, fmt.Sprintf("regexp_extract(%s, %s, [%s]) AS __extracted_struct",
					bodyCol, sqlQuote(pat), strings.Join(quoted, ", ")))
			}
			pb.push(sel, pb.top(), nil)

			if len(names) > 0 {
				extract := []string{pb.top() + ".*"}
				for _, name := range names {
					if strings.HasPrefix(name, "__var_") {
						continue
					}
					extract = append(extract, fmt.Sprintf("__extracted_struct.%s AS %s", quoteIdent(name), quoteIdent(name)))
				}
				pb.push(extract, pb.top(), nil)

				created := mkSet(names)
				now, later := partitionByNames(*remainingLF, created)
				if len(now) > 0 {
					where := buildLabelFilterWhere(now, nil)
					pb.push([]string{pb.top() + ".*"}, pb.top(), where)
				}
				*remainingLF = later
			}

		case "json":
			// keys needed by filters + group-by + unwrap, excluding future label_format
			needKeys := uniqLabels(*remainingLF)
			needKeys = append(needKeys, groupKeys...)
			for f := range unwrapNeeded {
				needKeys = append(needKeys, f)
			}
			needKeys = dedupeStrings(excludeFuture(needKeys))

			sel := []string{pb.top() + ".*"}
			for _, k := range needKeys {
				path := jsonPathForKey(k)
				sel = append(sel, fmt.Sprintf("json_extract_string(%s, %s) AS %s", bodyCol, sqlQuote(path), quoteIdent(k)))
			}
			pb.push(sel, pb.top(), nil)

			if len(needKeys) > 0 {
				created := mkSet(needKeys)
				now, later := partitionByNames(*remainingLF, created)
				if len(now) > 0 {
					where := buildLabelFilterWhere(now, nil)
					pb.push([]string{pb.top() + ".*"}, pb.top(), where)
				}
				*remainingLF = later
			}

		case "logfmt":
			needKeys := uniqLabels(*remainingLF)
			needKeys = append(needKeys, groupKeys...)
			for f := range unwrapNeeded {
				needKeys = append(needKeys, f)
			}
			needKeys = dedupeStrings(excludeFuture(needKeys))

			sel := []string{pb.top() + ".*"}
			for _, k := range needKeys {
				reKey := fmt.Sprintf(`(?:^|\s)%s=([^\s]+)`, regexp.QuoteMeta(k))
				sel = append(sel, fmt.Sprintf("regexp_extract(%s, %s, 1) AS %s", bodyCol, sqlQuote(reKey), quoteIdent(k)))
			}
			pb.push(sel, pb.top(), nil)

			if len(needKeys) > 0 {
				created := mkSet(needKeys)
				now, later := partitionByNames(*remainingLF, created)
				if len(now) > 0 {
					where := buildLabelFilterWhere(now, nil)
					pb.push([]string{pb.top() + ".*"}, pb.top(), where)
				}
				*remainingLF = later
			}

		case "label_format", "label-format", "labelformat":
			sel := []string{pb.top() + ".*"}
			created := make(map[string]struct{})

			if len(p.LabelFormats) > 0 {
				for _, lf := range p.LabelFormats {
					sel = append(sel, fmt.Sprintf("(%s) AS %s", lf.SQL, quoteIdent(lf.Out)))
					created[lf.Out] = struct{}{}
				}
			} else {
				keys := sortedKeys(p.Params)
				for _, out := range keys {
					expr, err := buildLabelFormatExprTemplate(p.Params[out], func(s string) string { return quoteIdent(s) })
					if err != nil {
						expr = "''"
					}
					sel = append(sel, fmt.Sprintf("(%s) AS %s", expr, quoteIdent(out)))
					created[out] = struct{}{}
				}
			}
			pb.push(sel, pb.top(), nil)

			now, later := partitionByNames(*remainingLF, created)
			if len(now) > 0 {
				where := buildLabelFilterWhere(now, nil)
				pb.push([]string{pb.top() + ".*"}, pb.top(), where)
			}
			*remainingLF = later

		case "unwrap":
			field := strings.TrimSpace(p.Params["field"])
			fn := strings.ToLower(strings.TrimSpace(p.Params["func"]))
			if field == "" {
				pb.push([]string{pb.top() + ".*"}, pb.top(), nil)
				break
			}
			col := quoteIdent(field)
			var expr string
			switch fn {
			case "duration":
				expr = unwrapDurationExpr(col)
			case "bytes":
				expr = unwrapBytesExpr(col)
			default:
				expr = fmt.Sprintf("try_cast(%s AS DOUBLE)", col)
			}
			pb.push([]string{pb.top() + ".*", expr + " AS __unwrap_value"}, pb.top(), nil)

		default:
			pb.push([]string{pb.top() + ".*"}, pb.top(), nil)
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

func isBaseCol(q string) bool {
	switch q {
	case "\"_cardinalhq.message\"", "\"_cardinalhq.timestamp\"", "\"_cardinalhq.id\"", "\"_cardinalhq.level\"", "\"_cardinalhq.fingerprint\"":
		return true
	default:
		return false
	}
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
