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
	"strings"
)

// ToSpansWorkerSQL generates SQL for spans queries with _cardinalhq.name and _cardinalhq.kind as default fields
func (be *LogLeaf) ToSpansWorkerSQL(limit int, order string, fields []string) string {
	const baseRel = "{table}"
	const spansNameCol = "\"_cardinalhq.name\""
	const tsCol = "\"_cardinalhq.timestamp\""

	// 1) Prepare sets: group keys, parser-created, feature flags
	groupKeys := dedupeStrings(be.OutBy)
	parserCreated, hasJSON, hasLogFmt := analyzeParsers(be)

	// If json/logfmt is present, treat all non-base groupKeys as parser-created
	if hasJSON || hasLogFmt {
		for _, k := range groupKeys {
			qk := quoteIdent(k)
			if !isSpansBaseCol(qk) {
				parserCreated[k] = struct{}{}
			}
		}
	}

	// 2) Build CTE pipeline
	pb := newPipelineBuilder()

	// s0: minimal base projection + matchers + non-parser groupKeys (+ fields param)
	s0Need := computeSpansS0Need(be, groupKeys, parserCreated)

	// Add fields parameter to s0 if they are base table columns and not being extracted by parsers
	for _, field := range fields {
		if _, created := parserCreated[field]; !created {
			qk := quoteIdent(field)
			s0Need[qk] = struct{}{}
		}
	}
	pb.push(selectListFromSet(s0Need), baseRel, nil)

	// s1: time window sentinel so segment filters can be spliced
	timePred := fmt.Sprintf("CAST(%s AS BIGINT) >= {start} AND CAST(%s AS BIGINT) < {end}", tsCol, tsCol)
	pb.push([]string{pb.top() + ".*"}, pb.top(),
		[]string{timePred, "true"})

	// 3) Apply selector & line filters before parsers
	emitSpansSelectorAndLineFilters(be, &pb, spansNameCol)

	// 4) Collect future-created labels (from label_format), unwrap needs
	futureCreated := collectFutureCreated(be)
	unwrapNeeded := collectUnwrapNeeds(be)

	// 5) Emit parsers left→right, pushing label filters as soon as labels exist
	remainingLF := append([]LabelFilter(nil), be.LabelFilters...)
	remainingLine := make([]LineFilter, 0)
	emitParsersWithPostLineFilters(be, &pb, spansNameCol,
		groupKeys,
		futureCreated, unwrapNeeded,
		&remainingLF, &remainingLine, fields, parserCreated)

	// 6) Any remaining label filters (base columns) → apply at the end
	if len(remainingLF) > 0 {
		where := buildLabelFilterWhere(remainingLF, nil)
		if len(where) > 0 {
			pb.push([]string{pb.top() + ".*"}, pb.top(), where)
		}
	}

	// 7) Final SELECT (exemplars → ORDER/LIMIT only when no range agg)
	return finalizeSelect(&pb, tsCol, be.RangeAggOp == "", order, limit)
}

// ToSpansWorkerSQLWithLimit is a convenience wrapper for ToSpansWorkerSQL
func (be *LogLeaf) ToSpansWorkerSQLWithLimit(limit int, order string, fields []string) string {
	return be.ToSpansWorkerSQL(limit, order, fields)
}

// isSpansBaseCol checks if a column is a base column for spans
func isSpansBaseCol(col string) bool {
	spansBaseCols := map[string]struct{}{
		"\"_cardinalhq.name\"":          {},
		"\"_cardinalhq.kind\"":          {},
		"\"_cardinalhq.span_id\"":       {},
		"\"_cardinalhq.span_trace_id\"": {},
		"\"_cardinalhq.status_code\"":   {},
		"\"_cardinalhq.span_duration\"": {},
		"\"_cardinalhq.timestamp\"":     {},
		"\"_cardinalhq.id\"":            {},
		"\"_cardinalhq.fingerprint\"":   {},
	}
	_, ok := spansBaseCols[col]
	return ok
}

// computeSpansS0Need computes the columns needed for the initial projection in spans queries
func computeSpansS0Need(be *LogLeaf, groupKeys []string, parserCreated map[string]struct{}) map[string]struct{} {
	// base columns always in s0 for spans
	need := map[string]struct{}{
		"\"_cardinalhq.id\"":            {},
		"\"_cardinalhq.kind\"":          {},
		"\"_cardinalhq.name\"":          {},
		"\"_cardinalhq.span_id\"":       {},
		"\"_cardinalhq.span_trace_id\"": {},
		"\"_cardinalhq.status_code\"":   {},
		"\"_cardinalhq.span_duration\"": {},
		"\"_cardinalhq.timestamp\"":     {},
		"CAST(\"_cardinalhq.fingerprint\" AS VARCHAR) AS \"_cardinalhq.fingerprint\"": {},
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

	// NEW: hoist base columns referenced by label_format / line_format templates.
	for _, p := range be.Parsers {
		switch strings.ToLower(p.Type) {
		case "label_format", "label-format", "labelformat":
			// If precompiled SQL in LabelFormats, we can't reliably recover deps.
			// But Params case is common and covers templates.
			for _, tmpl := range p.Params {
				for _, dep := range collectTemplateDeps(tmpl) {
					q := quoteIdent(dep)
					if isSpansBaseCol(q) {
						need[q] = struct{}{}
					}
				}
			}
		case "line_format", "line-format", "lineformat":
			if tmpl := strings.TrimSpace(p.Params["template"]); tmpl != "" {
				for _, dep := range collectTemplateDeps(tmpl) {
					q := quoteIdent(dep)
					if isSpansBaseCol(q) {
						need[q] = struct{}{}
					}
				}
			}
		}
	}

	return need
}

// emitSpansSelectorAndLineFilters applies selector and line filters for spans queries
func emitSpansSelectorAndLineFilters(be *LogLeaf, pb *pipelineBuilder, spansNameCol string) {
	// Apply selector matchers early
	if len(be.Matchers) > 0 {
		mLfs := make([]LabelFilter, 0, len(be.Matchers))
		for _, m := range be.Matchers {
			mLfs = append(mLfs, LabelFilter{Label: m.Label, Op: m.Op, Value: m.Value})
		}
		mWhere := buildLabelFilterWhere(mLfs, nil)
		if len(mWhere) > 0 {
			pb.push([]string{pb.top() + ".*"}, pb.top(), mWhere)
		}
	}

	// Line filters are not applicable to spans queries
}
