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

	// s0: always SELECT * from the base relation
	pb.push([]string{"*"}, baseRel, nil)

	// s0+: normalize fingerprint type to string once up-front so downstream filters/clients are stable
	pb.push([]string{
		pb.top() + `.* REPLACE(CAST("_cardinalhq.fingerprint" AS VARCHAR) AS "_cardinalhq.fingerprint")`,
	}, pb.top(), nil)

	// s1: time window sentinel so segment filters can be spliced
	timePred := fmt.Sprintf(
		"CAST(%s AS BIGINT) >= {start} AND CAST(%s AS BIGINT) < {end}",
		tsCol, tsCol,
	)
	pb.push([]string{pb.top() + ".*"}, pb.top(), []string{"1=1", "true", timePred})

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
