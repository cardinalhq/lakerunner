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

package expressionindex

import (
	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/promql"
)

// ExpressionFromPromBaseExpr normalizes a PromQL leaf into indexable form.
func ExpressionFromPromBaseExpr(be promql.BaseExpr, signal Signal) Expression {
	expr := Expression{
		ID:     be.ID,
		Signal: signal,
		Metric: be.Metric,
	}

	for i := range be.Matchers {
		if m, ok := fromPromMatcher(be.Matchers[i]); ok {
			expr.Matchers = append(expr.Matchers, m)
		}
	}

	if be.LogLeaf != nil {
		// Log-/trace-derived aggregate leaves use selector matchers from LogLeaf.
		if signal != SignalMetrics {
			expr.Metric = ""
		}

		for i := range be.LogLeaf.Matchers {
			if m, ok := fromLogMatcher(be.LogLeaf.Matchers[i]); ok {
				expr.Matchers = append(expr.Matchers, m)
			}
		}

		// Include only raw label filters. Parser-dependent filters need materialized
		// parsed fields and should not be used for ingest-time candidate pruning.
		for i := range be.LogLeaf.LabelFilters {
			lf := be.LogLeaf.LabelFilters[i]
			if lf.ParserIdx != nil {
				continue
			}
			if m, ok := fromLogLabelFilter(lf); ok {
				expr.Matchers = append(expr.Matchers, m)
			}
		}
	}

	expr.Matchers = dedupeMatchers(expr.Matchers)
	return expr
}

// ExpressionFromLogLeaf normalizes a LogQL leaf into indexable form.
func ExpressionFromLogLeaf(leaf logql.LogLeaf, signal Signal) Expression {
	expr := Expression{
		ID:     leaf.ID,
		Signal: signal,
	}

	for i := range leaf.Matchers {
		if m, ok := fromLogMatcher(leaf.Matchers[i]); ok {
			expr.Matchers = append(expr.Matchers, m)
		}
	}

	for i := range leaf.LabelFilters {
		lf := leaf.LabelFilters[i]
		if lf.ParserIdx != nil {
			continue
		}
		if m, ok := fromLogLabelFilter(lf); ok {
			expr.Matchers = append(expr.Matchers, m)
		}
	}

	expr.Matchers = dedupeMatchers(expr.Matchers)
	return expr
}

func fromPromMatcher(m promql.LabelMatch) (Matcher, bool) {
	op, ok := fromPromOp(m.Op)
	if !ok {
		return Matcher{}, false
	}
	return Matcher{
		Label: m.Label,
		Op:    op,
		Value: m.Value,
	}, true
}

func fromLogMatcher(m logql.LabelMatch) (Matcher, bool) {
	op, ok := fromLogOp(m.Op)
	if !ok {
		return Matcher{}, false
	}
	return Matcher{
		Label: m.Label,
		Op:    op,
		Value: m.Value,
	}, true
}

func fromLogLabelFilter(m logql.LabelFilter) (Matcher, bool) {
	op, ok := fromLogOp(m.Op)
	if !ok {
		return Matcher{}, false
	}
	return Matcher{
		Label: m.Label,
		Op:    op,
		Value: m.Value,
	}, true
}

func fromPromOp(op promql.MatchOp) (MatchOp, bool) {
	switch op {
	case promql.MatchEq:
		return MatchEq, true
	case promql.MatchNe:
		return MatchNe, true
	case promql.MatchRe:
		return MatchRe, true
	case promql.MatchNre:
		return MatchNre, true
	default:
		return "", false
	}
}

func fromLogOp(op logql.MatchOp) (MatchOp, bool) {
	switch op {
	case logql.MatchEq:
		return MatchEq, true
	case logql.MatchNe:
		return MatchNe, true
	case logql.MatchRe:
		return MatchRe, true
	case logql.MatchNre:
		return MatchNre, true
	default:
		return "", false
	}
}

func dedupeMatchers(in []Matcher) []Matcher {
	if len(in) < 2 {
		return in
	}

	seen := make(map[Matcher]struct{}, len(in))
	out := make([]Matcher, 0, len(in))
	for i := range in {
		m := in[i]
		if _, ok := seen[m]; ok {
			continue
		}
		seen[m] = struct{}{}
		out = append(out, m)
	}
	return out
}
