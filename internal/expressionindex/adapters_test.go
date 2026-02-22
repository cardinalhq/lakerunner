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
	"slices"
	"testing"

	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/promql"
)

func TestExpressionFromPromBaseExprMetrics(t *testing.T) {
	be := promql.BaseExpr{
		ID:     "p1",
		Metric: "cpu_usage",
		Matchers: []promql.LabelMatch{
			{Label: "env", Op: promql.MatchEq, Value: "prod"},
			{Label: "pod", Op: promql.MatchRe, Value: "api-.*"},
		},
	}

	expr := ExpressionFromPromBaseExpr(be, SignalMetrics)

	if expr.ID != "p1" || expr.Signal != SignalMetrics || expr.Metric != "cpu_usage" {
		t.Fatalf("unexpected expression header: %+v", expr)
	}
	got := matcherStrings(expr.Matchers)
	want := []string{"env=prod", "pod=~api-.*"}
	if !slices.Equal(got, want) {
		t.Fatalf("matcher conversion mismatch: got=%v want=%v", got, want)
	}
}

func TestExpressionFromPromBaseExprLogLeaf(t *testing.T) {
	parserIdx := 0
	leaf := logql.LogLeaf{
		ID: "leaf-1",
		Matchers: []logql.LabelMatch{
			{Label: "service", Op: logql.MatchEq, Value: "payments"},
		},
		LabelFilters: []logql.LabelFilter{
			{Label: "level", Op: logql.MatchEq, Value: "error"},
			{Label: "user_id", Op: logql.MatchEq, Value: "123", ParserIdx: &parserIdx},
		},
	}
	be := promql.BaseExpr{
		ID:      "p2",
		Metric:  "__logql_logs_total",
		LogLeaf: &leaf,
	}

	expr := ExpressionFromPromBaseExpr(be, SignalLogs)
	if expr.Metric != "" {
		t.Fatalf("expected metric to be cleared for log signal, got=%q", expr.Metric)
	}

	got := matcherStrings(expr.Matchers)
	want := []string{"level=error", "service=payments"}
	if !slices.Equal(got, want) {
		t.Fatalf("log leaf matcher conversion mismatch: got=%v want=%v", got, want)
	}
}

func TestExpressionFromLogLeafSkipsUnsupportedOps(t *testing.T) {
	leaf := logql.LogLeaf{
		ID: "leaf-2",
		Matchers: []logql.LabelMatch{
			{Label: "service", Op: logql.MatchEq, Value: "api"},
		},
		LabelFilters: []logql.LabelFilter{
			{Label: "latency_ms", Op: logql.MatchGt, Value: "100"},
		},
	}

	expr := ExpressionFromLogLeaf(leaf, SignalTraces)
	got := matcherStrings(expr.Matchers)
	want := []string{"service=api"}
	if !slices.Equal(got, want) {
		t.Fatalf("unexpected matcher conversion: got=%v want=%v", got, want)
	}
}

func matcherStrings(matchers []Matcher) []string {
	out := make([]string, 0, len(matchers))
	for i := range matchers {
		m := matchers[i]
		out = append(out, m.Label+string(m.Op)+m.Value)
	}
	slices.Sort(out)
	return out
}
