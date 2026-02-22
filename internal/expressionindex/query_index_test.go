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
)

func TestQueryIndexFindCandidates(t *testing.T) {
	idx := NewQueryIndex()

	mustAdd(t, idx, Expression{
		ID:     "m1",
		Signal: SignalMetrics,
		Metric: "cpu_usage",
		Matchers: []Matcher{
			{Label: "env", Op: MatchEq, Value: "prod"},
		},
	})
	mustAdd(t, idx, Expression{
		ID:     "m2",
		Signal: SignalMetrics,
		Metric: "cpu_usage",
		Matchers: []Matcher{
			{Label: "pod", Op: MatchRe, Value: "api-.*"},
		},
	})
	mustAdd(t, idx, Expression{
		ID:     "m3",
		Signal: SignalMetrics,
		Metric: "memory_usage",
		Matchers: []Matcher{
			{Label: "env", Op: MatchEq, Value: "prod"},
		},
	})
	mustAdd(t, idx, Expression{
		ID:     "l1",
		Signal: SignalLogs,
		Matchers: []Matcher{
			{Label: "service", Op: MatchEq, Value: "payments"},
		},
	})

	got := ids(idx.FindCandidates(SignalMetrics, "cpu_usage", map[string]string{
		"env": "prod",
		"pod": "api-1",
	}))
	want := []string{"m1", "m2"}
	if !slices.Equal(got, want) {
		t.Fatalf("FindCandidates metrics mismatch: got=%v want=%v", got, want)
	}

	got = ids(idx.FindCandidates(SignalMetrics, "cpu_usage", map[string]string{
		"env": "dev",
		"pod": "api-1",
	}))
	want = []string{"m2"}
	if !slices.Equal(got, want) {
		t.Fatalf("FindCandidates metrics regex mismatch: got=%v want=%v", got, want)
	}

	got = ids(idx.FindCandidates(SignalLogs, "", map[string]string{
		"service": "payments",
	}))
	want = []string{"l1"}
	if !slices.Equal(got, want) {
		t.Fatalf("FindCandidates logs mismatch: got=%v want=%v", got, want)
	}
}

func TestQueryIndexMissingLabelSemantics(t *testing.T) {
	idx := NewQueryIndex()
	mustAdd(t, idx, Expression{
		ID:     "eq_empty",
		Signal: SignalMetrics,
		Matchers: []Matcher{
			{Label: "zone", Op: MatchEq, Value: ""},
		},
	})
	mustAdd(t, idx, Expression{
		ID:     "ne_prod",
		Signal: SignalMetrics,
		Matchers: []Matcher{
			{Label: "zone", Op: MatchNe, Value: "prod"},
		},
	})
	mustAdd(t, idx, Expression{
		ID:     "re_any",
		Signal: SignalMetrics,
		Matchers: []Matcher{
			{Label: "zone", Op: MatchRe, Value: ".*"},
		},
	})
	mustAdd(t, idx, Expression{
		ID:     "nre_prod",
		Signal: SignalMetrics,
		Matchers: []Matcher{
			{Label: "zone", Op: MatchNre, Value: "prod"},
		},
	})

	got := ids(idx.FindCandidates(SignalMetrics, "cpu_usage", map[string]string{
		"env": "prod",
	}))
	want := []string{"eq_empty", "ne_prod", "nre_prod", "re_any"}
	if !slices.Equal(got, want) {
		t.Fatalf("FindCandidates missing-label mismatch: got=%v want=%v", got, want)
	}
}

func TestQueryIndexRemoveAndReplace(t *testing.T) {
	idx := NewQueryIndex()

	mustAdd(t, idx, Expression{
		ID:     "a",
		Signal: SignalMetrics,
		Matchers: []Matcher{
			{Label: "env", Op: MatchEq, Value: "prod"},
		},
	})
	mustAdd(t, idx, Expression{
		ID:     "b",
		Signal: SignalMetrics,
		Matchers: []Matcher{
			{Label: "env", Op: MatchEq, Value: "dev"},
		},
	})

	if !idx.Remove("b") {
		t.Fatal("expected Remove to return true")
	}
	if idx.Remove("b") {
		t.Fatal("expected second Remove to return false")
	}

	got := ids(idx.FindCandidates(SignalMetrics, "", map[string]string{"env": "dev"}))
	if len(got) != 0 {
		t.Fatalf("expected removed expression to be absent, got=%v", got)
	}

	err := idx.Replace([]Expression{
		{
			ID:     "x",
			Signal: SignalMetrics,
			Matchers: []Matcher{
				{Label: "region", Op: MatchEq, Value: "us-east-1"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Replace failed: %v", err)
	}

	got = ids(idx.FindCandidates(SignalMetrics, "", map[string]string{"env": "prod"}))
	if len(got) != 0 {
		t.Fatalf("expected old entries replaced, got=%v", got)
	}
}

func TestQueryIndexRejectsInvalidRegex(t *testing.T) {
	idx := NewQueryIndex()
	err := idx.Add(Expression{
		ID:     "bad",
		Signal: SignalMetrics,
		Matchers: []Matcher{
			{Label: "pod", Op: MatchRe, Value: "("},
		},
	})
	if err == nil {
		t.Fatal("expected regex compile error")
	}
}

func TestQueryIndexSignalSize(t *testing.T) {
	idx := NewQueryIndex()

	mustAdd(t, idx, Expression{
		ID:     "m1",
		Signal: SignalMetrics,
	})
	mustAdd(t, idx, Expression{
		ID:     "l1",
		Signal: SignalLogs,
	})

	if got := idx.SignalSize(SignalMetrics); got != 1 {
		t.Fatalf("SignalSize(metrics) mismatch: got=%d want=1", got)
	}
	if got := idx.SignalSize(SignalLogs); got != 1 {
		t.Fatalf("SignalSize(logs) mismatch: got=%d want=1", got)
	}
	if got := idx.SignalSize(SignalTraces); got != 0 {
		t.Fatalf("SignalSize(traces) mismatch: got=%d want=0", got)
	}
	if !idx.HasSignal(SignalLogs) {
		t.Fatal("expected HasSignal(logs)=true")
	}
}

func mustAdd(t *testing.T, idx *QueryIndex, expr Expression) {
	t.Helper()
	if err := idx.Add(expr); err != nil {
		t.Fatalf("Add(%s) failed: %v", expr.ID, err)
	}
}

func ids(exprs []Expression) []string {
	out := make([]string, 0, len(exprs))
	for i := range exprs {
		out = append(out, exprs[i].ID)
	}
	slices.Sort(out)
	return out
}
