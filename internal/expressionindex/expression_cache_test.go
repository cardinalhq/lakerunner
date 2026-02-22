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

func TestExpressionCacheOrgIsolation(t *testing.T) {
	cache := NewExpressionCache()

	err := cache.ReplaceOrg("org-a", []Expression{
		{
			ID:     "a1",
			Signal: SignalMetrics,
			Matchers: []Matcher{
				{Label: "env", Op: MatchEq, Value: "prod"},
			},
		},
	})
	if err != nil {
		t.Fatalf("ReplaceOrg(org-a) failed: %v", err)
	}

	err = cache.ReplaceOrg("org-b", []Expression{
		{
			ID:     "b1",
			Signal: SignalMetrics,
			Matchers: []Matcher{
				{Label: "env", Op: MatchEq, Value: "prod"},
			},
		},
	})
	if err != nil {
		t.Fatalf("ReplaceOrg(org-b) failed: %v", err)
	}

	gotA := ids(cache.FindCandidates("org-a", SignalMetrics, "", map[string]string{"env": "prod"}))
	gotB := ids(cache.FindCandidates("org-b", SignalMetrics, "", map[string]string{"env": "prod"}))
	if !slices.Equal(gotA, []string{"a1"}) {
		t.Fatalf("org-a mismatch: got=%v", gotA)
	}
	if !slices.Equal(gotB, []string{"b1"}) {
		t.Fatalf("org-b mismatch: got=%v", gotB)
	}
}

func TestExpressionCacheUpsertRemove(t *testing.T) {
	cache := NewExpressionCache()

	if err := cache.Upsert("org-a", Expression{
		ID:     "x1",
		Signal: SignalLogs,
		Matchers: []Matcher{
			{Label: "service", Op: MatchEq, Value: "api"},
		},
	}); err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}

	if got := cache.OrgSize("org-a"); got != 1 {
		t.Fatalf("OrgSize mismatch: got=%d want=1", got)
	}

	if !cache.Remove("org-a", "x1") {
		t.Fatal("expected Remove to return true")
	}
	if cache.Remove("org-a", "x1") {
		t.Fatal("expected second Remove to return false")
	}
}

func TestExpressionCacheSignalSize(t *testing.T) {
	cache := NewExpressionCache()

	if err := cache.Upsert("org-a", Expression{
		ID:     "m1",
		Signal: SignalMetrics,
	}); err != nil {
		t.Fatalf("Upsert(m1) failed: %v", err)
	}
	if err := cache.Upsert("org-a", Expression{
		ID:     "l1",
		Signal: SignalLogs,
	}); err != nil {
		t.Fatalf("Upsert(l1) failed: %v", err)
	}

	if got := cache.SignalSize("org-a", SignalMetrics); got != 1 {
		t.Fatalf("SignalSize(metrics) mismatch: got=%d want=1", got)
	}
	if got := cache.SignalSize("org-a", SignalLogs); got != 1 {
		t.Fatalf("SignalSize(logs) mismatch: got=%d want=1", got)
	}
	if got := cache.SignalSize("org-a", SignalTraces); got != 0 {
		t.Fatalf("SignalSize(traces) mismatch: got=%d want=0", got)
	}
	if !cache.HasSignal("org-a", SignalLogs) {
		t.Fatal("expected HasSignal(logs)=true")
	}
}
