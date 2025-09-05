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
	"github.com/cardinalhq/lakerunner/logql"
	"reflect"
	"testing"
)

// --- helpers ---------------------------------------------------------------

func mkLeaf(id, rng, off string) logql.LogLeaf {
	return logql.LogLeaf{
		ID:     id,
		Range:  rng,
		Offset: off,
	}
}

func leafIDs(ls map[string]logql.LogLeaf) []string {
	out := make([]string, len(ls))
	i := 0
	for _, l := range ls {
		out[i] = l.ID
		i++
	}
	return out
}

func assertLeavesExactly(t *testing.T, rr RewriteResult, want map[string]logql.LogLeaf) {
	t.Helper()
	if len(rr.Leaves) != len(want) {
		t.Fatalf("leaves length mismatch: want %d, got %d", len(want), len(rr.Leaves))
	}
	for id, w := range want {
		g, ok := rr.Leaves[id]
		if !ok {
			t.Fatalf("missing leaf %q in result", id)
		}
		if !reflect.DeepEqual(w, g) {
			t.Fatalf("leaf %q mismatch:\n  want: %#v\n  got : %#v", id, w, g)
		}
	}
}

// --- tests -----------------------------------------------------------------

func TestRewrite_CountOverTime(t *testing.T) {
	leaf := logql.LogLeaf{ID: "leafA", Range: "5m"}

	root := &logql.LRangeAggNode{
		Op:    "count_over_time",
		Child: &logql.LLeafNode{Leaf: leaf},
	}

	rr, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("RewriteToPromQL error: %v", err)
	}

	// Expect increase on SynthLogCount
	wantProm := `increase(` + SynthLogCount + `{` + LeafMatcher + `="leafA"}[5m])`
	if rr.PromQL != wantProm {
		t.Fatalf("promql mismatch:\n  want: %s\n  got : %s", wantProm, rr.PromQL)
	}

	assertLeavesExactly(t, rr, map[string]logql.LogLeaf{
		"leafA": leaf,
	})
}

func TestRewrite_Rate_WithSumBy(t *testing.T) {
	leaf := logql.LogLeaf{
		ID:    "id123",
		Range: "1m",
	}

	// sum by(level) (rate(<leaf>[1m]))
	root := &logql.LAggNode{
		Op: "sum",
		By: []string{"level"},
		Child: &logql.LRangeAggNode{
			Op:    "rate",
			Child: &logql.LLeafNode{Leaf: leaf},
		},
	}

	rr, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("RewriteToPromQL error: %v", err)
	}

	// PromQL string should include the leaf selector by ID and group-by
	wantProm := `sum by (level) (rate(` + SynthLogCount + `{` + LeafMatcher + `="id123"}[1m]))`
	if rr.PromQL != wantProm {
		t.Fatalf("promql mismatch:\n  want: %s\n  got : %s", wantProm, rr.PromQL)
	}

	// Leaves are returned as a map keyed by ID now.
	if len(rr.Leaves) != 1 {
		t.Fatalf("leaves length mismatch: want 1, got %d", len(rr.Leaves))
	}
	gotLeaf, ok := rr.Leaves["id123"]
	if !ok {
		t.Fatalf(`missing leaf "id123" in result map`)
	}
	if !reflect.DeepEqual(gotLeaf, leaf) {
		t.Fatalf("leaf mismatch:\n  want: %#v\n  got : %#v", leaf, gotLeaf)
	}
}

func TestRewrite_BytesOverTime_WithWithout(t *testing.T) {
	leaf := logql.LogLeaf{ID: "XYZ", Range: "10m"}

	// bytes_over_time selector aggregated with "sum without (pod)"
	root := &logql.LAggNode{
		Op:      "sum",
		Without: []string{"pod"},
		Child: &logql.LRangeAggNode{
			Op:    "bytes_over_time",
			Child: &logql.LLeafNode{Leaf: leaf},
		},
	}

	rr, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("RewriteToPromQL error: %v", err)
	}

	wantProm := `sum without (pod) (increase(` + SynthLogBytes + `{` + LeafMatcher + `="XYZ"}[10m]))`
	if rr.PromQL != wantProm {
		t.Fatalf("promql mismatch:\n  want: %s\n  got : %s", wantProm, rr.PromQL)
	}

	assertLeavesExactly(t, rr, map[string]logql.LogLeaf{
		"XYZ": leaf,
	})
}

func TestRewrite_Offset_Propagation(t *testing.T) {
	leaf := mkLeaf("off1", "2m", "30s")
	root := &logql.LRangeAggNode{
		Op:    "rate",
		Child: &logql.LLeafNode{Leaf: leaf},
	}

	got, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantProm := `rate(__logql_logs_total{__leaf="off1"}[2m]) offset 30s`
	if got.PromQL != wantProm {
		t.Fatalf("promql mismatch:\nwant: %s\ngot : %s", wantProm, got.PromQL)
	}
}

func TestRewrite_BinaryOp_TwoLeaves_DedupAndOrder(t *testing.T) {
	leafA := mkLeaf("A", "5m", "")
	leafB := mkLeaf("B", "5m", "")

	root := &logql.LBinOpNode{
		Op: "+",
		LHS: &logql.LRangeAggNode{
			Op:    "count_over_time",
			Child: &logql.LLeafNode{Leaf: leafA},
		},
		RHS: &logql.LRangeAggNode{
			Op:    "bytes_rate",
			Child: &logql.LLeafNode{Leaf: leafB},
		},
	}

	got, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	wantProm := `(increase(__logql_logs_total{__leaf="A"}[5m])) + (rate(__logql_log_bytes_total{__leaf="B"}[5m]))`
	if got.PromQL != wantProm {
		t.Fatalf("promql mismatch:\nwant: %s\ngot : %s", wantProm, got.PromQL)
	}

	ids := leafIDs(got.Leaves)
	if !reflect.DeepEqual(ids, sorted([]string{"A", "B"})) {
		t.Fatalf("leaf IDs mismatch:\nwant: %v\ngot : %v", sorted([]string{"A", "B"}), ids)
	}
}

func TestRewrite_Dedup_SameLeafTwice(t *testing.T) {
	leaf := mkLeaf("same", "1m", "")

	root := &logql.LBinOpNode{
		Op: "+",
		LHS: &logql.LRangeAggNode{
			Op:    "rate",
			Child: &logql.LLeafNode{Leaf: leaf},
		},
		RHS: &logql.LRangeAggNode{
			Op:    "count_over_time",
			Child: &logql.LLeafNode{Leaf: leaf}, // same ID on purpose
		},
	}

	got, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(got.Leaves) != 1 {
		t.Fatalf("expected 1 unique leaf, got %d", len(got.Leaves))
	}
	if l, ok := got.Leaves["same"]; !ok || l.ID != "same" {
		t.Fatalf("dedup failed; got: %+v", got.Leaves)
	}
}

func TestRewrite_ErrorOnBareLeaf(t *testing.T) {
	root := &logql.LLeafNode{Leaf: mkLeaf("x", "", "")}

	_, err := RewriteToPromQL(root)
	if err == nil {
		t.Fatalf("expected error for bare leaf, got nil")
	}
}

func TestRewrite_ErrorOnMissingRange(t *testing.T) {
	leaf := mkLeaf("noRange", "", "") // Range missing
	root := &logql.LRangeAggNode{
		Op:    "rate",
		Child: &logql.LLeafNode{Leaf: leaf},
	}

	_, err := RewriteToPromQL(root)
	if err == nil {
		t.Fatalf("expected error for missing range, got nil")
	}
}

func TestRewrite_ErrorOnUnsupportedRangeAgg(t *testing.T) {
	leaf := mkLeaf("X", "5m", "")
	root := &logql.LRangeAggNode{
		Op:    "median_over_time", // unsupported by the rewriter
		Child: &logql.LLeafNode{Leaf: leaf},
	}

	_, err := RewriteToPromQL(root)
	if err == nil {
		t.Fatalf("expected error for unsupported range agg, got nil")
	}
}
