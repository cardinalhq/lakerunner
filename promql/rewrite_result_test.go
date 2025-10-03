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
	"reflect"
	"testing"

	"github.com/cardinalhq/lakerunner/logql"
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
	wantProm := `count_over_time(` + SynthLogCount + `{` + LeafMatcher + `="leafA"}[5m])`
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

	// offset sticks to the range selector
	wantProm := `rate(` + SynthLogCount + `{` + LeafMatcher + `="off1"}[2m] offset 30s)`
	if got.PromQL != wantProm {
		t.Fatalf("promql mismatch:\nwant: %s\ngot : %s", wantProm, got.PromQL)
	}

	assertLeavesExactly(t, got, map[string]logql.LogLeaf{
		"off1": {ID: "off1", Range: "2m", Offset: "30s"},
	})
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

	wantProm := `(count_over_time(__logql_logs_total{__leaf="A"}[5m])) + (rate(__logql_log_bytes_total{__leaf="B"}[5m]))`
	if got.PromQL != wantProm {
		t.Fatalf("promql mismatch:\nwant: %s\ngot : %s", wantProm, got.PromQL)
	}

	ids := leafIDs(got.Leaves)
	if !reflect.DeepEqual(sorted(ids), sorted([]string{"A", "B"})) {
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

func TestRewrite_AvgOverTime_Unwrap_Simple(t *testing.T) {
	leaf := logql.LogLeaf{ID: "uw1", Range: "1m"}

	root := &logql.LRangeAggNode{
		Op:    "avg_over_time",
		Child: &logql.LLeafNode{Leaf: leaf},
	}

	rr, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("RewriteToPromQL error: %v", err)
	}

	wantProm := `avg_over_time(` + SynthLogUnwrap + `{` + LeafMatcher + `="uw1"}[1m])`
	if rr.PromQL != wantProm {
		t.Fatalf("promql mismatch:\n  want: %s\n  got : %s", wantProm, rr.PromQL)
	}

	assertLeavesExactly(t, rr, map[string]logql.LogLeaf{
		"uw1": leaf,
	})
}

func TestRewrite_MaxOverTime_Unwrap_WithBy(t *testing.T) {
	leaf := logql.LogLeaf{ID: "uwBy", Range: "5m"}

	// sum by(category) (max_over_time(<unwrap>[5m]))
	root := &logql.LAggNode{
		Op: "sum",
		By: []string{"category"},
		Child: &logql.LRangeAggNode{
			Op:    "max_over_time",
			Child: &logql.LLeafNode{Leaf: leaf},
		},
	}

	rr, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("RewriteToPromQL error: %v", err)
	}

	wantProm := `sum by (category) (max_over_time(` + SynthLogUnwrap + `{` + LeafMatcher + `="uwBy"}[5m]))`
	if rr.PromQL != wantProm {
		t.Fatalf("promql mismatch:\n  want: %s\n  got : %s", wantProm, rr.PromQL)
	}

	assertLeavesExactly(t, rr, map[string]logql.LogLeaf{
		"uwBy": leaf,
	})
}

func TestRewrite_MinOverTime_Unwrap_WithOffset(t *testing.T) {
	leaf := mkLeaf("uwOff", "2m", "15s")

	root := &logql.LRangeAggNode{
		Op:    "min_over_time",
		Child: &logql.LLeafNode{Leaf: leaf},
	}

	rr, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("RewriteToPromQL error: %v", err)
	}

	// offset sticks to the range selector inside the function call
	wantProm := `min_over_time(` + SynthLogUnwrap + `{` + LeafMatcher + `="uwOff"}[2m] offset 15s)`
	if rr.PromQL != wantProm {
		t.Fatalf("promql mismatch:\n  want: %s\n  got : %s", wantProm, rr.PromQL)
	}

	assertLeavesExactly(t, rr, map[string]logql.LogLeaf{
		"uwOff": {
			ID:     "uwOff",
			Range:  "2m",
			Offset: "15s",
		},
	})
}

func TestRewrite_Grouping_QuotesDotLabels_By(t *testing.T) {
	leaf := logql.LogLeaf{ID: "idDotBy", Range: "1m"}

	// sum by("resource.cluster","chq_foo",plain) (rate(...))
	root := &logql.LAggNode{
		Op: "sum",
		By: []string{"resource.cluster", "test.foo", "plain"},
		Child: &logql.LRangeAggNode{
			Op:    "rate",
			Child: &logql.LLeafNode{Leaf: leaf},
		},
	}

	got, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("RewriteToPromQL error: %v", err)
	}

	want := `sum by ("resource.cluster","test.foo",plain) (rate(` + SynthLogCount + `{` + LeafMatcher + `="idDotBy"}[1m]))`
	if got.PromQL != want {
		t.Fatalf("promql mismatch:\nwant: %s\ngot : %s", want, got.PromQL)
	}

	// leaves map contains the single referenced leaf
	if len(got.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(got.Leaves))
	}
	if !reflect.DeepEqual(got.Leaves["idDotBy"], leaf) {
		t.Fatalf("leaf mismatch: want %#v got %#v", leaf, got.Leaves["idDotBy"])
	}
}

func TestRewrite_Grouping_QuotesDotLabels_Without(t *testing.T) {
	leaf := logql.LogLeaf{ID: "idDotWo", Range: "2m"}

	// sum without("log.level","test.foo",plain) (increase(...))
	root := &logql.LAggNode{
		Op:      "sum",
		Without: []string{"log.level", "test.foo", "plain"},
		Child: &logql.LRangeAggNode{
			Op:    "bytes_over_time",
			Child: &logql.LLeafNode{Leaf: leaf},
		},
	}

	got, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("RewriteToPromQL error: %v", err)
	}

	want := `sum without ("log.level","test.foo",plain) (increase(` + SynthLogBytes + `{` + LeafMatcher + `="idDotWo"}[2m]))`
	if got.PromQL != want {
		t.Fatalf("promql mismatch:\nwant: %s\ngot : %s", want, got.PromQL)
	}

	if len(got.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(got.Leaves))
	}
	if !reflect.DeepEqual(got.Leaves["idDotWo"], leaf) {
		t.Fatalf("leaf mismatch: want %#v got %#v", leaf, got.Leaves["idDotWo"])
	}
}

func TestRewrite_Grouping_Mixed_NoDotNotQuoted(t *testing.T) {
	leaf := logql.LogLeaf{ID: "plainOnly", Range: "30s"}

	// Only plain label → no quotes expected
	root := &logql.LAggNode{
		Op: "max",
		By: []string{"plain"},
		Child: &logql.LRangeAggNode{
			Op:    "rate",
			Child: &logql.LLeafNode{Leaf: leaf},
		},
	}

	got, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("RewriteToPromQL error: %v", err)
	}

	want := `max by (plain) (rate(` + SynthLogCount + `{` + LeafMatcher + `="plainOnly"}[30s]))`
	if got.PromQL != want {
		t.Fatalf("promql mismatch:\nwant: %s\ngot : %s", want, got.PromQL)
	}
}

func TestRewrite_TopK(t *testing.T) {
	leaf := logql.LogLeaf{ID: "topkNoGroup", Range: "2m"}
	k := 5

	// topk(5, rate(<leaf>[2m]))
	root := &logql.LAggNode{
		Op:    "topk",
		Param: &k,
		Child: &logql.LRangeAggNode{
			Op:    "rate",
			Child: &logql.LLeafNode{Leaf: leaf},
		},
	}

	rr, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("RewriteToPromQL error: %v", err)
	}

	wantProm := `topk(5, rate(` + SynthLogCount + `{` + LeafMatcher + `="topkNoGroup"}[2m]))`
	if rr.PromQL != wantProm {
		t.Fatalf("promql mismatch:\n  want: %s\n  got : %s", wantProm, rr.PromQL)
	}

	assertLeavesExactly(t, rr, map[string]logql.LogLeaf{
		"topkNoGroup": leaf,
	})
}

func TestRewrite_BottomK(t *testing.T) {
	leaf := logql.LogLeaf{ID: "bottomkNoGroup", Range: "1m"}
	k := 3

	// bottomk(3, count_over_time(<leaf>[1m]))
	root := &logql.LAggNode{
		Op:    "bottomk",
		Param: &k,
		Child: &logql.LRangeAggNode{
			Op:    "count_over_time",
			Child: &logql.LLeafNode{Leaf: leaf},
		},
	}

	rr, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("RewriteToPromQL error: %v", err)
	}

	wantProm := `bottomk(3, count_over_time(` + SynthLogCount + `{` + LeafMatcher + `="bottomkNoGroup"}[1m]))`
	if rr.PromQL != wantProm {
		t.Fatalf("promql mismatch:\n  want: %s\n  got : %s", wantProm, rr.PromQL)
	}

	assertLeavesExactly(t, rr, map[string]logql.LogLeaf{
		"bottomkNoGroup": leaf,
	})
}

func TestRewrite_Offset_Propagation_1(t *testing.T) {
	leaf := mkLeaf("off1", "2m", "30s")
	root := &logql.LRangeAggNode{
		Op:    "rate",
		Child: &logql.LLeafNode{Leaf: leaf},
	}

	got, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// offset belongs on the range selector
	wantProm := `rate(` + SynthLogCount + `{` + LeafMatcher + `="off1"}[2m] offset 30s)`
	if got.PromQL != wantProm {
		t.Fatalf("promql mismatch:\nwant: %s\ngot : %s", wantProm, got.PromQL)
	}

	assertLeavesExactly(t, got, map[string]logql.LogLeaf{
		"off1": {
			ID:     "off1",
			Range:  "2m",
			Offset: "30s",
		},
	})
}

func TestRewrite_Offset_Propagation_WithGrouping(t *testing.T) {
	leaf := mkLeaf("off2", "5m", "1m")
	root := &logql.LAggNode{
		Op: "sum",
		By: []string{"level"},
		Child: &logql.LRangeAggNode{
			Op:    "count_over_time",
			Child: &logql.LLeafNode{Leaf: leaf},
		},
	}

	got, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// note the closing )) and offset on the selector
	wantProm := `sum by (level) (count_over_time(` + SynthLogCount + `{` + LeafMatcher + `="off2"}[5m] offset 1m))`
	if got.PromQL != wantProm {
		t.Fatalf("promql mismatch:\nwant: %s\ngot : %s", wantProm, got.PromQL)
	}

	assertLeavesExactly(t, got, map[string]logql.LogLeaf{
		"off2": {
			ID:     "off2",
			Range:  "5m",
			Offset: "1m",
		},
	})
}

func TestRewrite_RatioVsOffsetPctDrop_Alert(t *testing.T) {
	// (
	//   sum_over_time(unwrap[5m])
	//   /
	//   sum_over_time(unwrap[5m] offset 1h),
	//   - 1
	// )
	// * 100 < -5
	num := &logql.LRangeAggNode{
		Op:    "sum_over_time",
		Child: &logql.LLeafNode{Leaf: mkLeaf("segRev", "5m", "")}, // current
	}
	den := &logql.LRangeAggNode{
		Op:    "sum_over_time",
		Child: &logql.LLeafNode{Leaf: mkLeaf("segRev", "5m", "1h")}, // baseline offset 1h
	}
	ratio := &logql.LBinOpNode{Op: "/", LHS: num, RHS: den}
	minusOne := &logql.LBinOpNode{Op: "-", LHS: ratio, RHS: &logql.LScalarNode{Value: 1}}
	times100 := &logql.LBinOpNode{Op: "*", LHS: minusOne, RHS: &logql.LScalarNode{Value: 100}}
	root := &logql.LBinOpNode{Op: "<", LHS: times100, RHS: &logql.LScalarNode{Value: -5}}

	rr, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("RewriteToPromQL error: %v", err)
	}

	// Expect strict parenthesization and the offset on the denominator's selector.
	want := `((((sum_over_time(` + SynthLogUnwrap + `{` + LeafMatcher + `="segRev"}[5m])) / ` +
		`(sum_over_time(` + SynthLogUnwrap + `{` + LeafMatcher + `="segRev"}[5m] offset 1h))) - (1)) * (100)) < (-5)`

	if rr.PromQL != want {
		t.Fatalf("promql mismatch:\n  want: %s\n  got : %s", want, rr.PromQL)
	}

	// Dedup by leaf ID; we only assert the ID/range are present.
	if len(rr.Leaves) != 1 {
		t.Fatalf("expected 1 unique leaf, got %d: %+v", len(rr.Leaves), rr.Leaves)
	}
	l, ok := rr.Leaves["segRev"]
	if !ok || l.ID != "segRev" || l.Range != "5m" {
		t.Fatalf("leaf mismatch: got %#v", l)
	}
}

func TestRewrite_SumByCardType_SumOverTime_Unwrap_WithOffset(t *testing.T) {
	// Leaf stands for: ({resource_service_name="segment"} | json revenue=..., card_type=... | unwrap revenue [1m] offset 1h)
	leaf := logql.LogLeaf{ID: "segLeaf", Range: "1m", Offset: "1h"}

	// sum by (card_type) (sum_over_time(<unwrap>[1m] offset 1h))
	root := &logql.LAggNode{
		Op: "sum",
		By: []string{"card_type"},
		Child: &logql.LRangeAggNode{
			Op:    "sum_over_time",
			Child: &logql.LLeafNode{Leaf: leaf},
		},
	}

	rr, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("RewriteToPromQL error: %v", err)
	}

	// Expect: sum by (card_type) (sum_over_time(__logql_unwrap{__leaf="segLeaf"}[1m] offset 1h))
	wantProm := `sum by (card_type) (sum_over_time(` + SynthLogUnwrap + `{` + LeafMatcher + `="segLeaf"}[1m] offset 1h))`
	if rr.PromQL != wantProm {
		t.Fatalf("promql mismatch:\n  want: %s\n  got : %s", wantProm, rr.PromQL)
	}

	// Leaves must include the single referenced leaf with offset preserved.
	assertLeavesExactly(t, rr, map[string]logql.LogLeaf{
		"segLeaf": {ID: "segLeaf", Range: "1m", Offset: "1h"},
	})
}

func TestRewrite_LastOverTime_Unwrap_24h(t *testing.T) {
	leaf := mkLeaf("hwLeaf", "24h", "")

	root := &logql.LRangeAggNode{
		Op:    "last_over_time",
		Child: &logql.LLeafNode{Leaf: leaf},
	}

	rr, err := RewriteToPromQL(root)
	if err != nil {
		t.Fatalf("RewriteToPromQL error: %v", err)
	}

	wantProm := `last_over_time(` + SynthLogUnwrap + `{` + LeafMatcher + `="hwLeaf"}[24h])`
	if rr.PromQL != wantProm {
		t.Fatalf("promql mismatch:\n  want: %s\n  got : %s", wantProm, rr.PromQL)
	}

	assertLeavesExactly(t, rr, map[string]logql.LogLeaf{
		"hwLeaf": leaf,
	})
}
