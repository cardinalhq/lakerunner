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
	"sort"
	"testing"
)

func mustParse(t *testing.T, q string) Expr {
	t.Helper()
	e, err := FromPromQL(q)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	return e
}

func sorted(ss []string) []string {
	cp := append([]string(nil), ss...)
	sort.Strings(cp)
	return cp
}

func TestForDots(t *testing.T) {
	q := `sum by ("resource.service.name") ( topk(5, sum by ("resource.service.name", endpoint) (rate({"http.requests.total"}[5m]))) )`
	root := mustParse(t, q)
	res, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}
	_, ok := res.Root.(*AggNode)
	if !ok {
		t.Fatalf("root not AggNode, got %T", res.Root)
	}
}

func TestCompile_TopK_Then_OuterSum(t *testing.T) {
	q := `sum by (job) ( topk(5, sum by (job, endpoint) (rate(http_requests_total[5m]))) )`
	root := mustParse(t, q)

	res, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	// Root should be Agg(sum by job)
	agg, ok := res.Root.(*AggNode)
	if !ok {
		t.Fatalf("root not AggNode, got %T", res.Root)
	}
	if agg.Op != AggSum {
		t.Fatalf("root agg op = %v, want sum", agg.Op)
	}
	if !reflect.DeepEqual(agg.By, []string{"job"}) {
		t.Fatalf("root agg by = %#v, want [job]", agg.By)
	}

	// Child should be TopKNode(5)
	topkNode, ok := agg.Child.(*TopKNode)
	if !ok {
		t.Fatalf("child not TopKNode, got %T", agg.Child)
	}
	if topkNode.K != 5 {
		t.Fatalf("TopK.K=%d, want 5", topkNode.K)
	}

	// Under TopK: inner Agg(sum by job,endpoint)
	innerAgg, ok := topkNode.Child.(*AggNode)
	if !ok {
		t.Fatalf("topk child not AggNode, got %T", topkNode.Child)
	}
	if innerAgg.Op != AggSum {
		t.Fatalf("inner agg op = %v, want sum", innerAgg.Op)
	}
	if !reflect.DeepEqual(sorted(innerAgg.By), []string{"endpoint", "job"}) {
		t.Fatalf("inner agg by = %#v, want [endpoint job]", innerAgg.By)
	}

	// Leaf under the inner Agg
	leaf, ok := innerAgg.Child.(*LeafNode)
	if !ok {
		t.Fatalf("inner agg child not LeafNode, got %T", innerAgg.Child)
	}
	be := leaf.BE

	// Leaf should carry the series func + range
	if be.FuncName != "rate" || be.Range != "5m" || be.Metric != "http_requests_total" {
		t.Fatalf("leaf func/range/metric wrong: %+v", be)
	}

	// Because parent keeps only job (dropping endpoint), planner should NOT request worker topk here.
	if be.WantTopK {
		t.Fatalf("WantTopK should be false on leaf (API-side TopK expected), got true")
	}

	// Leaf grouping should match the inner agg (job, endpoint)
	if !reflect.DeepEqual(sorted(be.GroupBy), []string{"endpoint", "job"}) {
		t.Fatalf("leaf GroupBy=%v, want [job endpoint]", be.GroupBy)
	}

	// One leaf expected
	if len(res.Leaves) != 1 {
		t.Fatalf("want 1 leaf, got %d", len(res.Leaves))
	}
}

func TestCompile_SimpleRateSumBy(t *testing.T) {
	q := `sum by (job)(rate(http_requests_total{status=~"5.."}[5m]))`
	root := mustParse(t, q)

	res, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}
	if len(res.Leaves) != 1 {
		t.Fatalf("want 1 leaf, got %d", len(res.Leaves))
	}

	// Exec graph: AggNode(sum by job) -> LeafNode
	agg, ok := res.Root.(*AggNode)
	if !ok {
		t.Fatalf("root not AggNode, got %T", res.Root)
	}
	if agg.Op != AggSum {
		t.Fatalf("agg op = %v, want sum", agg.Op)
	}
	if !reflect.DeepEqual(agg.By, []string{"job"}) {
		t.Fatalf("agg by = %#v, want [job]", agg.By)
	}
	leaf, ok := agg.Child.(*LeafNode)
	if !ok {
		t.Fatalf("child not LeafNode, got %T", agg.Child)
	}

	be := leaf.BE
	if be.FuncName != "rate" {
		t.Fatalf("leaf FuncName=%q, want rate", be.FuncName)
	}
	if be.Range != "5m" {
		t.Fatalf("leaf Range=%q, want 5m", be.Range)
	}
	if be.Metric != "http_requests_total" {
		t.Fatalf("metric=%q, want http_requests_total", be.Metric)
	}
	// status=~"5.."
	found := false
	for _, m := range be.Matchers {
		if m.Label == "status" && m.Op == MatchRe && m.Value == "5.." {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("missing matcher status=~\"5..\": %#v", be.Matchers)
	}
	// No special hints
	if be.WantTopK || be.WantBottomK || be.WantCount {
		t.Fatalf("unexpected hints set on leaf: %+v", be)
	}
	// No CountOn
	if len(be.CountOnBy) != 0 {
		t.Fatalf("unexpected CountOnBy on leaf: %+v", be)
	}
}

func TestCompile_TopKHint(t *testing.T) {
	q := `topk(3, sum by (svc)(rate(http_requests_total[5m])))`
	root := mustParse(t, q)

	res, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}
	if len(res.Leaves) != 1 {
		t.Fatalf("want 1 leaf, got %d", len(res.Leaves))
	}

	topk, ok := res.Root.(*TopKNode)
	if !ok {
		t.Fatalf("root not TopKNode, got %T", res.Root)
	}
	if topk.K != 3 {
		t.Fatalf("TopK.K=%d, want 3", topk.K)
	}
	agg, ok := topk.Child.(*AggNode)
	if !ok {
		t.Fatalf("child not AggNode, got %T", topk.Child)
	}
	if !reflect.DeepEqual(agg.By, []string{"svc"}) {
		t.Fatalf("agg by = %#v, want [svc]", agg.By)
	}
	leaf, ok := agg.Child.(*LeafNode)
	if !ok {
		t.Fatalf("grandchild not LeafNode, got %T", agg.Child)
	}
	if !leaf.BE.WantTopK {
		t.Fatalf("WantTopK should be true on leaf")
	}
	if leaf.BE.Range != "5m" || leaf.BE.FuncName != "rate" {
		t.Fatalf("leaf wrong range/func: %+v", leaf.BE)
	}
	if !reflect.DeepEqual(leaf.BE.GroupBy, []string{"svc"}) {
		t.Fatalf("leaf GroupBy=%v, want [svc]", leaf.BE.GroupBy)
	}
	// No CountOn
	if len(leaf.BE.CountOnBy) != 0 {
		t.Fatalf("unexpected CountOnBy on TopK leaf: %+v", leaf.BE)
	}
}

func TestCompile_CountHint(t *testing.T) {
	q := `count by (job)(http_requests_total{instance="i1"})`
	root := mustParse(t, q)

	res, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}
	if len(res.Leaves) != 1 {
		t.Fatalf("want 1 leaf, got %d", len(res.Leaves))
	}

	agg, ok := res.Root.(*AggNode)
	if !ok {
		t.Fatalf("root not AggNode, got %T", res.Root)
	}
	if agg.Op != AggCount {
		t.Fatalf("agg op = %v, want count", agg.Op)
	}
	leaf, ok := agg.Child.(*LeafNode)
	if !ok {
		t.Fatalf("child not LeafNode, got %T", agg.Child)
	}
	if !leaf.BE.WantCount {
		t.Fatalf("WantCount should be true on leaf")
	}
	// Count works on instant selector (no range/func required)
	if leaf.BE.Range != "" || leaf.BE.FuncName != "" {
		t.Fatalf("unexpected range/func on count leaf: %+v", leaf.BE)
	}
	// instance="i1" matcher present
	found := false
	for _, m := range leaf.BE.Matchers {
		if m.Label == "instance" && m.Op == MatchEq && m.Value == "i1" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("missing matcher instance=\"i1\": %#v", leaf.BE.Matchers)
	}
	// Series-level distinct (have CountOnBy)
	if len(leaf.BE.CountOnBy) == 0 {
		t.Fatalf("CountOnBy should NOT be empty for series-level count, got %v", leaf.BE.CountOnBy)
	}
	// No TopK hints
	if leaf.BE.WantTopK || leaf.BE.WantBottomK {
		t.Fatalf("unexpected topk hints on count leaf: %+v", leaf.BE)
	}
}

func TestCompile_OuterCount(t *testing.T) {
	q := `count by (job) (
  		sum by (job, instance) (rate(http_requests_total[5m]))
	)`
	root := mustParse(t, q)

	res, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	// Root is count-by(job)
	aggCnt, ok := res.Root.(*AggNode)
	if !ok {
		t.Fatalf("root not AggNode, got %T", res.Root)
	}
	if aggCnt.Op != AggCount {
		t.Fatalf("agg op = %v, want count", aggCnt.Op)
	}

	// Child should be the inner sum-by(job,instance)
	innerAgg, ok := aggCnt.Child.(*AggNode)
	if !ok {
		t.Fatalf("count child not AggNode, got %T", aggCnt.Child)
	}
	if got, want := sorted(innerAgg.By), []string{"instance", "job"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("inner agg By=%v, want %v", got, want)
	}

	leaf, ok := innerAgg.Child.(*LeafNode)
	if !ok {
		t.Fatalf("inner agg child not LeafNode, got %T", innerAgg.Child)
	}
	if !leaf.BE.WantCount {
		t.Fatalf("WantCount should be true on leaf")
	}

	// Leaf should aggregate at child identity and count on the parent keep-set.
	if got, want := sorted(leaf.BE.GroupBy), []string{"instance", "job"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("leaf GroupBy=%v, want %v", got, want)
	}
	if got, want := sorted(leaf.BE.CountOnBy), []string{"job"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("leaf CountOnBy=%v, want %v", got, want)
	}
	// No TopK hints
	if leaf.BE.WantTopK || leaf.BE.WantBottomK {
		t.Fatalf("unexpected topk hints on count leaf: %+v", leaf.BE)
	}
}

func TestCompile_BinaryWithClampMin(t *testing.T) {
	q := `
		sum(rate(a_metric[5m])) /
		clamp_min(sum(rate(b_metric[5m])), 1)
	`
	root := mustParse(t, q)

	res, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	bin, ok := res.Root.(*BinaryNode)
	if !ok {
		t.Fatalf("root not BinaryNode, got %T", res.Root)
	}
	if bin.Op != OpDiv {
		t.Fatalf("bin op = %v, want /", bin.Op)
	}
	// Left subtree should boil down to a Leaf under an Agg
	lAgg, ok := bin.LHS.(*AggNode)
	if !ok {
		t.Fatalf("LHS not AggNode, got %T", bin.LHS)
	}
	if _, ok := lAgg.Child.(*LeafNode); !ok {
		t.Fatalf("LHS child not LeafNode, got %T", lAgg.Child)
	}
	// Right subtree should be ClampMinNode(... Agg( Leaf ))
	rClamp, ok := bin.RHS.(*ClampMinNode)
	if !ok {
		t.Fatalf("RHS not ClampMinNode, got %T", bin.RHS)
	}
	if rClamp.Min != 1 {
		t.Fatalf("ClampMin.Max=%v, want 1", rClamp.Min)
	}
	rAgg, ok := rClamp.Child.(*AggNode)
	if !ok {
		t.Fatalf("Clamp child not AggNode, got %T", rClamp.Child)
	}
	if _, ok := rAgg.Child.(*LeafNode); !ok {
		t.Fatalf("Clamp->Agg child not LeafNode, got %T", rAgg.Child)
	}

	// Expect two unique leaves (a_metric and b_metric)
	if len(res.Leaves) != 2 {
		t.Fatalf("want 2 leaves, got %d", len(res.Leaves))
	}
	names := map[string]bool{}
	for _, be := range res.Leaves {
		names[be.Metric] = true
		if be.Range != "5m" || be.FuncName != "rate" {
			t.Fatalf("leaf wrong range/func: %+v", be)
		}
	}
	if !(names["a_metric"] && names["b_metric"]) {
		t.Fatalf("leaves missing expected metrics: %v", names)
	}
}

func TestCompile_BinaryScalarTimesVector(t *testing.T) {
	q := `100 * sum(rate(a_metric[5m]))`
	root := mustParse(t, q)

	res, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	// Root should be a BinaryNode with *
	bin, ok := res.Root.(*BinaryNode)
	if !ok {
		t.Fatalf("root not BinaryNode, got %T", res.Root)
	}
	if bin.Op != OpMul {
		t.Fatalf("bin op = %v, want *", bin.Op)
	}

	// We don’t assert LHS node shape here (scalar literal lowering may differ).
	// But we do assert the RHS subtree is Agg(sum(rate(...))) -> Leaf
	rAgg, ok := bin.RHS.(*AggNode)
	if !ok {
		t.Fatalf("RHS not AggNode, got %T", bin.RHS)
	}
	if rAgg.Op != AggSum {
		t.Fatalf("RHS agg op = %v, want sum", rAgg.Op)
	}
	rLeaf, ok := rAgg.Child.(*LeafNode)
	if !ok {
		t.Fatalf("RHS child not LeafNode, got %T", rAgg.Child)
	}

	be := rLeaf.BE
	if be.Metric != "a_metric" {
		t.Fatalf("leaf metric=%q, want a_metric", be.Metric)
	}
	if be.FuncName != "rate" || be.Range != "5m" {
		t.Fatalf("leaf wrong func/range: %+v", be)
	}

	// Expect exactly one unique leaf.
	if len(res.Leaves) != 1 {
		t.Fatalf("want 1 leaf, got %d", len(res.Leaves))
	}
}

func TestCompile_ScalarOf_Vector(t *testing.T) {
	q := `scalar(sum(rate(a_metric[5m])))`
	root := mustParse(t, q)

	res, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	// Root should be ScalarOfNode
	sc, ok := res.Root.(*ScalarOfNode)
	if !ok {
		t.Fatalf("root not ScalarOfNode, got %T", res.Root)
	}

	// Child should be Agg(sum(...))
	agg, ok := sc.Child.(*AggNode)
	if !ok {
		t.Fatalf("scalar child not AggNode, got %T", sc.Child)
	}
	if agg.Op != AggSum {
		t.Fatalf("agg op = %v, want sum", agg.Op)
	}

	// Under Agg: Leaf
	leaf, ok := agg.Child.(*LeafNode)
	if !ok {
		t.Fatalf("agg child not LeafNode, got %T", agg.Child)
	}
	be := leaf.BE
	if be.Metric != "a_metric" {
		t.Fatalf("leaf metric=%q, want a_metric", be.Metric)
	}
	if be.FuncName != "rate" || be.Range != "5m" {
		t.Fatalf("leaf wrong func/range: %+v", be)
	}

	// Exactly one leaf
	if len(res.Leaves) != 1 {
		t.Fatalf("want 1 leaf, got %d", len(res.Leaves))
	}
}

func TestCompile_QuantileOverTime_WantDDS(t *testing.T) {
	q := `quantile_over_time(0.95, http_request_duration_seconds[5m])`
	root := mustParse(t, q)

	res, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	// Expect a single leaf (range-vector func pushed down)
	leaf, ok := res.Root.(*LeafNode)
	if !ok {
		t.Fatalf("root not LeafNode, got %T", res.Root)
	}
	be := leaf.BE
	if be.FuncName != "quantile_over_time" {
		t.Fatalf("leaf FuncName=%q, want quantile_over_time", be.FuncName)
	}
	if be.Range != "5m" || be.Metric != "http_request_duration_seconds" {
		t.Fatalf("leaf wrong range/metric: %+v", be)
	}
	// Critical: worker should return DDSketch
	if !be.WantDDS {
		t.Fatalf("WantDDS should be true on leaf for quantile_over_time")
	}
	if len(res.Leaves) != 1 {
		t.Fatalf("want 1 leaf, got %d", len(res.Leaves))
	}
}

func TestCompile_TopK_HistogramQuantile(t *testing.T) {
	q := `topk(5, histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket[5m])) by (le, service)))`
	root := mustParse(t, q)

	res, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	// Root: TopKNode(5)
	topkNode, ok := res.Root.(*TopKNode)
	if !ok {
		t.Fatalf("root not TopKNode, got %T", res.Root)
	}
	if topkNode.K != 5 {
		t.Fatalf("TopK.K=%d, want 5", topkNode.K)
	}

	// Child: QuantileNode(0.95)
	qnode, ok := topkNode.Child.(*QuantileNode)
	if !ok {
		t.Fatalf("topk child not QuantileNode, got %T", topkNode.Child)
	}
	if qnode.Q != 0.95 {
		t.Fatalf("Quantile.Q=%v, want 0.95", qnode.Q)
	}

	// Under Quantile: Agg(sum by le,service)
	innerAgg, ok := qnode.Child.(*AggNode)
	if !ok {
		t.Fatalf("quantile child not AggNode, got %T", qnode.Child)
	}
	if innerAgg.Op != AggSum {
		t.Fatalf("inner agg op = %v, want sum", innerAgg.Op)
	}
	wantBy := []string{"le", "service"}
	if !reflect.DeepEqual(sorted(innerAgg.By), sorted(wantBy)) {
		t.Fatalf("inner agg by = %#v, want %v", innerAgg.By, wantBy)
	}

	// Leaf: rate(http_request_duration_seconds_bucket[5m])
	leaf, ok := innerAgg.Child.(*LeafNode)
	if !ok {
		t.Fatalf("inner agg child not LeafNode, got %T", innerAgg.Child)
	}
	be := leaf.BE
	if be.Metric != "http_request_duration_seconds_bucket" {
		t.Fatalf("metric=%q, want http_request_duration_seconds_bucket", be.Metric)
	}
	if be.FuncName != "rate" || be.Range != "5m" {
		t.Fatalf("leaf wrong func/range: %+v", be)
	}
	// No special count/topk hints on leaf here (topk runs at the API over scalars).
	if be.WantCount {
		t.Fatalf("unexpected WantCount on leaf: %+v", be)
	}

	// Expect one leaf
	if len(res.Leaves) != 1 {
		t.Fatalf("want 1 leaf, got %d", len(res.Leaves))
	}
}

func TestCompile_UnaryMathFuncs(t *testing.T) {
	funcs := []string{
		"abs", "ceil", "floor", "exp", "ln", "log2", "log10", "sqrt", "sgn",
	}
	for _, fn := range funcs {
		t.Run(fn, func(t *testing.T) {
			// Example: abs(sum(rate(a_metric[5m])))
			q := fn + `(sum(rate(a_metric[5m])))`
			root := mustParse(t, q)

			res, err := Compile(root)
			if err != nil {
				t.Fatalf("Compile error for %s: %v", fn, err)
			}

			// Root should be UnaryNode
			un, ok := res.Root.(*UnaryNode)
			if !ok {
				t.Fatalf("root not UnaryNode for %s, got %T", fn, res.Root)
			}
			if un.Func != fn {
				t.Fatalf("UnaryNode.Op=%q, want %q", un.Func, fn)
			}

			// Child should be Agg(sum(...))
			agg, ok := un.Child.(*AggNode)
			if !ok {
				t.Fatalf("unary child not AggNode, got %T", un.Child)
			}
			if agg.Op != AggSum {
				t.Fatalf("agg op = %v, want sum", agg.Op)
			}

			// Leaf under Agg
			leaf, ok := agg.Child.(*LeafNode)
			if !ok {
				t.Fatalf("agg child not LeafNode, got %T", agg.Child)
			}
			be := leaf.BE

			// Leaf: rate(a_metric[5m])
			if be.Metric != "a_metric" {
				t.Fatalf("leaf metric=%q, want a_metric", be.Metric)
			}
			if be.FuncName != "rate" || be.Range != "5m" {
				t.Fatalf("leaf wrong func/range: %+v", be)
			}

			// No grouping at leaf for this query
			if len(be.GroupBy) != 0 || len(be.Without) != 0 {
				t.Fatalf("unexpected grouping on leaf: GroupBy=%v Without=%v", be.GroupBy, be.Without)
			}

			// No special hints
			if be.WantTopK || be.WantBottomK || be.WantCount || be.WantDDS {
				t.Fatalf("unexpected hints set on leaf: %+v", be)
			}

			// Exactly one leaf
			if got := len(res.Leaves); got != 1 {
				t.Fatalf("want 1 leaf, got %d: %+v", got, res.Leaves)
			}
			// Leaves[0] should match the leaf we observed
			if !reflect.DeepEqual(res.Leaves[0], be) {
				t.Fatalf("deduped leaf mismatch: got %+v want %+v", res.Leaves[0], be)
			}
		})
	}
}

func TestCompile_CountByInstance_Grouped(t *testing.T) {
	q := `count by (instance) (http_requests_total{job="api"})`
	root := mustParse(t, q)

	res, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	// Root: count by (instance)
	agg, ok := res.Root.(*AggNode)
	if !ok {
		t.Fatalf("root not AggNode, got %T", res.Root)
	}
	if agg.Op != AggCount {
		t.Fatalf("agg op = %v, want count", agg.Op)
	}
	if !reflect.DeepEqual(agg.By, []string{"instance"}) {
		t.Fatalf("agg by = %#v, want [instance]", agg.By)
	}

	// Child should be a Leaf
	leaf, ok := agg.Child.(*LeafNode)
	if !ok {
		t.Fatalf("child not LeafNode, got %T", agg.Child)
	}
	be := leaf.BE

	// Hints
	if !be.WantCount || be.WantTopK || be.WantBottomK {
		t.Fatalf("unexpected hints on leaf: %+v", be)
	}

	// Leaf should group by instance (per-group counting)
	if got, want := be.GroupBy, []string{"instance"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("leaf GroupBy=%v, want %v", got, want)
	}

	// Matcher job="api" should be present
	found := false
	for _, m := range be.Matchers {
		if m.Label == "job" && m.Op == MatchEq && m.Value == "api" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf(`missing matcher job="api": %#v`, be.Matchers)
	}

	// Exactly one leaf expected
	if len(res.Leaves) != 1 {
		t.Fatalf("want 1 leaf, got %d", len(res.Leaves))
	}
}
