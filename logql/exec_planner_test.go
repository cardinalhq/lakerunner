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
	"strings"
	"testing"
)

func TestBasicQuery(t *testing.T) {
	q := `{app="api-gateway"}`
	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}
	plan, err := CompileLog(ast)
	if err != nil {
		t.Fatalf("CompileLog() error: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(plan.Leaves))
	}
	if len(plan.Leaves[0].LabelFilters) != 0 {
		t.Fatalf("expected 0 label filters, got %d", len(plan.Leaves[0].LabelFilters))
	}
}

func TestBasicQueryWithDots(t *testing.T) {
	q := `{resource_service_name="api-gateway"}` // legal LogQL
	ast, _ := FromLogQL(q)
	if ast.LogSel.Matchers[0].Label != "resource_service_name" {
		t.Fatalf("expected label resource_service_name, got %q", ast.LogSel.Matchers[0].Label)
	}
}

func TestPipelineFilterNormalizationWithDots(t *testing.T) {
	q := `{job="x"} | resource_service_name="api-gateway"`
	ast, _ := FromLogQL(q)
	found := false
	for _, lf := range ast.LogSel.LabelFilters {
		if lf.Label == "resource_service_name" && lf.Op == MatchEq && lf.Value == "api-gateway" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected label filter resource_service_name=\"api-gateway\", got %#v", ast.LogSel.LabelFilters)
	}
}

func TestSplitPipelineStages_NoPipeline(t *testing.T) {
	in := `{app="api-gateway"}`
	got := splitPipelineStages(in)
	if len(got) != 0 {
		t.Fatalf("expected 0 stages, got %d: %#v", len(got), got)
	}
}

func TestSplitPipelineStages_RespectsBackticks(t *testing.T) {
	in := `{job="x"} | json | label_format api=` + "`{{ .msg | lower }}`" + ` | api="foo"`
	got := splitPipelineStages(in)
	if len(got) != 3 {
		t.Fatalf("expected 3 stages, got %d: %#v", len(got), got)
	}
	if got[0] != "json" {
		t.Fatalf("stage[0] = %q, want json", got[0])
	}
	if !strings.Contains(got[1], "`{{ .msg | lower }}`") || !strings.HasPrefix(got[1], "label_format ") {
		t.Fatalf("stage[1] malformed: %q", got[1])
	}
	if got[2] != `api="foo"` {
		t.Fatalf("stage[2] = %q, want api=\"foo\"", got[2])
	}
}

func TestPlanner_RegexpPipelineLeaf_Root(t *testing.T) {
	q := `{job="my-app"} | regexp "level=(?P<log_level>\\w+).*user=(?P<username>\\w+)"`
	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}

	plan, err := CompileLog(ast)
	if err != nil {
		t.Fatalf("CompileLog() error: %v", err)
	}

	// Expect exactly one pushdown leaf.
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d; plan=%#v", len(plan.Leaves), plan)
	}
	gotLeaf := plan.Leaves[0]

	// Root must be that same leaf node.
	rootLeafNode, ok := plan.Root.(*LLeafNode)
	if !ok {
		t.Fatalf("root is not *LLeafNode, got %T", plan.Root)
	}
	if rootLeafNode.Leaf.ID != gotLeaf.ID {
		t.Fatalf("root leaf ID mismatch: got %s, want %s", rootLeafNode.Leaf.ID, gotLeaf.ID)
	}

	// Spot-check the root leaf’s content.
	if !hasMatcher(rootLeafNode.Leaf.Matchers, "job", "my-app") {
		t.Fatalf("root leaf missing matcher job=my-app; got %#v", rootLeafNode.Leaf.Matchers)
	}

	// Should include a regexp parser stage; if parser records params, verify them.
	foundRegexp := false
	for _, p := range rootLeafNode.Leaf.Parsers {
		if p.Type == "regexp" {
			foundRegexp = true
			// Optional deeper checks if available:
			if pat := p.Params["pattern"]; pat != "" && !strings.Contains(pat, "(?P<log_level>") {
				t.Fatalf("regexp pattern missing named capture: %q", pat)
			}
			break
		}
	}
	if !foundRegexp {
		t.Fatalf("root leaf missing regexp parser stage; parsers=%#v", rootLeafNode.Leaf.Parsers)
	}

	// No range/vector aggregation in this query.
	if rootLeafNode.Leaf.RangeAggOp != "" {
		t.Fatalf("unexpected RangeAggOp on root leaf: %q", rootLeafNode.Leaf.RangeAggOp)
	}
	if len(rootLeafNode.Leaf.OutBy) != 0 || len(rootLeafNode.Leaf.OutWithout) != 0 {
		t.Fatalf("unexpected OutBy/OutWithout on root leaf: by=%v wo=%v",
			rootLeafNode.Leaf.OutBy, rootLeafNode.Leaf.OutWithout)
	}
}

func TestPlanner_RangeAgg_RootAndLeaf(t *testing.T) {
	q := `count_over_time({job="my-app"} | regexp "level=(?P<log_level>\\w+).*user=(?P<username>\\w+)" [5m])`
	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}
	plan, err := CompileLog(ast)
	if err != nil {
		t.Fatalf("CompileLog() error: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(plan.Leaves))
	}
	leaf := plan.Leaves[0]

	rn, ok := plan.Root.(*LRangeAggNode)
	if !ok {
		t.Fatalf("root not *LRangeAggNode, got %T", plan.Root)
	}
	if rn.Op != "count_over_time" {
		t.Fatalf("range agg op = %q, want count_over_time", rn.Op)
	}
	childLeaf, ok := rn.Child.(*LLeafNode)
	if !ok {
		t.Fatalf("range-agg child not *LLeafNode, got %T", rn.Child)
	}
	if childLeaf.Leaf.ID != leaf.ID {
		t.Fatalf("child leaf ID mismatch: got %s, want %s", childLeaf.Leaf.ID, leaf.ID)
	}
	if childLeaf.Leaf.Range != "5m" {
		t.Fatalf("leaf range = %q, want %q", childLeaf.Leaf.Range, "5m")
	}
}

func TestPlanner_RegexpPipelineLeaf_WithLabelFilters_AttachedToParser(t *testing.T) {
	q := `{job="my-app"} | regexp "level=(?P<level>\\w+).*user=(?P<username>\\w+)" | level="ERROR" | username=~"(alice|bob)"`

	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}

	plan, err := CompileLog(ast)
	if err != nil {
		t.Fatalf("CompileLog() error: %v", err)
	}

	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d; plan=%#v", len(plan.Leaves), plan)
	}
	leaf := plan.Leaves[0]
	rootLeaf, ok := plan.Root.(*LLeafNode)
	if !ok {
		t.Fatalf("root is not *LLeafNode, got %T", plan.Root)
	}
	if rootLeaf.Leaf.ID != leaf.ID {
		t.Fatalf("root leaf ID mismatch: got %s, want %s", rootLeaf.Leaf.ID, leaf.ID)
	}

	// Matchers include job="my-app".
	if !hasMatcher(leaf.Matchers, "job", "my-app") {
		t.Fatalf("missing matcher job=my-app; got %#v", leaf.Matchers)
	}

	reIdx := findParserIndexByType(leaf.Parsers, "regexp")
	if reIdx < 0 {
		t.Fatalf("expected a regexp parser stage; parsers=%#v", leaf.Parsers)
	}
	reStage := leaf.Parsers[reIdx]

	if pat := reStage.Params["pattern"]; pat != "" && !contains(pat, "(?P<level>") {
		t.Fatalf("regexp pattern missing named capture: %q", pat)
	}

	if !parserHasFilter(reStage, "level", MatchEq, "ERROR") {
		t.Fatalf("regexp stage missing filter level=\"ERROR\"; filters=%#v", reStage.Filters)
	}
	if !parserHasFilter(reStage, "username", MatchRe, "(alice|bob)") {
		t.Fatalf("regexp stage missing filter username=~\"(alice|bob)\"; filters=%#v", reStage.Filters)
	}

	// They must also appear in the flat LabelFilters with AfterParser=true and ParserIdx=reIdx.
	if !hasLabelFilterAttached(leaf.LabelFilters, "level", MatchEq, "ERROR", reIdx) {
		t.Fatalf("missing attached label filter log_level=\"ERROR\"; got %#v", leaf.LabelFilters)
	}
	if !hasLabelFilterAttached(leaf.LabelFilters, "username", MatchRe, "(alice|bob)", reIdx) {
		t.Fatalf("missing attached label filter username=~\"(alice|bob)\"; got %#v", leaf.LabelFilters)
	}

	if ia, oka := indexOfLabel(leaf.LabelFilters, "level"); oka {
		if ib, okb := indexOfLabel(leaf.LabelFilters, "username"); okb && !(ia < ib) {
			t.Fatalf("label filters order wrong: got %v", leaf.LabelFilters)
		}
	}

	if len(leaf.LineFilters) != 0 {
		t.Fatalf("unexpected line filters: %#v", leaf.LineFilters)
	}

	if leaf.RangeAggOp != "" {
		t.Fatalf("unexpected RangeAggOp on leaf: %q", leaf.RangeAggOp)
	}
	if len(leaf.OutBy) != 0 || len(leaf.OutWithout) != 0 {
		t.Fatalf("unexpected OutBy/OutWithout on leaf: by=%v wo=%v", leaf.OutBy, leaf.OutWithout)
	}
}

func TestPlannerWithNumericComparisons(t *testing.T) {
	q := `{resource_service_name="kafka"} | regexp "(?P<dur>[0-9]+(?:\\.[0-9]+)?)\\s*(?:ns|us|µs|ms|s|m|h)" | dur > 0`
	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}
	plan, err := CompileLog(ast)
	if err != nil {
		t.Fatalf("CompileLog() error: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d; plan=%#v", len(plan.Leaves), plan)
	}
	leaf := plan.Leaves[0]
	rootLeaf, ok := plan.Root.(*LLeafNode)
	if !ok {
		t.Fatalf("root is not *LLeafNode, got %T", plan.Root)
	}
	if rootLeaf.Leaf.ID != leaf.ID {
		t.Fatalf("root leaf ID mismatch: got %s, want %s", rootLeaf.Leaf.ID, leaf.ID)
	}
	if len(leaf.LabelFilters) != 1 {
		t.Fatalf("expected 1 line filter, got %d: %#v", len(leaf.LineFilters), leaf.LineFilters)
	}
	lf := leaf.LabelFilters[0]
	if lf.Label != "dur" || lf.Op != MatchGt || lf.Value != "0" {
		t.Fatalf("line filter mismatch: got %+v", lf)
	}
}

func TestPlannerWithUnwrap(t *testing.T) {
	q := `max_over_time(
  {job="kafka"} |= "Rolled new log segment"
  | regexp "in (?P<roll_dur>[0-9]+(?:\\.[0-9]+)?\\s*(?:ns|us|µs|ms|s|m|h))"
  | unwrap duration(roll_dur)
  [5m]
)`
	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}
	plan, err := CompileLog(ast)
	if err != nil {
		t.Fatalf("CompileLog() error: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d; plan=%#v", len(plan.Leaves), plan)
	}
	leaf := plan.Leaves[0]
	rootLeaf, ok := plan.Root.(*LRangeAggNode)
	if !ok {
		t.Fatalf("root is not *LRangeAggNode, got %T", plan.Root)
	}
	if rootLeaf.Child == nil {
		t.Fatalf("root range agg has nil child")
	}
	childLeaf, ok := rootLeaf.Child.(*LLeafNode)
	if !ok {
		t.Fatalf("root range agg child is not *LLeafNode, got %T", rootLeaf.Child)
	}
	if childLeaf.Leaf.ID != leaf.ID {
		t.Fatalf("root leaf ID mismatch: got %s, want %s", childLeaf.Leaf.ID, leaf.ID)
	}
}

func TestPlanner_JSONPipelineLeaf_WithLabelFilters_AttachedToParser(t *testing.T) {
	q := `{job="my-app"} | json | level="ERROR" | user=~"(alice|bob)"`

	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}

	plan, err := CompileLog(ast)
	if err != nil {
		t.Fatalf("CompileLog() error: %v", err)
	}

	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d; plan=%#v", len(plan.Leaves), plan)
	}
	leaf := plan.Leaves[0]
	rootLeaf, ok := plan.Root.(*LLeafNode)
	if !ok {
		t.Fatalf("root is not *LLeafNode, got %T", plan.Root)
	}
	if rootLeaf.Leaf.ID != leaf.ID {
		t.Fatalf("root leaf ID mismatch: got %s, want %s", rootLeaf.Leaf.ID, leaf.ID)
	}

	if !hasMatcher(leaf.Matchers, "job", "my-app") {
		t.Fatalf("missing matcher job=my-app; got %#v", leaf.Matchers)
	}

	jsonIdx := findParserIndexByType(leaf.Parsers, "json")
	if jsonIdx < 0 {
		t.Fatalf("expected a json parser stage; parsers=%#v", leaf.Parsers)
	}
	jsonStage := leaf.Parsers[jsonIdx]

	if !parserHasFilter(jsonStage, "level", MatchEq, "ERROR") {
		t.Fatalf("json stage missing filter level=\"ERROR\"; filters=%#v", jsonStage.Filters)
	}
	if !parserHasFilter(jsonStage, "user", MatchRe, "(alice|bob)") {
		t.Fatalf("json stage missing filter user=~\"(alice|bob)\"; filters=%#v", jsonStage.Filters)
	}

	if !hasLabelFilterAttached(leaf.LabelFilters, "level", MatchEq, "ERROR", jsonIdx) {
		t.Fatalf("missing attached label filter level=\"ERROR\"; got %#v", leaf.LabelFilters)
	}
	if !hasLabelFilterAttached(leaf.LabelFilters, "user", MatchRe, "(alice|bob)", jsonIdx) {
		t.Fatalf("missing attached label filter user=~\"(alice|bob)\"; got %#v", leaf.LabelFilters)
	}

	if ia, oka := indexOfLabel(leaf.LabelFilters, "level"); oka {
		if ib, okb := indexOfLabel(leaf.LabelFilters, "user"); okb && !(ia < ib) {
			t.Fatalf("label filters order wrong: got %v", leaf.LabelFilters)
		}
	}

	if len(leaf.LineFilters) != 0 {
		t.Fatalf("unexpected line filters: %#v", leaf.LineFilters)
	}

	if leaf.RangeAggOp != "" {
		t.Fatalf("unexpected RangeAggOp on leaf: %q", leaf.RangeAggOp)
	}
	if len(leaf.OutBy) != 0 || len(leaf.OutWithout) != 0 {
		t.Fatalf("unexpected OutBy/OutWithout on leaf: by=%v wo=%v", leaf.OutBy, leaf.OutWithout)
	}
}

func TestPlanner_MaxOverTime_WithLineFilter_JSONMap_Unwrap(t *testing.T) {
	q := `max_over_time({job="svc"} |= "/foo" | json lat_ms="req.lat_ms" | unwrap lat_ms [1m])`

	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}

	plan, err := CompileLog(ast)
	if err != nil {
		t.Fatalf("CompileLog() error: %v", err)
	}

	// One pushdown leaf expected.
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d; plan=%#v", len(plan.Leaves), plan)
	}
	leaf := plan.Leaves[0]

	// Root should be a range-agg node (max_over_time) with a leaf child.
	rn, ok := plan.Root.(*LRangeAggNode)
	if !ok {
		t.Fatalf("root not *LRangeAggNode, got %T", plan.Root)
	}
	if rn.Op != "max_over_time" {
		t.Fatalf("range agg op = %q, want max_over_time", rn.Op)
	}
	childLeaf, ok := rn.Child.(*LLeafNode)
	if !ok {
		t.Fatalf("range-agg child not *LLeafNode, got %T", rn.Child)
	}
	if childLeaf.Leaf.ID != leaf.ID {
		t.Fatalf("child leaf ID mismatch: got %s, want %s", childLeaf.Leaf.ID, leaf.ID)
	}
	if childLeaf.Leaf.Range != "1m" {
		t.Fatalf("leaf range = %q, want %q", childLeaf.Leaf.Range, "1m")
	}

	// Matchers include job="svc".
	if !hasMatcher(leaf.Matchers, "job", "svc") {
		t.Fatalf("missing matcher job=svc; got %#v", leaf.Matchers)
	}

	// Line filter |= "/foo" is present.
	foundContainsFoo := false
	for _, lf := range leaf.LineFilters {
		if lf.Op == LineContains && lf.Match == "/foo" {
			foundContainsFoo = true
			break
		}
	}
	if !foundContainsFoo {
		t.Fatalf("line filter |= \"/foo\" not found; line filters = %#v", leaf.LineFilters)
	}

	// JSON stage with mapping lat_ms="req.lat_ms".
	jsonIdx := findParserIndexByType(leaf.Parsers, "json")
	if jsonIdx < 0 {
		t.Fatalf("expected a json parser stage; parsers=%#v", leaf.Parsers)
	}
	jsonStage := leaf.Parsers[jsonIdx]
	if got := jsonStage.Params["lat_ms"]; got != "req.lat_ms" {
		t.Fatalf(`json mapping not captured: want Params["lat_ms"]="req.lat_ms", got %q (parser=%#v)`, got, jsonStage)
	}

	// Unwrap stage for lat_ms (identity) appears after json.
	unwrapIdx := findParserIndexByType(leaf.Parsers, "unwrap")
	if unwrapIdx < 0 {
		t.Fatalf("expected an unwrap parser stage; parsers=%#v", leaf.Parsers)
	}
	if !(unwrapIdx > jsonIdx) {
		t.Fatalf("unwrap stage should come after json: jsonIdx=%d unwrapIdx=%d", jsonIdx, unwrapIdx)
	}
	unwrapStage := leaf.Parsers[unwrapIdx]
	if unwrapStage.Params["func"] != "identity" || unwrapStage.Params["field"] != "lat_ms" {
		t.Fatalf("unwrap params = %#v (want func=identity, field=lat_ms)", unwrapStage.Params)
	}

	// No label filters introduced by this pipeline.
	if len(leaf.LabelFilters) != 0 {
		t.Fatalf("unexpected label filters: %#v", leaf.LabelFilters)
	}

	// No by/without on the leaf for this query.
	if len(leaf.OutBy) != 0 || len(leaf.OutWithout) != 0 {
		t.Fatalf("unexpected OutBy/OutWithout on leaf: by=%v wo=%v", leaf.OutBy, leaf.OutWithout)
	}
}

func TestPlanner_AvgOverTime_Regexp_UnwrapBytes(t *testing.T) {
	q := `avg_over_time({resource_service_name="kafka"} | regexp "\\[([^\\]]*)\\] ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z0-9-_.:]+) ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+) (?P<bytes>[0-9]+) ([A-Za-z0-9-_.:]+)" | unwrap bytes[5m])`

	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}

	plan, err := CompileLog(ast)
	if err != nil {
		t.Fatalf("CompileLog() error: %v", err)
	}

	// Exactly one leaf is expected.
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d; plan=%#v", len(plan.Leaves), plan)
	}
	leaf := plan.Leaves[0]

	// Root should be a range-agg node with avg_over_time.
	rn, ok := plan.Root.(*LRangeAggNode)
	if !ok {
		t.Fatalf("root is not *LRangeAggNode, got %T", plan.Root)
	}
	if rn.Op != "avg_over_time" {
		t.Fatalf("range agg op = %q, want avg_over_time", rn.Op)
	}

	// Child of range-agg must be our leaf.
	childLeaf, ok := rn.Child.(*LLeafNode)
	if !ok {
		t.Fatalf("range-agg child not *LLeafNode, got %T", rn.Child)
	}
	if childLeaf.Leaf.ID != leaf.ID {
		t.Fatalf("child leaf ID mismatch: got %s, want %s", childLeaf.Leaf.ID, leaf.ID)
	}

	// Window should be [5m].
	if childLeaf.Leaf.Range != "5m" {
		t.Fatalf("leaf range = %q, want %q", childLeaf.Leaf.Range, "5m")
	}

	// Matcher normalization: resource_service_name → resource.service.name
	if !hasMatcher(leaf.Matchers, "resource_service_name", "kafka") {
		t.Fatalf("missing matcher resource_service_name=kafka; matchers=%#v", leaf.Matchers)
	}

	// Must have both regexp and unwrap stages, in that order.
	reIdx := findParserIndexByType(leaf.Parsers, "regexp")
	uwIdx := findParserIndexByType(leaf.Parsers, "unwrap")
	if reIdx < 0 {
		t.Fatalf("missing regexp parser stage; parsers=%#v", leaf.Parsers)
	}
	if uwIdx < 0 {
		t.Fatalf("missing unwrap parser stage; parsers=%#v", leaf.Parsers)
	}
	if !(reIdx < uwIdx) {
		t.Fatalf("expected regexp before unwrap; got reIdx=%d, uwIdx=%d", reIdx, uwIdx)
	}

	// Regexp should include the named capture (?P<bytes>...).
	reStage := leaf.Parsers[reIdx]
	if pat := reStage.Params["pattern"]; pat == "" || !strings.Contains(pat, "(?P<bytes>") {
		t.Fatalf("regexp pattern missing (?P<bytes>...): %q", pat)
	}

	// Unwrap should target field "bytes" and (func "" or "identity") is acceptable.
	uwStage := leaf.Parsers[uwIdx]
	if uwStage.Params["field"] != "bytes" {
		t.Fatalf("unwrap field = %q, want %q; params=%#v", uwStage.Params["field"], "bytes", uwStage.Params)
	}
	if fn := uwStage.Params["func"]; fn != "" && fn != "identity" {
		t.Fatalf("unwrap func = %q, want identity/empty", fn)
	}

	// Sanity: no grouping on the leaf for this query.
	if len(leaf.OutBy) != 0 || len(leaf.OutWithout) != 0 {
		t.Fatalf("unexpected grouping on leaf: by=%v wo=%v", leaf.OutBy, leaf.OutWithout)
	}
}

func TestPlanner_CountOverTime_WithRegexLineFilter(t *testing.T) {
	q := `count_over_time({resource_service_name="kafka"} |~ "deleted"[1m])`

	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}

	plan, err := CompileLog(ast)
	if err != nil {
		t.Fatalf("CompileLog() error: %v", err)
	}

	// Exactly one pushdown leaf.
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d; plan=%#v", len(plan.Leaves), plan)
	}
	leaf := plan.Leaves[0]

	// Root must be a range-agg node with count_over_time
	rn, ok := plan.Root.(*LRangeAggNode)
	if !ok {
		t.Fatalf("root not *LRangeAggNode, got %T", plan.Root)
	}
	if rn.Op != "count_over_time" {
		t.Fatalf("range agg op = %q, want count_over_time", rn.Op)
	}

	// Child must be our leaf, with 1m range window.
	childLeaf, ok := rn.Child.(*LLeafNode)
	if !ok {
		t.Fatalf("range-agg child not *LLeafNode, got %T", rn.Child)
	}
	if childLeaf.Leaf.ID != leaf.ID {
		t.Fatalf("child leaf ID mismatch: got %s, want %s", childLeaf.Leaf.ID, leaf.ID)
	}
	if childLeaf.Leaf.Range != "1m" {
		t.Fatalf("leaf range = %q, want %q", childLeaf.Leaf.Range, "1m")
	}

	// Matcher normalization: resource_service_name → resource.service.name
	if !hasMatcher(leaf.Matchers, "resource_service_name", "kafka") {
		t.Fatalf("missing matcher resource_service_name=\"kafka\"; matchers=%#v", leaf.Matchers)
	}

	// Regex line filter |~ "deleted" must be present.
	foundRegex := false
	for _, lf := range leaf.LineFilters {
		if lf.Op == LineRegex && lf.Match == "deleted" {
			foundRegex = true
			break
		}
	}
	if !foundRegex {
		t.Fatalf("regex line filter |~ \"deleted\" not found; line filters = %#v", leaf.LineFilters)
	}

	// No parser stages and no attached label filters for this query.
	if len(leaf.Parsers) != 0 {
		t.Fatalf("unexpected parser stages: %#v", leaf.Parsers)
	}
	if len(leaf.LabelFilters) != 0 {
		t.Fatalf("unexpected label filters: %#v", leaf.LabelFilters)
	}

	// No grouping on the leaf.
	if len(leaf.OutBy) != 0 || len(leaf.OutWithout) != 0 {
		t.Fatalf("unexpected OutBy/OutWithout on leaf: by=%v wo=%v", leaf.OutBy, leaf.OutWithout)
	}
}

func TestPlanner_LineFormat_StageAndAttachedFilter(t *testing.T) {
	q := `{app="web"} | json msg="message" | line_format "{{ .msg }}" | level="ERROR"`

	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}
	plan, err := CompileLog(ast)
	if err != nil {
		t.Fatalf("CompileLog() error: %v", err)
	}

	// One pushdown leaf expected.
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d; plan=%#v", len(plan.Leaves), plan)
	}
	leaf := plan.Leaves[0]

	// Root should be that same leaf.
	rootLeaf, ok := plan.Root.(*LLeafNode)
	if !ok {
		t.Fatalf("root is not *LLeafNode, got %T", plan.Root)
	}
	if rootLeaf.Leaf.ID != leaf.ID {
		t.Fatalf("root leaf ID mismatch: got %s, want %s", rootLeaf.Leaf.ID, leaf.ID)
	}

	// Matchers include app="web".
	if !hasMatcher(leaf.Matchers, "app", "web") {
		t.Fatalf("missing matcher app=web; got %#v", leaf.Matchers)
	}

	// Stage order: json → line_format
	if len(leaf.Parsers) != 2 {
		t.Fatalf("expected 2 parsers (json, line_format), got %d: %#v", len(leaf.Parsers), leaf.Parsers)
	}
	if leaf.Parsers[0].Type != "json" {
		t.Fatalf("parsers[0].Type = %q, want %q", leaf.Parsers[0].Type, "json")
	}
	if leaf.Parsers[1].Type != "line_format" {
		t.Fatalf("parsers[1].Type = %q, want %q", leaf.Parsers[1].Type, "line_format")
	}

	// JSON mapping should be preserved.
	if got := leaf.Parsers[0].Params["msg"]; got != "message" {
		t.Fatalf(`json mapping not captured: want Params["msg"]="message", got %q`, got)
	}

	// Current parser does not extract outputs/templates for line_format.
	lfIdx := 1
	lfStage := leaf.Parsers[lfIdx]
	if len(lfStage.LabelFormats) != 0 {
		t.Fatalf("line_format should not expose LabelFormats; got %#v", lfStage.LabelFormats)
	}

	// The label filter after line_format (level="ERROR") must be attached to that stage.
	found := false
	for _, f := range leaf.LabelFilters {
		if f.Label == "level" && f.Op == MatchEq && f.Value == "ERROR" {
			found = true
			if !f.AfterParser {
				t.Fatalf("label filter %+v should have AfterParser=true", f)
			}
			if f.ParserIdx == nil || *f.ParserIdx != lfIdx {
				t.Fatalf("label filter %+v should have ParserIdx=%d, got %v", f, lfIdx, f.ParserIdx)
			}
			// also ensure it’s attached on the line_format stage itself
			if len(lfStage.Filters) != 1 ||
				lfStage.Filters[0].Label != "level" ||
				lfStage.Filters[0].Op != MatchEq ||
				lfStage.Filters[0].Value != "ERROR" {
				t.Fatalf("line_format stage filters not attached correctly: %#v", lfStage.Filters)
			}
			break
		}
	}
	if !found {
		t.Fatalf(`did not find expected label filter: level = "ERROR"; got: %#v`, leaf.LabelFilters)
	}

	// Pretty label rendering should include "line_format".
	lbl := (&rootLeaf.Leaf).Label()
	if !strings.Contains(lbl, "line_format") {
		t.Fatalf("pretty label missing line_format: %q", lbl)
	}
}

// Helper functions for tests.

func findParserIndexByType(ps []ParserStage, typ string) int {
	for i, p := range ps {
		if p.Type == typ {
			return i
		}
	}
	return -1
}

func parserHasFilter(p ParserStage, label string, op MatchOp, value string) bool {
	for _, f := range p.Filters {
		if f.Label == label && f.Op == op && f.Value == value {
			return true
		}
	}
	return false
}

func hasLabelFilterAttached(lfs []LabelFilter, label string, op MatchOp, value string, parserIdx int) bool {
	for _, lf := range lfs {
		if lf.Label == label && lf.Op == op && lf.Value == value && lf.AfterParser && lf.ParserIdx != nil && *lf.ParserIdx == parserIdx {
			return true
		}
	}
	return false
}

func indexOfLabel(lfs []LabelFilter, label string) (int, bool) {
	for i, lf := range lfs {
		if lf.Label == label {
			return i, true
		}
	}
	return -1, false
}

// --- Tests for IsSimpleAggregation and RequiredColumns ---

func TestLogLeaf_IsSimpleAggregation_True(t *testing.T) {
	leaf := LogLeaf{
		ID:       "simple1",
		Matchers: []LabelMatch{{Label: "resource_service_name", Op: MatchEq, Value: "svc1"}},
		OutBy:    []string{"log_level"},
	}
	if !leaf.IsSimpleAggregation() {
		t.Fatal("expected IsSimpleAggregation() = true for leaf with no parsers/filters")
	}
}

func TestLogLeaf_IsSimpleAggregation_FalseWithParser(t *testing.T) {
	leaf := LogLeaf{
		ID:       "complex1",
		Matchers: []LabelMatch{{Label: "resource_service_name", Op: MatchEq, Value: "svc1"}},
		OutBy:    []string{"log_level"},
		Parsers:  []ParserStage{{Type: "json"}},
	}
	if leaf.IsSimpleAggregation() {
		t.Fatal("expected IsSimpleAggregation() = false for leaf with parser")
	}
}

func TestLogLeaf_IsSimpleAggregation_FalseWithLineFilter(t *testing.T) {
	leaf := LogLeaf{
		ID:          "complex2",
		Matchers:    []LabelMatch{{Label: "resource_service_name", Op: MatchEq, Value: "svc1"}},
		OutBy:       []string{"log_level"},
		LineFilters: []LineFilter{{Op: LineContains, Match: "error"}},
	}
	if leaf.IsSimpleAggregation() {
		t.Fatal("expected IsSimpleAggregation() = false for leaf with line filter")
	}
}

func TestLogLeaf_IsSimpleAggregation_FalseWithLabelFilter(t *testing.T) {
	leaf := LogLeaf{
		ID:           "complex3",
		Matchers:     []LabelMatch{{Label: "resource_service_name", Op: MatchEq, Value: "svc1"}},
		OutBy:        []string{"log_level"},
		LabelFilters: []LabelFilter{{Label: "level", Op: MatchEq, Value: "ERROR"}},
	}
	if leaf.IsSimpleAggregation() {
		t.Fatal("expected IsSimpleAggregation() = false for leaf with label filter")
	}
}
