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
