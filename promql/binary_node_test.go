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

package promql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// staticNode is a test helper that returns a fixed result map.
type staticNode struct {
	results map[string]EvalResult
}

func (s *staticNode) Hints() ExecHints                                          { return ExecHints{} }
func (s *staticNode) Eval(_ SketchGroup, _ time.Duration) map[string]EvalResult { return s.results }
func (s *staticNode) Label(_ map[string]any) string                             { return "static" }

func scalarResult(ts int64, v float64, tags map[string]any) EvalResult {
	return EvalResult{
		Timestamp: ts,
		Value:     Value{Kind: ValScalar, Num: v},
		Tags:      tags,
	}
}

// tags helper for concise test data.
func tags(kvs ...any) map[string]any {
	m := make(map[string]any, len(kvs)/2)
	for i := 0; i < len(kvs); i += 2 {
		m[kvs[i].(string)] = kvs[i+1]
	}
	return m
}

func TestEvalOr(t *testing.T) {
	sg := SketchGroup{Timestamp: 1000}
	step := time.Minute

	tests := []struct {
		name   string
		lhs    map[string]EvalResult
		rhs    map[string]EvalResult
		expect map[string]EvalResult
	}{
		{
			name: "overlapping matchKey - LHS wins",
			lhs: map[string]EvalResult{
				"a": scalarResult(1000, 1, tags("job", "api")),
				"b": scalarResult(1000, 2, tags("job", "web")),
			},
			rhs: map[string]EvalResult{
				"b2": scalarResult(1000, 20, tags("job", "web")),
				"c":  scalarResult(1000, 3, tags("job", "db")),
			},
			expect: map[string]EvalResult{
				"a": scalarResult(1000, 1, tags("job", "api")),
				"b": scalarResult(1000, 2, tags("job", "web")),
				"c": scalarResult(1000, 3, tags("job", "db")),
			},
		},
		{
			name: "LHS empty",
			lhs:  map[string]EvalResult{},
			rhs: map[string]EvalResult{
				"a": scalarResult(1000, 1, tags("job", "api")),
			},
			expect: map[string]EvalResult{
				"a": scalarResult(1000, 1, tags("job", "api")),
			},
		},
		{
			name: "RHS empty",
			lhs: map[string]EvalResult{
				"a": scalarResult(1000, 1, tags("job", "api")),
			},
			rhs:    map[string]EvalResult{},
			expect: map[string]EvalResult{"a": scalarResult(1000, 1, tags("job", "api"))},
		},
		{
			name:   "both empty",
			lhs:    map[string]EvalResult{},
			rhs:    map[string]EvalResult{},
			expect: map[string]EvalResult{},
		},
		{
			name: "disjoint matchKeys",
			lhs:  map[string]EvalResult{"a": scalarResult(1000, 1, tags("job", "api"))},
			rhs:  map[string]EvalResult{"b": scalarResult(1000, 2, tags("job", "web"))},
			expect: map[string]EvalResult{
				"a": scalarResult(1000, 1, tags("job", "api")),
				"b": scalarResult(1000, 2, tags("job", "web")),
			},
		},
		{
			name:   "identical matchKey - LHS wins",
			lhs:    map[string]EvalResult{"a": scalarResult(1000, 1, tags("job", "api"))},
			rhs:    map[string]EvalResult{"a2": scalarResult(1000, 99, tags("job", "api"))},
			expect: map[string]EvalResult{"a": scalarResult(1000, 1, tags("job", "api"))},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &BinaryNode{
				Op:  OpOr,
				LHS: &staticNode{results: tt.lhs},
				RHS: &staticNode{results: tt.rhs},
			}
			got := n.Eval(sg, step)
			assert.Equal(t, tt.expect, got)
		})
	}
}

func TestEvalAnd(t *testing.T) {
	sg := SketchGroup{Timestamp: 1000}
	step := time.Minute

	tests := []struct {
		name   string
		lhs    map[string]EvalResult
		rhs    map[string]EvalResult
		expect map[string]EvalResult
	}{
		{
			name: "overlapping matchKey - LHS values kept",
			lhs: map[string]EvalResult{
				"a": scalarResult(1000, 1, tags("job", "api")),
				"b": scalarResult(1000, 2, tags("job", "web")),
			},
			rhs: map[string]EvalResult{
				"b2": scalarResult(1000, 20, tags("job", "web")),
				"c":  scalarResult(1000, 3, tags("job", "db")),
			},
			expect: map[string]EvalResult{
				"b": scalarResult(1000, 2, tags("job", "web")),
			},
		},
		{
			name: "LHS empty",
			lhs:  map[string]EvalResult{},
			rhs: map[string]EvalResult{
				"a": scalarResult(1000, 1, tags("job", "api")),
			},
			expect: map[string]EvalResult{},
		},
		{
			name: "RHS empty",
			lhs: map[string]EvalResult{
				"a": scalarResult(1000, 1, tags("job", "api")),
			},
			rhs:    map[string]EvalResult{},
			expect: map[string]EvalResult{},
		},
		{
			name:   "disjoint matchKeys",
			lhs:    map[string]EvalResult{"a": scalarResult(1000, 1, tags("job", "api"))},
			rhs:    map[string]EvalResult{"b": scalarResult(1000, 2, tags("job", "web"))},
			expect: map[string]EvalResult{},
		},
		{
			name:   "identical matchKey - LHS values kept",
			lhs:    map[string]EvalResult{"a": scalarResult(1000, 1, tags("job", "api"))},
			rhs:    map[string]EvalResult{"a2": scalarResult(1000, 99, tags("job", "api"))},
			expect: map[string]EvalResult{"a": scalarResult(1000, 1, tags("job", "api"))},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &BinaryNode{
				Op:  OpAnd,
				LHS: &staticNode{results: tt.lhs},
				RHS: &staticNode{results: tt.rhs},
			}
			got := n.Eval(sg, step)
			assert.Equal(t, tt.expect, got)
		})
	}
}

func TestEvalUnless(t *testing.T) {
	sg := SketchGroup{Timestamp: 1000}
	step := time.Minute

	tests := []struct {
		name   string
		lhs    map[string]EvalResult
		rhs    map[string]EvalResult
		expect map[string]EvalResult
	}{
		{
			name: "overlapping matchKey",
			lhs: map[string]EvalResult{
				"a": scalarResult(1000, 1, tags("job", "api")),
				"b": scalarResult(1000, 2, tags("job", "web")),
			},
			rhs: map[string]EvalResult{
				"b2": scalarResult(1000, 20, tags("job", "web")),
				"c":  scalarResult(1000, 3, tags("job", "db")),
			},
			expect: map[string]EvalResult{
				"a": scalarResult(1000, 1, tags("job", "api")),
			},
		},
		{
			name: "LHS empty",
			lhs:  map[string]EvalResult{},
			rhs: map[string]EvalResult{
				"a": scalarResult(1000, 1, tags("job", "api")),
			},
			expect: map[string]EvalResult{},
		},
		{
			name: "RHS empty - all LHS kept",
			lhs: map[string]EvalResult{
				"a": scalarResult(1000, 1, tags("job", "api")),
			},
			rhs: map[string]EvalResult{},
			expect: map[string]EvalResult{
				"a": scalarResult(1000, 1, tags("job", "api")),
			},
		},
		{
			name: "disjoint matchKeys - all LHS kept",
			lhs:  map[string]EvalResult{"a": scalarResult(1000, 1, tags("job", "api"))},
			rhs:  map[string]EvalResult{"b": scalarResult(1000, 2, tags("job", "web"))},
			expect: map[string]EvalResult{
				"a": scalarResult(1000, 1, tags("job", "api")),
			},
		},
		{
			name:   "identical matchKey - nothing kept",
			lhs:    map[string]EvalResult{"a": scalarResult(1000, 1, tags("job", "api"))},
			rhs:    map[string]EvalResult{"a2": scalarResult(1000, 99, tags("job", "api"))},
			expect: map[string]EvalResult{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &BinaryNode{
				Op:  OpUnless,
				LHS: &staticNode{results: tt.lhs},
				RHS: &staticNode{results: tt.rhs},
			}
			got := n.Eval(sg, step)
			assert.Equal(t, tt.expect, got)
		})
	}
}

func TestEvalArithmetic_Regression(t *testing.T) {
	sg := SketchGroup{Timestamp: 1000}
	step := time.Minute

	lhs := map[string]EvalResult{"a": scalarResult(1000, 10, tags("job", "api"))}
	rhs := map[string]EvalResult{"a": scalarResult(1000, 3, tags("job", "api"))}

	tests := []struct {
		op     BinOp
		expect float64
	}{
		{OpAdd, 13},
		{OpSub, 7},
		{OpMul, 30},
		{OpDiv, 10.0 / 3.0},
	}

	for _, tt := range tests {
		t.Run(string(tt.op), func(t *testing.T) {
			n := &BinaryNode{
				Op:  tt.op,
				LHS: &staticNode{results: lhs},
				RHS: &staticNode{results: rhs},
			}
			got := n.Eval(sg, step)
			require.Len(t, got, 1)
			assert.InDelta(t, tt.expect, got["a"].Value.Num, 1e-9)
		})
	}
}

func TestEvalComparison_Regression(t *testing.T) {
	sg := SketchGroup{Timestamp: 1000}
	step := time.Minute

	lhs := map[string]EvalResult{"a": scalarResult(1000, 10, tags("job", "api"))}
	rhs := map[string]EvalResult{"a": scalarResult(1000, 5, tags("job", "api"))}

	tests := []struct {
		op      BinOp
		kept    bool
		boolVal float64
	}{
		{OpGT, true, 1},
		{OpGE, true, 1},
		{OpLT, false, 0},
		{OpLE, false, 0},
		{OpEQ, false, 0},
		{OpNE, true, 1},
	}

	for _, tt := range tests {
		t.Run(string(tt.op)+"_filter", func(t *testing.T) {
			n := &BinaryNode{
				Op:  tt.op,
				LHS: &staticNode{results: lhs},
				RHS: &staticNode{results: rhs},
			}
			got := n.Eval(sg, step)
			if tt.kept {
				require.Len(t, got, 1)
			} else {
				assert.Empty(t, got)
			}
		})

		t.Run(string(tt.op)+"_bool", func(t *testing.T) {
			n := &BinaryNode{
				Op:         tt.op,
				LHS:        &staticNode{results: lhs},
				RHS:        &staticNode{results: rhs},
				ReturnBool: true,
			}
			got := n.Eval(sg, step)
			require.Len(t, got, 1)
			assert.Equal(t, tt.boolVal, got["a"].Value.Num)
		})
	}
}

func TestLabel_SetOperators(t *testing.T) {
	tags := map[string]any{}
	lhs := &staticNode{}
	rhs := &staticNode{}

	tests := []struct {
		op     BinOp
		expect string
	}{
		{OpOr, "(static or static)"},
		{OpAnd, "(static and static)"},
		{OpUnless, "(static unless static)"},
	}

	for _, tt := range tests {
		t.Run(string(tt.op), func(t *testing.T) {
			n := &BinaryNode{Op: tt.op, LHS: lhs, RHS: rhs}
			assert.Equal(t, tt.expect, n.Label(tags))
		})
	}
}

// --- matchKey tests ---

func TestMatchKey(t *testing.T) {
	tests := []struct {
		name  string
		tags  map[string]any
		match *VectorMatch
		want  string
	}{
		{
			name: "nil tags",
			tags: nil,
			want: "",
		},
		{
			name: "strips name",
			tags: tags("name", "metric_a", "job", "api"),
			want: "job=api",
		},
		{
			name: "strips __name__",
			tags: tags("__name__", "metric_a", "job", "api"),
			want: "job=api",
		},
		{
			name: "strips both name and __name__",
			tags: tags("name", "metric_a", "__name__", "metric_a", "job", "api"),
			want: "job=api",
		},
		{
			name:  "on() includes only specified labels",
			tags:  tags("job", "api", "env", "prod", "region", "us"),
			match: &VectorMatch{On: []string{"job"}},
			want:  "job=api",
		},
		{
			name:  "on() excludes name even if listed",
			tags:  tags("name", "metric_a", "job", "api"),
			match: &VectorMatch{On: []string{"name", "job"}},
			want:  "job=api",
		},
		{
			name:  "ignoring() excludes specified labels",
			tags:  tags("job", "api", "env", "prod", "region", "us"),
			match: &VectorMatch{Ignoring: []string{"env"}},
			want:  "job=api,region=us",
		},
		{
			name:  "ignoring() also excludes name automatically",
			tags:  tags("name", "metric_a", "job", "api", "env", "prod"),
			match: &VectorMatch{Ignoring: []string{"env"}},
			want:  "job=api",
		},
		{
			name: "sorted keys",
			tags: tags("z", "1", "a", "2", "m", "3"),
			want: "a=2,m=3,z=1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchKey(tt.tags, tt.match)
			assert.Equal(t, tt.want, got)
		})
	}
}

// --- dropMetricName tests ---

func TestDropMetricName(t *testing.T) {
	t.Run("no name present - returns same map", func(t *testing.T) {
		m := tags("job", "api")
		got := dropMetricName(m)
		assert.Equal(t, m, got)
	})

	t.Run("removes name", func(t *testing.T) {
		got := dropMetricName(tags("name", "metric_a", "job", "api"))
		assert.Equal(t, tags("job", "api"), got)
	})

	t.Run("removes __name__", func(t *testing.T) {
		got := dropMetricName(tags("__name__", "metric_a", "job", "api"))
		assert.Equal(t, tags("job", "api"), got)
	})

	t.Run("removes both", func(t *testing.T) {
		got := dropMetricName(tags("name", "x", "__name__", "y", "job", "api"))
		assert.Equal(t, tags("job", "api"), got)
	})
}

// --- Cross-metric matching tests ---

func TestEvalArithmetic_CrossMetric(t *testing.T) {
	sg := SketchGroup{Timestamp: 1000}
	step := time.Minute

	// Different name tags, same other tags → should match via matchKey.
	// Need 2+ entries per side to avoid asScalar heuristic.
	lhs := map[string]EvalResult{
		"l1": scalarResult(1000, 10, tags("name", "metric_a", "job", "api")),
		"l2": scalarResult(1000, 20, tags("name", "metric_a", "job", "web")),
	}
	rhs := map[string]EvalResult{
		"r1": scalarResult(1000, 3, tags("name", "metric_b", "job", "api")),
		"r2": scalarResult(1000, 7, tags("name", "metric_b", "job", "web")),
	}

	n := &BinaryNode{
		Op:  OpSub,
		LHS: &staticNode{results: lhs},
		RHS: &staticNode{results: rhs},
	}
	got := n.Eval(sg, step)
	require.Len(t, got, 2)
	assert.InDelta(t, 7.0, got["l1"].Value.Num, 1e-9)
	assert.InDelta(t, 13.0, got["l2"].Value.Num, 1e-9)
	// name should be dropped from result
	assert.Equal(t, tags("job", "api"), got["l1"].Tags)
	assert.Equal(t, tags("job", "web"), got["l2"].Tags)
}

func TestEvalArithmetic_NameDropped(t *testing.T) {
	sg := SketchGroup{Timestamp: 1000}
	step := time.Minute

	// Need 2+ entries per side to avoid asScalar heuristic.
	lhs := map[string]EvalResult{
		"l1": scalarResult(1000, 5, tags("name", "foo", "job", "api")),
		"l2": scalarResult(1000, 6, tags("name", "foo", "job", "web")),
	}
	rhs := map[string]EvalResult{
		"r1": scalarResult(1000, 2, tags("name", "bar", "job", "api")),
		"r2": scalarResult(1000, 3, tags("name", "bar", "job", "web")),
	}

	n := &BinaryNode{
		Op:  OpAdd,
		LHS: &staticNode{results: lhs},
		RHS: &staticNode{results: rhs},
	}
	got := n.Eval(sg, step)
	require.Len(t, got, 2)
	_, hasName := got["l1"].Tags["name"]
	assert.False(t, hasName, "name should be dropped from arithmetic result")
}

func TestEvalComparison_Filter_NameDropped(t *testing.T) {
	sg := SketchGroup{Timestamp: 1000}
	step := time.Minute

	lhs := map[string]EvalResult{
		"l1": scalarResult(1000, 10, tags("name", "foo", "job", "api")),
		"l2": scalarResult(1000, 20, tags("name", "foo", "job", "web")),
	}
	rhs := map[string]EvalResult{
		"r1": scalarResult(1000, 5, tags("name", "bar", "job", "api")),
		"r2": scalarResult(1000, 15, tags("name", "bar", "job", "web")),
	}

	n := &BinaryNode{
		Op:  OpGT,
		LHS: &staticNode{results: lhs},
		RHS: &staticNode{results: rhs},
	}
	got := n.Eval(sg, step)
	require.Len(t, got, 2)
	_, hasName := got["l1"].Tags["name"]
	assert.False(t, hasName, "name should be dropped from comparison filter result")
}

func TestEvalComparison_Bool_NameDropped(t *testing.T) {
	sg := SketchGroup{Timestamp: 1000}
	step := time.Minute

	lhs := map[string]EvalResult{
		"l1": scalarResult(1000, 10, tags("name", "foo", "job", "api")),
		"l2": scalarResult(1000, 20, tags("name", "foo", "job", "web")),
	}
	rhs := map[string]EvalResult{
		"r1": scalarResult(1000, 5, tags("name", "bar", "job", "api")),
		"r2": scalarResult(1000, 15, tags("name", "bar", "job", "web")),
	}

	n := &BinaryNode{
		Op:         OpGT,
		LHS:        &staticNode{results: lhs},
		RHS:        &staticNode{results: rhs},
		ReturnBool: true,
	}
	got := n.Eval(sg, step)
	require.Len(t, got, 2)
	_, hasName := got["l1"].Tags["name"]
	assert.False(t, hasName, "name should be dropped from comparison bool result")
}

func TestEvalSetOps_NamePreserved(t *testing.T) {
	sg := SketchGroup{Timestamp: 1000}
	step := time.Minute

	lhs := map[string]EvalResult{
		"l": scalarResult(1000, 10, tags("name", "foo", "job", "api")),
	}
	rhs := map[string]EvalResult{
		"r": scalarResult(1000, 5, tags("name", "bar", "job", "web")),
	}

	for _, op := range []BinOp{OpOr, OpAnd, OpUnless} {
		t.Run(string(op), func(t *testing.T) {
			n := &BinaryNode{
				Op:  op,
				LHS: &staticNode{results: lhs},
				RHS: &staticNode{results: rhs},
			}
			got := n.Eval(sg, step)
			for _, er := range got {
				_, hasName := er.Tags["name"]
				assert.True(t, hasName, "set operators should preserve name in result tags")
			}
		})
	}
}

func TestEvalVectorScalar_NameDropped(t *testing.T) {
	sg := SketchGroup{Timestamp: 1000}
	step := time.Minute

	lhs := map[string]EvalResult{
		"l": scalarResult(1000, 10, tags("name", "foo", "job", "api")),
	}
	rhs := map[string]EvalResult{
		"default": scalarResult(1000, 2, nil),
	}

	n := &BinaryNode{
		Op:  OpMul,
		LHS: &staticNode{results: lhs},
		RHS: &staticNode{results: rhs},
	}
	got := n.Eval(sg, step)
	require.Len(t, got, 1)
	_, hasName := got["l"].Tags["name"]
	assert.False(t, hasName, "name should be dropped from vector-scalar result")
	assert.Equal(t, "api", got["l"].Tags["job"])
	assert.InDelta(t, 20.0, got["l"].Value.Num, 1e-9)
}

// --- on()/ignoring() matching tests ---

func TestEvalArithmetic_OnMatching(t *testing.T) {
	sg := SketchGroup{Timestamp: 1000}
	step := time.Minute

	// Need 2+ entries per side to avoid asScalar heuristic.
	lhs := map[string]EvalResult{
		"l1": scalarResult(1000, 10, tags("job", "api", "env", "prod")),
		"l2": scalarResult(1000, 20, tags("job", "web", "env", "prod")),
	}
	rhs := map[string]EvalResult{
		"r1": scalarResult(1000, 3, tags("job", "api", "env", "staging")),
		"r2": scalarResult(1000, 7, tags("job", "web", "env", "staging")),
	}

	// Without on(), env differs so no match.
	n := &BinaryNode{
		Op:  OpAdd,
		LHS: &staticNode{results: lhs},
		RHS: &staticNode{results: rhs},
	}
	got := n.Eval(sg, step)
	assert.Empty(t, got, "without on(), different env should not match")

	// With on(job), only job is considered → match.
	n.Match = &VectorMatch{On: []string{"job"}}
	got = n.Eval(sg, step)
	require.Len(t, got, 2)
	assert.InDelta(t, 13.0, got["l1"].Value.Num, 1e-9)
	assert.InDelta(t, 27.0, got["l2"].Value.Num, 1e-9)
}

func TestEvalArithmetic_IgnoringMatching(t *testing.T) {
	sg := SketchGroup{Timestamp: 1000}
	step := time.Minute

	// Need 2+ entries per side to avoid asScalar heuristic.
	lhs := map[string]EvalResult{
		"l1": scalarResult(1000, 10, tags("job", "api", "env", "prod", "instance", "i1")),
		"l2": scalarResult(1000, 20, tags("job", "web", "env", "prod", "instance", "i3")),
	}
	rhs := map[string]EvalResult{
		"r1": scalarResult(1000, 3, tags("job", "api", "env", "prod", "instance", "i2")),
		"r2": scalarResult(1000, 7, tags("job", "web", "env", "prod", "instance", "i4")),
	}

	// Without ignoring(), instance differs so no match.
	n := &BinaryNode{
		Op:  OpAdd,
		LHS: &staticNode{results: lhs},
		RHS: &staticNode{results: rhs},
	}
	got := n.Eval(sg, step)
	assert.Empty(t, got, "without ignoring(), different instance should not match")

	// With ignoring(instance), instance is excluded → match.
	n.Match = &VectorMatch{Ignoring: []string{"instance"}}
	got = n.Eval(sg, step)
	require.Len(t, got, 2)
	assert.InDelta(t, 13.0, got["l1"].Value.Num, 1e-9)
	assert.InDelta(t, 27.0, got["l2"].Value.Num, 1e-9)
}

// --- Duplicate match-key conflict tests ---

func TestEvalArithmetic_DuplicateMatchKeyConflict(t *testing.T) {
	sg := SketchGroup{Timestamp: 1000}
	step := time.Minute

	// Need 2+ entries on LHS to avoid asScalar heuristic.
	lhs := map[string]EvalResult{
		"l1": scalarResult(1000, 10, tags("job", "api")),
		"l2": scalarResult(1000, 20, tags("job", "web")),
	}
	// Two RHS entries with the same matchKey on(job) → conflict for "api".
	rhs := map[string]EvalResult{
		"r1": scalarResult(1000, 3, tags("job", "api", "instance", "i1")),
		"r2": scalarResult(1000, 5, tags("job", "api", "instance", "i2")),
		"r3": scalarResult(1000, 7, tags("job", "web")),
	}

	n := &BinaryNode{
		Op:    OpAdd,
		LHS:   &staticNode{results: lhs},
		RHS:   &staticNode{results: rhs},
		Match: &VectorMatch{On: []string{"job"}},
	}
	got := n.Eval(sg, step)
	// "api" has a conflict (two RHS entries) → dropped
	// "web" has no conflict → should match
	require.Len(t, got, 1)
	assert.InDelta(t, 27.0, got["l2"].Value.Num, 1e-9)
}

func TestEvalSetOp_MatchKeyBasedJoin(t *testing.T) {
	sg := SketchGroup{Timestamp: 1000}
	step := time.Minute

	// `and` with on(job) — entries match on job alone.
	lhs := map[string]EvalResult{
		"l1": scalarResult(1000, 10, tags("name", "metric_a", "job", "api")),
		"l2": scalarResult(1000, 20, tags("name", "metric_a", "job", "web")),
	}
	rhs := map[string]EvalResult{
		"r1": scalarResult(1000, 5, tags("name", "metric_b", "job", "api")),
	}

	n := &BinaryNode{
		Op:  OpAnd,
		LHS: &staticNode{results: lhs},
		RHS: &staticNode{results: rhs},
	}
	got := n.Eval(sg, step)
	// name is excluded from matchKey, so job=api matches.
	// name is preserved in set op results.
	require.Len(t, got, 1)
	assert.Equal(t, 10.0, got["l1"].Value.Num)
	assert.Equal(t, "metric_a", got["l1"].Tags["name"])
}

// --- buildMatchLookup tests ---

func TestBuildMatchLookup(t *testing.T) {
	m := map[string]EvalResult{
		"a": scalarResult(1000, 1, tags("job", "api")),
		"b": scalarResult(1000, 2, tags("job", "web")),
	}

	lookup, conflicts := buildMatchLookup(m, nil)
	assert.Len(t, lookup, 2)
	assert.Nil(t, conflicts)

	t.Run("conflict detected", func(t *testing.T) {
		m2 := map[string]EvalResult{
			"a": scalarResult(1000, 1, tags("job", "api", "instance", "i1")),
			"b": scalarResult(1000, 2, tags("job", "api", "instance", "i2")),
		}
		lookup2, conflicts2 := buildMatchLookup(m2, &VectorMatch{On: []string{"job"}})
		assert.Len(t, lookup2, 1) // both map to same matchKey
		assert.True(t, conflicts2["job=api"])
	})
}
