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
			name: "overlapping keys - LHS wins",
			lhs: map[string]EvalResult{
				"a": scalarResult(1000, 1, nil),
				"b": scalarResult(1000, 2, nil),
			},
			rhs: map[string]EvalResult{
				"b": scalarResult(1000, 20, nil),
				"c": scalarResult(1000, 3, nil),
			},
			expect: map[string]EvalResult{
				"a": scalarResult(1000, 1, nil),
				"b": scalarResult(1000, 2, nil),
				"c": scalarResult(1000, 3, nil),
			},
		},
		{
			name: "LHS empty",
			lhs:  map[string]EvalResult{},
			rhs: map[string]EvalResult{
				"a": scalarResult(1000, 1, nil),
			},
			expect: map[string]EvalResult{
				"a": scalarResult(1000, 1, nil),
			},
		},
		{
			name: "RHS empty",
			lhs: map[string]EvalResult{
				"a": scalarResult(1000, 1, nil),
			},
			rhs:    map[string]EvalResult{},
			expect: map[string]EvalResult{"a": scalarResult(1000, 1, nil)},
		},
		{
			name:   "both empty",
			lhs:    map[string]EvalResult{},
			rhs:    map[string]EvalResult{},
			expect: map[string]EvalResult{},
		},
		{
			name: "disjoint keys",
			lhs:  map[string]EvalResult{"a": scalarResult(1000, 1, nil)},
			rhs:  map[string]EvalResult{"b": scalarResult(1000, 2, nil)},
			expect: map[string]EvalResult{
				"a": scalarResult(1000, 1, nil),
				"b": scalarResult(1000, 2, nil),
			},
		},
		{
			name:   "identical keys - LHS wins",
			lhs:    map[string]EvalResult{"a": scalarResult(1000, 1, nil)},
			rhs:    map[string]EvalResult{"a": scalarResult(1000, 99, nil)},
			expect: map[string]EvalResult{"a": scalarResult(1000, 1, nil)},
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
			name: "overlapping keys - LHS values kept",
			lhs: map[string]EvalResult{
				"a": scalarResult(1000, 1, nil),
				"b": scalarResult(1000, 2, nil),
			},
			rhs: map[string]EvalResult{
				"b": scalarResult(1000, 20, nil),
				"c": scalarResult(1000, 3, nil),
			},
			expect: map[string]EvalResult{
				"b": scalarResult(1000, 2, nil),
			},
		},
		{
			name: "LHS empty",
			lhs:  map[string]EvalResult{},
			rhs: map[string]EvalResult{
				"a": scalarResult(1000, 1, nil),
			},
			expect: map[string]EvalResult{},
		},
		{
			name: "RHS empty",
			lhs: map[string]EvalResult{
				"a": scalarResult(1000, 1, nil),
			},
			rhs:    map[string]EvalResult{},
			expect: map[string]EvalResult{},
		},
		{
			name:   "disjoint keys",
			lhs:    map[string]EvalResult{"a": scalarResult(1000, 1, nil)},
			rhs:    map[string]EvalResult{"b": scalarResult(1000, 2, nil)},
			expect: map[string]EvalResult{},
		},
		{
			name:   "identical keys - LHS values kept",
			lhs:    map[string]EvalResult{"a": scalarResult(1000, 1, nil)},
			rhs:    map[string]EvalResult{"a": scalarResult(1000, 99, nil)},
			expect: map[string]EvalResult{"a": scalarResult(1000, 1, nil)},
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
			name: "overlapping keys",
			lhs: map[string]EvalResult{
				"a": scalarResult(1000, 1, nil),
				"b": scalarResult(1000, 2, nil),
			},
			rhs: map[string]EvalResult{
				"b": scalarResult(1000, 20, nil),
				"c": scalarResult(1000, 3, nil),
			},
			expect: map[string]EvalResult{
				"a": scalarResult(1000, 1, nil),
			},
		},
		{
			name: "LHS empty",
			lhs:  map[string]EvalResult{},
			rhs: map[string]EvalResult{
				"a": scalarResult(1000, 1, nil),
			},
			expect: map[string]EvalResult{},
		},
		{
			name: "RHS empty - all LHS kept",
			lhs: map[string]EvalResult{
				"a": scalarResult(1000, 1, nil),
			},
			rhs: map[string]EvalResult{},
			expect: map[string]EvalResult{
				"a": scalarResult(1000, 1, nil),
			},
		},
		{
			name: "disjoint keys - all LHS kept",
			lhs:  map[string]EvalResult{"a": scalarResult(1000, 1, nil)},
			rhs:  map[string]EvalResult{"b": scalarResult(1000, 2, nil)},
			expect: map[string]EvalResult{
				"a": scalarResult(1000, 1, nil),
			},
		},
		{
			name:   "identical keys - nothing kept",
			lhs:    map[string]EvalResult{"a": scalarResult(1000, 1, nil)},
			rhs:    map[string]EvalResult{"a": scalarResult(1000, 99, nil)},
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

	lhs := map[string]EvalResult{"a": scalarResult(1000, 10, nil)}
	rhs := map[string]EvalResult{"a": scalarResult(1000, 3, nil)}

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

	lhs := map[string]EvalResult{"a": scalarResult(1000, 10, nil)}
	rhs := map[string]EvalResult{"a": scalarResult(1000, 5, nil)}

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
