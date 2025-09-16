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
	"fmt"
	"math"
	"time"
)

// BinaryNode : arithmetic (+ - * /) and comparisons (>,>=,<,<=,==,!=).
// Vector matching is simplified: we "join" by identical map keys.
// For scalar–vector or vector–scalar, we apply the scalar to each sample on
// the vector side. The `ReturnBool` flag implements PromQL `bool` modifier
// for comparisons (emit 1 when true, 0 when false; otherwise comparisons
// drop falsy samples).
type BinaryNode struct {
	Op         BinOp
	LHS        ExecNode
	RHS        ExecNode
	Match      *VectorMatch // currently unused (on/ignoring not implemented yet)
	ReturnBool bool         // PromQL `bool` modifier for comparisons
}

func (n *BinaryNode) Hints() ExecHints {
	l, r := n.LHS.Hints(), n.RHS.Hints()
	return ExecHints{
		WantTopK:    l.WantTopK || r.WantTopK,
		WantBottomK: l.WantBottomK || r.WantBottomK,
		WantCount:   l.WantCount || r.WantCount,
	}
}

func (n *BinaryNode) Eval(sg SketchGroup, step time.Duration) map[string]EvalResult {
	lmap := n.LHS.Eval(sg, step)
	rmap := n.RHS.Eval(sg, step)
	if len(lmap) == 0 || len(rmap) == 0 {
		return map[string]EvalResult{}
	}

	// Detect scalar operands (scalar literal or scalar-of-vector).
	if sval, ok := asScalar(rmap); ok {
		// vector (left) OP scalar (right)
		return n.evalVectorScalar(sg, lmap, sval, true)
	}
	if sval, ok := asScalar(lmap); ok {
		// scalar (left) OP vector (right)
		return n.evalVectorScalar(sg, rmap, sval, false)
	}

	// vector OP vector — inner join by identical key.
	out := make(map[string]EvalResult, minVal(len(lmap), len(rmap)))
	for lk, l := range lmap {
		r, ok := rmap[lk]
		if !ok {
			continue // inner join semantics
		}
		le, re, ok := bothScalars(l, r)
		if !ok {
			continue
		}
		switch n.Op {
		// Arithmetic: always emit a numeric sample if defined.
		case OpAdd, OpSub, OpMul, OpDiv:
			if v, ok := applyArith(le, re, n.Op); ok {
				out[lk] = EvalResult{
					Timestamp: sg.Timestamp,
					Value:     Value{Kind: ValScalar, Num: v},
					Tags:      mergeTagsPreferL(l.Tags, r.Tags),
				}
			}

		// Comparisons: keep LHS sample if predicate true; otherwise drop.
		// With ReturnBool, emit 1 or 0 instead (keeping LHS/RHS tags).
		case OpGT, OpGE, OpLT, OpLE, OpEQ, OpNE:
			pred := applyCmp(le, re, n.Op)
			if n.ReturnBool {
				// Emit 1 if true, 0 if false; keep LHS tags by convention.
				out[lk] = EvalResult{
					Timestamp: sg.Timestamp,
					Value:     Value{Kind: ValScalar, Num: bool01(pred)},
					Tags:      mergeTagsPreferL(l.Tags, r.Tags),
				}
			} else if pred {
				// Emit the LHS sample (value as-is).
				out[lk] = EvalResult{
					Timestamp: sg.Timestamp,
					Value:     l.Value,
					Tags:      l.Tags,
				}
			}
		default:
			// Unsupported binop — drop
		}
	}
	return out
}

func (n *BinaryNode) evalVectorScalar(sg SketchGroup, vec map[string]EvalResult, scalar float64, leftIsVector bool) map[string]EvalResult {
	out := make(map[string]EvalResult, len(vec))
	for k, e := range vec {
		if e.Value.Kind != ValScalar {
			continue
		}
		a := e.Value.Num
		var v float64
		switch n.Op {
		// Arithmetic: apply with proper operand order
		case OpAdd:
			if leftIsVector {
				v = a + scalar
			} else {
				v = scalar + a
			}
			out[k] = EvalResult{Timestamp: sg.Timestamp, Value: Value{Kind: ValScalar, Num: v}, Tags: e.Tags}
		case OpSub:
			if leftIsVector {
				v = a - scalar
			} else {
				v = scalar - a
			}
			out[k] = EvalResult{Timestamp: sg.Timestamp, Value: Value{Kind: ValScalar, Num: v}, Tags: e.Tags}
		case OpMul:
			if leftIsVector {
				v = a * scalar
			} else {
				v = scalar * a
			}
			out[k] = EvalResult{Timestamp: sg.Timestamp, Value: Value{Kind: ValScalar, Num: v}, Tags: e.Tags}
		case OpDiv:
			if leftIsVector {
				if scalar == 0 {
					continue // drop undefined
				}
				v = a / scalar
			} else {
				if a == 0 {
					continue // drop undefined
				}
				v = scalar / a
			}
			if !math.IsNaN(v) {
				out[k] = EvalResult{Timestamp: sg.Timestamp, Value: Value{Kind: ValScalar, Num: v}, Tags: e.Tags}
			}

		// Comparisons
		case OpGT, OpGE, OpLT, OpLE, OpEQ, OpNE:
			var pred bool
			if leftIsVector {
				pred = applyCmp(a, scalar, n.Op)
			} else {
				pred = applyCmp(scalar, a, n.Op)
			}
			if n.ReturnBool {
				out[k] = EvalResult{
					Timestamp: sg.Timestamp,
					Value:     Value{Kind: ValScalar, Num: bool01(pred)},
					Tags:      e.Tags,
				}
			} else if pred {
				// Keep the vector-side sample
				out[k] = EvalResult{
					Timestamp: sg.Timestamp,
					Value:     e.Value,
					Tags:      e.Tags,
				}
			}
		default:
			// Unsupported — drop
		}
	}
	return out
}

func (n *BinaryNode) Label(tags map[string]any) string {
	leftLabel := n.LHS.Label(tags)
	rightLabel := n.RHS.Label(tags)
	switch n.Op {
	case OpAdd:
		return fmt.Sprintf("(%s + %s)", leftLabel, rightLabel)
	case OpSub:
		return fmt.Sprintf("(%s - %s)", leftLabel, rightLabel)
	case OpMul:
		return fmt.Sprintf("(%s * %s)", leftLabel, rightLabel)
	case OpDiv:
		return fmt.Sprintf("(%s / %s)", leftLabel, rightLabel)
	case OpGT:
		return fmt.Sprintf("(%s > %s)", leftLabel, rightLabel)
	case OpGE:
		return fmt.Sprintf("(%s >= %s)", leftLabel, rightLabel)
	case OpLT:
		return fmt.Sprintf("(%s < %s)", leftLabel, rightLabel)
	case OpLE:
		return fmt.Sprintf("(%s <= %s)", leftLabel, rightLabel)
	case OpEQ:
		return fmt.Sprintf("(%s == %s)", leftLabel, rightLabel)
	case OpNE:
		return fmt.Sprintf("(%s != %s)", leftLabel, rightLabel)
	}
	return "unknown"
}

// ---- helpers ----

func asScalar(m map[string]EvalResult) (float64, bool) {
	if len(m) != 1 {
		return 0, false
	}
	for k, er := range m {
		if er.Value.Kind != ValScalar {
			return 0, false
		}
		// Heuristic: scalar nodes typically have key "default" and/or empty tags.
		if k == "default" || len(er.Tags) == 0 {
			return er.Value.Num, true
		}
		// If it's a single-entry map with a scalar, treat as scalar literal anyway.
		return er.Value.Num, true
	}
	return 0, false
}

func bothScalars(l, r EvalResult) (float64, float64, bool) {
	if l.Value.Kind != ValScalar || r.Value.Kind != ValScalar {
		return 0, 0, false
	}
	return l.Value.Num, r.Value.Num, true
}

func applyArith(a, b float64, op BinOp) (float64, bool) {
	switch op {
	case OpAdd:
		return a + b, true
	case OpSub:
		return a - b, true
	case OpMul:
		return a * b, true
	case OpDiv:
		if b == 0 {
			return math.NaN(), false
		}
		return a / b, true
	default:
		return math.NaN(), false
	}
}

func applyCmp(a, b float64, op BinOp) bool {
	switch op {
	case OpGT:
		return a > b
	case OpGE:
		return a >= b
	case OpLT:
		return a < b
	case OpLE:
		return a <= b
	case OpEQ:
		return a == b
	case OpNE:
		return a != b
	default:
		return false
	}
}

func bool01(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func mergeTagsPreferL(l, r map[string]any) map[string]any {
	if len(l) == 0 {
		return r
	}
	// copy LHS first, then fill missing from RHS
	out := make(map[string]any, len(l)+len(r))
	for k, v := range l {
		out[k] = v
	}
	for k, v := range r {
		if _, ok := out[k]; !ok {
			out[k] = v
		}
	}
	return out
}

func minVal(a, b int) int {
	if a < b {
		return a
	}
	return b
}
