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
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"
)

// BinaryNode : arithmetic (+ - * /), comparisons (>,>=,<,<=,==,!=), and
// set operators (or, and, unless).
// For arithmetic/comparison ops, matchKey() excludes name/__name__ so
// cross-metric math works. For set ops, name/__name__ is included so
// series with different metric names are treated as distinct.
// on()/ignoring() modifiers control which labels participate.
// group_left/group_right are rejected at parse time (not supported).
// For scalar–vector or vector–scalar, we apply the scalar to each sample on
// the vector side. The `ReturnBool` flag implements PromQL `bool` modifier
// for comparisons (emit 1 when true, 0 when false; otherwise comparisons
// drop falsy samples).
// Set operators are vector-vector only and have different empty-side semantics.
type BinaryNode struct {
	Op         BinOp
	LHS        ExecNode
	RHS        ExecNode
	Match      *VectorMatch // on/ignoring vector matching
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

	// Set operators dispatch first: they have different empty-side semantics
	// and are vector-vector only.
	switch n.Op {
	case OpOr:
		return n.evalOr(lmap, rmap)
	case OpAnd:
		return n.evalAnd(lmap, rmap)
	case OpUnless:
		return n.evalUnless(lmap, rmap)
	}

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

	// vector OP vector — join by matchKey (excludes name/__name__).
	rhsLookup, rhsConflicts := buildMatchLookup(rmap, n.Match, false)
	out := make(map[string]EvalResult, minVal(len(lmap), len(rhsLookup)))
	for lk, l := range lmap {
		mk := matchKey(l.Tags, n.Match, false)
		if rhsConflicts[mk] {
			continue // many-to-one conflict → drop
		}
		r, ok := rhsLookup[mk]
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
					Tags:      dropMetricName(mergeTagsPreferL(l.Tags, r.Tags)),
				}
			}

		// Comparisons: keep LHS sample if predicate true; otherwise drop.
		// With ReturnBool, emit 1 or 0 instead.
		case OpGT, OpGE, OpLT, OpLE, OpEQ, OpNE:
			pred := applyCmp(le, re, n.Op)
			if n.ReturnBool {
				out[lk] = EvalResult{
					Timestamp: sg.Timestamp,
					Value:     Value{Kind: ValScalar, Num: bool01(pred)},
					Tags:      dropMetricName(mergeTagsPreferL(l.Tags, r.Tags)),
				}
			} else if pred {
				out[lk] = EvalResult{
					Timestamp: sg.Timestamp,
					Value:     l.Value,
					Tags:      dropMetricName(l.Tags),
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
		a, ok := resolveScalar(e.Value)
		if !ok {
			continue
		}
		tags := dropMetricName(e.Tags)
		var v float64
		switch n.Op {
		// Arithmetic: apply with proper operand order
		case OpAdd:
			if leftIsVector {
				v = a + scalar
			} else {
				v = scalar + a
			}
			out[k] = EvalResult{Timestamp: sg.Timestamp, Value: Value{Kind: ValScalar, Num: v}, Tags: tags}
		case OpSub:
			if leftIsVector {
				v = a - scalar
			} else {
				v = scalar - a
			}
			out[k] = EvalResult{Timestamp: sg.Timestamp, Value: Value{Kind: ValScalar, Num: v}, Tags: tags}
		case OpMul:
			if leftIsVector {
				v = a * scalar
			} else {
				v = scalar * a
			}
			out[k] = EvalResult{Timestamp: sg.Timestamp, Value: Value{Kind: ValScalar, Num: v}, Tags: tags}
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
				out[k] = EvalResult{Timestamp: sg.Timestamp, Value: Value{Kind: ValScalar, Num: v}, Tags: tags}
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
					Tags:      tags,
				}
			} else if pred {
				out[k] = EvalResult{
					Timestamp: sg.Timestamp,
					Value:     e.Value,
					Tags:      tags,
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
	case OpOr:
		return fmt.Sprintf("(%s or %s)", leftLabel, rightLabel)
	case OpAnd:
		return fmt.Sprintf("(%s and %s)", leftLabel, rightLabel)
	case OpUnless:
		return fmt.Sprintf("(%s unless %s)", leftLabel, rightLabel)
	}
	return "unknown"
}

// evalOr implements PromQL `or` (union): all LHS entries, plus RHS entries
// whose matchKey is not already in LHS. Set ops preserve metric name.
func (n *BinaryNode) evalOr(lmap, rmap map[string]EvalResult) map[string]EvalResult {
	out := make(map[string]EvalResult, len(lmap)+len(rmap))
	lKeys := make(map[string]bool, len(lmap))
	for k, v := range lmap {
		mk := matchKey(v.Tags, n.Match, true)
		lKeys[mk] = true
		out[k] = v
	}
	for k, v := range rmap {
		mk := matchKey(v.Tags, n.Match, true)
		if !lKeys[mk] {
			outKey := k
			for _, exists := out[outKey]; exists; _, exists = out[outKey] {
				outKey = outKey + "\x00"
			}
			out[outKey] = v
		}
	}
	return out
}

// evalAnd implements PromQL `and` (intersection): only LHS entries whose
// matchKey also exists in RHS. LHS values are kept. Set ops preserve metric name.
func (n *BinaryNode) evalAnd(lmap, rmap map[string]EvalResult) map[string]EvalResult {
	rhsKeys := make(map[string]bool, len(rmap))
	for _, v := range rmap {
		rhsKeys[matchKey(v.Tags, n.Match, true)] = true
	}
	out := make(map[string]EvalResult, minVal(len(lmap), len(rmap)))
	for k, v := range lmap {
		if rhsKeys[matchKey(v.Tags, n.Match, true)] {
			out[k] = v
		}
	}
	return out
}

// evalUnless implements PromQL `unless` (complement): only LHS entries whose
// matchKey does NOT exist in RHS. LHS values are kept. Set ops preserve metric name.
func (n *BinaryNode) evalUnless(lmap, rmap map[string]EvalResult) map[string]EvalResult {
	rhsKeys := make(map[string]bool, len(rmap))
	for _, v := range rmap {
		rhsKeys[matchKey(v.Tags, n.Match, true)] = true
	}
	out := make(map[string]EvalResult, len(lmap))
	for k, v := range lmap {
		if !rhsKeys[matchKey(v.Tags, n.Match, true)] {
			out[k] = v
		}
	}
	return out
}

// ---- matching helpers ----

// matchKey computes a join key from tags. For arithmetic and comparison
// operators, name/__name__ is excluded (so cross-metric math works). For
// set operators (or/and/unless), name/__name__ is included so that series
// with different metric names are treated as distinct (Prometheus semantics).
// on()/ignoring() modifiers control which labels participate.
func matchKey(tags map[string]any, match *VectorMatch, includeName bool) string {
	if len(tags) == 0 {
		return ""
	}

	isNameKey := func(k string) bool {
		return !includeName && (k == "name" || k == "__name__")
	}

	var include func(string) bool
	if match != nil && len(match.On) > 0 {
		onSet := make(map[string]bool, len(match.On))
		for _, l := range match.On {
			onSet[l] = true
		}
		include = func(k string) bool {
			return onSet[k] && !isNameKey(k)
		}
	} else if match != nil && len(match.Ignoring) > 0 {
		ignoreSet := make(map[string]bool, len(match.Ignoring)+2)
		for _, l := range match.Ignoring {
			ignoreSet[l] = true
		}
		if !includeName {
			ignoreSet["name"] = true
			ignoreSet["__name__"] = true
		}
		include = func(k string) bool {
			return !ignoreSet[k]
		}
	} else {
		include = func(k string) bool {
			return !isNameKey(k)
		}
	}

	keys := make([]string, 0, len(tags))
	for k := range tags {
		if include(k) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)

	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		fmt.Fprint(&sb, tags[k])
	}
	return sb.String()
}

// dropMetricName returns tags without name/__name__. If neither key is
// present, the original map is returned (no allocation).
func dropMetricName(tags map[string]any) map[string]any {
	_, hasName := tags["name"]
	_, hasDunder := tags["__name__"]
	if !hasName && !hasDunder {
		return tags
	}
	out := make(map[string]any, len(tags))
	for k, v := range tags {
		if k != "name" && k != "__name__" {
			out[k] = v
		}
	}
	return out
}

// buildMatchLookup builds a map from matchKey → EvalResult for the RHS
// of a binary operation. If multiple entries share the same matchKey,
// the key is recorded in the conflicts map and the match is dropped
// (many-to-one without group modifier).
func buildMatchLookup(m map[string]EvalResult, match *VectorMatch, includeName bool) (lookup map[string]EvalResult, conflicts map[string]bool) {
	lookup = make(map[string]EvalResult, len(m))
	for _, er := range m {
		mk := matchKey(er.Tags, match, includeName)
		if _, exists := lookup[mk]; exists {
			if conflicts == nil {
				conflicts = make(map[string]bool)
			}
			conflicts[mk] = true
		}
		lookup[mk] = er
	}
	return lookup, conflicts
}

// ---- scalar helpers ----

func asScalar(m map[string]EvalResult) (float64, bool) {
	if len(m) != 1 {
		return 0, false
	}
	for k, er := range m {
		v, ok := resolveScalar(er.Value)
		if !ok {
			return 0, false
		}
		// Heuristic: scalar nodes typically have key "default" and/or empty tags.
		if k == "default" || len(er.Tags) == 0 {
			return v, true
		}
		// If it's a single-entry map with a scalar, treat as scalar literal anyway.
		return v, true
	}
	return 0, false
}

// resolveScalar extracts a float64 from a Value. For ValScalar it returns
// Num directly. For ValMap (instant selector rollups containing sum/count
// from LeafNode) it returns sum/count — the actual metric value at that
// instant, equivalent to what Prometheus returns for a bare selector.
func resolveScalar(v Value) (float64, bool) {
	switch v.Kind {
	case ValScalar:
		return v.Num, true
	case ValMap:
		if v.AggMap == nil {
			return 0, false
		}
		s := v.AggMap[SUM]
		c := v.AggMap[COUNT]
		if c == 0 {
			return math.NaN(), true
		}
		return s / c, true
	default:
		return 0, false
	}
}

func bothScalars(l, r EvalResult) (float64, float64, bool) {
	lv, lok := resolveScalar(l.Value)
	rv, rok := resolveScalar(r.Value)
	if !lok || !rok {
		return 0, 0, false
	}
	return lv, rv, true
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
	maps.Copy(out, l)
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
