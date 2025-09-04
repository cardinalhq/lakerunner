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

// BinaryNode : arithmetic (+ - * /) with vector matching (on-only).
type BinaryNode struct {
	Op    BinOp
	LHS   ExecNode
	RHS   ExecNode
	Match *VectorMatch // Only On []string is used
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

	// Index RHS by join key.

	// Join and compute op on scalars.
	out := make(map[string]EvalResult)
	for lk, l := range lmap {
		r, ok := rmap[lk]
		if !ok {
			continue // inner join
		}
		// Only operate on scalars; skip pairs with non-scalar kinds.
		if l.Value.Kind != ValScalar || r.Value.Kind != ValScalar {
			continue
		}

		a, b := l.Value.Num, r.Value.Num

		var v float64
		switch n.Op {
		case OpAdd:
			v = a + b
		case OpSub:
			v = a - b
		case OpMul:
			v = a * b
		case OpDiv:
			if b == 0 {
				v = math.NaN()
			} else {
				v = a / b
			}
		default:
			v = math.NaN()
		}

		if math.IsNaN(v) {
			continue
		}

		emitTags := make(map[string]any)
		// merge tags from LHS and RHS
		for k, val := range l.Tags {
			emitTags[k] = val
		}

		if len(emitTags) == 0 {
			emitTags = r.Tags
		}

		out[lk] = EvalResult{
			Timestamp: sg.Timestamp,
			Value:     Value{Kind: ValScalar, Num: v},
			Tags:      emitTags,
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
	}
	return "unknown"
}
