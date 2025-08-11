package promql

import (
	"fmt"
	"math"
	"sort"
	"strings"
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

	// Build a join key from tags according to on(...).
	buildKey := func(tags map[string]any) (string, map[string]any) {
		// If on(...) is provided, keep only those labels as the join key + output tags.
		if n.Match != nil && len(n.Match.On) > 0 {
			parts := make([]string, 0, len(n.Match.On))
			outTags := make(map[string]any, len(n.Match.On))
			for _, l := range n.Match.On {
				if v, ok := tags[l]; ok {
					parts = append(parts, fmt.Sprintf("%s=%v", l, v))
					outTags[l] = v
				}
			}
			if len(parts) == 0 {
				return "default", map[string]any{}
			}
			return strings.Join(parts, ","), outTags
		}

		// Default: match on full label set.
		if len(tags) == 0 {
			return "default", map[string]any{}
		}
		parts := make([]string, 0, len(tags))
		for k, v := range tags {
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
		sort.Strings(parts)
		return strings.Join(parts, ","), tags
	}

	// Index RHS by join key.
	type keyed struct {
		tags map[string]any
		res  EvalResult
	}
	ridx := make(map[string]keyed, len(rmap))
	for _, r := range rmap {
		k, ktags := buildKey(r.Tags)
		ridx[k] = keyed{tags: ktags, res: r}
	}

	// Join and compute op on scalars.
	out := make(map[string]EvalResult)
	for _, l := range lmap {
		lk, ktags := buildKey(l.Tags)
		rr, ok := ridx[lk]
		if !ok {
			continue // inner join
		}
		// Only operate on scalars; skip pairs with non-scalar kinds.
		if l.Value.Kind != ValScalar || rr.res.Value.Kind != ValScalar {
			continue
		}

		a, b := l.Value.Num, rr.res.Value.Num
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

		out[lk] = EvalResult{
			Timestamp: sg.Timestamp,
			Value:     Value{Kind: ValScalar, Num: v},
			Tags:      ktags, // only the on(...) labels (or full set if no on)
		}
	}

	return out
}
