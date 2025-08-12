package planner

import (
	"math"
	"sort"
	"time"
)

// TopKNode : TopK select K per step from child outputs (by current grouping).
type TopKNode struct {
	K     int
	Child ExecNode
}

func (n *TopKNode) Hints() ExecHints {
	// We still surface the hint upward, but worker-side topK is not used
	// since we decided to do ranking on the API over scalars.
	h := n.Child.Hints()
	h.WantTopK = true
	return h
}

func (n *TopKNode) Eval(sg SketchGroup, step time.Duration) map[string]EvalResult {
	if n.K <= 0 {
		return map[string]EvalResult{}
	}
	child := n.Child.Eval(sg, step)
	if len(child) == 0 {
		return map[string]EvalResult{}
	}

	type entry struct {
		key string
		val EvalResult
	}

	// Collect scalar, finite entries only.
	buf := make([]entry, 0, len(child))
	for k, r := range child {
		if r.Value.Kind != ValScalar || math.IsNaN(r.Value.Num) {
			continue
		}
		buf = append(buf, entry{key: k, val: r})
	}
	if len(buf) == 0 {
		return map[string]EvalResult{}
	}

	// Sort by value descending; tie-break by key for determinism.
	sort.SliceStable(buf, func(i, j int) bool {
		vi := buf[i].val.Value.Num
		vj := buf[j].val.Value.Num
		if vi == vj {
			return buf[i].key < buf[j].key
		}
		return vi > vj
	})

	// Take top K.
	if len(buf) > n.K {
		buf = buf[:n.K]
	}

	out := make(map[string]EvalResult, len(buf))
	for _, e := range buf {
		// Pass through timestamp, tags, and scalar value.
		out[e.key] = e.val
	}
	return out
}
