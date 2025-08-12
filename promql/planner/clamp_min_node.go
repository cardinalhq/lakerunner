package planner

import (
	"math"
	"time"
)

// ClampMinNode : applies max(value, Min) to child stream.
type ClampMinNode struct {
	Min   float64
	Child ExecNode
}

func (n *ClampMinNode) Hints() ExecHints { return n.Child.Hints() }

func (n *ClampMinNode) Eval(sg SketchGroup, step time.Duration) map[string]EvalResult {
	child := n.Child.Eval(sg, step)
	out := make(map[string]EvalResult, len(child))
	for k, r := range child {
		val := r.Value
		if val.Kind == ValScalar && !math.IsNaN(val.Num) {
			if val.Num < n.Min {
				val.Num = n.Min
			}
		}
		out[k] = EvalResult{
			Timestamp: r.Timestamp,
			Value:     val,
			Tags:      r.Tags,
		}
	}
	return out
}
