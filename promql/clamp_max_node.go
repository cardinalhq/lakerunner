package promql

import (
	"math"
	"time"
)

// ClampMaxNode : applies max(value, Max) to child stream.
type ClampMaxNode struct {
	Max   float64
	Child ExecNode
}

func (n *ClampMaxNode) Hints() ExecHints { return n.Child.Hints() }

func (n *ClampMaxNode) Eval(sg SketchGroup, step time.Duration) map[string]EvalResult {
	child := n.Child.Eval(sg, step)
	out := make(map[string]EvalResult, len(child))
	for k, r := range child {
		val := r.Value
		if val.Kind == ValScalar && !math.IsNaN(val.Num) {
			if val.Num > n.Max {
				val.Num = n.Max
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
