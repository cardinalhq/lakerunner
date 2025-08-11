package promql

import "time"

// ClampMinNode : applies max(value, Min) to child stream.
type ClampMinNode struct {
	Min   float64
	Child ExecNode
}

func (n *ClampMinNode) Hints() ExecHints { return n.Child.Hints() }

func (n *ClampMinNode) Eval(sg SketchGroup, step time.Duration) map[string]EvalResult {
	// This is a stub; real evaluation happens in the worker.
	// The worker will use n.Min and n.Child to compute clamped results.
	return map[string]EvalResult{}
}
