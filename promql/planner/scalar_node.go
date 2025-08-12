package planner

import (
	"math"
	"time"
)

// ScalarNode holds a literal scalar value.
type ScalarNode struct {
	Value float64
}

func (n *ScalarNode) Hints() ExecHints { return ExecHints{} }

func (n *ScalarNode) Eval(sg SketchGroup, step time.Duration) map[string]EvalResult {
	return map[string]EvalResult{
		"default": {
			Timestamp: sg.Timestamp,
			Value:     Value{Kind: ValScalar, Num: n.Value},
			Tags:      map[string]any{}, // no labels on a pure scalar
		},
	}
}

type ScalarOfNode struct {
	Child ExecNode
}

func (n *ScalarOfNode) Hints() ExecHints { return n.Child.Hints() }

func (n *ScalarOfNode) Eval(sg SketchGroup, step time.Duration) map[string]EvalResult {
	m := n.Child.Eval(sg, step)
	if len(m) != 1 {
		return map[string]EvalResult{"default": {
			Timestamp: sg.Timestamp,
			Value:     Value{Kind: ValScalar, Num: math.NaN()},
			Tags:      map[string]any{},
		}}
	}
	for _, r := range m { // the only element
		if r.Value.Kind != ValScalar || math.IsNaN(r.Value.Num) {
			return map[string]EvalResult{"default": {
				Timestamp: sg.Timestamp,
				Value:     Value{Kind: ValScalar, Num: math.NaN()},
				Tags:      map[string]any{},
			}}
		}
		return map[string]EvalResult{"default": {
			Timestamp: sg.Timestamp,
			Value:     Value{Kind: ValScalar, Num: r.Value.Num},
			Tags:      map[string]any{},
		}}
	}
	// unreachable, but keep compiler happy
	return map[string]EvalResult{}
}
