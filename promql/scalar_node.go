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
	"strings"
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

func (n *ScalarNode) Label(tags map[string]any) string {
	return formatFloatNoTrailingZeros(n.Value)
}

func formatFloatNoTrailingZeros(f float64) string {
	s := fmt.Sprintf("%.2f", f)
	if !strings.Contains(s, ".") {
		return s
	}
	s = strings.TrimRight(s, "0")
	s = strings.TrimRight(s, ".")
	return s
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

func (n *ScalarOfNode) Label(tags map[string]any) string {
	return "scalar(" + n.Child.Label(tags) + ")"
}
