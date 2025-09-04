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
	"math"
	"time"
)

// UnaryNode applies a single-argument math function to the child's scalars.
type UnaryNode struct {
	Func  string // "abs","ceil","floor","exp","ln","log2","log10","sqrt","sgn"
	Child ExecNode
}

func (n *UnaryNode) Hints() ExecHints { return n.Child.Hints() }

func (n *UnaryNode) Eval(sg SketchGroup, step time.Duration) map[string]EvalResult {
	in := n.Child.Eval(sg, step)
	if len(in) == 0 {
		return map[string]EvalResult{}
	}
	out := make(map[string]EvalResult, len(in))

	for k, r := range in {
		var num float64
		if r.Value.Kind == ValScalar {
			num = applyUnary(n.Func, r.Value.Num)
		} else {
			// Math funcs are scalar-only; sketches -> NaN
			num = math.NaN()
		}
		out[k] = EvalResult{
			Timestamp: r.Timestamp,
			Tags:      r.Tags,
			Value:     Value{Kind: ValScalar, Num: num},
		}
	}
	return out
}

func applyUnary(fn string, x float64) float64 {
	switch fn {
	case "abs":
		return math.Abs(x)
	case "ceil":
		return math.Ceil(x)
	case "floor":
		return math.Floor(x)
	case "exp":
		return math.Exp(x)
	case "ln":
		if x <= 0 || math.IsNaN(x) {
			return math.NaN()
		}
		return math.Log(x)
	case "log2":
		if x <= 0 || math.IsNaN(x) {
			return math.NaN()
		}
		return math.Log2(x)
	case "log10":
		if x <= 0 || math.IsNaN(x) {
			return math.NaN()
		}
		return math.Log10(x)
	case "sqrt":
		if x < 0 || math.IsNaN(x) {
			return math.NaN()
		}
		return math.Sqrt(x)
	case "sgn":
		if math.IsNaN(x) {
			return math.NaN()
		}
		if x > 0 {
			return 1
		}
		if x < 0 {
			return -1
		}
		return 0
	default:
		return math.NaN()
	}
}

func (n *UnaryNode) Label(tags map[string]any) string {
	return n.Func + "(" + n.Child.Label(tags) + ")"
}
