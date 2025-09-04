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

func (n *ClampMinNode) Label(tags map[string]any) string {
	return "clamp_min(" + n.Child.Label(tags) + ")"
}
