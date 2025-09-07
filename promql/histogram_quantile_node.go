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
	"time"
)

type HistogramQuantileNode struct {
	Q     float64 // 0..1
	Child ExecNode
}

func (n *HistogramQuantileNode) Hints() ExecHints { return n.Child.Hints() }

func (n *HistogramQuantileNode) Eval(sg SketchGroup, step time.Duration) map[string]EvalResult {
	return nil
}

func (n *HistogramQuantileNode) Label(tags map[string]any) string {
	return fmt.Sprintf("histogram_quantile(%.3f, %s)", n.Q, n.Child)
}
