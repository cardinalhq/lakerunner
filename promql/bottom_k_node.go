// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"sort"
	"time"
)

// BottomKNode : BottomK selects the smallest K per step from child outputs.
type BottomKNode struct {
	K     int
	Child ExecNode
}

func (n *BottomKNode) Hints() ExecHints {
	h := n.Child.Hints()
	h.WantBottomK = true
	return h
}

func (n *BottomKNode) Eval(sg SketchGroup, step time.Duration) map[string]EvalResult {
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

	// Keep only scalar, finite values.
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

	// Sort ascending; tie-break by key for determinism.
	sort.SliceStable(buf, func(i, j int) bool {
		vi := buf[i].val.Value.Num
		vj := buf[j].val.Value.Num
		if vi == vj {
			return buf[i].key < buf[j].key
		}
		return vi < vj
	})

	// Take bottom K.
	if len(buf) > n.K {
		buf = buf[:n.K]
	}

	out := make(map[string]EvalResult, len(buf))
	for _, e := range buf {
		out[e.key] = e.val
	}
	return out
}

func (n *BottomKNode) Label(tags map[string]any) string {
	return "bottomK(" + n.Child.Label(tags) + ", " + fmt.Sprint(n.K) + ")"
}
