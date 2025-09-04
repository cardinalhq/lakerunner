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
	"time"

	"log/slog"

	"github.com/DataDog/sketches-go/ddsketch"
)

// QuantileNode computes value-at-quantile from DDSketch child (e.g. histogram_quantile).
type QuantileNode struct {
	Q     float64 // 0..1
	Child ExecNode
}

func (n *QuantileNode) Hints() ExecHints { return n.Child.Hints() }

func (n *QuantileNode) Eval(sg SketchGroup, step time.Duration) map[string]EvalResult {
	child := n.Child.Eval(sg, step)
	if len(child) == 0 {
		return map[string]EvalResult{}
	}

	// Clamp quantile to [0,1]
	q := n.Q
	if q < 0 {
		q = 0
	} else if q > 1 {
		q = 1
	}

	// Accumulate DDSketches per child key (grouping is unchanged at this node).
	type acc struct {
		sk   *ddsketch.DDSketch
		tags map[string]any
		ts   int64
	}
	accs := make(map[string]*acc, len(child))

	hasDDS := false
	for k, r := range child {
		if r.Value.Kind != ValDDS || r.Value.DDS == nil {
			continue
		}
		hasDDS = true
		a := accs[k]
		if a == nil {
			// Use the first incoming sketch as accumulator.
			// (Optionally deep-copy if you prefer immutability.)
			accs[k] = &acc{
				sk:   r.Value.DDS,
				tags: r.Tags,
				ts:   r.Timestamp,
			}
			continue
		}
		// Merge subsequent sketches for this key.
		if err := a.sk.MergeWith(r.Value.DDS); err != nil {
			slog.Error("ddsketch merge failed", "err", err)
		}
	}

	// If no DDS were found, return NaNs for each key (quantile undefined).
	if !hasDDS {
		out := make(map[string]EvalResult, len(child))
		for k, r := range child {
			out[k] = EvalResult{
				Timestamp: r.Timestamp,
				Tags:      r.Tags,
				Value: Value{
					Kind: ValScalar,
					Num:  math.NaN(),
				},
			}
		}
		return out
	}

	// Compute quantile for each key and emit scalars.
	out := make(map[string]EvalResult, len(accs))
	for k, a := range accs {
		val, err := a.sk.GetValueAtQuantile(q)
		if err != nil {
			slog.Error("ddsketch quantile failed", "q", q, "err", err)
			val = math.NaN()
		}
		out[k] = EvalResult{
			Timestamp: a.ts,
			Tags:      a.tags,
			Value: Value{
				Kind: ValScalar, // quantile result is a scalar
				Num:  val,
			},
		}
	}
	return out
}

func (n *QuantileNode) Label(tags map[string]any) string {
	return "quantile(" + fmt.Sprintf("%f", n.Q) + ", " + n.Child.Label(tags) + ")"
}
