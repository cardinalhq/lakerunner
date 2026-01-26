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
	"log/slog"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/axiomhq/hyperloglog"
)

// AggNode wraps a child and performs sum/avg/min/max/count with by/without.
type AggNode struct {
	Op      AggOp
	By      []string
	Without []string
	Child   ExecNode
}

func (n *AggNode) Hints() ExecHints { return n.Child.Hints() }

func (n *AggNode) Eval(sg SketchGroup, step time.Duration) map[string]EvalResult {
	child := n.Child.Eval(sg, step)
	if len(child) == 0 {
		return map[string]EvalResult{}
	}

	var childGroupBy []string
	if ln, ok := n.Child.(*LeafNode); ok {
		childGroupBy = ln.BE.GroupBy
	}
	childHints := n.Child.Hints()
	countPassThrough := n.Op == AggCount &&
		len(n.By) > 0 &&
		childHints.WantCount &&
		!childHints.IsLogLeaf &&
		len(n.Without) == 0 &&
		equalStringSets(n.By, childGroupBy)

	makeKey := func(tags map[string]any) string {
		if len(n.By) > 0 {
			parts := make([]string, 0, len(n.By))
			for _, l := range n.By {
				if v, ok := tags[l]; ok {
					parts = append(parts, fmt.Sprintf("%s=%v", l, v))
				}
			}
			if len(parts) == 0 {
				return "default"
			}
			return strings.Join(parts, ",")
		}
		if len(n.Without) > 0 {
			drop := make(map[string]struct{}, len(n.Without))
			for _, l := range n.Without {
				drop[l] = struct{}{}
			}
			parts := make([]string, 0, len(tags))
			for k, v := range tags {
				if _, skip := drop[k]; !skip {
					parts = append(parts, fmt.Sprintf("%s=%v", k, v))
				}
			}
			if len(parts) == 0 {
				return "default"
			}
			sort.Strings(parts)
			return strings.Join(parts, ",")
		}
		return "default"
	}

	// Detect what kind(s) of values we have from child.
	hasDDS, hasHLL := false, false
	for _, r := range child {
		switch r.Value.Kind {
		case ValDDS:
			hasDDS = true
		case ValHLL:
			hasHLL = true
		default:
		}
	}

	switch {
	// --------- DDSketch merge path ----------
	case hasDDS:
		type dacc struct {
			d    *ddsketch.DDSketch
			ts   int64
			tags map[string]any
		}
		accs := map[string]*dacc{}

		for _, r := range child {
			if r.Value.Kind != ValDDS || r.Value.DDS == nil {
				continue
			}
			k := makeKey(r.Tags)
			a := accs[k]
			if a == nil {
				// Clone first incoming sketch to preserve mapping/accuracy.
				pb := r.Value.DDS.ToProto()
				d, err := ddsketch.FromProto(pb)
				if err != nil {
					slog.Error("dds fromproto (clone) failed", "err", err)
					if d2, e2 := ddsketch.NewDefaultDDSketch(0.01); e2 == nil {
						a = &dacc{d: d2, ts: sg.Timestamp, tags: r.Tags}
					} else {
						continue
					}
				} else {
					a = &dacc{d: d, ts: sg.Timestamp, tags: r.Tags}
				}
				accs[k] = a
			}
			if err := a.d.MergeWith(r.Value.DDS); err != nil {
				slog.Error("dds merge failed", "err", err)
			}
		}

		out := make(map[string]EvalResult, len(accs))
		for k, a := range accs {
			out[k] = EvalResult{
				Timestamp: a.ts,
				Value:     Value{Kind: ValDDS, DDS: a.d},
				Tags:      a.tags,
			}
		}
		return out

	// --------- HLL merge path ----------
	case hasHLL:
		type hacc struct {
			h    *hyperloglog.Sketch
			ts   int64
			tags map[string]any
		}
		accs := map[string]*hacc{}

		for _, r := range child {
			if r.Value.Kind != ValHLL || r.Value.HLL == nil {
				continue
			}
			k := makeKey(r.Tags)
			a := accs[k]
			if a == nil {
				acc := &hyperloglog.Sketch{}
				if b, err := r.Value.HLL.MarshalBinary(); err == nil {
					_ = acc.UnmarshalBinary(b)
				} else {
					acc = hyperloglog.New14()
				}
				a = &hacc{
					h:    acc,
					ts:   sg.Timestamp,
					tags: r.Tags,
				}
				accs[k] = a
			}
			_ = a.h.Merge(r.Value.HLL)
		}

		out := make(map[string]EvalResult, len(accs))
		for k, a := range accs {
			if n.Op == AggCount {
				// Cardinality estimate → scalar number result
				est := a.h.Estimate()
				out[k] = EvalResult{
					Timestamp: a.ts,
					Value:     Value{Kind: ValScalar, Num: float64(est)},
					Tags:      a.tags,
				}
			} else {
				// For non-count ops, keep the HLL (if you ever need it)
				out[k] = EvalResult{
					Timestamp: a.ts,
					Value:     Value{Kind: ValHLL, HLL: a.h},
					Tags:      a.tags,
				}
			}
		}
		return out

	// --------- Scalar vector-agg path ----------
	default:
		type acc struct {
			sum   float64
			count int
			min   float64
			max   float64
			ts    int64
			tags  map[string]any
		}
		aggs := make(map[string]*acc, len(child))

		for _, r := range child {
			if math.IsNaN(r.Value.Num) && r.Value.Kind == ValScalar {
				continue
			}
			k := makeKey(r.Tags)
			a := aggs[k]
			if a == nil {
				a = &acc{
					sum:   0,
					count: 0,
					min:   math.Inf(1),
					max:   math.Inf(-1),
					ts:    sg.Timestamp,
					tags:  r.Tags,
				}
				aggs[k] = a
			}
			var v float64
			switch r.Value.Kind {
			case ValScalar:
				v = r.Value.Num
			case ValMap:
				// if there is a map, extract the right aggregation value from it.
				switch n.Op {
				case AggSum:
					v = r.Value.AggMap[SUM]
				case AggAvg:
					s := r.Value.AggMap[SUM]
					c := r.Value.AggMap[COUNT]
					if c == 0 {
						v = math.NaN()
					} else {
						v = s / c
					}
				case AggMin:
					v = r.Value.AggMap[MIN]
				case AggMax:
					v = r.Value.AggMap[MAX]
				case AggCount:
					if countPassThrough {
						if c, ok := r.Value.AggMap[COUNT]; ok {
							v = c
						} else if s, ok := r.Value.AggMap[SUM]; ok {
							v = s
						} else {
							v = 0
						}
					} else {
						v = float64(a.count)
					}
				default:
					v = math.NaN()
				}
			default:
				continue
			}
			a.sum += v
			a.count++
			if v < a.min {
				a.min = v
			}
			if v > a.max {
				a.max = v
			}
		}

		out := make(map[string]EvalResult, len(aggs))
		for k, a := range aggs {
			var v float64
			switch n.Op {
			case AggSum:
				v = a.sum
			case AggAvg:
				if a.count == 0 {
					v = math.NaN()
				} else {
					v = a.sum / float64(a.count)
				}
			case AggMin:
				v = a.min
			case AggMax:
				v = a.max
			case AggCount:
				if countPassThrough {
					v = a.sum
				} else {
					v = float64(a.count)
				}
			default:
				v = math.NaN()
			}
			out[k] = EvalResult{
				Timestamp: a.ts,
				Value:     Value{Kind: ValScalar, Num: v},
				Tags:      a.tags,
			}
		}
		return out
	}
}

func (n *AggNode) Label(tags map[string]any) string {
	var b strings.Builder
	switch n.Op {
	case AggSum:
		b.WriteString("sum")
	case AggAvg:
		b.WriteString("avg")
	case AggMin:
		b.WriteString("min")
	case AggMax:
		b.WriteString("max")
	case AggCount:
		b.WriteString("count")
	default:
		b.WriteString("unknown-agg")
	}
	if len(n.By) > 0 {
		b.WriteString(" by (")
		b.WriteString(strings.Join(n.By, ","))
		b.WriteString(")")
	} else if len(n.Without) > 0 {
		b.WriteString(" without (")
		b.WriteString(strings.Join(n.Without, ","))
		b.WriteString(")")
	}
	if n.Child != nil {
		b.WriteString("(")
		b.WriteString(n.Child.Label(tags))
		b.WriteString(")")
	}
	return b.String()
}
