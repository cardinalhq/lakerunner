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
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
	"github.com/axiomhq/hyperloglog"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/common/model"
)

// LeafNode corresponds to one BaseExpr pushdown (one worker stream).
type LeafNode struct {
	BE        BaseExpr
	mu        sync.Mutex
	windows   map[string]*winSumCount // for sum/rate/increase/avg
	windowsMM map[string]*winMinMax   // for min/max
}

func (n *LeafNode) Hints() ExecHints {
	return ExecHints{
		WantTopK:    n.BE.WantTopK,
		WantBottomK: n.BE.WantBottomK,
		WantCount:   n.BE.WantCount,
		WantDDS:     n.BE.WantDDS,
	}
}

type winEntry struct {
	ts    int64   // bucket start (ms)
	sum   float64 // bucket SUM(rollup_sum)
	count float64 // bucket SUM(rollup_count)
}

// winSumCount maintains a left-inclusive sliding window over step-buckets.
// Coverage at time nowTs (start of current bucket) is (nowTs - firstTs) + stepMs.
type winSumCount struct {
	rangeMs int64
	entries []winEntry
	sum     float64
	count   float64
}

func (w *winSumCount) add(ts int64, addSum, addCount float64) {
	w.entries = append(w.entries, winEntry{ts: ts, sum: addSum, count: addCount})
	w.sum += addSum
	w.count += addCount
}

// evict rows strictly older than keepFromTs (i.e. ts < keepFromTs).
func (w *winSumCount) evict(keepFromTs int64) {
	i := 0
	for i < len(w.entries) && w.entries[i].ts < keepFromTs {
		w.sum -= w.entries[i].sum
		w.count -= w.entries[i].count
		i++
	}
	if i > 0 {
		w.entries = append([]winEntry(nil), w.entries[i:]...)
	}
}

// coveredMs returns how much wall time is covered by the current window,
// assuming each bucket spans [ts, ts+step).
func (w *winSumCount) coveredMs(nowTs, stepMs int64) int64 {
	if len(w.entries) == 0 {
		return 0
	}
	first := w.entries[0].ts
	return (nowTs - first) + stepMs
}

// ---------- window state for min/max family (monotonic deques) --------------

type mmEntry struct {
	ts  int64
	val float64
}

type winMinMax struct {
	rangeMs int64
	allTs   []int64 // FIFO of all bucket timestamps to compute coverage
	minDQ   []mmEntry
	maxDQ   []mmEntry
}

func (w *winMinMax) add(ts int64, bktMin, bktMax float64) {
	w.allTs = append(w.allTs, ts)

	for len(w.minDQ) > 0 && w.minDQ[len(w.minDQ)-1].val >= bktMin {
		w.minDQ = w.minDQ[:len(w.minDQ)-1]
	}
	w.minDQ = append(w.minDQ, mmEntry{ts: ts, val: bktMin})

	for len(w.maxDQ) > 0 && w.maxDQ[len(w.maxDQ)-1].val <= bktMax {
		w.maxDQ = w.maxDQ[:len(w.maxDQ)-1]
	}
	w.maxDQ = append(w.maxDQ, mmEntry{ts: ts, val: bktMax})
}

func (w *winMinMax) evict(keepFromTs int64) {
	// Evict timestamps (coverage queue)
	for len(w.allTs) > 0 && w.allTs[0] < keepFromTs {
		w.allTs = w.allTs[1:]
	}
	// Evict from deques
	for len(w.minDQ) > 0 && w.minDQ[0].ts < keepFromTs {
		w.minDQ = w.minDQ[1:]
	}
	for len(w.maxDQ) > 0 && w.maxDQ[0].ts < keepFromTs {
		w.maxDQ = w.maxDQ[1:]
	}
}

func (w *winMinMax) coveredMs(nowTs, stepMs int64) int64 {
	if len(w.allTs) == 0 {
		return 0
	}
	first := w.allTs[0]
	return (nowTs - first) + stepMs
}

func (w *winMinMax) min() float64 {
	if len(w.minDQ) == 0 {
		return math.NaN()
	}
	return w.minDQ[0].val
}

func (w *winMinMax) max() float64 {
	if len(w.maxDQ) == 0 {
		return math.NaN()
	}
	return w.maxDQ[0].val
}

// ---------------------------------------------------------------------------

func (n *LeafNode) Eval(sg SketchGroup, step time.Duration) map[string]EvalResult {
	rows := sg.Group[n.BE.ID]
	if len(rows) == 0 {
		return map[string]EvalResult{}
	}

	keyFor := func(tags map[string]any) string {
		if len(n.BE.GroupBy) == 0 {
			return "default"
		}
		parts := make([]string, 0, len(n.BE.GroupBy))
		for _, lbl := range n.BE.GroupBy {
			if v, ok := tags[lbl]; ok {
				parts = append(parts, fmt.Sprintf("%s=%v", lbl, v))
			}
		}
		if len(parts) == 0 {
			return "default"
		}
		return strings.Join(parts, ",")
	}

	stepMs := step.Milliseconds()
	rangeMs := RangeMsFromRange(n.BE.Range)

	out := make(map[string]EvalResult, len(rows))
	for _, si := range rows {
		var v Value

		switch si.SketchTags.SketchType {
		case SketchHLL:
			if len(si.SketchTags.Bytes) == 0 {
				v = Value{Kind: ValScalar, Num: math.NaN()}
				break
			}
			var h hyperloglog.Sketch
			if err := h.UnmarshalBinary(si.SketchTags.Bytes); err != nil {
				slog.Error("failed to unmarshal HLL sketch", "error", err)
				v = Value{Kind: ValScalar, Num: math.NaN()}
				break
			}
			v = Value{Kind: ValHLL, HLL: &h}

		case SketchDDS:
			if len(si.SketchTags.Bytes) == 0 {
				v = Value{Kind: ValScalar, Num: math.NaN()}
				break
			}
			var pb sketchpb.DDSketch
			if err := proto.Unmarshal(si.SketchTags.Bytes, &pb); err != nil {
				slog.Error("failed to unmarshal DDSketch proto", "error", err)
				v = Value{Kind: ValScalar, Num: math.NaN()}
				break
			}
			sk, err := ddsketch.FromProto(&pb)
			if err != nil {
				slog.Error("failed to build DDSketch from proto", "error", err)
				v = Value{Kind: ValScalar, Num: math.NaN()}
				break
			}
			v = Value{Kind: ValDDS, DDS: sk}

		case SketchMAP:
			k := keyFor(si.SketchTags.Tags)
			num := n.evalRangeAwareScalar(k, si, stepMs, rangeMs)
			v = Value{Kind: ValScalar, Num: num}

		default:
			// Unknown sketch type
			v = Value{Kind: ValScalar, Num: math.NaN()}
		}

		k := keyFor(si.SketchTags.Tags)
		out[k] = EvalResult{
			Timestamp: si.Timestamp,
			Value:     v,
			Tags:      si.SketchTags.Tags,
		}
	}
	return out
}

// evalRangeAwareScalar applies RANGE window math on top of raw step aggregates.
func (n *LeafNode) evalRangeAwareScalar(key string, in SketchInput, stepMs, rangeMs int64) float64 {
	if rangeMs <= 0 {
		return evalLeafValuePerBucket(n.BE, in, float64(stepMs)/1000.0)
	}

	ts := in.Timestamp

	switch n.BE.FuncName {
	case "sum_over_time", "increase", "rate", "avg_over_time":
		bktSum := in.SketchTags.getAggValue(SUM)
		bktCnt := in.SketchTags.getAggValue(COUNT)

		n.mu.Lock()
		if n.windows == nil {
			n.windows = make(map[string]*winSumCount, 64)
		}
		w := n.windows[key]
		if w == nil || w.rangeMs != rangeMs {
			w = &winSumCount{rangeMs: rangeMs}
			n.windows[key] = w
		}
		w.add(ts, bktSum, bktCnt)
		w.evict(ts - rangeMs)
		covered := w.coveredMs(ts, stepMs)
		sum, cnt := w.sum, w.count
		n.mu.Unlock()

		if covered < rangeMs {
			return math.NaN()
		}
		switch n.BE.FuncName {
		case "increase", "sum_over_time":
			return sum
		case "rate":
			return sum / (float64(rangeMs) / 1000.0)
		case "avg_over_time":
			if cnt == 0 {
				return math.NaN()
			}
			return sum / cnt
		}

	case "min_over_time", "max_over_time":
		bktMin := in.SketchTags.getAggValue(MIN)
		bktMax := in.SketchTags.getAggValue(MAX)

		n.mu.Lock()
		if n.windowsMM == nil {
			n.windowsMM = make(map[string]*winMinMax, 64)
		}
		w := n.windowsMM[key]
		if w == nil || w.rangeMs != rangeMs {
			w = &winMinMax{rangeMs: rangeMs}
			n.windowsMM[key] = w
		}
		w.add(ts, bktMin, bktMax)
		// Left-inclusive window: keep [ts-rangeMs, ts].
		w.evict(ts - rangeMs)
		covered := w.coveredMs(ts, stepMs)
		var out float64
		if covered < rangeMs {
			out = math.NaN()
		} else if n.BE.FuncName == "min_over_time" {
			out = w.min()
		} else {
			out = w.max()
		}
		n.mu.Unlock()
		return out
	}

	// Fallback (unknown func)
	return evalLeafValuePerBucket(n.BE, in, float64(stepMs)/1000.0)
}

// Per-bucket (no-range) evaluation for all supported funcs.
func evalLeafValuePerBucket(be BaseExpr, in SketchInput, stepSecs float64) float64 {
	if in.SketchTags.SketchType != SketchMAP {
		return math.NaN()
	}
	if be.WantCount {
		return in.SketchTags.getAggValue(COUNT)
	}
	switch be.FuncName {
	case "rate":
		rs := rangeSeconds(be, stepSecs)
		if rs <= 0 {
			return math.NaN()
		}
		return in.SketchTags.getAggValue(SUM) / rs
	case "increase", "sum_over_time", "":
		return in.SketchTags.getAggValue(SUM)
	case "avg_over_time":
		sum := in.SketchTags.getAggValue(SUM)
		cnt := in.SketchTags.getAggValue(COUNT)
		if cnt == 0 {
			return math.NaN()
		}
		return sum / cnt
	case "min_over_time":
		return in.SketchTags.getAggValue(MIN)
	case "max_over_time":
		return in.SketchTags.getAggValue(MAX)
	default:
		return math.NaN()
	}
}

func rangeSeconds(be BaseExpr, stepSecs float64) float64 {
	if be.Range == "" {
		return stepSecs
	}
	d, err := model.ParseDuration(be.Range)
	if err != nil {
		return math.NaN()
	}
	return time.Duration(d).Seconds()
}
