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
	windows   map[string]*winSumCount // for sum/rate/increase/avg/count/last
	windowsMM map[string]*winMinMax   // for min/max

	// Tracks previous bucket timestamp per grouping key to infer bucket span.
	prevTs map[string]int64
}

func (n *LeafNode) Hints() ExecHints {
	return ExecHints{
		WantTopK:    n.BE.WantTopK,
		WantBottomK: n.BE.WantBottomK,
		WantCount:   n.BE.WantCount,
		WantDDS:     n.BE.WantDDS,
		IsLogLeaf:   n.BE.LogLeaf != nil,
	}
}

/* ---------------- sum/count window (exact interval math) ------------------ */

type winEntry struct {
	ts    int64   // bucket start (ms)
	span  int64   // bucket span (ms), bucket interval is [ts, ts+span)
	sum   float64 // bucket SUM(rollup_sum)
	count float64 // bucket SUM(rollup_count)
}

type winSumCount struct {
	rangeMs int64
	entries []winEntry
	sum     float64
	count   float64
}

// Add a new bucket.
func (w *winSumCount) add(ts int64, spanMs int64, addSum, addCount float64) {
	if spanMs <= 0 {
		spanMs = 1
	}
	w.entries = append(w.entries, winEntry{ts: ts, span: spanMs, sum: addSum, count: addCount})
	w.sum += addSum
	w.count += addCount
}

// Evict buckets whose end time is at/before keepFromTs (no overlap with window).
func (w *winSumCount) evict(keepFromTs int64) {
	i := 0
	for i < len(w.entries) {
		e := w.entries[i]
		if e.ts+e.span > keepFromTs {
			break
		}
		w.sum -= e.sum
		w.count -= e.count
		i++
	}
	if i > 0 {
		w.entries = append([]winEntry(nil), w.entries[i:]...)
	}
}

// coveredMs computes exact overlap of all buckets with [nowTs-rangeMs, nowTs).
func (w *winSumCount) coveredMs(nowTs int64) int64 {
	if len(w.entries) == 0 {
		return 0
	}
	winStart := nowTs - w.rangeMs
	winEnd := nowTs

	var covered int64
	for _, e := range w.entries {
		bStart := e.ts
		bEnd := e.ts + e.span
		start := max64(bStart, winStart)
		end := min64(bEnd, winEnd)
		if end > start {
			covered += end - start
		}
	}
	return covered
}

/* -------------------- min/max window (exact interval math) ---------------- */

type mmEntry struct {
	ts   int64   // start
	span int64   // span
	min  float64 // bucket min
	max  float64 // bucket max
}

type winMinMax struct {
	rangeMs int64
	segs    []mmEntry // FIFO of segments for coverage & eviction
	minDQ   []mmEntry // monotonic increasing by min (front is current window min)
	maxDQ   []mmEntry // monotonic decreasing by max (front is current window max)
}

// Add a new bucket (ts, span, min, max).
func (w *winMinMax) add(ts, spanMs int64, bktMin, bktMax float64) {
	if spanMs <= 0 {
		spanMs = 1
	}
	e := mmEntry{ts: ts, span: spanMs, min: bktMin, max: bktMax}
	w.segs = append(w.segs, e)

	// Maintain min deque (increasing by min).
	for len(w.minDQ) > 0 && w.minDQ[len(w.minDQ)-1].min >= bktMin {
		w.minDQ = w.minDQ[:len(w.minDQ)-1]
	}
	w.minDQ = append(w.minDQ, e)

	// Maintain max deque (decreasing by max).
	for len(w.maxDQ) > 0 && w.maxDQ[len(w.maxDQ)-1].max <= bktMax {
		w.maxDQ = w.maxDQ[:len(w.maxDQ)-1]
	}
	w.maxDQ = append(w.maxDQ, e)
}

// Evict all segments whose end ≤ keepFromTs; pop from deques if they match.
func (w *winMinMax) evict(keepFromTs int64) {
	i := 0
	for i < len(w.segs) && w.segs[i].ts+w.segs[i].span <= keepFromTs {
		seg := w.segs[i]
		// If the evicted seg is at the front of a deque, pop it too.
		if len(w.minDQ) > 0 && w.minDQ[0].ts == seg.ts {
			w.minDQ = w.minDQ[1:]
		}
		if len(w.maxDQ) > 0 && w.maxDQ[0].ts == seg.ts {
			w.maxDQ = w.maxDQ[1:]
		}
		i++
	}
	if i > 0 {
		w.segs = append([]mmEntry(nil), w.segs[i:]...)
	}
}

// coveredMs computes exact overlap with [nowTs-rangeMs, nowTs).
func (w *winMinMax) coveredMs(nowTs int64) int64 {
	if len(w.segs) == 0 {
		return 0
	}
	winStart := nowTs - w.rangeMs
	winEnd := nowTs

	var covered int64
	for _, e := range w.segs {
		start := max64(e.ts, winStart)
		end := min64(e.ts+e.span, winEnd)
		if end > start {
			covered += end - start
		}
	}
	return covered
}

func (w *winMinMax) min() float64 {
	if len(w.minDQ) == 0 {
		return math.NaN()
	}
	return w.minDQ[0].min
}

func (w *winMinMax) max() float64 {
	if len(w.maxDQ) == 0 {
		return math.NaN()
	}
	return w.maxDQ[0].max
}

/* -------------------------------------------------------------------------- */

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

	defaultSpanMs := step.Milliseconds()
	rangeMs := RangeMsFromRange(n.BE.Range)

	out := make(map[string]EvalResult, len(rows))
	for _, si := range rows {
		k := keyFor(si.SketchTags.Tags)
		spanMs := n.computeSpanMs(k, si.Timestamp, defaultSpanMs)

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
			if n.BE.WantCount {
				v = Value{
					Kind: ValScalar,
					Num:  si.SketchTags.getAggValue(COUNT),
				}
			} else if n.BE.FuncName != "" {
				num := n.evalRangeAwareScalar(k, si, spanMs, rangeMs)
				v = Value{Kind: ValScalar, Num: num}
			} else {
				// Instant selector: merge agg maps across workers (sum,sum; min=min; max=max).
				merged := map[string]float64{}
				if prev, ok := out[k]; ok && prev.Value.Kind == ValMap && prev.Value.AggMap != nil {
					for name, val := range prev.Value.AggMap {
						merged[name] = val
					}
				}
				if v, ok := si.SketchTags.Agg["sum"]; ok {
					merged["sum"] += v
				}
				if v, ok := si.SketchTags.Agg["count"]; ok {
					merged["count"] += v
				}
				if v, ok := si.SketchTags.Agg["min"]; ok {
					if cur, ok2 := merged["min"]; !ok2 || v < cur {
						merged["min"] = v
					}
				}
				if v, ok := si.SketchTags.Agg["max"]; ok {
					if cur, ok2 := merged["max"]; !ok2 || v > cur {
						merged["max"] = v
					}
				}
				v = Value{Kind: ValMap, AggMap: merged}
			}

		default:
			// Unknown sketch type
			v = Value{Kind: ValScalar, Num: math.NaN()}
		}

		out[k] = EvalResult{
			Timestamp: si.Timestamp,
			Value:     v,
			Tags:      si.SketchTags.Tags,
		}
	}
	return out
}

// computeSpanMs infers the bucket span for a key from the delta between consecutive timestamps.
// Falls back to defaultSpanMs (query step) if this is the first sample or the delta is non-positive.
func (n *LeafNode) computeSpanMs(key string, ts int64, defaultSpanMs int64) int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.prevTs == nil {
		n.prevTs = make(map[string]int64, 64)
	}
	prev := n.prevTs[key]
	span := defaultSpanMs
	if prev > 0 && ts > prev {
		span = ts - prev
	}
	if span <= 0 {
		span = defaultSpanMs
	}
	n.prevTs[key] = ts
	return span
}

// evalRangeAwareScalar applies RANGE window math on top of raw step aggregates,
// using the *actual incoming bucket span* (spanMs), not the query step.
func (n *LeafNode) evalRangeAwareScalar(key string, in SketchInput, spanMs, rangeMs int64) float64 {
	if rangeMs <= 0 {
		// Instant math: use bucket span for rate denominator.
		return evalLeafValuePerBucket(n.BE, in, float64(spanMs)/1000.0)
	}

	ts := in.Timestamp

	switch n.BE.FuncName {
	case "sum_over_time", "increase", "rate", "avg_over_time", "count_over_time", "last_over_time":
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
		w.add(ts, spanMs, bktSum, bktCnt)

		// Window is [ts-rangeMs, ts). Evict buckets with end ≤ ts-rangeMs.
		keepFromTs := ts - w.rangeMs
		w.evict(keepFromTs)

		covered := w.coveredMs(ts)
		sum, cnt := w.sum, w.count

		// last_over_time: approximate last bucket value as (sum/count) of the last entry.
		lastVal := math.NaN()
		if nBE := len(w.entries); nBE > 0 {
			ls := w.entries[nBE-1].sum
			lc := w.entries[nBE-1].count
			if lc == 0 {
				lc = 1
			}
			lastVal = ls / lc
		}
		n.mu.Unlock()

		if covered < rangeMs {
			return math.NaN()
		}
		switch n.BE.FuncName {
		case "increase", "sum_over_time":
			return sum
		case "count_over_time":
			return cnt
		case "rate":
			return sum / (float64(rangeMs) / 1000.0)
		case "avg_over_time":
			if cnt == 0 {
				return math.NaN()
			}
			return sum / cnt
		case "last_over_time":
			return lastVal
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
		w.add(ts, spanMs, bktMin, bktMax)

		// Evict segments with end ≤ ts-rangeMs.
		w.evict(ts - w.rangeMs)

		covered := w.coveredMs(ts)
		var out float64
		if covered < rangeMs {
			slog.Info("Insufficient coverage", "coveredMs", covered, "requiredMs", rangeMs)
			out = math.NaN()
		} else if n.BE.FuncName == "min_over_time" {
			out = w.min()
		} else {
			out = w.max()
		}
		n.mu.Unlock()
		return out
	}

	// Fallback to per-bucket behavior.
	return evalLeafValuePerBucket(n.BE, in, float64(spanMs)/1000.0)
}

// evalLeafValuePerBucket computes per-bucket values; bucketSpanSecs reflects the *incoming* bucket width.
func evalLeafValuePerBucket(be BaseExpr, in SketchInput, bucketSpanSecs float64) float64 {
	if in.SketchTags.SketchType != SketchMAP {
		return math.NaN()
	}
	if be.WantCount {
		return in.SketchTags.getAggValue(COUNT)
	}
	switch be.FuncName {
	case "rate":
		rs := rangeSeconds(be, bucketSpanSecs)
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
	case "count_over_time":
		return in.SketchTags.getAggValue(COUNT)
	case "last_over_time":
		// No window -> just use the bucket’s value (we store it under SUM)
		return in.SketchTags.getAggValue(SUM)
	default:
		return in.SketchTags.getAggValue(SUM)
	}
}

func rangeSeconds(be BaseExpr, bucketSpanSecs float64) float64 {
	// For instant rate, be.Range == "" and we use bucketSpanSecs.
	// For range rate, we use the parsed range.
	if be.Range == "" {
		return bucketSpanSecs
	}
	d, err := model.ParseDuration(be.Range)
	if err != nil {
		return math.NaN()
	}
	return time.Duration(d).Seconds()
}

func (n *LeafNode) Label(tags map[string]any) string {
	out := ""
	if n.BE.FuncName != "" {
		out += n.BE.FuncName + "("
	}
	out += n.BE.Metric

	if len(n.BE.GroupBy) > 0 {
		parts := make([]string, 0, len(n.BE.GroupBy))
		groupByTags := n.BE.GroupBy
		for _, lbl := range groupByTags {
			if v, ok := tags[lbl]; ok {
				parts = append(parts, fmt.Sprintf("%s=%v", lbl, v))
			}
		}
		if len(parts) > 0 {
			out += "{" + strings.Join(parts, ",") + "}"
		}
	} else {
		for _, matcher := range n.BE.Matchers {
			parts := fmt.Sprintf("%s%s%v", matcher.Label, matcher.Op, matcher.Value)
			out += "{" + parts + "}"
		}
	}
	if n.BE.Range != "" {
		out += fmt.Sprintf("[%s]", n.BE.Range)
	}
	out += ")"
	return out
}

/* ------------------------------- utils ------------------------------------ */

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
