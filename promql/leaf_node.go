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

type winEntry struct {
	ts    int64   // bucket start (ms)
	sum   float64 // bucket SUM(rollup_sum)
	count float64 // bucket SUM(rollup_count)
}

// winSumCount maintains a left-inclusive sliding window over real bucket spans.
// Coverage at time nowTs (start of current bucket) is (nowTs - firstTs) + lastSpanMs.
type winSumCount struct {
	rangeMs    int64
	entries    []winEntry
	sum        float64
	count      float64
	lastSpanMs int64 // span of the most-recent bucket (ms)
}

func (w *winSumCount) add(ts int64, addSum, addCount float64, spanMs int64) {
	if spanMs <= 0 {
		spanMs = 1
	}
	w.entries = append(w.entries, winEntry{ts: ts, sum: addSum, count: addCount})
	w.sum += addSum
	w.count += addCount
	w.lastSpanMs = spanMs
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
// assuming the current bucket spans [nowTs, nowTs+lastSpanMs).
func (w *winSumCount) coveredMs(nowTs int64) int64 {
	if len(w.entries) == 0 {
		return 0
	}
	span := w.lastSpanMs
	if span <= 0 {
		span = 1
	}
	first := w.entries[0].ts
	return (nowTs - first) + span
}

// ---------- window state for min/max family (monotonic deques) --------------

type mmEntry struct {
	ts  int64
	val float64
}

type winMinMax struct {
	rangeMs    int64
	allTs      []int64 // FIFO of all bucket timestamps to compute coverage
	minDQ      []mmEntry
	maxDQ      []mmEntry
	lastSpanMs int64
}

func (w *winMinMax) add(ts int64, bktMin, bktMax float64, spanMs int64) {
	if spanMs <= 0 {
		spanMs = 1
	}
	w.allTs = append(w.allTs, ts)
	w.lastSpanMs = spanMs

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

// coveredMs returns how much wall time is covered by the current window,
// assuming the current bucket spans [nowTs, nowTs+lastSpanMs).
func (w *winMinMax) coveredMs(nowTs int64) int64 {
	if len(w.allTs) == 0 {
		return 0
	}
	span := w.lastSpanMs
	if span <= 0 {
		span = 1
	}
	first := w.allTs[0]
	return (nowTs - first) + span
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
				// Instant selector: MERGE Agg maps across workers (sum,sum; min=min; max=max).
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
		w.add(ts, bktSum, bktCnt, spanMs)
		// Evict based on real last span, not query step.
		w.evict(ts - w.rangeMs + w.lastSpanMs)
		covered := w.coveredMs(ts)
		sum, cnt := w.sum, w.count

		// "last_over_time": last bucket's value approximated as sum/count for that bucket
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
		w.add(ts, bktMin, bktMax, spanMs)
		w.evict(ts - w.rangeMs + w.lastSpanMs)
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
