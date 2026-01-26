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

	// Tracks previous bucket timestamp per grouping key to infer incoming bucket span
	// (used for instant math and to stabilize sparse data in range windows).
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
	sum   float64 // SUM(chq_rollup_sum) within bucket
	count float64 // SUM(chq_rollup_count) within bucket
}

// winSumCount maintains a left-inclusive sliding window over *query-step/effective* buckets.
// Coverage at time nowTs is (nowTs - firstTs) + stepMs (or an effective span passed in).
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
		k := keyFor(si.SketchTags.Tags)

		// Per-key inferred incoming span (ms); used to stabilize sparse data.
		inferredSpanMs := n.inferSpanMs(k, si.Timestamp, stepMs)
		effSpanMs := max64(stepMs, inferredSpanMs) // use the larger of (query step, incoming cadence)

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
				v = Value{Kind: ValScalar, Num: si.SketchTags.getAggValue(COUNT)}
			} else if n.BE.FuncName != "" {
				// RANGE math: use effSpanMs for eviction/coverage to handle sparse (e.g., 60s) buckets.
				num := n.evalRangeAwareScalar(k, si, stepMs, inferredSpanMs, rangeMs)
				v = Value{Kind: ValScalar, Num: num}
			} else {
				// Instant selector: merge maps across workers
				merged := map[string]float64{}
				if prev, ok := out[k]; ok && prev.Value.Kind == ValMap && prev.Value.AggMap != nil {
					for name, val := range prev.Value.AggMap {
						merged[name] = val
					}
				}
				if vv, ok := si.SketchTags.Agg["sum"]; ok {
					merged["sum"] += vv
				}
				if vv, ok := si.SketchTags.Agg["count"]; ok {
					merged["count"] += vv
				}
				if vv, ok := si.SketchTags.Agg["min"]; ok {
					if cur, ok2 := merged["min"]; !ok2 || vv < cur {
						merged["min"] = vv
					}
				}
				if vv, ok := si.SketchTags.Agg["max"]; ok {
					if cur, ok2 := merged["max"]; !ok2 || vv > cur {
						merged["max"] = vv
					}
				}
				v = Value{Kind: ValMap, AggMap: merged}
			}

			// Instant rate (no range): denominator should be effSpanMs (seconds).
			if n.BE.FuncName == "rate" && n.BE.Range == "" {
				num := evalLeafValuePerBucket(n.BE, si, float64(effSpanMs)/1000.0)
				v = Value{Kind: ValScalar, Num: num}
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

// inferSpanMs returns the delta between consecutive timestamps for a given key,
// falling back to stepMs if this is the first sample or the delta is non-positive.
func (n *LeafNode) inferSpanMs(key string, ts int64, stepMs int64) int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.prevTs == nil {
		n.prevTs = make(map[string]int64, 64)
	}
	prev := n.prevTs[key]
	span := stepMs
	if prev > 0 && ts > prev {
		span = ts - prev
	}
	if span <= 0 {
		span = stepMs
	}
	n.prevTs[key] = ts
	return span
}

// evalRangeAwareScalar applies RANGE window math.
// It uses effSpanMs = max(stepMs, inferredSpanMs) for eviction and coverage to
// avoid gaps when incoming buckets are sparser than the query step.
func (n *LeafNode) evalRangeAwareScalar(
	key string,
	in SketchInput,
	stepMs int64,
	inferredSpanMs int64,
	rangeMs int64,
) float64 {
	effSpanMs := max64(stepMs, inferredSpanMs)

	if rangeMs <= 0 {
		// Instant math: use effSpan for denominators (e.g., instant rate)
		return evalLeafValuePerBucket(n.BE, in, float64(effSpanMs)/1000.0)
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
		w.add(ts, bktSum, bktCnt)

		// Evict using effective span; left-inclusive coverage uses the same span.
		w.evict(ts + effSpanMs - rangeMs)
		covered := w.coveredMs(ts, effSpanMs)
		sum, cnt := w.sum, w.count

		// Approximate "last" as the last bucket's average (sum/count for that bucket).
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
			// Windowed rate uses the range width in seconds.
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
		w.add(ts, bktMin, bktMax)
		w.evict(ts + effSpanMs - rangeMs)
		covered := w.coveredMs(ts, effSpanMs)
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

	// Fallback to per-bucket behavior (use effSpan for instant denominators).
	return evalLeafValuePerBucket(n.BE, in, float64(effSpanMs)/1000.0)
}

// evalLeafValuePerBucket computes per-bucket values; bucketSpanSecs is the
// denominator for instant rate (caller passes effSpanSecs where appropriate).
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

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
