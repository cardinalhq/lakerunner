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
	"github.com/google/uuid"
	"math"
	"sort"
	"time"
)

type SegmentInfo struct {
	DateInt        int       `json:"dateInt"`
	Hour           string    `json:"hour"`
	SegmentID      int64     `json:"segmentId"`
	StartTs        int64     `json:"startTs"`
	EndTs          int64     `json:"endTs"`
	ExprID         string    `json:"exprId"`
	Dataset        string    `json:"dataset"`
	OrganizationID uuid.UUID `json:"organizationID"`
	InstanceNum    int16     `json:"instanceNum"`
	Frequency      int64     `json:"frequency"`
}

type SegmentGroup struct {
	StartTs  int64
	EndTs    int64
	Segments []SegmentInfo
}

// ComputeReplayBatchesWithWorkers Public entrypoint: takes workers, computes targetSize internally.
func ComputeReplayBatchesWithWorkers(
	segments []SegmentInfo,
	step time.Duration,
	queryStartTs, queryEndTs int64,
	workers int,
	reverseSort bool,
) []SegmentGroup {
	ts := TargetSize(len(segments), workers)
	return ComputeReplayBatches(
		segments,
		step,
		queryStartTs,
		queryEndTs,
		ts,
		reverseSort,
	)
}

// ComputeReplayBatches computes batches of segments to replay over a time window.
func ComputeReplayBatches(
	segments []SegmentInfo,
	step time.Duration,
	queryStartTs, queryEndTs int64,
	targetSize int,
	reverseSort bool,
) []SegmentGroup {
	if len(segments) == 0 {
		return nil
	}
	if targetSize <= 0 {
		targetSize = len(segments)
	}

	windows := buildWindows(segments, step, queryStartTs, queryEndTs)

	sort.Slice(windows, func(i, j int) bool {
		if windows[i].StartTs == windows[j].StartTs {
			return windows[i].EndTs < windows[j].EndTs
		}
		return windows[i].StartTs < windows[j].StartTs
	})

	batches := coalesceContiguous(windows, targetSize)

	if reverseSort {
		for i, j := 0, len(batches)-1; i < j; i, j = i+1, j-1 {
			batches[i], batches[j] = batches[j], batches[i]
		}
	}

	for i := range batches {
		batches[i] = normalizeAndMerge(batches[i], queryStartTs, queryEndTs)
	}
	return batches
}

// ---- internals ----

// Align each segment to step, clamp to query, then group by exact [start,end).
func buildWindows(segs []SegmentInfo, step time.Duration, qStart, qEnd int64) []SegmentGroup {
	stepMs := step.Milliseconds()
	if stepMs <= 0 {
		stepMs = 1
	}

	type key struct{ s, e int64 }
	buckets := map[key][]SegmentInfo{}

	align := func(ts int64) int64 {
		// Truncate down to step boundary
		return ts - (ts % stepMs)
	}
	ceilToStep := func(ts int64) int64 {
		if r := ts % stepMs; r == 0 {
			return ts
		}
		return ts + (stepMs - (ts % stepMs))
	}

	for _, s := range segs {
		// align to step
		as := align(s.StartTs)
		ae := ceilToStep(s.EndTs)
		// clamp to query
		if as < qStart {
			as = qStart - (qStart % stepMs) // align/clamp boundary too
		}
		if ae > qEnd {
			ae = ceilToStep(qEnd)
		}
		if as >= ae {
			continue
		}
		s2 := s
		s2.StartTs = as
		s2.EndTs = ae
		k := key{as, ae}
		buckets[k] = append(buckets[k], s2)
	}

	wins := make([]SegmentGroup, 0, len(buckets))
	for k, list := range buckets {
		wins = append(wins, SegmentGroup{
			StartTs:  k.s,
			EndTs:    k.e,
			Segments: list,
		})
	}
	return wins
}

// Greedy pack adjacent windows into batches by time. Flush on gaps or when targetSize reached.
func coalesceContiguous(wins []SegmentGroup, targetSize int) []SegmentGroup {
	if len(wins) == 0 {
		return nil
	}
	var out []SegmentGroup

	var curStart, curEnd int64
	var curSegs []SegmentInfo
	var curCount int

	flush := func() {
		if len(curSegs) == 0 {
			return
		}
		out = append(out, SegmentGroup{
			StartTs:  curStart,
			EndTs:    curEnd,
			Segments: curSegs,
		})
		curStart, curEnd = 0, 0
		curSegs = nil
		curCount = 0
	}

	for idx, w := range wins {
		if idx == 0 {
			curStart, curEnd = w.StartTs, w.EndTs
			curSegs = append(curSegs, w.Segments...)
			curCount += len(w.Segments)
			continue
		}

		// Contiguity check: no gaps between previous end and this start.
		if w.StartTs != curEnd {
			// Time gap → flush regardless of count
			flush()
			curStart, curEnd = w.StartTs, w.EndTs
			curSegs = append(curSegs, w.Segments...)
			curCount += len(w.Segments)
			continue
		}

		// Extend current batch
		curEnd = w.EndTs
		curSegs = append(curSegs, w.Segments...)
		curCount += len(w.Segments)

		// If we've hit target size (or exceeded), flush this contiguous block.
		if curCount >= targetSize {
			flush()
		}
	}
	flush()

	// (Optional) small-tail rebalance: if we produced ≥2 batches and the last batch is tiny,
	// merge it into the previous one to avoid stragglers.
	if len(out) >= 2 {
		last := &out[len(out)-1]
		prev := &out[len(out)-2]
		if len(last.Segments) < targetSize/3 {
			// merge into prev
			prev.EndTs = last.EndTs
			prev.Segments = append(prev.Segments, last.Segments...)
			out = out[:len(out)-1]
		}
	}

	return out
}

// Within a batch, widen duplicates by (SegmentID, ExprID) to the batch window
// and clamp to [queryStart, queryEnd]. This ensures each worker reads a single
// contiguous time window per (segment, expr).
func normalizeAndMerge(g SegmentGroup, qStart, qEnd int64) SegmentGroup {
	start := g.StartTs
	end := g.EndTs
	if start < qStart {
		start = qStart
	}
	if end > qEnd {
		end = qEnd
	}
	if start >= end {
		return SegmentGroup{}
	}

	type key struct{ sid, eid string }
	merged := map[key]SegmentInfo{}

	for _, s := range g.Segments {
		k := key{s.SegmentID, s.ExprID}
		if cur, ok := merged[k]; ok {
			// widen to batch bounds
			cur.StartTs = start
			cur.EndTs = end
			merged[k] = cur
		} else {
			ss := s
			ss.StartTs = start
			ss.EndTs = end
			merged[k] = ss
		}
	}

	out := make([]SegmentInfo, 0, len(merged))
	for _, s := range merged {
		out = append(out, s)
	}
	return SegmentGroup{
		StartTs:  start,
		EndTs:    end,
		Segments: out,
	}
}

func TargetSize(totalSegments, workers int) int {
	if workers <= 0 {
		return totalSegments
	}
	return int(math.Ceil(float64(totalSegments) / float64(workers)))
}
