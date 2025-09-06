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

package queryapi

import (
	"math"
	"sort"
	"time"

	"github.com/google/uuid"
)

type SegmentInfo struct {
	DateInt        int       `json:"dateInt"`
	Hour           string    `json:"hour"`
	SegmentID      int64     `json:"segmentId"`
	StartTs        int64     `json:"startTs"`
	EndTs          int64     `json:"endTs"`
	ExprID         string    `json:"exprId"`
	OrganizationID uuid.UUID `json:"organizationID"`
	InstanceNum    int16     `json:"instanceNum"`
	Frequency      int64     `json:"frequency"`
}

type SegmentGroup struct {
	StartTs  int64
	EndTs    int64
	Segments []SegmentInfo
}

// ComputeReplayBatchesWithWorkers public entrypoint. Computes a per-group target size
// from total #segments and worker count (capped), then delegates.
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

// ComputeReplayBatches builds aligned time windows, orders them, then packs groups
// purely by segment-count (ignoring time gaps) to match the Scala behavior.
// On flush, it merges per (SegmentID, ExprID), clamps to [queryStartTs, queryEndTs],
// and *seals* each segment to the group window.
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

	stepMs := step.Milliseconds()
	if stepMs <= 0 {
		stepMs = 1
	}

	// 1) Build aligned windows by exact (start,end) buckets.
	windows := buildWindows(segments, stepMs)

	// 2) Order the windows by EndTs (Scala sorts by endTs; ties by startTs).
	sort.Slice(windows, func(i, j int) bool {
		if windows[i].EndTs == windows[j].EndTs {
			return windows[i].StartTs < windows[j].StartTs
		}
		return windows[i].EndTs < windows[j].EndTs
	})
	if reverseSort && len(windows) > 1 {
		for i, j := 0, len(windows)-1; i < j; i, j = i+1, j-1 {
			windows[i], windows[j] = windows[j], windows[i]
		}
	}

	// 3) Pack ignoring gaps; flush when we’ve accumulated >= targetSize segments.
	out := packByCount(windows, targetSize, queryStartTs, queryEndTs)
	return out
}

// ---- internals ----

func alignDown(ts, stepMs int64) int64 {
	return ts - (ts % stepMs)
}
func alignUp(ts, stepMs int64) int64 {
	if r := ts % stepMs; r == 0 {
		return ts
	}
	return ts + (stepMs - (ts % stepMs))
}

// buildWindows aligns each segment to step boundaries and buckets by (alignedStart, alignedEnd).
// We *don’t* clamp to the query here; clamping happens at flush to mimic Scala’s merge behavior.
func buildWindows(segs []SegmentInfo, stepMs int64) []SegmentGroup {
	type key struct{ s, e int64 }
	buckets := make(map[key][]SegmentInfo, len(segs))

	for _, s := range segs {
		as := alignDown(s.StartTs, stepMs)
		ae := alignUp(s.EndTs, stepMs)
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

// packByCount merges windows by accumulating segment count until >= minGroupSize.
// On flush, it merges parts by (SegmentID, ExprID), computes the group window as
// minStart..maxEnd across the parts, clamps to [qStart,qEnd], and seals segments
// to that group window.
func packByCount(wins []SegmentGroup, minGroupSize int, qStart, qEnd int64) []SegmentGroup {
	if len(wins) == 0 {
		return nil
	}
	if minGroupSize <= 0 {
		minGroupSize = len(wins)
	}

	var out []SegmentGroup
	var parts []SegmentInfo
	count := 0

	flush := func() {
		if len(parts) == 0 {
			return
		}

		// Compute window across all parts.
		gs, ge := parts[0].StartTs, parts[0].EndTs
		for _, p := range parts[1:] {
			if p.StartTs < gs {
				gs = p.StartTs
			}
			if p.EndTs > ge {
				ge = p.EndTs
			}
		}
		// Clamp to query.
		if gs < qStart {
			gs = qStart
		}
		if ge > qEnd {
			ge = qEnd
		}
		if gs >= ge {
			parts = parts[:0]
			count = 0
			return
		}

		// Merge per (SegmentID, ExprID) by widening to the group window.
		type key struct {
			id   int64
			expr string
		}
		merged := make(map[key]SegmentInfo, len(parts))
		for _, s := range parts {
			k := key{s.SegmentID, s.ExprID}
			if _, ok := merged[k]; !ok {
				ss := s
				ss.StartTs = gs
				ss.EndTs = ge
				merged[k] = ss
			}
			// Already sealed to gs..ge; nothing else to do.
		}

		segs := make([]SegmentInfo, 0, len(merged))
		for _, v := range merged {
			segs = append(segs, v)
		}
		out = append(out, SegmentGroup{
			StartTs:  gs,
			EndTs:    ge,
			Segments: segs,
		})
		parts = parts[:0]
		count = 0
	}

	for _, w := range wins {
		parts = append(parts, w.Segments...)
		count += len(w.Segments)
		if count >= minGroupSize {
			flush()
		}
	}
	flush()

	return out
}

func TargetSize(totalSegments, workers int) int {
	if workers <= 0 {
		return totalSegments
	}
	// Keep previous cap behavior (max ~30 per batch) to avoid huge groups.
	return int(math.Min(30, math.Ceil(float64(totalSegments)/float64(workers))))
}
