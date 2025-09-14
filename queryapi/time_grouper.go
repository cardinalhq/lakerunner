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
	"log/slog"
	"math"
	"sort"
	"time"

	"github.com/google/uuid"
)

type SegmentInfo struct {
	DateInt        int       `json:"dateInt"`
	Hour           string    `json:"hour"`
	SegmentID      int64     `json:"segmentId"`
	StartTs        int64     `json:"startTs"` // millis
	EndTs          int64     `json:"endTs"`   // millis; treat as inclusive for coalescing
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

// ComputeReplayBatchesWithWorkers: pick a target size from total/worker count.
func ComputeReplayBatchesWithWorkers(
	segments []SegmentInfo,
	step time.Duration, // used for epsilon AND step alignment
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

// ComputeReplayBatches builds coverage bands from the data, merges near-touching
// segments with a small epsilon to avoid micro-gaps, then splits each band by
// count to form groups. Each group's window is derived from its segments and
// clamped to [queryStart, queryEnd]. **Key fix**: we then step-align group edges
// so no [T, T+step) bucket straddles two groups (prevents multi-worker "trickle").
// Within a band we optionally tile edges after snapping to remove any tiny overlaps.
func ComputeReplayBatches(
	segments []SegmentInfo,
	step time.Duration, // used for epsilon + alignment
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

	// Sort by start asc, then end asc.
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].StartTs == segments[j].StartTs {
			return segments[i].EndTs < segments[j].EndTs
		}
		return segments[i].StartTs < segments[j].StartTs
	})

	// Choose epsilon to close tiny gaps.
	stepMs := step.Milliseconds()
	if stepMs <= 0 {
		stepMs = 1000 // default 1s
	}
	closeGapEpsMs := stepMs / 8
	if closeGapEpsMs < 1 {
		closeGapEpsMs = 1
	}
	if closeGapEpsMs > 2000 {
		closeGapEpsMs = 2000
	}

	// 1) Coalesce into contiguous bands.
	bands := coalesceBands(segments, closeGapEpsMs)

	// 2) Reverse band order if needed (for final emission order).
	if reverseSort {
		for i, j := 0, len(bands)-1; i < j; i, j = i+1, j-1 {
			bands[i], bands[j] = bands[j], bands[i]
		}
	}

	// 3) For each band: split into count-sized chunks in traversal order,
	//    finalize each chunk into a group, then **step-align** per-band boundaries.
	var out []SegmentGroup
	for _, band := range bands {
		n := len(band)
		if n == 0 {
			continue
		}

		// Build pre-align groups for this band.
		var bandGroups []SegmentGroup
		if n <= targetSize {
			if g, ok := finalizeGroup(band, queryStartTs, queryEndTs); ok {
				bandGroups = append(bandGroups, g)
			}
		} else if !reverseSort {
			// Forward: oldest→newest
			for i := 0; i < n; i += targetSize {
				j := i + targetSize
				if j > n {
					j = n
				}
				if g, ok := finalizeGroup(band[i:j], queryStartTs, queryEndTs); ok {
					bandGroups = append(bandGroups, g)
				}
			}
		} else {
			// Reverse: newest→oldest
			for i := n; i > 0; i -= targetSize {
				j := i - targetSize
				if j < 0 {
					j = 0
				}
				if g, ok := finalizeGroup(band[j:i], queryStartTs, queryEndTs); ok {
					bandGroups = append(bandGroups, g)
				}
			}
		}

		if len(bandGroups) == 0 {
			continue
		}

		// Ensure band groups are in traversal order we expect.
		if !reverseSort {
			sort.Slice(bandGroups, func(i, j int) bool {
				if bandGroups[i].StartTs == bandGroups[j].StartTs {
					return bandGroups[i].EndTs < bandGroups[j].EndTs
				}
				return bandGroups[i].StartTs < bandGroups[j].StartTs
			})
		} else {
			sort.Slice(bandGroups, func(i, j int) bool {
				if bandGroups[i].StartTs == bandGroups[j].StartTs {
					return bandGroups[i].EndTs > bandGroups[j].EndTs
				}
				return bandGroups[i].StartTs > bandGroups[j].StartTs
			})
		}

		// **Key fix**: step-align boundaries within this band so a [T,T+step) bucket
		// can't be split across adjacent groups.
		bandGroups = snapGroupsToStep(bandGroups, step, queryStartTs, queryEndTs, reverseSort)

		// Optional: after snapping, tile to ensure zero-overlap/zero-gap within band.
		bandGroups = tileBandEdges(bandGroups, reverseSort)

		out = append(out, bandGroups...)
	}

	// Debug log resulting groups.
	for _, group := range out {
		slog.Info("Segment Group",
			slog.Int("numSegments", len(group.Segments)),
			slog.String("startTime", time.UnixMilli(group.StartTs).UTC().Format(time.RFC3339)),
			slog.String("endTime", time.UnixMilli(group.EndTs).UTC().Format(time.RFC3339)))
	}

	return out
}

// coalesceBands merges a sorted slice of segments into contiguous "bands" where
// each band’s coverage is continuous within epsilon. Returns a slice of bands;
// each band is a []SegmentInfo preserving order.
func coalesceBands(segs []SegmentInfo, epsMs int64) [][]SegmentInfo {
	if len(segs) == 0 {
		return nil
	}
	var bands [][]SegmentInfo

	cur := []SegmentInfo{segs[0]}
	curEnd := segs[0].EndTs

	for i := 1; i < len(segs); i++ {
		s := segs[i]
		// Merge if overlapping or touching within epsilon.
		if s.StartTs <= curEnd+epsMs {
			cur = append(cur, s)
			if s.EndTs > curEnd {
				curEnd = s.EndTs
			}
		} else {
			bands = append(bands, cur)
			cur = []SegmentInfo{s}
			curEnd = s.EndTs
		}
	}
	bands = append(bands, cur)
	return bands
}

// finalizeGroup merges by (SegmentID, ExprID) inside a chunk, computes envelope,
// clamps to [qStart,qEnd], drops empty results, and returns a SegmentGroup.
func finalizeGroup(chunk []SegmentInfo, qStart, qEnd int64) (SegmentGroup, bool) {
	if len(chunk) == 0 {
		return SegmentGroup{}, false
	}

	// Merge per (SegmentID, ExprID).
	type key struct {
		id   int64
		expr string
	}
	merged := make(map[key]SegmentInfo, len(chunk))
	for _, s := range chunk {
		k := key{s.SegmentID, s.ExprID}
		if m, ok := merged[k]; ok {
			if s.StartTs < m.StartTs {
				m.StartTs = s.StartTs
			}
			if s.EndTs > m.EndTs {
				m.EndTs = s.EndTs
			}
			merged[k] = m
		} else {
			merged[k] = s
		}
	}

	// Compute envelope & clamp each segment.
	var gs, ge int64
	first := true
	outSegs := make([]SegmentInfo, 0, len(merged))
	for _, v := range merged {
		if v.StartTs < qStart {
			v.StartTs = qStart
		}
		if v.EndTs > qEnd {
			v.EndTs = qEnd
		}
		// Keep only if still non-empty after clamp. Use Start<End (half-open group window later).
		if v.StartTs < v.EndTs {
			outSegs = append(outSegs, v)
			if first {
				gs, ge = v.StartTs, v.EndTs
				first = false
			} else {
				if v.StartTs < gs {
					gs = v.StartTs
				}
				if v.EndTs > ge {
					ge = v.EndTs
				}
			}
		}
	}
	if len(outSegs) == 0 || gs >= ge {
		return SegmentGroup{}, false
	}

	return SegmentGroup{
		StartTs:  gs,
		EndTs:    ge,
		Segments: outSegs,
	}, true
}

// snapGroupsToStep ensures per-band groups are contiguous **and** step-aligned,
// so each [T,T+step) bucket belongs to exactly one group. We align forward bands
// by snapping ENDs up to the next step; reverse bands by snapping STARTs down.
// We also clamp to the query window to avoid overshoot.
func snapGroupsToStep(groups []SegmentGroup, step time.Duration, qStart, qEnd int64, reverse bool) []SegmentGroup {
	if len(groups) == 0 {
		return groups
	}
	stepMs := step.Milliseconds()
	if stepMs <= 0 {
		return groups
	}

	snapUp := func(ms int64) int64 {
		return ((ms + stepMs - 1) / stepMs) * stepMs
	}
	snapDown := func(ms int64) int64 {
		return (ms / stepMs) * stepMs
	}

	clamp := func(g SegmentGroup, s, e int64) (SegmentGroup, bool) {
		if e <= s {
			return SegmentGroup{}, false
		}
		segs := make([]SegmentInfo, 0, len(g.Segments))
		for _, x := range g.Segments {
			if x.StartTs < s {
				x.StartTs = s
			}
			if x.EndTs > e {
				x.EndTs = e
			}
			if x.StartTs < x.EndTs {
				segs = append(segs, x)
			}
		}
		if len(segs) == 0 {
			return SegmentGroup{}, false
		}
		return SegmentGroup{StartTs: s, EndTs: e, Segments: segs}, true
	}

	out := make([]SegmentGroup, 0, len(groups))
	if !reverse {
		// Forward traversal: keep the first group's start; snap its END up to the next step.
		firstEnd := snapUp(groups[0].EndTs)
		if firstEnd > qEnd {
			firstEnd = qEnd
		}
		if g, ok := clamp(groups[0], groups[0].StartTs, firstEnd); ok {
			out = append(out, g)
		}
		edge := firstEnd
		for i := 1; i < len(groups); i++ {
			end := snapUp(groups[i].EndTs)
			if end > qEnd {
				end = qEnd
			}
			if end <= edge {
				continue
			}
			if g, ok := clamp(groups[i], edge, end); ok {
				out = append(out, g)
				edge = end
			}
		}
	} else {
		// Reverse traversal: keep the first group's end; snap its START down to the prev step.
		firstStart := snapDown(groups[0].StartTs)
		if firstStart < qStart {
			firstStart = qStart
		}
		if g, ok := clamp(groups[0], firstStart, groups[0].EndTs); ok {
			out = append(out, g)
		}
		edge := firstStart
		for i := 1; i < len(groups); i++ {
			start := snapDown(groups[i].StartTs)
			if start < qStart {
				start = qStart
			}
			if start >= edge {
				continue
			}
			if g, ok := clamp(groups[i], start, edge); ok {
				out = append(out, g)
				edge = start
			}
		}
	}
	return out
}

// tileBandEdges enforces zero-overlap, zero-gap windows **within one band**,
// preserving real gaps between bands. Works for forward (oldest→newest)
// and reverse (newest→oldest) traversal.
func tileBandEdges(groups []SegmentGroup, reverse bool) []SegmentGroup {
	if len(groups) == 0 {
		return groups
	}
	out := make([]SegmentGroup, 0, len(groups))

	// We iterate in the provided order. For i==0 we emit as-is.
	// We maintain an "edge" that subsequent groups must align to.
	var edge int64
	if !reverse {
		edge = groups[0].StartTs // forward: first group's start
	} else {
		edge = groups[0].StartTs // reverse: we want the next group's END to hit this group's START
	}

	for idx, g := range groups {
		gs, ge := g.StartTs, g.EndTs

		if idx == 0 {
			// Emit first as-is; set edge for subsequent groups.
			out = append(out, g)
			if !reverse {
				edge = ge // next group's start must be this end
			} else {
				edge = gs // next group's end must be this start
			}
			continue
		}

		if !reverse {
			// Forward: force start to current edge.
			if ge <= edge {
				continue // fully before/at edge after tiling → skip
			}
			gs = edge
		} else {
			// Reverse: force end to current edge.
			if gs >= edge {
				continue // fully after/at edge after tiling → skip
			}
			ge = edge
		}

		// Trim member segments to the tiled window.
		segs := make([]SegmentInfo, 0, len(g.Segments))
		for _, s := range g.Segments {
			if s.StartTs < gs {
				s.StartTs = gs
			}
			if s.EndTs > ge {
				s.EndTs = ge
			}
			if s.StartTs < s.EndTs {
				segs = append(segs, s)
			}
		}
		if len(segs) == 0 || gs >= ge {
			continue
		}

		out = append(out, SegmentGroup{
			StartTs:  gs,
			EndTs:    ge,
			Segments: segs,
		})

		// Advance band-local tiling edge.
		if !reverse {
			edge = ge
		} else {
			edge = gs
		}
	}
	return out
}

func TargetSize(totalSegments, workers int) int {
	if workers <= 0 {
		return totalSegments
	}
	return int(math.Ceil(float64(totalSegments) / float64(workers)))
}
