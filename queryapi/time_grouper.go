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
	DateInt          int       `json:"dateInt"`
	Hour             string    `json:"hour"`
	SegmentID        int64     `json:"segmentId"`
	StartTs          int64     `json:"startTs"`
	EndTs            int64     `json:"endTs"`
	EffectiveStartTs int64     `json:"effectiveStartTs"`
	EffectiveEndTs   int64     `json:"effectiveEndTs"`
	ExprID           string    `json:"exprId"`
	OrganizationID   uuid.UUID `json:"organizationID"`
	InstanceNum      int16     `json:"instanceNum"`
	Frequency        int64     `json:"frequency"`
}

type SegmentGroup struct {
	StartTs  int64
	EndTs    int64
	Segments []SegmentInfo
}

// Helpers to safely read effective windows (fallback to storage-time if unset).
func effStart(s SegmentInfo) int64 {
	if s.EffectiveStartTs != 0 {
		return s.EffectiveStartTs
	}
	return s.StartTs
}
func effEnd(s SegmentInfo) int64 {
	if s.EffectiveEndTs != 0 {
		return s.EffectiveEndTs
	}
	return s.EndTs
}

// ComputeReplayBatchesWithWorkers pick a target size from total/worker count.
func ComputeReplayBatchesWithWorkers(
	segments []SegmentInfo,
	step time.Duration, // used for epsilon AND step alignment
	queryStartTs, queryEndTs int64, // in effective/evaluation time
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

// ComputeReplayBatches builds coverage bands from the data (in **effective time**),
// merges near-touching segments with a small epsilon to avoid micro-gaps, then
// splits each band by count to form groups. Each group's window is derived from
// its segments and clamped to [queryStart, queryEnd]. Then we step-align group
// edges so no [T,T+step) bucket straddles two groups. Finally we tile edges
// to remove any tiny overlaps within a band.
func ComputeReplayBatches(
	segments []SegmentInfo,
	step time.Duration, // used for epsilon + alignment
	queryStartTs, queryEndTs int64, // effective/evaluation window
	targetSize int,
	reverseSort bool,
) []SegmentGroup {
	if len(segments) == 0 {
		return nil
	}
	if targetSize <= 0 {
		targetSize = len(segments)
	}

	// Sort by effective start asc, then effective end asc.
	sort.Slice(segments, func(i, j int) bool {
		esi, esj := effStart(segments[i]), effStart(segments[j])
		if esi == esj {
			return effEnd(segments[i]) < effEnd(segments[j])
		}
		return esi < esj
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

	// 1) Coalesce into contiguous bands (effective time).
	bands := coalesceBandsEffective(segments, closeGapEpsMs)

	// 2) Reverse band order if needed (for final emission order).
	if reverseSort {
		for i, j := 0, len(bands)-1; i < j; i, j = i+1, j-1 {
			bands[i], bands[j] = bands[j], bands[i]
		}
	}

	// 3) For each band: split into count-sized chunks in traversal order,
	//    finalize each chunk into a group, step-align, then tile edges.
	var out []SegmentGroup
	for _, band := range bands {
		n := len(band)
		if n == 0 {
			continue
		}

		// Build pre-align groups for this band.
		var bandGroups []SegmentGroup
		if n <= targetSize {
			if g, ok := finalizeGroupEffective(band, queryStartTs, queryEndTs); ok {
				bandGroups = append(bandGroups, g)
			}
		} else if !reverseSort {
			// Forward: oldest→newest
			for i := 0; i < n; i += targetSize {
				j := i + targetSize
				if j > n {
					j = n
				}
				if g, ok := finalizeGroupEffective(band[i:j], queryStartTs, queryEndTs); ok {
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
				if g, ok := finalizeGroupEffective(band[j:i], queryStartTs, queryEndTs); ok {
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

		// Step-align within this band so a [T,T+step) bucket can't be split.
		bandGroups = snapGroupsToStepEffective(bandGroups, step, queryStartTs, queryEndTs, reverseSort)

		// After snapping, tile to ensure zero-overlap/zero-gap within band.
		bandGroups = tileBandEdgesEffective(bandGroups, reverseSort)

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

// coalesceBandsEffective merges a sorted slice of segments into contiguous "bands"
// using **effective** windows. Treats ends as touching within epsMs.
func coalesceBandsEffective(segs []SegmentInfo, epsMs int64) [][]SegmentInfo {
	if len(segs) == 0 {
		return nil
	}
	var bands [][]SegmentInfo

	cur := []SegmentInfo{segs[0]}
	curEnd := effEnd(segs[0])

	for i := 1; i < len(segs); i++ {
		s := segs[i]
		es, ee := effStart(s), effEnd(s)
		// Merge if overlapping or touching within epsilon.
		if es <= curEnd+epsMs {
			cur = append(cur, s)
			if ee > curEnd {
				curEnd = ee
			}
		} else {
			bands = append(bands, cur)
			cur = []SegmentInfo{s}
			curEnd = ee
		}
	}
	bands = append(bands, cur)
	return bands
}

// finalizeGroupEffective merges by (SegmentID, ExprID) inside a chunk, computes
// the **effective** envelope, clamps to [qStart,qEnd], drops empty results,
// and returns a SegmentGroup whose Start/End are **effective**.
func finalizeGroupEffective(chunk []SegmentInfo, qStart, qEnd int64) (SegmentGroup, bool) {
	if len(chunk) == 0 {
		return SegmentGroup{}, false
	}

	// Merge per (SegmentID, ExprID) over **effective** windows.
	type key struct {
		id   int64
		expr string
	}
	merged := make(map[key]SegmentInfo, len(chunk))
	for _, s := range chunk {
		k := key{s.SegmentID, s.ExprID}
		sES, sEE := effStart(s), effEnd(s)
		if m, ok := merged[k]; ok {
			mES, mEE := effStart(m), effEnd(m)
			if sES < mES {
				m.EffectiveStartTs = sES
			}
			if sEE > mEE {
				m.EffectiveEndTs = sEE
			}
			merged[k] = m
		} else {
			// Ensure Effective* are populated for downstream code.
			s.EffectiveStartTs, s.EffectiveEndTs = sES, sEE
			merged[k] = s
		}
	}

	// Compute envelope & clamp each segment to [qStart,qEnd] in **effective** time.
	var gs, ge int64
	first := true
	outSegs := make([]SegmentInfo, 0, len(merged))
	for _, v := range merged {
		es, ee := effStart(v), effEnd(v)
		if es < qStart {
			es = qStart
		}
		if ee > qEnd {
			ee = qEnd
		}
		if es < ee {
			v.EffectiveStartTs, v.EffectiveEndTs = es, ee
			outSegs = append(outSegs, v)
			if first {
				gs, ge = es, ee
				first = false
			} else {
				if es < gs {
					gs = es
				}
				if ee > ge {
					ge = ee
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

// snapGroupsToStepEffective ensures per-band groups are contiguous **and** step-aligned
// in **effective** time, so each [T,T+step) bucket belongs to exactly one group.
func snapGroupsToStepEffective(groups []SegmentGroup, step time.Duration, qStart, qEnd int64, reverse bool) []SegmentGroup {
	if len(groups) == 0 {
		return groups
	}
	stepMs := step.Milliseconds()
	if stepMs <= 0 {
		return groups
	}

	snapUp := func(ms int64) int64 { return ((ms + stepMs - 1) / stepMs) * stepMs }
	snapDown := func(ms int64) int64 { return (ms / stepMs) * stepMs }
	clampSeg := func(g SegmentGroup, s, e int64) (SegmentGroup, bool) {
		if e <= s {
			return SegmentGroup{}, false
		}
		segs := make([]SegmentInfo, 0, len(g.Segments))
		for _, x := range g.Segments {
			es, ee := effStart(x), effEnd(x)
			if es < s {
				es = s
			}
			if ee > e {
				ee = e
			}
			if es < ee {
				x.EffectiveStartTs, x.EffectiveEndTs = es, ee
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
		// Forward traversal: keep the first group's start; snap END up to the next step.
		firstEnd := snapUp(groups[0].EndTs)
		if firstEnd > qEnd {
			firstEnd = qEnd
		}
		if g, ok := clampSeg(groups[0], groups[0].StartTs, firstEnd); ok {
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
			if g, ok := clampSeg(groups[i], edge, end); ok {
				out = append(out, g)
				edge = end
			}
		}
	} else {
		// Reverse traversal: keep the first group's end; snap START down to the prev step.
		firstStart := snapDown(groups[0].StartTs)
		if firstStart < qStart {
			firstStart = qStart
		}
		if g, ok := clampSeg(groups[0], firstStart, groups[0].EndTs); ok {
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
			if g, ok := clampSeg(groups[i], start, edge); ok {
				out = append(out, g)
				edge = start
			}
		}
	}
	return out
}

// tileBandEdgesEffective enforces zero-overlap, zero-gap windows within one band,
// in **effective** time, preserving real gaps between bands.
func tileBandEdgesEffective(groups []SegmentGroup, reverse bool) []SegmentGroup {
	if len(groups) == 0 {
		return groups
	}
	out := make([]SegmentGroup, 0, len(groups))

	// We iterate in the provided order. For i==0 we emit as-is.
	// We maintain an "edge" that subsequent groups must align to.
	var edge int64
	if !reverse {
		edge = groups[0].StartTs
	} else {
		edge = groups[0].StartTs // next group's END must land on this START
	}

	for idx, g := range groups {
		gs, ge := g.StartTs, g.EndTs

		if idx == 0 {
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

		// Trim member segments to the tiled window in **effective** time.
		segs := make([]SegmentInfo, 0, len(g.Segments))
		for _, s := range g.Segments {
			es, ee := effStart(s), effEnd(s)
			if es < gs {
				es = gs
			}
			if ee > ge {
				ee = ge
			}
			if es < ee {
				s.EffectiveStartTs, s.EffectiveEndTs = es, ee
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
