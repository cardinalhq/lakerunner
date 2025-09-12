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
	step time.Duration, // used only to pick a sensible epsilon; no step snapping
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

// ComputeReplayBatches builds coverage bands from the data (no step alignment),
// merges near-touching segments with a small epsilon to avoid micro-gaps,
// then splits each band by count to form groups. Each group's Start/End is
// derived from its contained segments and finally clamped to [queryStart, queryEnd].
// Within a group, segments are merged by (SegmentID, ExprID) before clamping.
func ComputeReplayBatches(
	segments []SegmentInfo,
	step time.Duration, // only to derive epsilon; not used for alignment
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

	// Helpful input logging
	for _, s := range segments {
		slog.Info("Input Segment",
			slog.Int64("segmentID", s.SegmentID),
			slog.String("startTime", time.UnixMilli(s.StartTs).UTC().Format(time.RFC3339)),
			slog.String("endTime", time.UnixMilli(s.EndTs).UTC().Format(time.RFC3339)))
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

	// 2) Reverse band order if needed.
	if reverseSort {
		for i, j := 0, len(bands)-1; i < j; i, j = i+1, j-1 {
			bands[i], bands[j] = bands[j], bands[i]
		}
	}

	// 3) Chunk each band by count. IMPORTANT: in reverse mode we emit latest chunk first.
	var out []SegmentGroup
	for _, band := range bands {
		n := len(band)
		if n == 0 {
			continue
		}

		if n <= targetSize {
			if g, ok := finalizeGroup(band, queryStartTs, queryEndTs); ok {
				out = append(out, g)
			}
			continue
		}

		if !reverseSort {
			// Forward: oldest→newest
			for i := 0; i < n; i += targetSize {
				j := i + targetSize
				if j > n {
					j = n
				}
				if g, ok := finalizeGroup(band[i:j], queryStartTs, queryEndTs); ok {
					out = append(out, g)
				}
			}
		} else {
			// Reverse: newest→oldest (this fixes your failing test)
			for i := n; i > 0; i -= targetSize {
				j := i - targetSize
				if j < 0 {
					j = 0
				}
				if g, ok := finalizeGroup(band[j:i], queryStartTs, queryEndTs); ok {
					out = append(out, g)
				}
			}
		}
	}

	// 4) Tile edges to avoid overlap/duplication across groups without inventing gaps.
	out = tileGroupEdges(out, queryStartTs, queryEndTs, reverseSort)

	// Debug log groups.
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

// tileGroupEdges enforces non-overlapping consecutive groups by trimming the edge
// against the previous emission edge, preserving order. This avoids duplicates
// without fabricating step-aligned gaps.
func tileGroupEdges(groups []SegmentGroup, qStart, qEnd int64, reverse bool) []SegmentGroup {
	if len(groups) == 0 {
		return groups
	}
	out := make([]SegmentGroup, 0, len(groups))

	edge := qStart
	if reverse {
		edge = qEnd
	}

	for _, g := range groups {
		gs, ge := g.StartTs, g.EndTs
		if reverse {
			// Trim forward edge (end) to not cross the previous edge in reverse walk.
			if ge > edge {
				ge = edge
			}
		} else {
			// Trim backward edge (start) to not cross the previous edge in forward walk.
			if gs < edge {
				gs = edge
			}
		}
		if gs >= ge {
			continue
		}

		// Trim member segments to the tiled window as well.
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
		if len(segs) == 0 {
			continue
		}

		out = append(out, SegmentGroup{
			StartTs:  gs,
			EndTs:    ge,
			Segments: segs,
		})

		// Advance tiling edge: half-open semantics across groups.
		if reverse {
			edge = gs
		} else {
			edge = ge
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
