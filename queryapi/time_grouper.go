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
// Finally, **tiling is applied per-band only** (forward or reverse) to avoid
// overlap/duplication across groups **without inventing cross-band gaps**.
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

	// Helpful input logging (safe in tests/dev)
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

	// 2) Reverse band order if needed (for final emission order).
	if reverseSort {
		for i, j := 0, len(bands)-1; i < j; i, j = i+1, j-1 {
			bands[i], bands[j] = bands[j], bands[i]
		}
	}

	// 3) For each band: split into count-sized chunks in traversal order,
	//    finalize each chunk into a group, then TILE **within that band** only.
	var out []SegmentGroup
	for _, band := range bands {
		n := len(band)
		if n == 0 {
			continue
		}

		// Build pre-tiling groups for this band.
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

		// **Key fix**: tile within this band only (zero-overlap, zero-gap within band).
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
