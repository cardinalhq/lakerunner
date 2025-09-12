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
	// let's log the segment groups for debugging purposes
	for _, group := range out {
		startStr := time.UnixMilli(group.StartTs).UTC().Format(time.RFC3339)
		endStr := time.UnixMilli(group.EndTs).UTC().Format(time.RFC3339)
		slog.Info("Segment Group", slog.Int("numSegments", len(group.Segments)),
			slog.String("startTime", startStr),
			slog.String("endTime", endStr))
	}
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

// packByCount (no gaps):
//   - Accumulates windows until either:
//     a) adding the next window would introduce a gap (strictly w.StartTs > currGE), or
//     b) we've reached >= minGroupSize segments.
//   - A "gap" means a strict hole in time; overlaps or touching edges are fine.
//   - On flush: merges parts per (SegmentID, ExprID), computes the group window from
//     the accumulated windows, clamps to [qStart,qEnd], and clamps each merged segment
//     to the same final window. Since groups are contiguous, sealing to [gs,ge] is safe
//     and does not introduce artificial holes.
func packByCount(wins []SegmentGroup, minGroupSize int, qStart, qEnd int64) []SegmentGroup {
	if len(wins) == 0 {
		return nil
	}
	if minGroupSize <= 0 {
		minGroupSize = len(wins)
	}

	var out []SegmentGroup

	var parts []SegmentInfo // accumulated segments in current group
	var currGS, currGE int64
	count := 0
	haveGroup := false

	flush := func() {
		if !haveGroup || len(parts) == 0 {
			// nothing to flush
			parts = parts[:0]
			count = 0
			haveGroup = false
			return
		}

		// Clamp the contiguous group window to the query range.
		gs, ge := currGS, currGE
		if gs < qStart {
			gs = qStart
		}
		if ge > qEnd {
			ge = qEnd
		}
		if gs >= ge {
			// No overlap with query window.
			parts = parts[:0]
			count = 0
			haveGroup = false
			return
		}

		// Merge per (SegmentID, ExprID). Because this group is contiguous,
		// sealing to [gs,ge] does not create hidden gaps.
		type key struct {
			id   int64
			expr string
		}
		merged := make(map[key]SegmentInfo, len(parts))
		for _, s := range parts {
			k := key{s.SegmentID, s.ExprID}
			if m, ok := merged[k]; ok {
				// widen to cover this part
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

		// Seal each merged segment to the (contiguous) group window, then clamp to [gs,ge].
		segs := make([]SegmentInfo, 0, len(merged))
		for _, v := range merged {
			if v.StartTs < gs {
				v.StartTs = gs
			}
			if v.EndTs > ge {
				v.EndTs = ge
			}
			// skip empty post-clamp
			if v.StartTs < v.EndTs {
				segs = append(segs, v)
			}
		}

		if len(segs) > 0 {
			out = append(out, SegmentGroup{
				StartTs:  gs,
				EndTs:    ge,
				Segments: segs,
			})
		}

		// reset
		parts = parts[:0]
		count = 0
		haveGroup = false
	}

	addWindow := func(w SegmentGroup) {
		// Initialize current group window
		if !haveGroup {
			currGS, currGE = w.StartTs, w.EndTs
			haveGroup = true
		} else {
			// Extend current contiguous window (overlap or touch is fine).
			if w.EndTs > currGE {
				currGE = w.EndTs
			}
			if w.StartTs < currGS {
				currGS = w.StartTs
			}
		}
		parts = append(parts, w.Segments...)
		count += len(w.Segments)
	}

	for i, w := range wins {
		if !haveGroup {
			addWindow(w)
		} else {
			// If adding w would introduce a *gap*, flush first.
			// Gap = next start strictly greater than current end.
			if w.StartTs > currGE {
				flush()
				addWindow(w)
			} else {
				// Overlap or contiguous (w.StartTs <= currGE): safe to extend.
				addWindow(w)
			}
		}

		// If we've reached the target size, flush early — but never with gaps.
		// (We only ever extend contiguously, so this is safe.)
		if count >= minGroupSize {
			flush()
			// If there are more windows, the next iteration will start a new group.
		}

		// Edge case: last window; fall through to final flush after loop.
		_ = i
	}

	// Flush any remainder.
	flush()

	return out
}

func TargetSize(totalSegments, workers int) int {
	if workers <= 0 {
		return totalSegments
	}
	return int(math.Ceil(float64(totalSegments) / float64(workers)))
}
