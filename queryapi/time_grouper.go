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

package queryapi

import (
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
	AggFields        []string  `json:"aggFields,omitempty"`
}

// SegmentKey is a comparable key for deduplicating segments in maps.
type SegmentKey struct {
	DateInt        int
	SegmentID      int64
	InstanceNum    int16
	OrganizationID uuid.UUID
}

// Key returns the SegmentKey for this SegmentInfo.
func (s SegmentInfo) Key() SegmentKey {
	return SegmentKey{
		DateInt:        s.DateInt,
		SegmentID:      s.SegmentID,
		InstanceNum:    s.InstanceNum,
		OrganizationID: s.OrganizationID,
	}
}

type SegmentGroup struct {
	StartTs  int64
	EndTs    int64
	Segments []SegmentInfo
}

// DefaultMaxSegmentsPerWorkerPerWave is the default hard cap per worker per wave.
const DefaultMaxSegmentsPerWorkerPerWave = 20

// maxSegmentsPerWorkerPerWave is the configured hard cap we want per worker per wave.
// A single wave (SegmentGroup) will be sized so that, assuming roughly
// even distribution across workers, no worker needs to handle more than
// this many segments in that wave.
var maxSegmentsPerWorkerPerWave = DefaultMaxSegmentsPerWorkerPerWave

// SetMaxSegmentsPerWorkerPerWave configures the max segments per worker per wave.
// If value is <= 0, the default is used.
func SetMaxSegmentsPerWorkerPerWave(value int) {
	if value <= 0 {
		maxSegmentsPerWorkerPerWave = DefaultMaxSegmentsPerWorkerPerWave
	} else {
		maxSegmentsPerWorkerPerWave = value
	}
}

// GetMaxSegmentsPerWorkerPerWave returns the current max segments per worker per wave setting.
func GetMaxSegmentsPerWorkerPerWave() int {
	return maxSegmentsPerWorkerPerWave
}

// ---- helpers for effective timing ----

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

// ComputeReplayBatchesWithWorkers public entrypoint. Computes a per-group target size
// from total #segments and worker count, then delegates.
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

	// 1) Build aligned windows by exact (effectiveStart,effectiveEnd) buckets.
	windows := buildWindowsEffective(segments, stepMs)

	// 2) Order the windows by StartTs; ties by EndTs (ascending).
	sort.Slice(windows, func(i, j int) bool {
		if windows[i].StartTs == windows[j].StartTs {
			return windows[i].EndTs < windows[j].EndTs
		}
		return windows[i].StartTs < windows[j].StartTs
	})
	// IMPORTANT: do NOT reverse windows here

	// 3) Pack forward; then reverse final batches if requested.
	out := packByCountEffective(windows, targetSize, queryStartTs, queryEndTs, stepMs)

	if reverseSort && len(out) > 1 {
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}
	return out
}

// ---- internals ----

func alignDown(ts, stepMs int64) int64 {
	return ts - (ts % stepMs)
}

func alignUp(ts, stepMs int64) int64 {
	r := ts % stepMs
	if r == 0 {
		return ts
	}
	return ts + (stepMs - r)
}

// buildWindowsEffective aligns each segment's **effective** window to step boundaries
// and buckets by (alignedEffectiveStart, alignedEffectiveEnd).
// We don’t clamp to the query here; clamping happens at flush.
func buildWindowsEffective(segs []SegmentInfo, stepMs int64) []SegmentGroup {
	type key struct{ s, e int64 }
	buckets := make(map[key][]SegmentInfo, len(segs))

	for _, s := range segs {
		as := alignDown(effStart(s), stepMs)
		ae := alignUp(effEnd(s), stepMs)
		if as >= ae {
			continue
		}
		s2 := s
		// preserve storage StartTs/EndTs as-is; set effective aligned window
		s2.EffectiveStartTs = as
		s2.EffectiveEndTs = ae
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

// packByCountEffective groups windows into SegmentGroups, bounded by minGroupSize-ish,
// while preserving temporal ordering and fixing seams across window boundaries.
func packByCountEffective(
	wins []SegmentGroup,
	minGroupSize int,
	qStart, qEnd int64,
	stepMs int64,
) []SegmentGroup {
	if len(wins) == 0 {
		return nil
	}
	if minGroupSize <= 0 {
		minGroupSize = len(wins)
	}

	alignDown := func(ts int64) int64 { return ts - (ts % stepMs) }
	alignUp := func(ts int64) int64 {
		r := ts % stepMs
		if r == 0 {
			return ts
		}
		return ts + (stepMs - r)
	}

	var out []SegmentGroup
	var parts []SegmentInfo
	count := 0

	// envelope across accumulated windows (for gap detection)
	var envStart, envEnd int64
	hasEnv := false

	// tiling edge to prevent overlaps within a band
	var lastEnd int64
	haveLast := false

	// Flush current 'parts' as a group. If lookaheadIdx >= 0, we’ll duplicate
	// any upcoming windows whose StartTs < ge (seam crossers) into this group.
	flush := func(lookaheadIdx int) {
		if len(parts) == 0 {
			return
		}

		// envelope (effective) across 'parts'
		gs, ge := effStart(parts[0]), effEnd(parts[0])
		for _, p := range parts[1:] {
			if es := effStart(p); es < gs {
				gs = es
			}
			if ee := effEnd(p); ee > ge {
				ge = ee
			}
		}

		// clamp to query bounds
		if gs < qStart {
			gs = qStart
		}
		if ge > qEnd {
			ge = qEnd
		}
		if gs >= ge {
			parts = parts[:0]
			count = 0
			hasEnv = false
			return
		}

		// snap AFTER clamp (keeps edges on the grid)
		gs = alignDown(gs)
		ge = alignUp(ge)

		// tile against the previous group's end (no overlaps)
		if haveLast && gs < lastEnd {
			gs = lastEnd
			if gs >= ge {
				parts = parts[:0]
				count = 0
				hasEnv = false
				return
			}
		}

		// merge helper (seal to [es,ee] which will be subset of [gs,ge])
		type key struct {
			id   int64
			expr string
		}
		merged := make(map[key]SegmentInfo, len(parts))
		add := func(s SegmentInfo, es, ee int64) {
			if es >= ee {
				return
			}
			s.EffectiveStartTs = es
			s.EffectiveEndTs = ee
			k := key{s.SegmentID, s.ExprID}
			if m, ok := merged[k]; ok {
				// widen just in case
				if es < m.EffectiveStartTs {
					m.EffectiveStartTs = es
				}
				if ee > m.EffectiveEndTs {
					m.EffectiveEndTs = ee
				}
				merged[k] = m
			} else {
				merged[k] = s
			}
		}

		// 1) add current parts, sealed to [gs,ge)
		for _, s := range parts {
			add(s, gs, ge)
		}

		// 2) seam fix: duplicate & clip any lookahead windows whose StartTs < ge
		if lookaheadIdx >= 0 && lookaheadIdx < len(wins) {
			for k := lookaheadIdx; k < len(wins) && wins[k].StartTs < ge; k++ {
				for _, s := range wins[k].Segments {
					es, ee := effStart(s), effEnd(s)
					// intersect with current group window
					if es < gs {
						es = gs
					}
					if ee > ge {
						ee = ge
					}
					if es < ee {
						// keep on grid (redundant if inputs already aligned)
						es = alignDown(es)
						ee = alignUp(ee)
						if es < gs {
							es = gs
						}
						if ee > ge {
							ee = ge
						}
						if es < ee {
							add(s, es, ee)
						}
					}
				}
			}
		}

		// finish group
		segs := make([]SegmentInfo, 0, len(merged))
		for _, v := range merged {
			segs = append(segs, v)
		}
		out = append(out, SegmentGroup{StartTs: gs, EndTs: ge, Segments: segs})

		// reset
		parts = parts[:0]
		count = 0
		hasEnv = false
		lastEnd = ge
		haveLast = true
	}

	for i := 0; i < len(wins); i++ {
		w := wins[i]
		wStart, wEnd := w.StartTs, w.EndTs

		// split on real gaps between aligned windows
		if hasEnv && (wStart > envEnd || wEnd < envStart) {
			flush(i)         // no seam duplication needed on gap flush
			haveLast = false // new band resets tiling edge
		}

		if !hasEnv {
			envStart, envEnd = wStart, wEnd
			hasEnv = true
		} else {
			if wStart < envStart {
				envStart = wStart
			}
			if wEnd > envEnd {
				envEnd = wEnd
			}
		}

		// accumulate this window's segments
		parts = append(parts, w.Segments...)
		count += len(w.Segments)

		// count-boundary flush
		if count >= minGroupSize {
			// if the next window begins before envEnd, it can contribute to buckets
			// right before the seam; duplicate from that lookahead window (and any
			// consecutive ones) into THIS group.
			lookaheadIdx := -1
			if i+1 < len(wins) && wins[i+1].StartTs < envEnd {
				lookaheadIdx = i + 1
			}
			flush(lookaheadIdx)
		}
	}

	// final flush (no lookahead at end)
	flush(len(wins))
	return out
}

// TargetSize computes the approximate number of segments per SegmentGroup ("wave").
// It enforces a hard per-worker-per-wave cap:
//
//	waveSegments <= workers * MaxSegmentsPerWorkerPerWave
func TargetSize(totalSegments, workers int) int {
	if totalSegments <= 0 {
		return 0
	}
	if workers <= 0 {
		workers = 1
	}

	// Old behavior: evenly spread segments across workers.
	base := int(math.Ceil(float64(totalSegments) / float64(workers)))

	// Hard cap based on per-worker-per-wave limit.
	waveCap := workers * maxSegmentsPerWorkerPerWave
	if waveCap <= 0 {
		waveCap = totalSegments
	}

	target := base
	if target > waveCap {
		target = waveCap
	}
	if target > totalSegments {
		target = totalSegments
	}
	if target < 1 {
		target = 1
	}
	return target
}
