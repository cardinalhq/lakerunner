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
	"testing"
	"time"
)

// --- helpers ---

func ts(base time.Time, d time.Duration) int64 { return base.Add(d).UnixMilli() }

func mkSeg(id int64, start, end int64) SegmentInfo {
	return SegmentInfo{
		SegmentID: id,
		StartTs:   start,
		EndTs:     end,
		ExprID:    "exprA",
	}
}

// assert basic invariants on batches: time-ordered (by StartTs), contiguous (EndTs of i == StartTs of i+1),
// and each batch has at least one segment.
func assertContiguousOrdered(t *testing.T, batches []SegmentGroup, reverse bool) {
	t.Helper()
	if len(batches) == 0 {
		t.Fatalf("no batches returned")
	}
	for i, b := range batches {
		if len(b.Segments) == 0 {
			t.Fatalf("batch %d has no segments", i)
		}
		if i == 0 {
			continue
		}
		if !reverse {
			if batches[i-1].EndTs != b.StartTs {
				t.Fatalf("batches not contiguous at %d: prev.End=%d, cur.Start=%d", i, batches[i-1].EndTs, b.StartTs)
			}
		} else {
			if batches[i-1].StartTs != b.EndTs {
				t.Fatalf("reverse batches not contiguous at %d: prev.Start=%d, cur.End=%d", i, batches[i-1].StartTs, b.EndTs)
			}
		}
	}
}

// --- tests ---

func TestComputeReplayBatches_SplitsContiguously(t *testing.T) {
	base := time.Unix(0, 0).UTC()
	step := time.Minute

	// Query window: [0, 6m)
	qStart := ts(base, 0)
	qEnd := ts(base, 6*step)

	// Create exactly one segment per 1m window so target splitting is predictable.
	// s0: [0,1m), s1: [1,2m), ... s5: [5,6m)
	var segs []SegmentInfo
	for i := 0; i < 6; i++ {
		start := ts(base, time.Duration(i)*step)
		end := ts(base, time.Duration(i+1)*step)
		segs = append(segs, mkSeg(
			// unique id per segment
			int64(i),
			start, end,
		))
	}

	// 2 workers → targetSize ≈ ceil(6/2)=3 ⇒ expect two batches: [0,3m), [3,6m)
	batches := ComputeReplayBatchesWithWorkers(segs, step, qStart, qEnd, 2, false)

	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}
	assertContiguousOrdered(t, batches, false)

	if got, want := batches[0].StartTs, qStart; got != want {
		t.Fatalf("batch0 start: got %d want %d", got, want)
	}
	if got, want := batches[0].EndTs, ts(base, 3*step); got != want {
		t.Fatalf("batch0 end: got %d want %d", got, want)
	}
	if got, want := batches[1].StartTs, ts(base, 3*step); got != want {
		t.Fatalf("batch1 start: got %d want %d", got, want)
	}
	if got, want := batches[1].EndTs, qEnd; got != want {
		t.Fatalf("batch1 end: got %d want %d", got, want)
	}

	// Each batch should carry ~3 segments (because each window contributed one segment).
	if got := len(batches[0].Segments); got != 3 {
		t.Fatalf("batch0 segments: got %d want 3", got)
	}
	if got := len(batches[1].Segments); got != 3 {
		t.Fatalf("batch1 segments: got %d want 3", got)
	}
}

func TestComputeReplayBatches_ReverseSort(t *testing.T) {
	base := time.Unix(0, 0).UTC()
	step := time.Minute

	qStart := ts(base, 0)
	qEnd := ts(base, 6*step)

	var segs []SegmentInfo
	for i := 0; i < 6; i++ {
		start := ts(base, time.Duration(i)*step)
		end := ts(base, time.Duration(i+1)*step)
		segs = append(segs, mkSeg(int64(i), start, end))
	}

	batches := ComputeReplayBatchesWithWorkers(segs, step, qStart, qEnd, 2, true)

	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(batches))
	}
	assertContiguousOrdered(t, batches, true)

	// In reverse, first batch should be the later window [3m,6m)
	if got, want := batches[0].StartTs, ts(base, 3*step); got != want {
		t.Fatalf("batch0 start (reverse): got %d want %d", got, want)
	}
	if got, want := batches[0].EndTs, qEnd; got != want {
		t.Fatalf("batch0 end (reverse): got %d want %d", got, want)
	}
	if got, want := batches[1].StartTs, qStart; got != want {
		t.Fatalf("batch1 start (reverse): got %d want %d", got, want)
	}
	if got, want := batches[1].EndTs, ts(base, 3*step); got != want {
		t.Fatalf("batch1 end (reverse): got %d want %d", got, want)
	}
}

func TestComputeReplayBatches_ClampsToQueryBounds(t *testing.T) {
	base := time.Unix(0, 0).UTC()
	step := time.Minute

	// Query [1m, 5m)
	qStart := ts(base, 1*step)
	qEnd := ts(base, 5*step)

	// Segments extend outside: one starts before, one ends after.
	segs := []SegmentInfo{
		mkSeg(1, ts(base, 0*step), ts(base, 3*step)), // overlaps into start
		mkSeg(2, ts(base, 3*step), ts(base, 6*step)), // overlaps into end
	}

	// 1 worker so everything ends up in one batch, clamped to [1m,5m)
	batches := ComputeReplayBatchesWithWorkers(segs, step, qStart, qEnd, 1, false)

	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	b := batches[0]
	if got, want := b.StartTs, qStart; got != want {
		t.Fatalf("batch start clamp: got %d want %d", got, want)
	}
	if got, want := b.EndTs, qEnd; got != want {
		t.Fatalf("batch end clamp: got %d want %d", got, want)
	}
	if len(b.Segments) == 0 {
		t.Fatalf("batch has no segments after clamp")
	}
}

func TestNoDuplicationAcrossGroups(t *testing.T) {
	// Segment covers [0m, 4m), query splits into two groups of 2m each
	seg := SegmentInfo{
		SegmentID: 1,
		ExprID:    "expr1",
		StartTs:   ts(time.Unix(0, 0), 0),
		EndTs:     ts(time.Unix(0, 0), 4*time.Minute),
	}
	step := time.Minute
	batches := ComputeReplayBatchesWithWorkers(
		[]SegmentInfo{seg}, step,
		ts(time.Unix(0, 0), 0), ts(time.Unix(0, 0), 4*time.Minute),
		2, false,
	)

	seen := map[int64]int{}
	for _, b := range batches {
		for _, s := range b.Segments {
			seen[s.SegmentID]++
		}
	}
	if seen[1] > 1 {
		t.Fatalf("segment duplicated across groups: %+v", seen)
	}
}
