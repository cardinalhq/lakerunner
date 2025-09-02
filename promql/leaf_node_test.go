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
	"math"
	"testing"
	"time"

	"github.com/axiomhq/hyperloglog"
)

// This test exercises LeafNode.Eval() on paths that don't depend on MAP aggregates,
// so it compiles without needing details of SketchTags.getAggValue().
//
// Covered:
//   - HLL sketch path (returns ValHLL with a materialized sketch)
//   - Unknown sketch type (returns scalar NaN)
//   - Empty input (returns empty map)
func TestLeafNode_Eval_Basics(t *testing.T) {
	step := 10 * time.Second

	t.Run("HLL sketch produces ValHLL and preserves timestamp/tags", func(t *testing.T) {
		// Build an HLL and serialize to bytes
		h := hyperloglog.New14()
		h.Insert([]byte("a"))
		h.Insert([]byte("b"))
		b, err := h.MarshalBinary()
		if err != nil {
			t.Fatalf("marshal HLL: %v", err)
		}

		now := time.Now().UnixMilli()

		leaf := &LeafNode{
			BE: BaseExpr{
				ID:       "metric_hll",
				FuncName: "", // not used for HLL path
				// No GroupBy -> key is "default"
			},
		}

		sg := SketchGroup{
			Group: map[string][]SketchInput{
				"metric_hll": {
					{
						Timestamp: now,
						SketchTags: SketchTags{
							SketchType: SketchHLL,
							Tags:       map[string]any{"src": "test"},
							Bytes:      b,
						},
					},
				},
			},
		}

		out := leaf.Eval(sg, step)
		if len(out) != 1 {
			t.Fatalf("expected 1 result, got %d", len(out))
		}
		r, ok := out["default"]
		if !ok {
			t.Fatalf("expected key 'default' (no group-by), got keys: %#v", out)
		}
		if r.Timestamp != now {
			t.Fatalf("timestamp mismatch: got %d want %d", r.Timestamp, now)
		}
		if r.Value.Kind != ValHLL || r.Value.HLL == nil {
			t.Fatalf("expected ValHLL with non-nil sketch, got %+v", r.Value)
		}
		// Basic sanity: cardinality should be >= 2 for the inserted items (approximate)
		if est := r.Value.HLL.Estimate(); float64(est) < 1.5 {
			t.Fatalf("unexpected low HLL estimate: got %d", est)
		}
	})

	t.Run("Unknown sketch type yields scalar NaN", func(t *testing.T) {
		leaf := &LeafNode{
			BE: BaseExpr{
				ID: "metric_unknown",
			},
		}
		sg := SketchGroup{
			Group: map[string][]SketchInput{
				"metric_unknown": {
					{
						Timestamp: time.Now().UnixMilli(),
						SketchTags: SketchTags{
							SketchType: SUM,
							Tags:       map[string]any{},
						},
					},
				},
			},
		}

		out := leaf.Eval(sg, step)
		if len(out) != 1 {
			t.Fatalf("expected 1 result, got %d", len(out))
		}
		r := out["default"]
		if r.Value.Kind != ValScalar || !math.IsNaN(r.Value.Num) {
			t.Fatalf("expected scalar NaN for unknown sketch type, got %+v", r.Value)
		}
	})

	t.Run("Empty rows for BE.ID returns empty map", func(t *testing.T) {
		leaf := &LeafNode{
			BE: BaseExpr{
				ID: "metric_empty",
			},
		}
		sg := SketchGroup{
			Group: map[string][]SketchInput{
				"metric_empty": {},
			},
		}
		out := leaf.Eval(sg, step)
		if len(out) != 0 {
			t.Fatalf("expected empty result, got %d entries", len(out))
		}
	})
}

func TestWinSumCount_FillThenSlide(t *testing.T) {
	const step = 10 * time.Second
	const rangeDur = 30 * time.Second
	stepMs := step.Milliseconds()
	rangeMs := rangeDur.Milliseconds()

	w := &winSumCount{rangeMs: rangeMs}

	w.add(0, 1, 1)
	w.add(10_000, 2, 1)
	w.add(20_000, 3, 1)

	if got := w.coveredMs(20_000, stepMs); got != rangeMs {
		t.Fatalf("coveredMs @20s = %d, want %d", got, rangeMs)
	}
	if w.sum != 6 || w.count != 3 {
		t.Fatalf("sum,count after fill = (%v,%v), want (6,3)", w.sum, w.count)
	}

	w.add(30_000, 4, 1)
	w.evict(0)
	if got := w.coveredMs(30_000, stepMs); got != 40_000 {
		t.Fatalf("coveredMs @30s = %d, want 40_000", got)
	}
	if w.sum != 10 || w.count != 4 {
		t.Fatalf("sum,count after +30s = (%v,%v), want (10,4)", w.sum, w.count)
	}

	w.add(40_000, 5, 1)
	w.evict(10_000)
	if got := w.coveredMs(40_000, stepMs); got != 40_000 {
		t.Fatalf("coveredMs @40s = %d, want 40_000", got)
	}
	if w.sum != 14 || w.count != 4 {
		t.Fatalf("sum,count after slide @40s = (%v,%v), want (14,4)", w.sum, w.count)
	}

	w.add(70_000, 8, 1)
	w.evict(40_000)
	if got := w.coveredMs(70_000, stepMs); got != 40_000 {
		t.Fatalf("coveredMs @70s = %d, want 40_000", got)
	}
	if w.sum != 13 || w.count != 2 {
		t.Fatalf("sum,count after slide @70s = (%v,%v), want (13,2)", w.sum, w.count)
	}
}

func TestWinSumCount_PartialCoverage(t *testing.T) {
	const step = 10 * time.Second
	const rangeDur = 30 * time.Second
	stepMs := step.Milliseconds()
	rangeMs := rangeDur.Milliseconds()

	w := &winSumCount{rangeMs: rangeMs}
	w.add(0, 5, 1)

	if got := w.coveredMs(0, stepMs); got != stepMs {
		t.Fatalf("coveredMs @0s = %d, want %d", got, stepMs)
	}
	if !(w.coveredMs(0, stepMs) < rangeMs) {
		t.Fatalf("expected partial coverage < rangeMs")
	}
}

func TestWinMinMax_FillThenSlide(t *testing.T) {
	const step = 10 * time.Second
	const rangeDur = 30 * time.Second
	stepMs := step.Milliseconds()
	rangeMs := rangeDur.Milliseconds()

	w := &winMinMax{rangeMs: rangeMs}

	// Add initial three buckets: (min,max)
	// 0s:  (5,9)
	// 10s: (4,6)
	// 20s: (7,8)
	w.add(0, 5, 9)
	w.add(10_000, 4, 6)
	w.add(20_000, 7, 8)

	// Full coverage at now=20s: (20s - 0s) + 10s = 30s
	if got := w.coveredMs(20_000, stepMs); got != rangeMs {
		t.Fatalf("coveredMs @20s = %d, want %d", got, rangeMs)
	}
	if w.min() != 4 {
		t.Fatalf("min after fill = %v, want 4", w.min())
	}
	if w.max() != 9 {
		t.Fatalf("max after fill = %v, want 9", w.max())
	}

	// Add 40s: (10,10), then evict keepFromTs=10s (drop 0s)
	w.add(40_000, 10, 10)
	w.evict(10_000)

	// Coverage now: (40s - 10s) + 10s = 40s
	if got := w.coveredMs(40_000, stepMs); got != 40_000 {
		t.Fatalf("coveredMs @40s = %d, want 40_000", got)
	}
	// Remaining windows: 10s(4,6), 20s(7,8), 40s(10,10)
	if w.min() != 4 {
		t.Fatalf("min after slide @40s = %v, want 4", w.min())
	}
	if w.max() != 10 {
		t.Fatalf("max after slide @40s = %v, want 10", w.max())
	}

	// Add 50s: (3,3), then evict keepFromTs=20s (drop 10s)
	w.add(50_000, 3, 3)
	w.evict(20_000)

	// Coverage now: (50s - 20s) + 10s = 40s
	if got := w.coveredMs(50_000, stepMs); got != 40_000 {
		t.Fatalf("coveredMs @50s = %d, want 40_000", got)
	}
	// Remaining windows: 20s(7,8), 40s(10,10), 50s(3,3)
	if w.min() != 3 {
		t.Fatalf("min after slide @50s = %v, want 3", w.min())
	}
	if w.max() != 10 {
		t.Fatalf("max after slide @50s = %v, want 10", w.max())
	}
}

func TestWinMinMax_PartialCoverage(t *testing.T) {
	const step = 10 * time.Second
	const rangeDur = 30 * time.Second
	stepMs := step.Milliseconds()

	w := &winMinMax{rangeMs: rangeDur.Milliseconds()}

	// Single bucket at 0s: (min,max) = (2,9)
	w.add(0, 2, 9)

	// Coverage at now=0s is exactly 1 bucket = 10s -> partial
	if got := w.coveredMs(0, stepMs); got != stepMs {
		t.Fatalf("coveredMs @0s = %d, want %d", got, stepMs)
	}
	if w.min() != 2 || w.max() != 9 {
		t.Fatalf("min,max after single bucket = (%v,%v), want (2,9)", w.min(), w.max())
	}
}
