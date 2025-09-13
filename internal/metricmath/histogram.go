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

package metricmath

import (
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/DataDog/sketches-go/ddsketch"
)

// ImportStandardHistogram imports a linear "standard histogram" into an existing DDSketch.
// The histogram is provided as counts for (lo, cutoffs[0]], (cutoffs[0], cutoffs[1]], ... (cutoffs[n-2], cutoffs[n-1]].
//   - lowerBound: the lower edge for the first bucket (lo).
//   - cutoffs:    upper edges for each bucket (length must equal len(counts)).
//   - counts:     bucket counts (uint64 to match many hist sources).
//   - alpha:      DDSketch relative accuracy used for selecting A vs B (e.g., 0.01).
//   - perBinSums: optional exact sums for each bucket (nil if unknown). If provided,
//     we preserve per-bin sum (to rounding tolerance) in the split.
//
// Heuristic:
//   - If a bucket spans ≤ ~1 DDSketch bucket, we use Option A (one representative).
//   - Otherwise, Option B: split across k (1..5) log- or linearly-spaced reps, preserving count and sum.
func ConvertHistogramToValues(
	sk *ddsketch.DDSketch,
	lowerBound float64,
	cutoffs []float64,
	counts []uint64,
	alpha float64,
	perBinSums []float64, // nil if unknown
) error {
	if sk == nil {
		return errors.New("sketch is nil")
	}
	if len(cutoffs) == 0 || len(cutoffs) != len(counts) {
		return errors.New("cutoffs and counts length must match and be > 0")
	}
	if perBinSums != nil && len(perBinSums) != len(counts) {
		return errors.New("perBinSums length must be nil or equal to counts")
	}
	if alpha <= 0 || alpha >= 1 {
		return errors.New("alpha must be in (0,1)")
	}

	lo := lowerBound
	for i := range cutoffs {
		hi := cutoffs[i]
		c := int64(counts[i])
		if c <= 0 || !(lo < hi) {
			lo = hi
			continue
		}

		// Compute span in DDSketch buckets: span = log_gamma(hi/lo).
		// If lo<=0, we can’t use log spacing; treat as “wide” and use linear below.
		useLog := (lo > 0)

		// Choose k representatives.
		k := repsForBin(lo, hi, alpha, c)
		if k == 1 {
			// Option A: one representative value.
			var rep float64
			if perBinSums != nil {
				rep = perBinSums[i] / float64(c)
			} else if useLog {
				rep = math.Sqrt(lo * hi) // geometric mean for positive ranges
			} else {
				rep = 0.5 * (lo + hi) // midpoint if touching/non-positive
			}
			if err := sk.AddWithCount(rep, float64(c)); err != nil {
				return fmt.Errorf("failed to add count to sketch: %w", err)
			}
		} else {
			// Option B: k reps, split counts to preserve count and sum.
			xs := repsInside(lo, hi, k, useLog)
			var S float64
			if perBinSums != nil {
				S = perBinSums[i]
			} else {
				// If no exact per-bin sum, anchor around a reasonable mean.
				// For log spans, geomean; otherwise midpoint.
				mu := 0.0
				if useLog {
					mu = math.Sqrt(lo * hi)
				} else {
					mu = 0.5 * (lo + hi)
				}
				S = float64(c) * mu
			}
			ns := splitCountsPreserveSum(c, S, xs)
			for j, n := range ns {
				if n > 0 {
					if err := sk.AddWithCount(xs[j], float64(n)); err != nil {
						return fmt.Errorf("failed to add split count to sketch: %w", err)
					}
				}
			}
		}

		lo = hi
	}
	return nil
}

// ---------- helpers ----------

// Select k reps for a bin given alpha & count.
// Heuristic: k = ceil(span), capped [1..5], reduced for tiny counts.
func repsForBin(lo, hi, alpha float64, count int64) int {
	if !(lo < hi) || count <= 0 {
		return 0
	}
	useLog := (lo > 0)
	if !useLog {
		// if we cross/contain 0, keep reps small
		if count < 10 {
			return 1
		}
		return 2
	}
	gamma := (1 + alpha) / (1 - alpha)
	span := math.Log(hi/lo) / math.Log(gamma)
	k := int(math.Ceil(span))
	if k < 1 {
		k = 1
	}
	if k > 5 {
		k = 5
	}
	if count < 10 && k > 2 {
		k = 2
	}
	return k
}

// repsInside builds k interior representatives inside (lo,hi].
// If useLog==true (lo>0), they are log-uniform; else linear positions.
func repsInside(lo, hi float64, k int, useLog bool) []float64 {
	if k < 1 {
		return nil
	}
	xs := make([]float64, 0, k)
	if useLog {
		r := hi / lo
		for i := 1; i <= k; i++ {
			p := float64(i) / float64(k+1)
			xs = append(xs, lo*math.Pow(r, p))
		}
	} else {
		step := (hi - lo) / float64(k+1)
		for i := 1; i <= k; i++ {
			xs = append(xs, lo+step*float64(i))
		}
	}
	return xs
}

// Distribute integer counts n[i] over xs[i] so that sum(n)=c and
// sum(n[i]*xs[i]) ≈ S. Uses a simple constrained nudging + Hamilton rounding.
func splitCountsPreserveSum(c int64, S float64, xs []float64) []int64 {
	k := len(xs)
	if c <= 0 || k == 0 {
		return nil
	}
	// Start with equal fractional weights.
	base := float64(c) / float64(k)
	w := make([]float64, k)
	var sumX float64
	for _, x := range xs {
		sumX += x
	}
	S0 := base * sumX
	mu := S / float64(c)
	for i := range w {
		w[i] = base
	}

	// Nudge weights to reduce delta in sum.
	delta := S - S0
	if delta != 0 {
		// Distribute by distance to mu (push mass toward mu).
		var denom float64
		for _, x := range xs {
			denom += math.Abs(mu - x)
		}
		if denom > 0 {
			for i, x := range xs {
				w[i] += delta * (mu - x) / denom
				if w[i] < 0 {
					w[i] = 0
				}
			}
		}
	}

	// Hamilton rounding: round down, then assign remainder to largest fractional parts.
	n := make([]int64, k)
	var sumN int64
	type frac struct {
		i int
		f float64
	}
	fracs := make([]frac, k)
	for i := range w {
		floor := math.Floor(w[i])
		n[i] = int64(floor)
		sumN += n[i]
		fracs[i] = frac{i: i, f: w[i] - floor}
	}
	rem := c - sumN
	sort.Slice(fracs, func(i, j int) bool { return fracs[i].f > fracs[j].f })
	for i := int64(0); i < rem && int(i) < k; i++ {
		n[fracs[i].i]++
	}

	// Optional tiny second-order correction (usually unnecessary):
	// Could swap 1 count between two nearest-to-mu reps if residual error is large.

	return n
}
