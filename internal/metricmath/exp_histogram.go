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
	"math"
)

// ExpHist represents the minimal shape matching OTel's ExponentialHistogramDataPoint components.
type ExpHist struct {
	Scale     int32
	ZeroCount uint64
	PosOffset int32
	PosCounts []uint64
	NegOffset int32
	NegCounts []uint64
}

// ConvertExponentialHistogramToValues converts exponential histogram buckets to representative values.
func ConvertExponentialHistogramToValues(h ExpHist) (counts, values []float64) {
	counts = []float64{}
	values = []float64{}

	// base b = 2^(2^-scale)
	base := math.Pow(2, math.Pow(2, -float64(h.Scale)))

	// Zero bucket
	if h.ZeroCount > 0 {
		counts = append(counts, float64(h.ZeroCount))
		values = append(values, 0.0)
	}

	// Positive buckets: index k = PosOffset + i, representative = b^(k+0.5)
	for i, c := range h.PosCounts {
		if c == 0 {
			continue
		}
		k := float64(int64(h.PosOffset) + int64(i))
		x := math.Pow(base, k+0.5)
		counts = append(counts, float64(c))
		values = append(values, x)
	}

	// Negative buckets: mirror of positive, representative = -b^(k+0.5)
	for i, c := range h.NegCounts {
		if c == 0 {
			continue
		}
		k := float64(int64(h.NegOffset) + int64(i))
		x := -math.Pow(base, k+0.5)
		counts = append(counts, float64(c))
		values = append(values, x)
	}

	return counts, values
}
