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

package filereader

import (
	"fmt"
	"io"
	"math"
	"sort"

	"github.com/DataDog/sketches-go/ddsketch"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/xpdata/pref"

	"github.com/cardinalhq/lakerunner/internal/helpers"
)

func init() {
	// Enable proto pooling for pmetric unmarshalling.
	// This reduces GC pressure by reusing metric structures.
	// Requires calling pref.UnrefMetrics() when done with the data.
	_ = pref.EnableRefCounting.IsEnabled() // ensure gate is registered
	_ = pref.UseProtoPooling.IsEnabled()   // ensure gate is registered
}

func parseProtoToOtelMetrics(reader io.Reader) (*pmetric.Metrics, error) {
	unmarshaler := &pmetric.ProtoUnmarshaler{}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	metrics, err := unmarshaler.UnmarshalMetrics(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal protobuf metrics: %w", err)
	}

	return &metrics, nil
}

func summaryToDDSketch(dp pmetric.SummaryDataPoint) (*ddsketch.DDSketch, error) {
	const maxSamples = 2048

	s, err := helpers.GetSketch()
	if err != nil {
		return nil, fmt.Errorf("failed to get sketch for summary: %w", err)
	}

	// Pull quantiles
	nq := dp.QuantileValues().Len()
	if nq == 0 {
		return s, nil
	}
	type qv struct{ q, v float64 }
	qs := make([]qv, 0, nq)
	for i := range nq {
		it := dp.QuantileValues().At(i)
		q, v := it.Quantile(), it.Value()
		if math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		// Clamp q to [0,1]
		if q < 0 {
			q = 0
		} else if q > 1 {
			q = 1
		}
		qs = append(qs, qv{q, v})
	}
	if len(qs) == 0 {
		return s, nil
	}
	sort.Slice(qs, func(i, j int) bool { return qs[i].q < qs[j].q })

	// Ensure endpoints for coverage of [0,1]
	if qs[0].q > 0 {
		qs = append([]qv{{0, qs[0].v}}, qs...)
	}
	if qs[len(qs)-1].q < 1 {
		last := qs[len(qs)-1]
		qs = append(qs, qv{1, last.v})
	}

	// Deduplicate non-monotone or repeated quantiles (keep first)
	dst := qs[:1]
	for i := 1; i < len(qs); i++ {
		if qs[i].q > dst[len(dst)-1].q {
			dst = append(dst, qs[i])
		}
	}
	qs = dst
	if len(qs) == 1 {
		// Single point: just add one representative
		_ = s.Add(qs[0].v)
		return s, nil
	}

	// Build segments
	type seg struct{ qL, qR, vL, vR float64 }
	segs := make([]seg, 0, len(qs)-1)
	totalMass := 0.0
	for i := 0; i+1 < len(qs); i++ {
		qL, qR := qs[i].q, qs[i+1].q
		if qR <= qL {
			continue
		}
		segs = append(segs, seg{qL, qR, qs[i].v, qs[i+1].v})
		totalMass += (qR - qL)
	}
	if totalMass == 0 || len(segs) == 0 {
		// Degenerate—dump endpoints
		for _, p := range qs {
			_ = s.Add(p.v)
		}
		return s, nil
	}

	// Sampling budget: distribute proportionally by mass; gently cap by dp.Count
	count := float64(dp.Count())
	budget := float64(maxSamples)
	if count > 0 && count < budget {
		// Don't invent more samples than the actual count (soft cap).
		budget = count
	}
	if budget < float64(len(segs)) {
		budget = float64(len(segs))
	}

	lin := func(vL, vR, t float64) float64 { return vL + t*(vR-vL) }
	loglin := func(vL, vR, t float64) float64 {
		// If either endpoint non-positive, fall back to linear.
		if vL <= 0 || vR <= 0 {
			return lin(vL, vR, t)
		}
		return math.Exp(math.Log(vL) + t*(math.Log(vR)-math.Log(vL)))
	}

	remaining := int(budget)
	for i, g := range segs {
		w := (g.qR - g.qL) / totalMass
		n := int(math.Round(w * budget))
		if n == 0 {
			n = 1
		}
		if n > remaining {
			n = remaining
		}
		remaining -= n

		for k := 1; k <= n; k++ {
			// Evenly spaced internal points per segment
			t := (float64(k) - 0.5) / float64(n)
			val := loglin(g.vL, g.vR, t)
			if !math.IsNaN(val) && !math.IsInf(val, 0) {
				_ = s.Add(val)
			}
		}
		if remaining <= 0 && i < len(segs)-1 {
			break
		}
	}

	// Optional sanity anchors when mean is wildly off.
	// If dp.Sum() and dp.Count() are set and the reconstructed mean differs a lot,
	// you can nudge the sketch by adding a few values near the global mean.
	if dp.Count() > 0 {
		targetMean := dp.Sum() / float64(dp.Count())
		if sCount, sSum := s.GetCount(), s.GetSum(); sCount > 0 {
			gotMean := sSum / sCount
			relErr := math.Abs(gotMean-targetMean) / math.Max(1e-12, targetMean)
			if relErr > 0.25 { // tolerate 25% drift; tune to taste
				anchor := targetMean
				// Add a couple of anchors; this won't distort quantiles much on large n.
				_ = s.Add(anchor)
				_ = s.Add(anchor)
			}
		}
	}

	return s, nil
}
