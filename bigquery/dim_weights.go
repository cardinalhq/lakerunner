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

package bigquery

import (
	"context"
	"fmt"
	"math"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

// DimWeightConfig tunes how we compute DimWeights.
type DimWeightConfig struct {
	// Sampling for stats
	SamplePercent float64 // e.g., 0.5 means 0.5% TABLESAMPLE; clamped to [0.01, 5]

	// Component weights (sum doesn't need to be 1; we'll re-normalize)
	WRole        float64 // prior from column role ("name","category","status","id","attr")
	WCardinality float64 // suitability of distinct count for group-by
	WCoverage    float64 // average coverage across adjacent facts
	WCentrality  float64 // how many facts a dimension touches (1-hop)

	// Cardinality shaping
	IdealDistinct   float64 // sweet spot (e.g., 1e3)
	HardMaxDistinct float64 // beyond this approaches 0 (e.g., 1e6)

	// Only consider 1-hop coverage for performance
	MaxHopsForCoverage int // default 1
}

// Defaults if fields are zero
func (c *DimWeightConfig) normalize() {
	if c.SamplePercent <= 0 {
		c.SamplePercent = 0.5
	}
	if c.SamplePercent < 0.01 {
		c.SamplePercent = 0.01
	}
	if c.SamplePercent > 5 {
		c.SamplePercent = 5
	}
	if c.WRole == 0 && c.WCardinality == 0 && c.WCoverage == 0 && c.WCentrality == 0 {
		c.WRole, c.WCardinality, c.WCoverage, c.WCentrality = 0.25, 0.35, 0.25, 0.15
	}
	if c.IdealDistinct <= 0 {
		c.IdealDistinct = 1_000 // 1k distinct is a nice group-by
	}
	if c.HardMaxDistinct <= 0 {
		c.HardMaxDistinct = 1_000_000 // 1M+ is usually too high-cardinality to display
	}
	if c.MaxHopsForCoverage <= 0 {
		c.MaxHopsForCoverage = 1
	}
}

// ComputeDimWeights derives table- and column-level weights for dimensions/attributes.
func ComputeDimWeights(
	ctx context.Context,
	client *bq.Client,
	projectID string,
	graph *BQGraph,
	onto *Ontology,
	cfg DimWeightConfig,
) (map[string]float64, error) {
	cfg.normalize()

	out := map[string]float64{}

	// Precompute fact adjacency (1-hop) for centrality & coverage.
	facts := factSet(onto)
	adjFacts := adjacentFacts(graph, facts) // dimID -> []factID

	for dimID, dim := range onto.Dimensions {
		// Centrality: dim directly connected to how many facts?
		cen := centralityScore(len(adjFacts[dimID]))

		// Table-level base weight = average of attribute weights (computed below),
		// combined with centrality.
		tableScores := []float64{}

		for _, attr := range dim.Attributes {
			role := rolePrior(attr.Role) // 0..1
			card := 0.5                  // default neutral; refined below
			if isNumeric(strings.ToUpper(attr.BQType)) {
				// numeric attributes are rarer as group-bys; still compute but slightly lower prior
				card = 0.4
			}

			// 1) Cardinality (APPROX_COUNT_DISTINCT on sample)
			dc, err := approxDistinct(ctx, client, projectID, dimID, attr.Column, cfg.SamplePercent)
			if err == nil && dc >= 0 {
				card = cardinalityScore(float64(dc), cfg.IdealDistinct, cfg.HardMaxDistinct)
			}

			// 2) Coverage: for each adjacent fact, estimate join resolution rate on 1-hop mapping
			cov := 0.5 // neutral if unknown
			if len(adjFacts[dimID]) > 0 {
				sum := 0.0
				cnt := 0.0
				for _, factID := range adjFacts[dimID] {
					ok, fk, pk := oneHopKeyPair(graph, factID, dimID)
					if !ok {
						continue
					}
					cv, err := coverageOneHop(ctx, client, projectID, factID, fk, dimID, pk, cfg.SamplePercent)
					if err != nil {
						continue
					}
					sum += cv
					cnt++
				}
				if cnt > 0 {
					cov = sum / cnt
				}
			}

			// Blend
			raw := cfg.WRole*role + cfg.WCardinality*card + cfg.WCoverage*cov + cfg.WCentrality*cen
			weight := clamp01(raw / (cfg.WRole + cfg.WCardinality + cfg.WCoverage + cfg.WCentrality))

			// Record per-column and table-level accumulation
			out[keyCol(dimID, attr.Column)] = weight
			tableScores = append(tableScores, weight)
		}

		// Table-level weight: average attribute weight, nudged by centrality
		tw := avg(tableScores)
		if tw == 0 && len(tableScores) == 0 {
			tw = cen
		} else {
			// slight boost by centrality (10% of difference)
			tw = clamp01(0.9*tw + 0.1*cen)
		}
		out[dimID] = tw
	}

	return out, nil
}

func keyCol(tableID, col string) string { return tableID + "." + col }

func factSet(o *Ontology) map[string]struct{} {
	m := map[string]struct{}{}
	for id := range o.Facts {
		m[id] = struct{}{}
	}
	return m
}

// adjacentFacts returns direct fact neighbors (1-hop) per dimension.
func adjacentFacts(g *BQGraph, facts map[string]struct{}) map[string][]string {
	out := map[string][]string{}
	// forward edges: fact -> dim or dim -> fact
	for from, outs := range g.Edges {
		for _, e := range outs {
			// from -> to
			if _, isFact := facts[from]; isFact {
				// e.To is neighbor of fact
				out[e.To] = appendUnique(out[e.To], from)
			}
			if _, isFact := facts[e.To]; isFact {
				// from is neighbor of fact
				out[from] = appendUnique(out[from], e.To)
			}
		}
	}
	return out
}

func appendUnique(sl []string, v string) []string {
	for _, x := range sl {
		if x == v {
			return sl
		}
	}
	return append(sl, v)
}

func centralityScore(numFacts int) float64 {
	// 0 facts => 0; >=5 facts => ~1
	if numFacts <= 0 {
		return 0
	}
	x := float64(numFacts)
	return clamp01(1 - math.Exp(-x/2.0))
}

func rolePrior(role string) float64 {
	switch role {
	case "name":
		return 1.0
	case "category":
		return 0.85
	case "status":
		return 0.75
	case "id":
		return 0.35
	default: // "attr" or unknown
		return 0.6
	}
}

func cardinalityScore(distinct, ideal, hardMax float64) float64 {
	if distinct <= 0 {
		return 0.0
	}
	// bell-ish curve: near ideal => ~1; grows/declines toward 0 as it moves away; zero beyond hardMax*10
	left := math.Exp(-math.Pow(math.Log10((distinct+1)/(ideal+1)), 2))
	// hard ceiling
	if distinct >= hardMax {
		// decay further beyond hardMax
		decay := math.Exp(-(distinct - hardMax) / (10 * hardMax))
		return 0.35 * decay * left
	}
	return clamp01(left)
}

// approxDistinct computes APPROX_COUNT_DISTINCT(col) over a TABLESAMPLE of the dimension table.
// Returns -1 on error (caller handles neutrality).
func approxDistinct(ctx context.Context, client *bq.Client, projectID, tableID, col string, samplePercent float64) (int64, error) {
	ds, tbl := splitTableID(tableID)
	if ds == "" || tbl == "" {
		return -1, fmt.Errorf("bad tableID: %s", tableID)
	}
	// clamp percent
	if samplePercent < 0.01 {
		samplePercent = 0.01
	}
	if samplePercent > 5 {
		samplePercent = 5
	}
	qText := fmt.Sprintf(
		"SELECT APPROX_COUNT_DISTINCT(%s) AS dc "+
			"FROM `%s.%s.%s` TABLESAMPLE SYSTEM (%.2f PERCENT)",
		bqIdent(col), projectID, ds, tbl, samplePercent,
	)
	it, err := client.Query(qText).Read(ctx)
	if err != nil {
		return -1, err
	}
	var row []bq.Value
	if err := it.Next(&row); err != nil && err != iterator.Done {
		return -1, err
	}
	if len(row) == 0 {
		return -1, nil
	}
	switch v := row[0].(type) {
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	default:
		return -1, nil
	}
}

// oneHopKeyPair finds a single (fk, pk) pair for a 1-hop join fact->dim.
// Returns ok=false if none found.
func oneHopKeyPair(g *BQGraph, factID, dimID string) (ok bool, fkCol, pkCol string) {
	// forward edges
	for _, e := range g.Edges[factID] {
		if e.To == dimID && len(e.Cols) > 0 {
			return true, e.Cols[0][0], e.Cols[0][1]
		}
	}
	// reverse edges (dim -> fact)
	for _, e := range g.Edges[dimID] {
		if e.To == factID && len(e.Cols) > 0 {
			// reverse mapping (fact fk = dim pk)
			return true, e.Cols[0][1], e.Cols[0][0]
		}
	}
	return false, "", ""
}

// coverageOneHop estimates fraction of sampled fact keys that resolve to a row in dim.
func coverageOneHop(
	ctx context.Context,
	client *bq.Client,
	projectID string,
	factID, factFK, dimID, dimPK string,
	samplePercent float64,
) (float64, error) {
	fds, ftbl := splitTableID(factID)
	dds, dtbl := splitTableID(dimID)
	if fds == "" || ftbl == "" || dds == "" || dtbl == "" {
		return 0, fmt.Errorf("bad ids: fact=%s dim=%s", factID, dimID)
	}
	if samplePercent < 0.01 {
		samplePercent = 0.01
	}
	if samplePercent > 5 {
		samplePercent = 5
	}
	q := fmt.Sprintf(
		"WITH F AS (\n"+
			"  SELECT %s AS k\n"+
			"  FROM `%s.%s.%s` TABLESAMPLE SYSTEM (%.2f PERCENT)\n"+
			"  WHERE %s IS NOT NULL\n"+
			"),\n"+
			"D AS (\n"+
			"  SELECT %s AS k\n"+
			"  FROM `%s.%s.%s`\n"+
			")\n"+
			"SELECT SAFE_DIVIDE(COUNTIF(D.k IS NOT NULL), COUNT(*)) AS coverage\n"+
			"FROM F LEFT JOIN D USING (k)",
		bqIdent(factFK), projectID, fds, ftbl, samplePercent, bqIdent(factFK),
		bqIdent(dimPK), projectID, dds, dtbl,
	)

	it, err := client.Query(q).Read(ctx)
	if err != nil {
		return 0, err
	}
	var row []bq.Value
	if err := it.Next(&row); err != nil && err != iterator.Done {
		return 0, err
	}
	if len(row) == 0 || row[0] == nil {
		return 0, nil
	}
	switch v := row[0].(type) {
	case float64:
		return v, nil
	default:
		return 0, nil
	}
}

func avg(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := 0.0
	for _, v := range xs {
		s += v
	}
	return s / float64(len(xs))
}
