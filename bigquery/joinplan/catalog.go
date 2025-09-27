// generator.go
package joinplan

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	bq "cloud.google.com/go/bigquery"
)

type LLM interface {
	GenerateQuestion(ctx context.Context, t QueryTemplate) (title string, description string, err error)
}

type BaselineLLM struct{}

func (BaselineLLM) GenerateQuestion(ctx context.Context, t QueryTemplate) (string, string, error) {
	var parts []string
	for _, d := range t.DimCols {
		parts = append(parts, labelOf(d))
	}
	by := ""
	if len(parts) > 0 {
		by = " by " + strings.Join(parts, " and ")
	}
	gr := ""
	if t.TimeAttr != nil && t.Grain != "" {
		gr = " (" + strings.ToLower(t.Grain) + ")"
	}
	title := fmt.Sprintf("%s of %s%s%s",
		strings.ToLower(firstAgg(t.Measure)), humanizeCol(t.Measure.Column), by, gr)

	desc := fmt.Sprintf("Measure: %s(%s) on %s. Dimensions: %s. Time: %s %s.",
		firstAgg(t.Measure), t.Measure.Column, t.FactTable, strings.Join(parts, ", "),
		ifOr("(none)", func() bool { return t.TimeAttr != nil }, t.TimeAttr.Column), strings.ToLower(t.Grain))
	return title, desc, nil
}

func firstAgg(m Measure) string {
	if len(m.Aggs) > 0 {
		return m.Aggs[0]
	}
	return "SUM"
}

func labelOf(d DimensionAttr) string {
	// prefer user-friendly labels
	toks := strings.Split(d.Column, "_")
	for i := range toks {
		toks[i] = strings.Title(toks[i])
	}
	return strings.Join(toks, " ")
}

func humanizeCol(col string) string { return labelOf(DimensionAttr{Column: col}) }

func ifOr[T any](fallback T, ok func() bool, val T) T {
	if ok() {
		return val
	}
	return fallback
}

//
// Candidate model + scoring
//

type Candidate struct {
	Title       string
	Description string
	SQL         string

	FactTable string
	Measure   Measure
	DimCols   []DimensionAttr
	TimeAttr  *TimeAttr
	Grain     string

	// Provenance and cost
	EstimatedBytes int64
	EstimatedUSD   float64 // optional (if pricing provided)
	GeneratedAt    time.Time

	// Scores
	Scores ScoreBreakdown

	Stage string
}

type ScoreBreakdown struct {
	Coverage       float64 // 0..1
	Explainability float64 // 0..1
	CostEfficiency float64 // 0..1 (lower bytes -> higher score)
	Demand         float64 // 0..1
	Composite      float64 // weighted sum
}

type CatalogConfig struct {
	// Coverage: mark certain facts/dims as high-value (0..1).
	FactWeights map[string]float64 // tableID -> [0..1]
	DimWeights  map[string]float64 // tableID or "table.col" -> [0..1]

	MaxDims      int
	JoinPenaltyK float64
	DimPenaltyK  float64

	CostBudgetBytes int64
	OnDemandPerTB   float64

	WCoverage       float64
	WExplainability float64
	WCost           float64
	WDemand         float64
}

type DemandModel interface {
	ScoreDemand(ctx context.Context, t QueryTemplate) (float64, error) // 0..1
}

// NullDemand gives 0 by default.
type NullDemand struct{}

func (NullDemand) ScoreDemand(ctx context.Context, t QueryTemplate) (float64, error) { return 0, nil }

// GenerateCatalog uses Ontology templates + LLM to phrase questions,
// dry-runs in BigQuery to attach cost, then scores and returns candidates sorted by Composite desc.
func GenerateCatalog(
	ctx context.Context,
	client *bq.Client,
	graph *BQGraph,
	onto *Ontology,
	llm LLM,
	demand DemandModel,
	cfg CatalogConfig,
	dryRunWindow func() (start, end time.Time), // supply default time window
	limit int,
) ([]Candidate, error) {

	if llm == nil {
		llm = BaselineLLM{}
	}
	if demand == nil {
		demand = NullDemand{}
	}
	normalizeConfig(&cfg)

	// Default time window: last 30 days.
	if dryRunWindow == nil {
		dryRunWindow = func() (time.Time, time.Time) {
			end := time.Now().UTC()
			start := end.AddDate(0, 0, -30)
			return start, end
		}
	}
	start, end := dryRunWindow()

	// Prepare a reverse adjacency map once for explainability calc (BFS).
	reverse := buildReverse(graph)

	cands := make([]Candidate, 0, len(onto.Templates))
	for _, t := range onto.Templates {
		// cap dims if configured
		if cfg.MaxDims > 0 && len(t.DimCols) > cfg.MaxDims {
			continue
		}

		// Turn template into a question
		title, desc, err := llm.GenerateQuestion(ctx, t)
		if err != nil {
			// fallback to baseline phrasing
			title, desc, _ = BaselineLLM{}.GenerateQuestion(ctx, t)
		}

		// Compute cost via dry-run (use start/end from dryRunWindow)
		bytes, err := estimateBytesDryRun(ctx, client, t.SQL, t.TimeAttr, start, end)
		if err != nil {
			// If dry-run fails (e.g., permissions), include the candidate but mark bytes unknown.
			bytes = -1
		}

		// Score pieces
		coverage := scoreCoverage(cfg, &t)
		explain := scoreExplainability(cfg, &t, graph, reverse)
		costEff := scoreCost(cfg, bytes)
		dmd, _ := demand.ScoreDemand(ctx, t)

		composite := cfg.WCoverage*coverage + cfg.WExplainability*explain + cfg.WCost*costEff + cfg.WDemand*dmd

		cand := Candidate{
			Title:          title,
			Description:    desc,
			SQL:            t.SQL,
			FactTable:      t.FactTable,
			Measure:        t.Measure,
			DimCols:        t.DimCols,
			TimeAttr:       t.TimeAttr,
			Grain:          t.Grain,
			EstimatedBytes: bytes,
			EstimatedUSD:   estimateUSD(cfg, bytes),
			GeneratedAt:    time.Now(),
			Scores: ScoreBreakdown{
				Coverage:       coverage,
				Explainability: explain,
				CostEfficiency: costEff,
				Demand:         dmd,
				Composite:      composite,
			},
			Stage: "dev",
		}
		cands = append(cands, cand)
	}

	sort.Slice(cands, func(i, j int) bool {
		if cands[i].Scores.Composite == cands[j].Scores.Composite {
			if cands[i].EstimatedBytes == cands[j].EstimatedBytes {
				return cands[i].Title < cands[j].Title
			}
			if cands[i].EstimatedBytes < 0 || cands[j].EstimatedBytes < 0 {
				return cands[i].EstimatedBytes < cands[j].EstimatedBytes
			}
			return cands[i].EstimatedBytes < cands[j].EstimatedBytes
		}
		return cands[i].Scores.Composite > cands[j].Scores.Composite
	})

	if limit > 0 && len(cands) > limit {
		cands = cands[:limit]
	}
	return cands, nil
}

func estimateBytesDryRun(ctx context.Context, client *bq.Client, sql string, tattr *TimeAttr, start, end time.Time) (int64, error) {
	q := client.Query(sql)
	// Bind required time params so the dry run is valid.
	if tattr != nil {
		q.Parameters = []bq.QueryParameter{
			{Name: "start_time", Value: start},
			{Name: "end_time", Value: end},
		}
	}
	q.DryRun = true
	q.DisableQueryCache = true

	job, err := q.Run(ctx)
	if err != nil {
		return 0, err
	}
	status, err := job.Status(ctx)
	if err != nil {
		return 0, err
	}
	if status.Statistics == nil {
		return 0, fmt.Errorf("no statistics in dry-run status")
	}
	return status.Statistics.TotalBytesProcessed, nil
}

func estimateUSD(cfg CatalogConfig, bytes int64) float64 {
	if cfg.OnDemandPerTB <= 0 || bytes < 0 {
		return 0
	}
	tb := float64(bytes) / (1 << 40) // TiB
	return tb * cfg.OnDemandPerTB
}

//
// Scoring
//

func normalizeConfig(cfg *CatalogConfig) {
	if cfg.MaxDims == 0 {
		cfg.MaxDims = 3
	}
	if cfg.CostBudgetBytes == 0 {
		cfg.CostBudgetBytes = 10 * (1 << 30) // 10 GiB
	}
	if cfg.JoinPenaltyK == 0 {
		cfg.JoinPenaltyK = 0.15
	}
	if cfg.DimPenaltyK == 0 {
		cfg.DimPenaltyK = 0.08
	}
	if cfg.WCoverage == 0 && cfg.WExplainability == 0 && cfg.WCost == 0 && cfg.WDemand == 0 {
		cfg.WCoverage, cfg.WExplainability, cfg.WCost, cfg.WDemand = 0.30, 0.25, 0.35, 0.10
	}
}

func scoreCoverage(cfg CatalogConfig, t *QueryTemplate) float64 {
	score := cfg.FactWeights[t.FactTable]
	for _, d := range t.DimCols {
		k1 := d.TableID
		k2 := d.TableID + "." + d.Column
		score += maxf(cfg.DimWeights[k1], cfg.DimWeights[k2]) * (1.0 / float64(len(t.DimCols)+1))
	}
	return clamp01(score)
}

func scoreExplainability(cfg CatalogConfig, t *QueryTemplate, g *BQGraph, reverse map[string][]*Edge) float64 {
	pen := 0.0
	for _, d := range t.DimCols {
		edges := shortestPathLen(g, reverse, t.FactTable, d.TableID)
		if edges <= 0 {
			edges = 4 // unreachable → heavy penalty
		}
		pen += cfg.JoinPenaltyK * float64(edges)
	}
	pen += cfg.DimPenaltyK * float64(len(t.DimCols))
	grBonus := 0.0
	if t.TimeAttr != nil && t.Grain != "" {
		switch strings.ToUpper(t.Grain) {
		case "DAY", "WEEK", "MONTH":
			grBonus = 0.1
		case "HOUR":
			grBonus = 0.05
		default:
			grBonus = 0
		}
	}
	return clamp01(1.0 - pen + grBonus)
}

func scoreCost(cfg CatalogConfig, bytes int64) float64 {
	if bytes < 0 {
		return 0.5
	}
	b := float64(bytes)
	budget := float64(cfg.CostBudgetBytes)
	if budget <= 0 {
		budget = 10 * (1 << 30) // 10 GiB
	}
	if b <= budget {
		return 1.0
	}
	return clamp01(budget / math.Sqrt(b*budget))
}

//
// Graph helpers (BFS shortest path in edges, allow reverse edges)
//

func buildReverse(g *BQGraph) map[string][]*Edge {
	rev := map[string][]*Edge{}
	for _, outs := range g.Edges {
		for _, e := range outs {
			rev[e.To] = append(rev[e.To], &Edge{
				From: e.To, To: e.From, Kind: e.Kind, Cols: reversePairs(e.Cols), Confidence: e.Confidence, Constraint: e.Constraint, Note: "reverse:" + e.Note,
			})
		}
	}
	return rev
}

func reversePairs(pairs [][2]string) [][2]string {
	out := make([][2]string, len(pairs))
	for i := range pairs {
		out[i] = [2]string{pairs[i][1], pairs[i][0]}
	}
	return out
}

func shortestPathLen(g *BQGraph, reverse map[string][]*Edge, from, to string) int {
	if from == to {
		return 0
	}
	type node struct {
		ID string
		D  int
	}
	q := []node{{from, 0}}
	seen := map[string]bool{from: true}
	for len(q) > 0 {
		cur := q[0]
		q = q[1:]
		// fwd
		for _, e := range g.Edges[cur.ID] {
			if e.To == to {
				return cur.D + 1
			}
			if !seen[e.To] {
				seen[e.To] = true
				q = append(q, node{e.To, cur.D + 1})
			}
		}
		// rev
		for _, e := range reverse[cur.ID] {
			if e.To == to {
				return cur.D + 1
			}
			if !seen[e.To] {
				seen[e.To] = true
				q = append(q, node{e.To, cur.D + 1})
			}
		}
	}
	return -1
}

//
// Small utils
//

func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}
func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
