// runner.go
package bigquery

import (
	"context"
	"fmt"
	"github.com/openai/openai-go/option"
	"log/slog"
	"os"
	"strings"
	"time"

	bq "cloud.google.com/go/bigquery"
)

type SimpleOverlapScorer struct{}

func (SimpleOverlapScorer) ScoreColumns(ctx context.Context, a ColumnSample, b ColumnSample) (float64, string, error) {
	set := map[string]int{}
	for _, v := range a.Sample {
		set[strings.ToLower(v)] |= 1
	}
	for _, v := range b.Sample {
		set[strings.ToLower(v)] |= 2
	}
	intersect, union := 0, 0
	for _, m := range set {
		if m == 3 {
			intersect++
			union++
		} else if m == 1 || m == 2 {
			union++
		}
	}
	var score float64
	if union > 0 {
		score = float64(intersect) / float64(union)
	} else {
		score = 0.0
	}
	rationale := fmt.Sprintf("sample-overlap jaccard=%.2f on %d/%d unique values", score, intersect, union)
	return score, rationale, nil
}

const projectID = "chip-473401"

// BuildAndAugment builds a BQGraph for the given datasets and augments it with sampled column data.
func BuildAndAugment(ctx context.Context, datasetIDs []string, sampleLimit int, minScore float64, maxPairsPerTable int) (*BQGraph, error) {
	g, err := BuildGraphForDatasets(ctx, projectID, datasetIDs)
	if err != nil {
		return nil, err
	}

	client, err := bq.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	defer func(client *bq.Client) {
		if cerr := client.Close(); cerr != nil {
			slog.Error("failed to close BQ client", "error", cerr)
		}
	}(client)

	scorer := SimpleOverlapScorer{}
	if err := AugmentGraphWithSamples(ctx, client, projectID, g, scorer, sampleLimit, minScore, maxPairsPerTable); err != nil {
		return nil, err
	}
	return g, nil
}

func computeFactWeights(onto *Ontology) map[string]float64 {
	weights := map[string]float64{}
	maxScore := 0.0
	tmp := map[string]float64{}
	for id, f := range onto.Facts {
		score := float64(len(f.Measures))*1.0 + float64(len(f.ReachableDims))*0.25
		tmp[id] = score
		if score > maxScore {
			maxScore = score
		}
	}
	if maxScore <= 0 {
		return weights
	}
	for id, s := range tmp {
		weights[id] = s / maxScore
	}
	return weights
}

func RunAll(ctx context.Context, datasetIDs []string, topN int) ([]Candidate, *Ontology, *BQGraph, error) {
	g, err := BuildAndAugment(ctx, datasetIDs, 10, 0.70, 50)
	if err != nil {
		return nil, nil, nil, err
	}

	client, err := bq.NewClient(ctx, projectID)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func(client *bq.Client) {
		err := client.Close()
		if err != nil {
			slog.Error("failed to close BQ client", "error", err)
		}
	}(client)

	onto, err := BuildOntology(projectID, g, 10)
	if err != nil {
		return nil, nil, nil, err
	}

	dwCfg := DimWeightConfig{
		SamplePercent:      0.5, // 0.5% samples
		WRole:              0.25,
		WCardinality:       0.35,
		WCoverage:          0.25,
		WCentrality:        0.15,
		IdealDistinct:      1_000,
		HardMaxDistinct:    1_000_000,
		MaxHopsForCoverage: 1,
	}
	dimW, err := ComputeDimWeights(ctx, client, projectID, g, onto, dwCfg)
	if err != nil {
		return nil, nil, nil, err
	}

	factW := computeFactWeights(onto)

	cfg := CatalogConfig{
		FactWeights:     factW,
		DimWeights:      dimW,
		MaxDims:         3,
		JoinPenaltyK:    0.15,
		DimPenaltyK:     0.08,
		CostBudgetBytes: 10 * (1 << 30), // 10 GiB
		OnDemandPerTB:   5.0,            // adjust as needed
		WCoverage:       0.30,
		WExplainability: 0.25,
		WCost:           0.35,
		WDemand:         0.10,
	}

	apiKey := os.Getenv("OPENAI_API_KEY")
	gptModel := os.Getenv("OPENAI_GPT_MODEL")
	llm := NewOpenAILLM(gptModel, option.WithAPIKey(apiKey))
	demand, err := NewOpenAIDemandModel(ctx, gptModel, g, onto, option.WithAPIKey(apiKey))
	if err != nil {
		return nil, nil, nil, err
	}

	// Last 30 days window for dry-run parameter binding
	window := func() (time.Time, time.Time) {
		end := time.Now().UTC()
		start := end.AddDate(0, 0, -30)
		return start, end
	}

	cands, err := GenerateCatalog(ctx, client, g, onto, llm, demand, cfg, window, topN)
	if err != nil {
		return nil, nil, nil, err
	}
	return cands, onto, g, nil
}

type CandidateRecord struct {
	Title          string
	Description    string
	SQL            string
	FactTable      string
	Dimensions     []string
	Grain          string
	EstimatedBytes int64
	EstimatedUSD   float64

	Coverage       float64
	Explainability float64
	CostEfficiency float64
	Demand         float64
	Composite      float64

	Stage string
}

func (c Candidate) Record() CandidateRecord {
	dims := make([]string, len(c.DimCols))
	for i, d := range c.DimCols {
		dims[i] = fmt.Sprintf("%s.%s", d.TableID, d.Column)
	}
	return CandidateRecord{
		Title:          c.Title,
		Description:    c.Description,
		SQL:            c.SQL,
		FactTable:      c.FactTable,
		Dimensions:     dims,
		Grain:          c.Grain,
		EstimatedBytes: c.EstimatedBytes,
		EstimatedUSD:   c.EstimatedUSD,
		Coverage:       c.Scores.Coverage,
		Explainability: c.Scores.Explainability,
		CostEfficiency: c.Scores.CostEfficiency,
		Demand:         c.Scores.Demand,
		Composite:      c.Scores.Composite,
		Stage:          c.Stage,
	}
}

// Records maps a slice of Candidates to a slice of CandidateRecord.
func Records(candidates []Candidate) []CandidateRecord {
	out := make([]CandidateRecord, len(candidates))
	for i, c := range candidates {
		out[i] = c.Record()
	}
	return out
}

func ExampleRunAll() {
	ctx := context.Background()
	cands, onto, g, err := RunAll(ctx, []string{"sales", "ops"}, 50)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Printf("facts=%d dims=%d nodes=%d edges=%d\n", len(onto.Facts), len(onto.Dimensions), len(g.Nodes), len(g.Edges))

	recs := Records(cands)
	for i, r := range recs {
		fmt.Printf("%2d) %.3f  %s  [bytes≈%d  $≈%.2f]  fact=%s  dims=%v  grain=%s  stage=%s\n",
			i+1, r.Composite, r.Title, r.EstimatedBytes, r.EstimatedUSD, r.FactTable, r.Dimensions, r.Grain, r.Stage)
		fmt.Printf("-- %s\n%s\n\n", r.Description, r.SQL)
	}
}
