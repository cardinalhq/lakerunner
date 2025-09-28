package bigquery

import (
	bq "cloud.google.com/go/bigquery"
	"context"
	"errors"
	"fmt"
	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"google.golang.org/api/iterator"
	"log/slog"
	"math"
	"sort"
	"strings"
)

func (s *AnalystServer) GetBigQueryDataSets(ctx context.Context) ([]string, error) {
	if s == nil || s.BQ == nil {
		return nil, fmt.Errorf("AnalystServer BigQuery client is not initialized")
	}
	it := s.BQ.Datasets(ctx) // lists datasets in s.ProjectID
	ids := make([]string, 0, 32)

	for {
		ds, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		// Defensive: if the iterator ever yields multi-project results, keep only ours.
		if s.ProjectID != "" && ds.ProjectID != "" && ds.ProjectID != s.ProjectID {
			continue
		}
		ids = append(ids, ds.DatasetID) // return bare dataset IDs (e.g., "chip_dataset")
	}

	sort.Strings(ids)
	return ids, nil
}

// buildGraphForDatasets builds a fresh graph for the given datasets.
// It includes FK edges and (when available) heuristic edges between similar columns.
func (s *AnalystServer) buildGraphForDatasets(ctx context.Context, datasets []string) (*BQGraph, error) {
	if len(datasets) == 0 {
		if s.Graph == nil {
			return nil, fmt.Errorf("no cached graph on server and no datasets provided")
		}
		return s.Graph, nil
	}
	g, err := BuildGraphForDatasets(ctx, s.ProjectID, datasets)
	if err != nil {
		return nil, err
	}
	// If your project supports heuristic edges, include them (ignore error, return FK-only if it fails).
	if err := AugmentGraphWithEdgesBetweenSimilarColumns(g, 50); err != nil {
		slog.Warn("augment heuristic edges failed; returning FK-only graph", "err", err)
	}
	return g, nil
}

// GetTableGraph returns the table schemas and the directed edges (FK + heuristic) among them
func (s *AnalystServer) GetTableGraph(ctx context.Context, datasets []string) (*TableGraphDTO, error) {
	g, err := s.buildGraphForDatasets(ctx, datasets)
	if err != nil {
		return nil, err
	}

	tables := make([]TableSchema, 0, len(g.Nodes))
	for id, t := range g.Nodes {
		cols := make(map[string]string, len(t.Columns))
		for c, ty := range t.Columns {
			cols[c] = ty
		}
		tables = append(tables, TableSchema{
			TableID: id,
			Columns: cols,
		})
	}
	sort.Slice(tables, func(i, j int) bool { return tables[i].TableID < tables[j].TableID })

	var edges []EdgeDTO
	for from, outs := range g.Edges {
		_ = from // present for clarity; edges already include From/To
		for _, e := range outs {
			edges = append(edges, EdgeDTO{
				From:       e.From,
				To:         e.To,
				Kind:       e.Kind,
				Cols:       e.Cols,
				Confidence: e.Confidence,
				Constraint: e.Constraint,
				Note:       e.Note,
			})
		}
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].From == edges[j].From {
			if edges[i].To == edges[j].To {
				if edges[i].Kind == edges[j].Kind {
					return edges[i].Confidence > edges[j].Confidence
				}
				return edges[i].Kind < edges[j].Kind
			}
			return edges[i].To < edges[j].To
		}
		return edges[i].From < edges[j].From
	})

	return &TableGraphDTO{
		Tables: tables,
		Edges:  edges,
	}, nil
}

// GetRelevantQuestions embeds the input question and ranks already-embedded candidates by cosine similarity.
func (s *AnalystServer) GetRelevantQuestions(
	ctx context.Context,
	datasets []string,
	question string,
	topK int,
) ([]RelevantQuestion, error) {
	if strings.TrimSpace(question) == "" {
		return nil, fmt.Errorf("question must not be empty")
	}
	if s.OpenAIKey == "" || s.OpenAIEmbedModel == "" {
		return nil, fmt.Errorf("OpenAI embedding config missing")
	}
	if topK <= 0 {
		topK = 10
	}

	// Embed the input question
	cli := openai.NewClient(option.WithAPIKey(s.OpenAIKey))
	qvec, err := s.embedOnce(ctx, &cli, s.OpenAIEmbedModel, question)
	if err != nil {
		return nil, err
	}

	dsSet := map[string]struct{}{}
	for _, ds := range datasets {
		dsSet[strings.ToLower(ds)] = struct{}{}
	}

	type scored struct {
		idx   int
		score float64
	}
	scoredList := make([]scored, 0, len(s.Candidates))
	for i := range s.Candidates {
		c := &s.Candidates[i]
		if len(dsSet) > 0 {
			ds := datasetOfTableID(c.FactTable)
			if _, ok := dsSet[strings.ToLower(ds)]; !ok {
				continue
			}
		}
		if len(c.Embedding) == 0 {
			continue
		}
		sim := cosine32(qvec, c.Embedding)
		sim = (sim + 1.0) / 2.0
		scoredList = append(scoredList, scored{i, sim})
	}

	sort.Slice(scoredList, func(i, j int) bool {
		if scoredList[i].score == scoredList[j].score {
			// tie-break by title for stability
			return s.Candidates[scoredList[i].idx].Title < s.Candidates[scoredList[j].idx].Title
		}
		return scoredList[i].score > scoredList[j].score
	})

	if topK > len(scoredList) {
		topK = len(scoredList)
	}

	out := make([]RelevantQuestion, 0, topK)
	for i := 0; i < topK; i++ {
		c := s.Candidates[scoredList[i].idx]
		out = append(out, RelevantQuestion{
			Sql:        c.SQL,
			Question:   c.Title,
			Similarity: scoredList[i].score,
		})
	}
	return out, nil
}

// GetUptoNDistinctStringValues returns up to `limit` distinct values for a non-numeric column.
// It first tries to read the column type from the cached graph; if missing, it falls back to BQ metadata.
// For non-numeric columns, values are returned as strings (CAST(... AS STRING)).
func (s *AnalystServer) GetUptoNDistinctStringValues(
	ctx context.Context,
	dataset, table, column string,
	limit int,
) ([]string, error) {
	if dataset == "" || table == "" || column == "" {
		return nil, fmt.Errorf("dataset, table, and column are required")
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 1000 {
		limit = 1000 // basic guardrail
	}

	// 1) Determine column type (graph -> fallback to table metadata)
	colType, err := s.lookupColumnType(ctx, dataset, table, column)
	if err != nil {
		return nil, err
	}
	if isBQNumeric(colType) {
		return nil, fmt.Errorf("column %q is numeric (%s); only non-numeric columns are supported", column, colType)
	}

	// 2) Build and run query
	full := fmt.Sprintf("`%s.%s.%s`", s.ProjectID, dataset, table)
	colIdent := bqQuoteIdent(column)
	sql := fmt.Sprintf(
		"SELECT DISTINCT CAST(t.%s AS STRING) AS v\nFROM %s t\nWHERE t.%s IS NOT NULL\nLIMIT %d",
		colIdent, full, colIdent, limit,
	)

	q := s.BQ.Query(sql)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}

	// 3) Collect results
	out := make([]string, 0, limit)
	for {
		var row []bq.Value
		if err := it.Next(&row); err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return nil, err
		}
		if len(row) > 0 && row[0] != nil {
			out = append(out, fmt.Sprintf("%v", row[0]))
		}
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// --- helpers ---

func (s *AnalystServer) lookupColumnType(ctx context.Context, dataset, table, column string) (string, error) {
	// Prefer cached graph schema if present
	if s.Graph != nil && s.Graph.Nodes != nil {
		key := dataset + "." + table
		if t, ok := s.Graph.Nodes[key]; ok && t.Columns != nil {
			for col, typ := range t.Columns {
				if strings.EqualFold(col, column) {
					return typ, nil
				}
			}
		}
	}
	// Fallback to BigQuery table metadata
	md, err := s.BQ.Dataset(dataset).Table(table).Metadata(ctx)
	if err != nil {
		return "", err
	}
	for _, f := range md.Schema {
		if strings.EqualFold(f.Name, column) {
			return strings.ToUpper(string(f.Type)), nil
		}
	}
	return "", fmt.Errorf("column %q not found in %s.%s", column, dataset, table)
}

func isBQNumeric(typ string) bool {
	up := strings.ToUpper(typ)
	return strings.Contains(up, "INT") ||
		strings.Contains(up, "INTEGER") ||
		strings.Contains(up, "FLOAT") ||
		strings.Contains(up, "FLOAT64") ||
		strings.Contains(up, "NUMERIC") ||
		strings.Contains(up, "BIGNUMERIC") ||
		strings.Contains(up, "DECIMAL")
}

// Backtick-quote an identifier (strip any existing backticks for safety).
func bqQuoteIdent(id string) string {
	id = strings.ReplaceAll(id, "`", "")
	return "`" + id + "`"
}

// --- helpers ---

func (s *AnalystServer) embedOnce(
	ctx context.Context,
	client *openai.Client,
	model string,
	text string,
) ([]float64, error) {
	resp, err := client.Embeddings.New(ctx, openai.EmbeddingNewParams{
		Model: model,
		Input: openai.EmbeddingNewParamsInputUnion{
			OfArrayOfStrings: []string{text},
		},
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Data) == 0 || len(resp.Data[0].Embedding) == 0 {
		return nil, fmt.Errorf("empty embedding result")
	}
	return resp.Data[0].Embedding, nil
}

func cosine32(a, b []float64) float64 {
	if len(a) == 0 || len(b) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		fa, fb := a[i], b[i]
		dot += fa * fb
		na += fa * fa
		nb += fb * fb
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

func datasetOfTableID(tableID string) string {
	// Accept "dataset.table" or "project.dataset.table"
	parts := strings.Split(tableID, ".")
	if len(parts) == 2 {
		return parts[0]
	}
	if len(parts) == 3 {
		return parts[1]
	}
	return ""
}
