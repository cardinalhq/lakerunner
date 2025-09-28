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
	bq "cloud.google.com/go/bigquery"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"google.golang.org/api/iterator"
	"os"
	"sort"
	"strings"
)

func uniqueStrings(in []string) []string {
	m := map[string]struct{}{}
	out := make([]string, 0, len(in))
	for _, v := range in {
		vv := strings.TrimSpace(v)
		if vv == "" {
			continue
		}
		lvv := strings.ToLower(vv)
		if _, ok := m[lvv]; ok {
			continue
		}
		m[lvv] = struct{}{}
		out = append(out, vv)
	}
	sort.Strings(out)
	return out
}

func rejectNonSelect(sql string) error {
	s := strings.ToUpper(sql)
	bad := []string{"INSERT ", "UPDATE ", "DELETE ", "MERGE ", "CREATE ", "DROP ", "ALTER ", "TRUNCATE "}
	for _, kw := range bad {
		if strings.Contains(s, kw) {
			return fmt.Errorf("only SELECT/WITH queries are allowed")
		}
	}
	if !strings.Contains(s, "SELECT") && !strings.Contains(s, "WITH") {
		return fmt.Errorf("query must contain SELECT or WITH")
	}
	return nil
}

// Convert "project.dataset.table" → "dataset.table". If already "dataset.table", returns as-is.
func toDatasetTable(fq string) string {
	parts := strings.Split(fq, ".")
	if len(parts) == 3 {
		return parts[1] + "." + parts[2]
	}
	return fq
}

// ExplainSQL runs EXPLAIN <sql> and returns each row as a map[field]value.
// This does NOT execute the query.
func (s *AnalystServer) ExplainSQL(ctx context.Context, sql string) ([]map[string]any, error) {
	explain := "EXPLAIN\n" + sql
	q := s.BQ.Query(explain)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	schema := it.Schema
	for {
		var vals []bq.Value
		if err := it.Next(&vals); err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return nil, err
		}
		row := make(map[string]any, len(schema))
		for i, f := range schema {
			row[f.Name] = vals[i]
		}
		out = append(out, row)
	}
	return out, nil
}

// SubgraphForTables builds a TableGraphDTO filtered to referenced tables.
// Accepts fully-qualified ("project.dataset.table") or dataset-qualified ("dataset.table").
func (s *AnalystServer) SubgraphForTables(refs []string) *TableGraphDTO {
	dto := &TableGraphDTO{}
	if s.Graph == nil || len(refs) == 0 {
		return dto
	}

	// Build allowlist of dataset.table
	allow := map[string]struct{}{}
	for _, r := range refs {
		dt := strings.ToLower(toDatasetTable(r))
		allow[dt] = struct{}{}
	}

	// Tables
	for id, t := range s.Graph.Nodes {
		lid := strings.ToLower(id) // graph keys are "dataset.table" (or sometimes fq)
		if _, ok := allow[lid]; !ok {
			continue
		}
		cols := make(map[string]string, len(t.Columns))
		for c, ty := range t.Columns {
			cols[c] = ty
		}
		dto.Tables = append(dto.Tables, TableSchema{
			TableID: id,
			Columns: cols,
		})
	}
	sort.Slice(dto.Tables, func(i, j int) bool { return dto.Tables[i].TableID < dto.Tables[j].TableID })

	// Edges (only those whose endpoints are kept)
	kept := map[string]struct{}{}
	for _, ts := range dto.Tables {
		kept[strings.ToLower(ts.TableID)] = struct{}{}
	}
	for from, outs := range s.Graph.Edges {
		if _, ok := kept[strings.ToLower(from)]; !ok {
			continue
		}
		for _, e := range outs {
			if _, ok := kept[strings.ToLower(e.To)]; !ok {
				continue
			}
			dto.Edges = append(dto.Edges, EdgeDTO{
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
	sort.Slice(dto.Edges, func(i, j int) bool {
		if dto.Edges[i].From == dto.Edges[j].From {
			if dto.Edges[i].To == dto.Edges[j].To {
				if dto.Edges[i].Kind == dto.Edges[j].Kind {
					return dto.Edges[i].Confidence > dto.Edges[j].Confidence
				}
				return dto.Edges[i].Kind < dto.Edges[j].Kind
			}
			return dto.Edges[i].To < dto.Edges[j].To
		}
		return dto.Edges[i].From < dto.Edges[j].From
	})

	return dto
}

// llmApproveWithPlan asks the LLM to decide if the query plan (and referenced tables + joins)
// can reasonably answer the question. It returns (approved, reason).
func (s *AnalystServer) llmApproveWithPlan(
	ctx context.Context,
	question string,
	planRows []map[string]any,
	graph *TableGraphDTO,
	referenced []string,
) (bool, string) {
	if s.OpenAIKey == "" {
		return true, "LLM check skipped (no key)"
	}
	model := os.Getenv("OPENAI_GPT_MODEL")
	if model == "" {
		model = "gpt-5-mini"
	}

	// Compact the inputs for the model
	planJSON, _ := json.Marshal(planRows)
	graphJSON, _ := json.Marshal(graph)
	refsJSON, _ := json.Marshal(referenced)

	sys := "You are a strict data analyst. Decide if a SQL query's plan and referenced tables " +
		"are sufficient to answer a natural-language question. Consider join paths, filterability, " +
		"aggregation/measure presence, and whether the plan touches relevant columns/tables. " +
		"Reply ONLY in strict JSON: {\"answerable\": <bool>, \"reason\": \"...\"}."

	user := fmt.Sprintf(
		"Question:\n%s\n\nReferencedTables(JSON): %s\n\nGraph(JSON): %s\n\nQueryPlan(JSON rows): %s\n\nReturn ONLY JSON with fields answerable and reason.",
		strings.TrimSpace(question), string(refsJSON), string(graphJSON), string(planJSON),
	)

	client := openai.NewClient(option.WithAPIKey(s.OpenAIKey))
	resp, err := client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model: model,
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage(sys),
			openai.UserMessage(user),
		},
		Temperature: openai.Float(0),
	})
	if err != nil || len(resp.Choices) == 0 {
		return true, "LLM check failed; defaulting to pass"
	}

	var out struct {
		Answerable bool   `json:"answerable"`
		Reason     string `json:"reason"`
	}
	_ = json.Unmarshal([]byte(resp.Choices[0].Message.Content), &out)
	if out.Reason == "" && resp.Choices[0].Message.Content != "" {
		out.Reason = strings.TrimSpace(resp.Choices[0].Message.Content)
	}
	return out.Answerable, out.Reason
}

//
// ---------- Public validation entrypoint (dry-run + explain + LLM) ----------
//

type QuestionValidationResult struct {
	ValidationResult

	Lineage     CTELineage `json:"lineage"` // kept for schema compatibility; unused in this path
	Referenced  []string   `json:"referenced_tables"`
	LLMAccepted bool       `json:"llm_accepted"`
	LLMReason   string     `json:"llm_reason,omitempty"`
}

// ValidateQuestionSQL validates via BQ dry-run, collects referenced tables,
// fetches the Query Plan via EXPLAIN, builds a subgraph among those tables,
// and asks the LLM whether the plan appears to answer the question.
func (s *AnalystServer) ValidateQuestionSQL(ctx context.Context, question, sql string) (*QuestionValidationResult, error) {
	res := &QuestionValidationResult{
		ValidationResult: ValidationResult{Valid: false},
	}

	// 0) quick safety gate
	if err := rejectNonSelect(sql); err != nil {
		res.ErrorMsg = err.Error()
		return res, nil
	}

	// 1) dry-run the query to verify it compiles & get referenced tables & bytes
	q := s.BQ.Query(sql)
	q.DryRun = true
	q.DisableQueryCache = true
	job, err := q.Run(ctx)
	if err != nil {
		res.ErrorMsg = err.Error()
		return res, nil
	}
	st, err := job.Status(ctx)
	if err != nil {
		res.ErrorMsg = err.Error()
		return res, nil
	}
	if st.Err() != nil {
		res.ErrorMsg = st.Err().Error()
		return res, nil
	}
	if st.Statistics != nil {
		if st.Statistics.TotalBytesProcessed > 0 {
			res.Cost = float64(st.Statistics.TotalBytesProcessed) / (1 << 40) * 5.0
		}
		if qs, ok := st.Statistics.Details.(*bq.QueryStatistics); ok && qs != nil {
			refs := make([]string, 0, len(qs.ReferencedTables))
			for _, rt := range qs.ReferencedTables {
				refs = append(refs, fmt.Sprintf("%s.%s.%s", rt.ProjectID, rt.DatasetID, rt.TableID))
			}
			res.Referenced = uniqueStrings(refs)
		}
	}

	// 2) get the query plan via EXPLAIN (best-effort)
	var planRows []map[string]any
	if rows, err := s.ExplainSQL(ctx, sql); err == nil {
		planRows = rows
	}

	// 3) build a subgraph across referenced tables
	subgraph := s.SubgraphForTables(res.Referenced)

	// 4) ask the LLM if this plan + graph can answer the question
	approved, reason := s.llmApproveWithPlan(ctx, question, planRows, subgraph, res.Referenced)
	res.LLMAccepted, res.LLMReason = approved, reason

	// 5) final flag
	res.Valid = approved
	return res, nil
}
