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
	"fmt"
	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"os"
	"sort"
	"strings"
	"time"
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

// ---- graph subset for referenced tables ----

func (s *AnalystServer) SubgraphForTables(refs []string) *TableGraphDTO {
	dto := &TableGraphDTO{}
	if s.Graph == nil || len(refs) == 0 {
		return dto
	}

	allow := map[string]struct{}{}
	for _, r := range refs {
		dt := strings.ToLower(toDatasetTable(r)) // dataset.table
		allow[dt] = struct{}{}
	}

	for id, t := range s.Graph.Nodes {
		lid := strings.ToLower(id)
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

func (s *AnalystServer) llmApproveWithPlan(
	ctx context.Context,
	question string,
	sql string,
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

	planJSON, _ := json.Marshal(planRows)
	graphJSON, _ := json.Marshal(graph)
	refsJSON, _ := json.Marshal(referenced)

	sys := "You are a strict data analyst. Decide if a SQL query can answer a natural-language question. " +
		"Use the SQL as the primary evidence; use the query plan and table graph if available. " +
		"Consider joins, filters, groupings, aggregations, and selected columns. " +
		"Reply ONLY in JSON: {\"answerable\": <bool>, \"reason\": \"...\"}."

	user := fmt.Sprintf(
		"Question:\n%s\n\nSQL:\n%s\n\nReferencedTables(JSON): %s\n\nGraph(JSON): %s\n\nQueryPlan(JSON rows, optional): %s\n\nReturn ONLY JSON with fields answerable and reason.",
		strings.TrimSpace(question), strings.TrimSpace(sql), string(refsJSON), string(graphJSON), string(planJSON),
	)

	client := openai.NewClient(option.WithAPIKey(s.OpenAIKey))
	resp, err := client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model:       model,
		Temperature: openai.Float(1),
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage(sys),
			openai.UserMessage(user),
		},
	})
	if err != nil || len(resp.Choices) == 0 {
		return true, "LLM check failed; defaulting to pass"
	}

	type llmOut struct {
		Answerable bool   `json:"answerable"`
		Reason     string `json:"reason"`
	}

	content := strings.TrimSpace(resp.Choices[0].Message.Content)

	// Strip common code fences if present
	clean := content
	if strings.HasPrefix(clean, "```") {
		if i := strings.Index(clean, "\n"); i >= 0 {
			clean = clean[i+1:]
		}
		if j := strings.LastIndex(clean, "```"); j >= 0 {
			clean = clean[:j]
		}
		clean = strings.TrimSpace(clean)
	}
	// If there’s extra prose, grab the first JSON object
	if !strings.HasPrefix(clean, "{") {
		if i := strings.Index(clean, "{"); i >= 0 {
			clean = clean[i:]
		}
		if j := strings.LastIndex(clean, "}"); j >= 0 && j+1 <= len(clean) {
			clean = clean[:j+1]
		}
	}

	var out llmOut
	if err := json.Unmarshal([]byte(clean), &out); err != nil {
		// Default to pass if parsing fails (don’t block the workflow)
		return true, "LLM JSON parse failed; defaulting to pass. Raw: " + content
	}
	if out.Reason == "" {
		out.Reason = content
	}
	return out.Answerable, out.Reason
}

// ---- public validation entrypoint ----

type QuestionValidationResult struct {
	ValidationResult

	Lineage     CTELineage `json:"lineage"` // kept for schema compatibility; unused in this path
	Referenced  []string   `json:"referenced_tables"`
	LLMAccepted bool       `json:"llm_accepted"`
	LLMReason   string     `json:"llm_reason,omitempty"`
}

func (s *AnalystServer) ValidateQuestionSQL(ctx context.Context, question, sql, dataset string) (*QuestionValidationResult, error) {
	res := &QuestionValidationResult{
		ValidationResult: ValidationResult{Valid: false},
	}

	// 0) safety
	if err := rejectNonSelect(sql); err != nil {
		res.ErrorMsg = err.Error()
		return res, nil
	}

	// 1) dry-run to compile & collect referenced tables & byte estimate
	q := s.BQ.Query(sql)
	q.DryRun = true
	q.DisableQueryCache = true
	q.UseLegacySQL = false
	q.DefaultProjectID = s.ProjectID
	if v := strings.TrimSpace(dataset); v != "" {
		q.DefaultDatasetID = v
	}
	q.DefaultProjectID = s.ProjectID

	job, err := q.Run(ctx)
	if err != nil {
		res.ErrorMsg = err.Error()
		return res, nil
	}
	st := job.LastStatus()
	if st == nil {
		if st, err = job.Status(ctx); err != nil {
			res.ErrorMsg = err.Error()
			return res, nil
		}
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
			var refs []string
			for _, rt := range qs.ReferencedTables {
				refs = append(refs, fmt.Sprintf("%s.%s.%s", rt.ProjectID, rt.DatasetID, rt.TableID))
			}
			res.Referenced = uniqueStrings(refs)
		}
	}

	// 2) get a plan via ephemeral run (no EXPLAIN). Best-effort.
	planRows, _ := s.planFromEphemeralRun(ctx, sql, dataset)

	// 3) subgraph across referenced tables
	subgraph := s.SubgraphForTables(res.Referenced)

	// 4) LLM approval (SQL is primary, plan optional)
	approved, reason := s.llmApproveWithPlan(ctx, question, sql, planRows, subgraph, res.Referenced)
	res.LLMAccepted, res.LLMReason = approved, reason
	res.Valid = approved
	return res, nil
}

// ---- plan via ephemeral run (no EXPLAIN) ----

// planFromEphemeralRun runs the ORIGINAL SQL with a small MaximumBytesBilled cap
func (s *AnalystServer) planFromEphemeralRun(ctx context.Context, sql, dataset string) ([]map[string]any, error) {
	q := s.BQ.Query(strings.TrimSpace(sql))
	q.DefaultProjectID = s.ProjectID
	if v := strings.TrimSpace(dataset); v != "" {
		q.DefaultDatasetID = v
	}
	q.DisableQueryCache = true
	q.UseLegacySQL = false

	const capMB = int64(64)
	q.MaxBytesBilled = capMB * 1024 * 1024

	job, err := q.Run(ctx)
	if err != nil {
		return nil, err
	}

	// Poll for a short period; plan often appears before completion.
	for i := 0; i < 6; i++ {
		st := job.LastStatus()
		if st == nil {
			if st, err = job.Status(ctx); err != nil {
				return nil, err
			}
		}
		if st != nil && st.Statistics != nil {
			if qs, ok := st.Statistics.Details.(*bq.QueryStatistics); ok && qs != nil && len(qs.QueryPlan) > 0 {
				return planRowsFromQueryStats(qs), nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Final attempt; even if it errors (bytes cap), stats may include a plan.
	st, err := job.Wait(ctx)
	if err == nil && st != nil && st.Statistics != nil {
		if qs, ok := st.Statistics.Details.(*bq.QueryStatistics); ok && qs != nil && len(qs.QueryPlan) > 0 {
			return planRowsFromQueryStats(qs), nil
		}
	}
	return nil, fmt.Errorf("no query plan available from ephemeral run")
}
