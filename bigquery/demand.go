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
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
)

type SchemaSketch struct {
	Tables []SketchTable
}
type SketchTable struct {
	ID      string         // "dataset.table"
	Role    string         // "fact", "dimension", or "unknown"
	Columns []SketchColumn // name + type only
}
type SketchColumn struct {
	Name string
	Type string
}

func MakeSchemaSketch(g *BQGraph, onto *Ontology) SchemaSketch {
	isFact := map[string]bool{}
	isDim := map[string]bool{}
	for id := range onto.Facts {
		isFact[id] = true
	}
	for id := range onto.Dimensions {
		isDim[id] = true
	}

	ids := make([]string, 0, len(g.Nodes))
	for id := range g.Nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	out := SchemaSketch{Tables: make([]SketchTable, 0, len(ids))}
	for _, id := range ids {
		role := "unknown"
		if isFact[id] {
			role = "fact"
		} else if isDim[id] {
			role = "dimension"
		}
		t := g.Nodes[id]
		cols := make([]SketchColumn, 0, len(t.Columns))
		names := make([]string, 0, len(t.Columns))
		for c := range t.Columns {
			names = append(names, c)
		}
		sort.Strings(names)
		for _, c := range names {
			cols = append(cols, SketchColumn{Name: c, Type: t.Columns[c]})
		}
		out.Tables = append(out.Tables, SketchTable{ID: id, Role: role, Columns: cols})
	}
	return out
}

// ---------- OpenAI-backed DemandModel ----------

type OpenAIDemandModel struct {
	client  openai.Client
	Model   openai.ChatModel
	Timeout time.Duration
	summary string // cached dataset summary
}

// NewOpenAIDemandModel builds the model, summarizes the dataset once, and caches it.
func NewOpenAIDemandModel(
	ctx context.Context,
	model string,
	g *BQGraph,
	onto *Ontology,
	opts ...option.RequestOption,
) (*OpenAIDemandModel, error) {
	m := &OpenAIDemandModel{
		client:  openai.NewClient(opts...),
		Model:   model,
		Timeout: 45 * time.Second,
	}

	// Build a compact schema sketch and summarize what's important (cached).
	sketch := MakeSchemaSketch(g, onto)
	sum, err := m.summarizeImportance(ctx, sketch)
	if err != nil {
		return nil, err
	}
	m.summary = sum
	return m, nil
}

// ScoreDemand Ensure OpenAIDemandModel satisfies:
func (m *OpenAIDemandModel) ScoreDemand(ctx context.Context, t QueryTemplate) (float64, error) {
	return m.scoreTemplate(ctx, m.summary, t)
}

// ---------- Internal OpenAI calls (structured outputs) ----------

type demandSummaryOut struct {
	Summary string   `json:"summary"`
	Themes  []string `json:"themes"`
}

func (m *OpenAIDemandModel) summarizeImportance(ctx context.Context, sketch SchemaSketch) (string, error) {
	if m.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.Timeout)
		defer cancel()
	}

	// JSON schema for the summary
	sumSchema := openai.ResponseFormatJSONSchemaJSONSchemaParam{
		Name:   "demand_summary",
		Strict: openai.Bool(true),
		Schema: map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"properties": map[string]any{
				"summary": map[string]any{"type": "string"},
				"themes":  map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			},
			"required": []string{"summary"},
		},
	}

	// Compact prompt with trimmed column list per table to keep context small
	var b strings.Builder
	b.WriteString("You are a data strategist. Given the dataset schema, identify the most important business themes (e.g., revenue, acquisition, retention, cost, risk). Return a concise 2–3 sentence summary and 3–8 theme tokens.\n\n")
	b.WriteString("SCHEMA:\n")
	for _, t := range sketch.Tables {
		_, _ = fmt.Fprintf(&b, "- %s: %s cols=[", t.Role, t.ID)
		for i, c := range t.Columns {
			if i > 0 {
				b.WriteString(", ")
			}
			if i == 12 {
				b.WriteString("…")
				break
			}
			b.WriteString(c.Name)
		}
		b.WriteString("]\n")
	}

	resp, err := m.client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model: m.Model,
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage("Be precise. Avoid speculative topics unrelated to the schema."),
			openai.UserMessage(b.String()),
		},
		ResponseFormat: openai.ChatCompletionNewParamsResponseFormatUnion{
			OfJSONSchema: &openai.ResponseFormatJSONSchemaParam{JSONSchema: sumSchema},
		},
		Temperature: openai.Float(0),
	})
	if err != nil {
		return "", err
	}
	if len(resp.Choices) == 0 {
		return "", fmt.Errorf("no choices returned")
	}

	var out demandSummaryOut
	if err := json.Unmarshal([]byte(resp.Choices[0].Message.Content), &out); err != nil {
		return resp.Choices[0].Message.Content, nil
	}
	if out.Summary == "" {
		return resp.Choices[0].Message.Content, nil
	}
	return out.Summary, nil
}

type demandScoreOut struct {
	Score     float64 `json:"score"`
	Rationale string  `json:"rationale"`
}

func (m *OpenAIDemandModel) scoreTemplate(ctx context.Context, summary string, t QueryTemplate) (float64, error) {
	if m.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.Timeout)
		defer cancel()
	}

	scoreSchema := openai.ResponseFormatJSONSchemaJSONSchemaParam{
		Name:   "demand_score",
		Strict: openai.Bool(true),
		Schema: map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"properties": map[string]any{
				"score":     map[string]any{"type": "number", "minimum": 0, "maximum": 1},
				"rationale": map[string]any{"type": "string"},
			},
			"required": []string{"score", "rationale"},
		},
	}

	// Concise payload describing the template
	var dims []string
	for _, d := range t.DimCols {
		dims = append(dims, fmt.Sprintf("%s.%s", d.TableID, d.Column))
	}
	payload := fmt.Sprintf(
		"Dataset themes: %s\nFact=%s | Measure=%s(%s) | Dims=%s | Grain=%s",
		summary, t.FactTable, firstAgg(t.Measure), t.Measure.Column, strings.Join(dims, ", "), t.Grain,
	)

	resp, err := m.client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model: m.Model,
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage("Score how important this query would be to typical stakeholders of this dataset (revenue impact, growth, cost/risk, operational usage). Return a number in [0,1]."),
			openai.UserMessage(payload),
		},
		ResponseFormat: openai.ChatCompletionNewParamsResponseFormatUnion{
			OfJSONSchema: &openai.ResponseFormatJSONSchemaParam{JSONSchema: scoreSchema},
		},
		Temperature: openai.Float(0),
	})
	if err != nil {
		return 0, err
	}
	if len(resp.Choices) == 0 {
		return 0, fmt.Errorf("no choices returned")
	}

	var out demandScoreOut
	if err := json.Unmarshal([]byte(resp.Choices[0].Message.Content), &out); err != nil {
		return 0, err
	}
	// Clamp to [0,1]
	if out.Score < 0 {
		out.Score = 0
	}
	if out.Score > 1 {
		out.Score = 1
	}
	return out.Score, nil
}
