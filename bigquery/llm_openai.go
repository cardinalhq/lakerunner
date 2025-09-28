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
	"strings"
	"time"

	openai "github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
)

// OpenAILLM implements the LLM interface using OpenAI chat w/ JSON schema outputs.
type OpenAILLM struct {
	client  *openai.Client
	Model   openai.ChatModel
	Timeout time.Duration
}

// NewOpenAILLM builds an OpenAI-backed LLM. Example:
func NewOpenAILLM(model string, opts ...option.RequestOption) *OpenAILLM {
	c := openai.NewClient(opts...)
	return &OpenAILLM{
		client:  &c,
		Model:   model,
		Timeout: 30 * time.Second,
	}
}

type questionOut struct {
	Title       string `json:"title"`
	Description string `json:"description"`
}

func (o *OpenAILLM) GenerateQuestion(ctx context.Context, t QueryTemplate) (string, string, error) {
	if o.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.Timeout)
		defer cancel()
	}

	respSchema := openai.ResponseFormatJSONSchemaJSONSchemaParam{
		Name:   "biz_question",
		Strict: openai.Bool(true),
		Schema: map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"properties": map[string]any{
				"title":       map[string]any{"type": "string"},
				"description": map[string]any{"type": "string"},
			},
			"required": []string{"title", "description"},
		},
	}

	var dimLabels []string
	for _, d := range t.DimCols {
		dimLabels = append(dimLabels, fmt.Sprintf("%s (%s.%s)", labelOf(d), d.TableID, d.Column))
	}
	dimsText := "(none)"
	if len(dimLabels) > 0 {
		dimsText = strings.Join(dimLabels, ", ")
	}

	timeCol := "(none)"
	if t.TimeAttr != nil {
		timeCol = t.TimeAttr.Column
	}
	grain := strings.ToLower(t.Grain)

	hasMeasure := strings.TrimSpace(t.Measure.Column) != ""

	// Build the brief WITHOUT forcing a measure when there isn't one
	var brief string
	if hasMeasure {
		brief = fmt.Sprintf(
			"Fact table: %s\nMeasure: %s(%s)\nDimensions: %s\nTime: %s %s\n\n",
			t.FactTable, firstAgg(t.Measure), t.Measure.Column, dimsText, timeCol, grain,
		)
	} else {
		brief = fmt.Sprintf(
			"Table: %s\nNo aggregate measure. Attributes (for grouping/filtering/display): %s\nTime: %s %s\n\n",
			ifOr("(unknown table)", func() bool { return t.FactTable != "" }, t.FactTable),
			dimsText, timeCol, grain,
		)
	}

	// System prompt tuned for both modes
	sys := "You generate business-friendly analytics questions.\n" +
		"When a measure is provided, write a concise KPI/aggregation title and a 1–2 sentence description.\n" +
		"When NO measure is provided, write an entity/lookup/relationship style question, e.g., 'Which …', 'What …', 'When …', " +
		"focusing on listing, describing, or relating records using the given attributes.\n" +
		"Keep titles ≤ 80 chars, no trailing punctuation, and only use fields provided. " +
		"If there is a time grain, include it in parentheses."

	// Slightly steadier generations
	temp := 0.3
	if hasMeasure {
		temp = 0.5
	}

	resp, err := o.client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model: o.Model,
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage(sys),
			openai.UserMessage(brief),
		},
		ResponseFormat: openai.ChatCompletionNewParamsResponseFormatUnion{
			OfJSONSchema: &openai.ResponseFormatJSONSchemaParam{JSONSchema: respSchema},
		},
		Temperature: openai.Float(temp),
	})
	if err != nil || len(resp.Choices) == 0 {
		title, desc, _ := BaselineLLM{}.GenerateQuestion(ctx, t)
		if err == nil && len(resp.Choices) == 0 {
			err = fmt.Errorf("no choices returned from OpenAI")
		}
		return title, desc, err
	}

	var out questionOut
	if jerr := json.Unmarshal([]byte(resp.Choices[0].Message.Content), &out); jerr != nil {
		title, desc, _ := BaselineLLM{}.GenerateQuestion(ctx, t)
		return title, desc, jerr
	}
	out.Title = strings.TrimSpace(out.Title)
	out.Description = strings.TrimSpace(out.Description)
	if out.Title == "" || out.Description == "" {
		title, desc, _ := BaselineLLM{}.GenerateQuestion(ctx, t)
		return title, desc, fmt.Errorf("empty title/description from model")
	}
	return out.Title, out.Description, nil
}
