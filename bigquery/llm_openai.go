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
	client  openai.Client
	Model   openai.ChatModel
	Timeout time.Duration
}

// NewOpenAILLM builds an OpenAI-backed LLM. Example:
func NewOpenAILLM(model string, opts ...option.RequestOption) *OpenAILLM {
	if model == "" {
		model = "gpt-5-thinking" // pick your default
	}
	return &OpenAILLM{
		client:  openai.NewClient(opts...),
		Model:   model,
		Timeout: 30 * time.Second,
	}
}

type questionOut struct {
	Title       string `json:"title"`
	Description string `json:"description"`
}

func (o *OpenAILLM) GenerateQuestion(ctx context.Context, t QueryTemplate) (string, string, error) {
	// Timeout
	if o.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, o.Timeout)
		defer cancel()
	}

	// JSON schema forcing {title, description}
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

	// Build a compact, deterministic payload for the LLM
	var dimLabels []string
	for _, d := range t.DimCols {
		// prefer human label but include the raw column for grounding
		dimLabels = append(dimLabels, fmt.Sprintf("%s (%s.%s)", labelOf(d), d.TableID, d.Column))
	}

	// Natural-language brief for the model (kept short; title/desc are enforced via schema)
	brief := fmt.Sprintf(
		"Fact table: %s\nMeasure: %s(%s)\nDimensions: %s\nTime: %s %s\n\n",
		t.FactTable, firstAgg(t.Measure), t.Measure.Column,
		ifOr("(none)", func() bool { return len(dimLabels) > 0 }, strings.Join(dimLabels, ", ")),
		ifOr("(none)", func() bool { return t.TimeAttr != nil }, t.TimeAttr.Column),
		strings.ToLower(t.Grain),
	)

	// System instructions keep titles concise and useful to BI users
	sys := "You are writing analytics chart questions. " +
		"Return a clear, business-friendly title and a 1–2 sentence description. " +
		"Title must be <= 80 characters, avoid trailing punctuation; use parentheses only for time grain. " +
		"Do not invent fields; use only what is provided."

	// Ask the model for a title/description
	resp, err := o.client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model: o.Model,
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage(sys),
			openai.UserMessage(brief),
		},
		ResponseFormat: openai.ChatCompletionNewParamsResponseFormatUnion{
			OfJSONSchema: &openai.ResponseFormatJSONSchemaParam{JSONSchema: respSchema},
		},
		Temperature: openai.Float(0),
	})
	if err != nil || len(resp.Choices) == 0 {
		// Fallback to deterministic baseline if API fails
		title, desc, _ := BaselineLLM{}.GenerateQuestion(ctx, t)
		if err == nil && len(resp.Choices) == 0 {
			err = fmt.Errorf("no choices returned from OpenAI")
		}
		return title, desc, err
	}

	var out questionOut
	if jerr := json.Unmarshal([]byte(resp.Choices[0].Message.Content), &out); jerr != nil {
		// Fallback if schema parsing failed
		title, desc, _ := BaselineLLM{}.GenerateQuestion(ctx, t)
		return title, desc, jerr
	}
	// Final guardrails
	out.Title = strings.TrimSpace(out.Title)
	out.Description = strings.TrimSpace(out.Description)
	if out.Title == "" || out.Description == "" {
		title, desc, _ := BaselineLLM{}.GenerateQuestion(ctx, t)
		return title, desc, fmt.Errorf("empty title/description from model")
	}
	return out.Title, out.Description, nil
}
