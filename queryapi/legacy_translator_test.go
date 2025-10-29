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

package queryapi

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTranslateToLogQL_EqOperator(t *testing.T) {
	baseExpr := BaseExpression{
		Dataset: "logs",
		Limit:   100,
		Order:   "DESC",
		Filter: Filter{
			K:  "resource.service.name",
			V:  []string{"my-service"},
			Op: "eq",
		},
	}

	logql, ctx, err := TranslateToLogQL(baseExpr)
	require.NoError(t, err)
	assert.Equal(t, `{resource_service_name="my-service"}`, logql)
	assert.Equal(t, "resource.service.name", ctx.QueryLabels["resource_service_name"])
}

func TestTranslateToLogQL_InOperator(t *testing.T) {
	baseExpr := BaseExpression{
		Dataset: "logs",
		Filter: Filter{
			K:  "log.level",
			V:  []string{"error", "warn", "fatal"},
			Op: "in",
		},
	}

	logql, _, err := TranslateToLogQL(baseExpr)
	require.NoError(t, err)
	assert.Equal(t, `{log_level=~"^(error|warn|fatal)$"}`, logql)
}

func TestTranslateToLogQL_ContainsOperator(t *testing.T) {
	baseExpr := BaseExpression{
		Dataset: "logs",
		Filter: Filter{
			K:  "_cardinalhq.message",
			V:  []string{"error occurred"},
			Op: "contains",
		},
	}

	logql, _, err := TranslateToLogQL(baseExpr)
	require.NoError(t, err)
	// Contains creates a label matcher with substring match: label=~".*value.*"
	assert.Equal(t, `{chq_message=~".*error occurred.*"}`, logql)
}

func TestTranslateToLogQL_RegexOperator(t *testing.T) {
	baseExpr := BaseExpression{
		Dataset: "logs",
		Filter: Filter{
			K:  "resource.pod.name",
			V:  []string{"^api-.*"},
			Op: "regex",
		},
	}

	logql, _, err := TranslateToLogQL(baseExpr)
	require.NoError(t, err)
	assert.Equal(t, `{resource_pod_name=~"^api-.*"}`, logql)
}

func TestTranslateToLogQL_AndOperator(t *testing.T) {
	baseExpr := BaseExpression{
		Dataset: "logs",
		Filter: BinaryClause{
			Op: "and",
			Clauses: []QueryClause{
				Filter{
					K:  "resource.service.name",
					V:  []string{"my-service"},
					Op: "eq",
				},
				Filter{
					K:  "log.level",
					V:  []string{"error"},
					Op: "eq",
				},
			},
		},
	}

	logql, _, err := TranslateToLogQL(baseExpr)
	require.NoError(t, err)
	assert.Equal(t, `{resource_service_name="my-service",log_level="error"}`, logql)
}

func TestTranslateToLogQL_InvalidDataset(t *testing.T) {
	baseExpr := BaseExpression{
		Dataset: "metrics",
		Filter: Filter{
			K:  "test",
			V:  []string{"value"},
			Op: "eq",
		},
	}

	_, _, err := TranslateToLogQL(baseExpr)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only 'logs' dataset is supported")
}

func TestTranslateToLogQL_UnsupportedOperator(t *testing.T) {
	baseExpr := BaseExpression{
		Dataset: "logs",
		Filter: Filter{
			K:  "resource.cpu.usage",
			V:  []string{"50"},
			Op: "gt",
		},
	}

	_, _, err := TranslateToLogQL(baseExpr)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "comparison operators not yet supported")
}

func TestNormalizeLabelName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"resource.service.name", "resource_service_name"},
		{"_cardinalhq.fingerprint", "chq_fingerprint"},
		{"_cardinalhq.level", "chq_level"},
		{"log.level", "log_level"},
		{"no_dots_here", "no_dots_here"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := normalizeLabelName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEscapeLogQLValue(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`simple value`, `simple value`},
		{`value with "quotes"`, `value with \"quotes\"`},
		{`value with \ backslash`, `value with \\ backslash`},
		{`both "quotes" and \ backslash`, `both \"quotes\" and \\ backslash`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := escapeLogQLValue(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEscapeRegexValue(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`simple-value`, `simple-value`},
		{`value.with.dots`, `value\\.with\\.dots`},
		{`value_with_underscores`, `value_with_underscores`},
		{`chewy.com-abu-8qt6qskwan4-1692736963.994821_2025-09-24-074731_controller`, `chewy\\.com-abu-8qt6qskwan4-1692736963\\.994821_2025-09-24-074731_controller`},
		{`special*chars?here`, `special\\*chars\\?here`},
		{`brackets[test]`, `brackets\\[test\\]`},
		{`parens(test)`, `parens\\(test\\)`},
		{`braces{test}`, `braces\\{test\\}`},
		{`caret^test`, `caret\\^test`},
		{`dollar$test`, `dollar\\$test`},
		{`pipe|test`, `pipe\\|test`},
		{`backslash\test`, `backslash\\\test`},
		{`plus+test`, `plus\\+test`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := escapeRegexValue(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTranslateToLogQL_InOperatorWithSpecialChars(t *testing.T) {
	baseExpr := BaseExpression{
		Dataset: "logs",
		Filter: BinaryClause{
			Op: "and",
			Clauses: []QueryClause{
				Filter{
					K:  "resource.bucket.name",
					V:  []string{"avxit-dev-s3-use2-datalake"},
					Op: "eq",
				},
				Filter{
					K:  "resource.file",
					V:  []string{"chewy.com-abu-8qt6qskwan4-1692736963.994821_2025-09-24-074731_controller"},
					Op: "in",
				},
			},
		},
	}

	logql, _, err := TranslateToLogQL(baseExpr)
	require.NoError(t, err)
	// Single value in "in" operator should use exact match, not regex
	assert.Equal(t, `{resource_bucket_name="avxit-dev-s3-use2-datalake",resource_file="chewy.com-abu-8qt6qskwan4-1692736963.994821_2025-09-24-074731_controller"}`, logql)
}

func TestTranslateToLogQL_InOperatorMultipleValues(t *testing.T) {
	baseExpr := BaseExpression{
		Dataset: "logs",
		Filter: Filter{
			K:  "resource.file",
			V:  []string{"file1.log", "file2.log", "file.with.dots.log"},
			Op: "in",
		},
	}

	logql, _, err := TranslateToLogQL(baseExpr)
	require.NoError(t, err)
	// Multiple values should use regex with anchored OR pattern
	assert.Equal(t, `{resource_file=~"^(file1\\.log|file2\\.log|file\\.with\\.dots\\.log)$"}`, logql)
}

func TestTranslateToLogQL_CustomerIssue_FileTypeFilter(t *testing.T) {
	// Reproduces issue where filtering for resource.file.type with exact value
	// should use exact match when single value in "in" operator
	baseExpr := BaseExpression{
		Dataset: "logs",
		Filter: BinaryClause{
			Op: "and",
			Clauses: []QueryClause{
				Filter{
					K:  "resource.bucket.name",
					V:  []string{"test-datalake-bucket"},
					Op: "eq",
				},
				Filter{
					K:  "resource.file",
					V:  []string{"example.com-id-12345_2025-10-23-193952_server-name"},
					Op: "in",
				},
				Filter{
					K:  "resource.file.type",
					V:  []string{"statesync"},
					Op: "in",
				},
			},
		},
	}

	logql, _, err := TranslateToLogQL(baseExpr)
	require.NoError(t, err)

	// Expected: all three filters should be in the stream selector
	// Single values in "in" operator should use exact match
	assert.Contains(t, logql, `resource_bucket_name="test-datalake-bucket"`)
	assert.Contains(t, logql, `resource_file="example.com-id-12345_2025-10-23-193952_server-name"`)
	assert.Contains(t, logql, `resource_file_type="statesync"`)
}

func TestTranslateToLogQL_ContainsOperator_FileType(t *testing.T) {
	// Test contains operator at label level for resource.file.type
	baseExpr := BaseExpression{
		Dataset: "logs",
		Filter: BinaryClause{
			Op: "and",
			Clauses: []QueryClause{
				Filter{
					K:  "resource.bucket.name",
					V:  []string{"test-bucket"},
					Op: "eq",
				},
				Filter{
					K:  "resource.file",
					V:  []string{"example-file-1234567890_2025-10-28-221611_server"},
					Op: "in",
				},
				Filter{
					K:  "resource.file.type",
					V:  []string{"cmd"},
					Op: "contains",
				},
			},
		},
	}

	logql, _, err := TranslateToLogQL(baseExpr)
	require.NoError(t, err)

	// Expected: all three filters should be in the stream selector
	// Contains should create a label matcher with substring match
	assert.Contains(t, logql, `resource_bucket_name="test-bucket"`)
	assert.Contains(t, logql, `resource_file="example-file-1234567890_2025-10-28-221611_server"`)
	assert.Contains(t, logql, `resource_file_type=~".*cmd.*"`)
}
