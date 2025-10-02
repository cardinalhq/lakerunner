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
	assert.Equal(t, `{log_level=~"error|warn|fatal"}`, logql)
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
	// Contains creates a line filter for text search in log messages
	// Note: empty {} because contains is a pipeline operation, not a stream selector
	assert.Equal(t, `{} |~ "error occurred"`, logql)
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
			Q1: Filter{
				K:  "resource.service.name",
				V:  []string{"my-service"},
				Op: "eq",
			},
			Q2: Filter{
				K:  "log.level",
				V:  []string{"error"},
				Op: "eq",
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
		{"_cardinalhq.fingerprint", "_cardinalhq_fingerprint"},
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
