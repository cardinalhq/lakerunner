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
	// _cardinalhq.message maps to log_message for backward compatibility
	assert.Equal(t, `{log_message=~".*error occurred.*"}`, logql)
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
		{"_cardinalhq.level", "log_level"},     // Backward compatibility: maps to log_level
		{"_cardinalhq.message", "log_message"}, // Backward compatibility: maps to log_message
		{"log.level", "log_level"},
		{"no_dots_here", "no_dots_here"},
		// Kubernetes labels with slashes
		{"app.kubernetes.io/component", "app_kubernetes_io_component"},
		{"app.kubernetes.io/name", "app_kubernetes_io_name"},
		// Uppercase becomes lowercase
		{"Resource.Service.Name", "resource_service_name"},
		{"HTTP_STATUS", "http_status"},
		// Dashes become underscores
		{"my-label-name", "my_label_name"},
		// Colons become underscores
		{"namespace:pod", "namespace_pod"},
		// Mixed special characters
		{"a/b:c-d.e", "a_b_c_d_e"},
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

func TestTranslateToLogQL_InOperatorThreeFileNames(t *testing.T) {
	// Reproduces issue where multiple file names with dots in 'in' operator should be regex escaped
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
					K: "resource.file",
					V: []string{
						"example.com-1234567890.11_2025-10-29-170129_server-04",
						"example.com-1234567890.11_2025-10-29-170129_controller",
						"example.com-1234567890.11_2025-10-29-170129_server-03",
					},
					Op: "in",
				},
			},
		},
	}

	logql, _, err := TranslateToLogQL(baseExpr)
	require.NoError(t, err)

	// Expected: bucket name should be exact match, file names should be regex OR with escaped dots
	assert.Contains(t, logql, `resource_bucket_name="test-bucket"`)
	assert.Contains(t, logql, `resource_file=~"^(example\\.com-1234567890\\.11_2025-10-29-170129_server-04|example\\.com-1234567890\\.11_2025-10-29-170129_controller|example\\.com-1234567890\\.11_2025-10-29-170129_server-03)$"`)
}

func TestGetIntervalForTimeRange(t *testing.T) {
	tests := []struct {
		name     string
		startMs  int64
		endMs    int64
		expected string
	}{
		{
			name:     "30 minutes - should use 10s interval",
			startMs:  1761523200000, // some base time in ms
			endMs:    1761525000000, // +30 minutes
			expected: "10s",
		},
		{
			name:     "1 hour - should use 10s interval",
			startMs:  1761523200000,
			endMs:    1761526800000, // +1 hour
			expected: "10s",
		},
		{
			name:     "6 hours - should use 1m interval",
			startMs:  1761523200000,
			endMs:    1761544800000, // +6 hours
			expected: "1m",
		},
		{
			name:     "12 hours - should use 1m interval",
			startMs:  1761523200000,
			endMs:    1761566400000, // +12 hours
			expected: "1m",
		},
		{
			name:     "19.3 hours (customer query) - should use 5m interval",
			startMs:  1761523200000,
			endMs:    1761592809368, // +19.3 hours (actual customer query)
			expected: "5m",
		},
		{
			name:     "24 hours - should use 5m interval",
			startMs:  1761523200000,
			endMs:    1761609600000, // +24 hours
			expected: "5m",
		},
		{
			name:     "2 days - should use 20m interval",
			startMs:  1761523200000,
			endMs:    1761696000000, // +2 days
			expected: "20m",
		},
		{
			name:     "3 days - should use 20m interval",
			startMs:  1761523200000,
			endMs:    1761782400000, // +3 days
			expected: "20m",
		},
		{
			name:     "7 days - should use 1h interval",
			startMs:  1761523200000,
			endMs:    1762128000000, // +7 days
			expected: "1h",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getIntervalForTimeRange(tt.startMs, tt.endMs)
			assert.Equal(t, tt.expected, result)
		})
	}
}
