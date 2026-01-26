// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package promql

import (
	"strings"
	"testing"
	"time"
)

func TestFieldNameNormalization(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "dots to underscores",
			input:    "resource.k8s.namespace.name",
			expected: "resource_k8s_namespace_name",
		},
		{
			name:     "already has underscores",
			input:    "resource_service_name",
			expected: "resource_service_name",
		},
		{
			name:     "mixed dots and underscores",
			input:    "resource.k8s_pod.name",
			expected: "resource_k8s_pod_name",
		},
		{
			name:     "chq fields unchanged",
			input:    "chq_timestamp",
			expected: "chq_timestamp",
		},
		{
			name:     "chq fields with underscores unchanged",
			input:    "chq_level",
			expected: "chq_level",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeFieldName(tt.input)
			if result != tt.expected {
				t.Errorf("normalizeFieldName(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestSQLGenerationWithNormalizedFields(t *testing.T) {
	be := &BaseExpr{
		Metric: "test_metric",
		Matchers: []LabelMatch{
			{Label: "resource.k8s.namespace.name", Op: MatchEq, Value: "default"},
			{Label: "resource.service.name", Op: MatchEq, Value: "api"},
			{Label: "metric_name", Op: MatchEq, Value: "test"},
		},
		GroupBy: []string{"resource.k8s.pod.name", "metric.http.status"},
	}

	sql := be.ToWorkerSQL(10 * time.Second)

	// Check that dots are converted to underscores in WHERE clause
	if !strings.Contains(sql, `"resource_k8s_namespace_name" = 'default'`) {
		t.Errorf("Expected normalized field name in WHERE clause, got: %s", sql)
	}
	if !strings.Contains(sql, `"resource_service_name" = 'api'`) {
		t.Errorf("Expected normalized field name in WHERE clause, got: %s", sql)
	}

	// Check that dots are converted to underscores in GROUP BY
	if !strings.Contains(sql, `"resource_k8s_pod_name"`) {
		t.Errorf("Expected normalized field name in GROUP BY, got: %s", sql)
	}
	if !strings.Contains(sql, `"metric_http_status"`) {
		t.Errorf("Expected normalized field name in GROUP BY, got: %s", sql)
	}

	// Check that chq fields are preserved
	if !strings.Contains(sql, `"metric_name"`) {
		t.Errorf("Expected chq field to be preserved, got: %s", sql)
	}
}
