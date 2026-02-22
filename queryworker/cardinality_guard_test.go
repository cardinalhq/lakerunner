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

package queryworker

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/promql"
	"github.com/cardinalhq/lakerunner/queryapi"
)

func TestShouldGuardGroupCardinality(t *testing.T) {
	validBE := promql.BaseExpr{
		Metric:  "cpu_usage",
		GroupBy: []string{"pod"},
	}

	tests := []struct {
		name string
		req  queryapi.PushDownRequest
		want bool
	}{
		{
			name: "no base expr",
			req:  queryapi.PushDownRequest{},
			want: false,
		},
		{
			name: "log leaf query",
			req: queryapi.PushDownRequest{
				BaseExpr: &promql.BaseExpr{
					GroupBy: []string{"pod"},
					LogLeaf: &logql.LogLeaf{},
				},
			},
			want: false,
		},
		{
			name: "no group by",
			req: queryapi.PushDownRequest{
				BaseExpr: &promql.BaseExpr{Metric: "cpu_usage"},
			},
			want: false,
		},
		{
			name: "tag value query",
			req: queryapi.PushDownRequest{
				BaseExpr: &validBE,
				TagName:  "pod",
			},
			want: false,
		},
		{
			name: "tag names query",
			req: queryapi.PushDownRequest{
				BaseExpr: &validBE,
				TagNames: true,
			},
			want: false,
		},
		{
			name: "summary query",
			req: queryapi.PushDownRequest{
				BaseExpr:  &validBE,
				IsSummary: true,
			},
			want: false,
		},
		{
			name: "regular grouped metrics query",
			req: queryapi.PushDownRequest{
				BaseExpr: &validBE,
			},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, shouldGuardGroupCardinality(tc.req))
		})
	}
}

func TestBuildCardinalityEstimateSQL_SingleGroupBy(t *testing.T) {
	be := promql.BaseExpr{
		Metric:  "cpu.utilization",
		GroupBy: []string{"resource.k8s.pod.name"},
		Matchers: []promql.LabelMatch{
			{Label: "env", Op: promql.MatchEq, Value: "prod"},
			{Label: promql.LeafMatcher, Op: promql.MatchEq, Value: "leaf-1"},
		},
	}
	req := queryapi.PushDownRequest{
		BaseExpr: &be,
		StartTs:  1000,
		EndTs:    2000,
	}

	sql, ok := buildCardinalityEstimateSQL(req, "read_parquet(['a'])")
	assert.True(t, ok)
	assert.Contains(t, sql, `approx_count_distinct(COALESCE(CAST("resource_k8s_pod_name" AS VARCHAR), '__null__'))`)
	assert.Contains(t, sql, `"chq_timestamp" >= 1000`)
	assert.Contains(t, sql, `"chq_timestamp" < 2000`)
	assert.Contains(t, sql, `"metric_name" = 'cpu.utilization'`)
	assert.Contains(t, sql, `"env" = 'prod'`)
	assert.False(t, strings.Contains(sql, promql.LeafMatcher))
}

func TestBuildCardinalityEstimateSQL_MultiGroupBy(t *testing.T) {
	be := promql.BaseExpr{
		Metric:  "http_requests_total",
		GroupBy: []string{"service", "resource.k8s.namespace.name"},
		Matchers: []promql.LabelMatch{
			{Label: "cluster", Op: promql.MatchRe, Value: "prod-.*"},
		},
	}
	req := queryapi.PushDownRequest{
		BaseExpr: &be,
		StartTs:  1,
		EndTs:    2,
	}

	sql, ok := buildCardinalityEstimateSQL(req, "read_parquet(['b'])")
	assert.True(t, ok)
	assert.Contains(t, sql, `approx_count_distinct(hash(`)
	assert.Contains(t, sql, `CAST("service" AS VARCHAR)`)
	assert.Contains(t, sql, `CAST("resource_k8s_namespace_name" AS VARCHAR)`)
	assert.Contains(t, sql, `"cluster" ~ 'prod-.*'`)
}

func TestCardinalityLimitError_Unwrap(t *testing.T) {
	err := &CardinalityLimitError{
		Estimated: 250,
		Limit:     100,
		GroupBy:   []string{"pod"},
	}
	assert.True(t, errors.Is(err, ErrCardinalityLimitExceeded))
	assert.Contains(t, err.Error(), "estimated cardinality 250 exceeds limit 100")
}
