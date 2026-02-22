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

package configservice

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetExpressionCatalogConfig_FallbackChain(t *testing.T) {
	ctx := context.Background()

	t.Run("falls back to system default", func(t *testing.T) {
		mock := newMockQuerier()
		svc := New(mock, 5*time.Minute)
		t.Cleanup(svc.Close)

		orgID := uuid.New()
		defaultCfg := `{"metrics":[{"id":"m-default","metric":"cpu_usage"}]}`
		mock.configs[mock.key(DefaultOrgID, configKeyExpressionCatalog)] = json.RawMessage(defaultCfg)

		cfg := svc.GetExpressionCatalogConfig(ctx, orgID)
		require.Len(t, cfg.Metrics, 1)
		assert.Equal(t, "m-default", cfg.Metrics[0].ID)
	})

	t.Run("org-specific takes precedence", func(t *testing.T) {
		mock := newMockQuerier()
		svc := New(mock, 5*time.Minute)
		t.Cleanup(svc.Close)

		orgID := uuid.New()
		defaultCfg := `{"metrics":[{"id":"m-default","metric":"cpu_usage"}]}`
		mock.configs[mock.key(DefaultOrgID, configKeyExpressionCatalog)] = json.RawMessage(defaultCfg)

		orgCfg := `{"metrics":[{"id":"m-org","metric":"memory_usage"}]}`
		mock.configs[mock.key(orgID, configKeyExpressionCatalog)] = json.RawMessage(orgCfg)

		cfg := svc.GetExpressionCatalogConfig(ctx, orgID)
		require.Len(t, cfg.Metrics, 1)
		assert.Equal(t, "m-org", cfg.Metrics[0].ID)
	})
}

func TestExpressionCatalogConfig_Normalization(t *testing.T) {
	cfg, ok := parseExpressionCatalogConfig(json.RawMessage(`{
		"metrics":[
			{"id":"", "metric":"cpu"},
			{"id":" m1 ","metric":" cpu ","materialized_metric":" cpu_materialized ","matchers":[{"label":"env","op":"=","value":"prod"},{"label":"","op":"=","value":"x"}]}
		],
		"derived_metrics":[
			{"id":"dm-empty-name","source_signal":"logs","metric_name":""},
			{"id":"dm-bad-signal","source_signal":"foo","metric_name":"x"},
			{"id":"dm-logs","source_signal":"LOGS","metric_name":"log_events"},
			{"id":"dm-metrics","source_signal":"metrics","metric_name":"http_rate"}
		],
		"promql":[{"query":"sum(rate(http_requests_total[5m]))"},{"query":""}],
		"logql":[{"query":"{service=\"api\"}"},{"query":""}]
	}`))
	require.True(t, ok)
	require.Len(t, cfg.Metrics, 1)
	assert.Equal(t, "m1", cfg.Metrics[0].ID)
	assert.Equal(t, "cpu", cfg.Metrics[0].Metric)
	assert.Equal(t, "cpu_materialized", cfg.Metrics[0].MaterializedMetric)
	require.Len(t, cfg.Metrics[0].Matchers, 1)
	assert.Equal(t, "env", cfg.Metrics[0].Matchers[0].Label)
	require.Len(t, cfg.DerivedMetrics, 2)
	assert.Equal(t, "logs", cfg.DerivedMetrics[0].SourceSignal)
	assert.Equal(t, "log_events", cfg.DerivedMetrics[0].MetricName)
	assert.Equal(t, "metrics", cfg.DerivedMetrics[1].SourceSignal)
	assert.Equal(t, "http_rate", cfg.DerivedMetrics[1].MetricName)
	require.Len(t, cfg.PromQL, 1)
	require.Len(t, cfg.LogQL, 1)
}

func TestExpressionCatalogConfigDirectCRUD(t *testing.T) {
	ctx := context.Background()
	mock := newMockQuerier()
	orgID := uuid.New()

	in := ExpressionCatalogConfig{
		Metrics: []ExpressionCatalogEntry{
			{
				ID:                 "m1",
				Metric:             "cpu_usage",
				MaterializedMetric: "cpu_usage_materialized",
				Matchers: []ExpressionCatalogMatcher{
					{Label: "env", Op: "=", Value: "prod"},
				},
			},
		},
		DerivedMetrics: []ExpressionCatalogDerivedMetric{
			{
				ID:           "dm1",
				SourceSignal: "logs",
				MetricName:   "log_events",
			},
		},
	}

	require.NoError(t, SetExpressionCatalogConfigDirect(ctx, mock, orgID, in))

	got, err := GetExpressionCatalogConfigDirect(ctx, mock, orgID)
	require.NoError(t, err)
	assert.False(t, got.IsDefault)
	require.Len(t, got.Config.Metrics, 1)
	assert.Equal(t, "m1", got.Config.Metrics[0].ID)
	assert.Equal(t, "cpu_usage_materialized", got.Config.Metrics[0].MaterializedMetric)
	require.Len(t, got.Config.DerivedMetrics, 1)
	assert.Equal(t, "logs", got.Config.DerivedMetrics[0].SourceSignal)
	assert.Equal(t, "log_events", got.Config.DerivedMetrics[0].MetricName)

	require.NoError(t, DeleteExpressionCatalogConfigDirect(ctx, mock, orgID))
	got, err = GetExpressionCatalogConfigDirect(ctx, mock, orgID)
	require.NoError(t, err)
	assert.True(t, got.IsDefault)
	assert.Len(t, got.Config.Metrics, 0)
}
