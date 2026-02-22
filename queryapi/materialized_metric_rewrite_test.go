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

package queryapi

import (
	"testing"

	"github.com/cardinalhq/lakerunner/internal/configservice"
	"github.com/cardinalhq/lakerunner/promql"
)

func TestRewritePromExprWithMaterializedRules_RewritesSelector(t *testing.T) {
	expr, err := promql.FromPromQL(`sum(rate(log_events{log_level="error",resource_service_name="payments"}[5m]))`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	rewritten, count := rewritePromExprWithMaterializedRules(expr, []materializedMetricRule{
		{
			sourceMetric:       "log_events",
			materializedMetric: "log_events_materialized",
			requiredMatchers: []promql.LabelMatch{
				{Label: "log_level", Op: promql.MatchEq, Value: "error"},
			},
		},
	})

	if count != 1 {
		t.Fatalf("rewrite count mismatch: got=%d want=1", count)
	}

	plan, err := promql.Compile(rewritten)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("leaf count mismatch: got=%d want=1", len(plan.Leaves))
	}
	if plan.Leaves[0].Metric != "log_events_materialized" {
		t.Fatalf("metric mismatch: got=%q want=%q", plan.Leaves[0].Metric, "log_events_materialized")
	}
}

func TestRewritePromExprWithMaterializedRules_RequiresMatchers(t *testing.T) {
	expr, err := promql.FromPromQL(`sum(rate(log_events{resource_service_name="payments"}[5m]))`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	rewritten, count := rewritePromExprWithMaterializedRules(expr, []materializedMetricRule{
		{
			sourceMetric:       "log_events",
			materializedMetric: "log_events_materialized",
			requiredMatchers: []promql.LabelMatch{
				{Label: "log_level", Op: promql.MatchEq, Value: "error"},
			},
		},
	})
	if count != 0 {
		t.Fatalf("rewrite count mismatch: got=%d want=0", count)
	}

	plan, err := promql.Compile(rewritten)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("leaf count mismatch: got=%d want=1", len(plan.Leaves))
	}
	if plan.Leaves[0].Metric != "log_events" {
		t.Fatalf("metric mismatch: got=%q want=%q", plan.Leaves[0].Metric, "log_events")
	}
}

func TestRewritePromExprWithMaterializedRules_PicksMostSpecificRule(t *testing.T) {
	expr, err := promql.FromPromQL(`sum(rate(log_events{log_level="error",resource_service_name="payments"}[5m]))`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	rewritten, count := rewritePromExprWithMaterializedRules(expr, []materializedMetricRule{
		{
			sourceMetric:       "log_events",
			materializedMetric: "log_events_generic",
		},
		{
			sourceMetric:       "log_events",
			materializedMetric: "log_events_errors",
			requiredMatchers: []promql.LabelMatch{
				{Label: "log_level", Op: promql.MatchEq, Value: "error"},
			},
		},
	})
	if count != 1 {
		t.Fatalf("rewrite count mismatch: got=%d want=1", count)
	}

	plan, err := promql.Compile(rewritten)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("leaf count mismatch: got=%d want=1", len(plan.Leaves))
	}
	if plan.Leaves[0].Metric != "log_events_errors" {
		t.Fatalf("metric mismatch: got=%q want=%q", plan.Leaves[0].Metric, "log_events_errors")
	}
}

func TestRewritePromExprWithMaterializedRules_RewritesBinarySides(t *testing.T) {
	expr, err := promql.FromPromQL(`rate(http_requests_total{env="prod"}[1m]) / rate(http_requests_total{env="dev"}[1m])`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	rewritten, count := rewritePromExprWithMaterializedRules(expr, []materializedMetricRule{
		{
			sourceMetric:       "http_requests_total",
			materializedMetric: "http_requests_materialized",
		},
	})
	if count != 2 {
		t.Fatalf("rewrite count mismatch: got=%d want=2", count)
	}

	plan, err := promql.Compile(rewritten)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(plan.Leaves) != 2 {
		t.Fatalf("leaf count mismatch: got=%d want=2", len(plan.Leaves))
	}
	for i := range plan.Leaves {
		if plan.Leaves[i].Metric != "http_requests_materialized" {
			t.Fatalf("metric mismatch at leaf %d: got=%q want=%q", i, plan.Leaves[i].Metric, "http_requests_materialized")
		}
	}
}

func TestRewritePromExprWithMaterializedRules_DropsLeafMatcherForSyntheticLogs(t *testing.T) {
	expr, err := promql.FromPromQL(`rate(__logql_logs_total{__leaf="leaf-1",log_level="error"}[1m])`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	rewritten, count := rewritePromExprWithMaterializedRules(expr, []materializedMetricRule{
		{
			sourceMetric:       promql.SynthLogCount,
			materializedMetric: "log_events_materialized",
		},
	})
	if count != 1 {
		t.Fatalf("rewrite count mismatch: got=%d want=1", count)
	}

	plan, err := promql.Compile(rewritten)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("leaf count mismatch: got=%d want=1", len(plan.Leaves))
	}
	if plan.Leaves[0].Metric != "log_events_materialized" {
		t.Fatalf("metric mismatch: got=%q want=%q", plan.Leaves[0].Metric, "log_events_materialized")
	}
	for i := range plan.Leaves[0].Matchers {
		if plan.Leaves[0].Matchers[i].Label == promql.LeafMatcher {
			t.Fatalf("unexpected %q matcher remained after rewrite", promql.LeafMatcher)
		}
	}
}

func TestMaterializedMetricRulesFromCatalog(t *testing.T) {
	cfg := configservice.ExpressionCatalogConfig{
		Metrics: []configservice.ExpressionCatalogEntry{
			{
				ID:                 "m1",
				Metric:             " log_events ",
				MaterializedMetric: " log_events_materialized ",
				Matchers: []configservice.ExpressionCatalogMatcher{
					{Label: "log_level", Op: "=", Value: "error"},
					{Label: "ignored", Op: "???", Value: "x"},
				},
			},
			{
				ID:                 "missing_source",
				MaterializedMetric: "mat",
			},
		},
	}

	rules := materializedMetricRulesFromCatalog(cfg)
	if len(rules) != 1 {
		t.Fatalf("rule count mismatch: got=%d want=1", len(rules))
	}
	if rules[0].sourceMetric != "log_events" {
		t.Fatalf("source metric mismatch: got=%q", rules[0].sourceMetric)
	}
	if rules[0].materializedMetric != "log_events_materialized" {
		t.Fatalf("materialized metric mismatch: got=%q", rules[0].materializedMetric)
	}
	if len(rules[0].requiredMatchers) != 1 {
		t.Fatalf("required matcher count mismatch: got=%d want=1", len(rules[0].requiredMatchers))
	}
}
