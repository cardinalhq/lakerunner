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

package expressionindex

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/configservice"
)

func TestBuildExpressionsFromCatalogConfig(t *testing.T) {
	cfg := configservice.ExpressionCatalogConfig{
		Metrics: []configservice.ExpressionCatalogEntry{
			{
				ID:     "m1",
				Metric: "cpu_usage",
				Matchers: []configservice.ExpressionCatalogMatcher{
					{Label: "env", Op: "=", Value: "prod"},
					{Label: "region", Op: "??", Value: "x"},
				},
			},
		},
		Logs: []configservice.ExpressionCatalogEntry{
			{
				ID: "l1",
				Matchers: []configservice.ExpressionCatalogMatcher{
					{Label: "service", Op: "=~", Value: "api-.*"},
				},
			},
		},
		PromQL: []configservice.ExpressionCatalogPromQLQuery{
			{Query: `sum by (service) (rate(http_requests_total{env="prod"}[5m]))`},
		},
		LogQL: []configservice.ExpressionCatalogLogQLQuery{
			{Signal: "logs", Query: `{service="payments"} |= "error"`},
		},
	}

	got := BuildExpressionsFromCatalogConfig(cfg)
	if len(got) < 4 {
		t.Fatalf("unexpected expression count: got=%d want>=4", len(got))
	}

	m := map[string]Expression{}
	for i := range got {
		m[got[i].ID] = got[i]
	}

	if m["m1"].Signal != SignalMetrics || m["m1"].Metric != "cpu_usage" {
		t.Fatalf("unexpected metrics expression: %+v", m["m1"])
	}
	if len(m["m1"].Matchers) != 1 {
		t.Fatalf("unexpected metrics matchers: %+v", m["m1"].Matchers)
	}

	if m["l1"].Signal != SignalLogs {
		t.Fatalf("unexpected logs expression: %+v", m["l1"])
	}
}

func TestCatalogRefresherMaybeRefresh(t *testing.T) {
	orgID := uuid.New()
	loadCount := 0
	cfg := configservice.ExpressionCatalogConfig{
		Metrics: []configservice.ExpressionCatalogEntry{
			{
				ID:     "m1",
				Metric: "cpu_usage",
				Matchers: []configservice.ExpressionCatalogMatcher{
					{Label: "env", Op: "=", Value: "prod"},
				},
			},
		},
	}

	ref := NewCatalogRefresher(
		NewExpressionCache(),
		100*time.Millisecond,
		func(context.Context, uuid.UUID) configservice.ExpressionCatalogConfig {
			loadCount++
			return cfg
		},
	)

	ctx := context.Background()
	if err := ref.MaybeRefreshOrg(ctx, orgID); err != nil {
		t.Fatalf("first refresh failed: %v", err)
	}
	if loadCount != 1 {
		t.Fatalf("load count mismatch after first refresh: got=%d want=1", loadCount)
	}

	// Within TTL, should not refresh.
	if err := ref.MaybeRefreshOrg(ctx, orgID); err != nil {
		t.Fatalf("second refresh failed: %v", err)
	}
	if loadCount != 1 {
		t.Fatalf("load count mismatch within interval: got=%d want=1", loadCount)
	}

	time.Sleep(120 * time.Millisecond)
	if err := ref.MaybeRefreshOrg(ctx, orgID); err != nil {
		t.Fatalf("third refresh failed: %v", err)
	}
	if loadCount != 2 {
		t.Fatalf("load count mismatch after interval: got=%d want=2", loadCount)
	}

	candidates := ref.Cache().FindCandidates(orgID.String(), SignalMetrics, "cpu_usage", map[string]string{"env": "prod"})
	if !slices.Equal(ids(candidates), []string{"m1"}) {
		t.Fatalf("unexpected candidates: %v", ids(candidates))
	}
}
