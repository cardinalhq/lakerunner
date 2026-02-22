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
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/configservice"
	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/promql"
)

// CatalogLoader loads expression catalog config for an organization.
type CatalogLoader func(context.Context, uuid.UUID) configservice.ExpressionCatalogConfig

// CatalogRefresher keeps an ExpressionCache in sync from catalog config.
type CatalogRefresher struct {
	cache           *ExpressionCache
	refreshInterval time.Duration
	loadCatalog     CatalogLoader

	mu          sync.Mutex
	lastRefresh map[string]time.Time
}

// NewCatalogRefresher constructs a refresher.
// If loadCatalog is nil, it uses configservice.Global() lookups.
func NewCatalogRefresher(
	cache *ExpressionCache,
	refreshInterval time.Duration,
	loadCatalog CatalogLoader,
) *CatalogRefresher {
	if cache == nil {
		cache = NewExpressionCache()
	}
	if loadCatalog == nil {
		loadCatalog = func(ctx context.Context, orgID uuid.UUID) configservice.ExpressionCatalogConfig {
			return configservice.Global().GetExpressionCatalogConfig(ctx, orgID)
		}
	}

	return &CatalogRefresher{
		cache:           cache,
		refreshInterval: refreshInterval,
		loadCatalog:     loadCatalog,
		lastRefresh:     make(map[string]time.Time),
	}
}

func (r *CatalogRefresher) Cache() *ExpressionCache {
	return r.cache
}

// MaybeRefreshOrg refreshes only when refresh interval has elapsed.
func (r *CatalogRefresher) MaybeRefreshOrg(ctx context.Context, orgID uuid.UUID) error {
	orgKey := orgID.String()
	now := time.Now()

	r.mu.Lock()
	last := r.lastRefresh[orgKey]
	if r.refreshInterval > 0 && !last.IsZero() && now.Sub(last) < r.refreshInterval {
		r.mu.Unlock()
		return nil
	}
	// Optimistically mark as refreshed to collapse concurrent callers.
	r.lastRefresh[orgKey] = now
	r.mu.Unlock()

	if err := r.RefreshOrg(ctx, orgID); err != nil {
		// Allow immediate retry on failure.
		r.mu.Lock()
		delete(r.lastRefresh, orgKey)
		r.mu.Unlock()
		return err
	}

	return nil
}

// RefreshOrg unconditionally refreshes one org from config.
func (r *CatalogRefresher) RefreshOrg(ctx context.Context, orgID uuid.UUID) error {
	cfg := r.loadCatalog(ctx, orgID)
	expressions := BuildExpressionsFromCatalogConfig(cfg)
	if err := r.cache.ReplaceOrg(orgID.String(), expressions); err != nil {
		return err
	}

	r.mu.Lock()
	r.lastRefresh[orgID.String()] = time.Now()
	r.mu.Unlock()
	return nil
}

// BuildExpressionsFromCatalogConfig converts configservice catalog format into
// canonical expressions used by QueryIndex.
func BuildExpressionsFromCatalogConfig(cfg configservice.ExpressionCatalogConfig) []Expression {
	total := len(cfg.Metrics) + len(cfg.Logs) + len(cfg.Traces)
	if total == 0 {
		total = len(cfg.PromQL) + len(cfg.LogQL)
		if total == 0 {
			return nil
		}
	}

	out := make([]Expression, 0, total)
	appendSignalEntries := func(signal Signal, entries []configservice.ExpressionCatalogEntry) {
		for i := range entries {
			entry := entries[i]
			if entry.ID == "" {
				continue
			}

			expr := Expression{
				ID:     entry.ID,
				Signal: signal,
				Metric: entry.Metric,
			}

			for j := range entry.Matchers {
				m := entry.Matchers[j]
				if m.Label == "" {
					continue
				}
				op, ok := matchOpFromString(m.Op)
				if !ok {
					continue
				}
				expr.Matchers = append(expr.Matchers, Matcher{
					Label: m.Label,
					Op:    op,
					Value: m.Value,
				})
			}

			out = append(out, expr)
		}
	}

	appendSignalEntries(SignalMetrics, cfg.Metrics)
	appendSignalEntries(SignalLogs, cfg.Logs)
	appendSignalEntries(SignalTraces, cfg.Traces)
	appendPromQLQueries(&out, cfg.PromQL)
	appendLogQLQueries(&out, cfg.LogQL)
	return out
}

func matchOpFromString(op string) (MatchOp, bool) {
	switch op {
	case string(MatchEq):
		return MatchEq, true
	case string(MatchNe):
		return MatchNe, true
	case string(MatchRe):
		return MatchRe, true
	case string(MatchNre):
		return MatchNre, true
	default:
		return "", false
	}
}

func appendPromQLQueries(out *[]Expression, queries []configservice.ExpressionCatalogPromQLQuery) {
	for i := range queries {
		q := strings.TrimSpace(queries[i].Query)
		if q == "" {
			continue
		}

		ast, err := promql.FromPromQL(q)
		if err != nil {
			continue
		}
		plan, err := promql.Compile(ast)
		if err != nil {
			continue
		}

		for j := range plan.Leaves {
			*out = append(*out, ExpressionFromPromBaseExpr(plan.Leaves[j], SignalMetrics))
		}
	}
}

func appendLogQLQueries(out *[]Expression, queries []configservice.ExpressionCatalogLogQLQuery) {
	for i := range queries {
		q := strings.TrimSpace(queries[i].Query)
		if q == "" {
			continue
		}

		signal := SignalLogs
		switch strings.ToLower(strings.TrimSpace(queries[i].Signal)) {
		case "", string(SignalLogs):
			signal = SignalLogs
		case string(SignalTraces):
			signal = SignalTraces
		default:
			continue
		}

		ast, err := logql.FromLogQL(q)
		if err != nil {
			continue
		}
		plan, err := logql.CompileLog(ast)
		if err != nil {
			continue
		}

		for j := range plan.Leaves {
			*out = append(*out, ExpressionFromLogLeaf(plan.Leaves[j], signal))
		}
	}
}
