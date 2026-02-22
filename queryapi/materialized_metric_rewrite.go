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
	"context"
	"strings"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/configservice"
	"github.com/cardinalhq/lakerunner/promql"
)

type materializedMetricRule struct {
	sourceMetric       string
	materializedMetric string
	requiredMatchers   []promql.LabelMatch
}

func rewritePromExprWithMaterializedCatalog(ctx context.Context, orgID uuid.UUID, expr promql.Expr) (promql.Expr, int) {
	cfgSvc := configservice.MaybeGlobal()
	if cfgSvc == nil {
		return expr, 0
	}

	cfg := cfgSvc.GetExpressionCatalogConfig(ctx, orgID)
	rules := materializedMetricRulesFromCatalog(cfg)
	return rewritePromExprWithMaterializedRules(expr, rules)
}

func materializedMetricRulesFromCatalog(cfg configservice.ExpressionCatalogConfig) []materializedMetricRule {
	if len(cfg.Metrics) == 0 {
		return nil
	}

	rules := make([]materializedMetricRule, 0, len(cfg.Metrics))
	for i := range cfg.Metrics {
		entry := cfg.Metrics[i]
		sourceMetric := strings.TrimSpace(entry.Metric)
		materializedMetric := strings.TrimSpace(entry.MaterializedMetric)
		if sourceMetric == "" || materializedMetric == "" || sourceMetric == materializedMetric {
			continue
		}

		rule := materializedMetricRule{
			sourceMetric:       sourceMetric,
			materializedMetric: materializedMetric,
		}

		for j := range entry.Matchers {
			m := entry.Matchers[j]
			op, ok := promMatchOpFromString(strings.TrimSpace(m.Op))
			if !ok {
				continue
			}
			label := strings.TrimSpace(m.Label)
			if label == "" {
				continue
			}
			rule.requiredMatchers = append(rule.requiredMatchers, promql.LabelMatch{
				Label: label,
				Op:    op,
				Value: strings.TrimSpace(m.Value),
			})
		}

		rules = append(rules, rule)
	}

	if len(rules) == 0 {
		return nil
	}
	return rules
}

func promMatchOpFromString(op string) (promql.MatchOp, bool) {
	switch op {
	case string(promql.MatchEq):
		return promql.MatchEq, true
	case string(promql.MatchNe):
		return promql.MatchNe, true
	case string(promql.MatchRe):
		return promql.MatchRe, true
	case string(promql.MatchNre):
		return promql.MatchNre, true
	default:
		return "", false
	}
}

func rewritePromExprWithMaterializedRules(expr promql.Expr, rules []materializedMetricRule) (promql.Expr, int) {
	if len(rules) == 0 {
		return expr, 0
	}

	rewritten := expr
	rewrites := rewritePromExprInPlace(&rewritten, rules)
	return rewritten, rewrites
}

func rewritePromExprInPlace(expr *promql.Expr, rules []materializedMetricRule) int {
	if expr == nil {
		return 0
	}

	switch expr.Kind {
	case promql.KindSelector:
		return rewriteSelectorMetric(expr.Selector, rules)

	case promql.KindRange:
		if expr.Range == nil {
			return 0
		}
		return rewritePromExprInPlace(&expr.Range.Expr, rules)

	case promql.KindFunc:
		if expr.Func == nil || expr.Func.Expr == nil {
			return 0
		}
		return rewritePromExprInPlace(expr.Func.Expr, rules)

	case promql.KindAgg:
		if expr.Agg == nil {
			return 0
		}
		return rewritePromExprInPlace(&expr.Agg.Expr, rules)

	case promql.KindTopK:
		if expr.TopK == nil {
			return 0
		}
		return rewritePromExprInPlace(&expr.TopK.Expr, rules)

	case promql.KindBottomK:
		if expr.BottomK == nil {
			return 0
		}
		return rewritePromExprInPlace(&expr.BottomK.Expr, rules)

	case promql.KindBinary:
		if expr.BinOp == nil {
			return 0
		}
		left := rewritePromExprInPlace(&expr.BinOp.LHS, rules)
		right := rewritePromExprInPlace(&expr.BinOp.RHS, rules)
		return left + right

	case promql.KindHistogramQuantile:
		if expr.HistQuant == nil {
			return 0
		}
		return rewritePromExprInPlace(&expr.HistQuant.Expr, rules)

	case promql.KindClampMin:
		if expr.ClampMin == nil {
			return 0
		}
		return rewritePromExprInPlace(&expr.ClampMin.Expr, rules)

	case promql.KindClampMax:
		if expr.ClampMax == nil {
			return 0
		}
		return rewritePromExprInPlace(&expr.ClampMax.Expr, rules)
	}

	return 0
}

func rewriteSelectorMetric(sel *promql.Selector, rules []materializedMetricRule) int {
	if sel == nil || sel.Metric == "" {
		return 0
	}
	sourceMetric := sel.Metric
	nextMetric := selectMaterializedMetric(*sel, rules)
	if nextMetric == "" || nextMetric == sel.Metric {
		return 0
	}
	sel.Metric = nextMetric
	if isSyntheticLogMetric(sourceMetric) {
		sel.Matchers = dropSelectorLabelMatchers(sel.Matchers, promql.LeafMatcher)
	}
	return 1
}

func isSyntheticLogMetric(metric string) bool {
	switch metric {
	case promql.SynthLogCount, promql.SynthLogBytes, promql.SynthLogUnwrap:
		return true
	default:
		return false
	}
}

func dropSelectorLabelMatchers(in []promql.LabelMatch, label string) []promql.LabelMatch {
	if len(in) == 0 {
		return in
	}
	out := make([]promql.LabelMatch, 0, len(in))
	for i := range in {
		m := in[i]
		if m.Label == label {
			continue
		}
		out = append(out, m)
	}
	return out
}

func selectMaterializedMetric(sel promql.Selector, rules []materializedMetricRule) string {
	bestIdx := -1
	bestSpecificity := -1

	for i := range rules {
		rule := rules[i]
		if rule.sourceMetric != sel.Metric {
			continue
		}
		if !selectorHasRequiredMatchers(sel.Matchers, rule.requiredMatchers) {
			continue
		}
		if len(rule.requiredMatchers) > bestSpecificity {
			bestIdx = i
			bestSpecificity = len(rule.requiredMatchers)
		}
	}

	if bestIdx < 0 {
		return ""
	}
	return rules[bestIdx].materializedMetric
}

func selectorHasRequiredMatchers(got []promql.LabelMatch, required []promql.LabelMatch) bool {
	if len(required) == 0 {
		return true
	}

	for i := range required {
		req := required[i]
		found := false
		for j := range got {
			have := got[j]
			if have.Label == req.Label && have.Op == req.Op && have.Value == req.Value {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
