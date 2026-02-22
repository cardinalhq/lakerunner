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
	"strings"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/configdb"
)

// ExpressionCatalogMatcher defines a normalized label matcher for expression catalog entries.
type ExpressionCatalogMatcher struct {
	Label string `json:"label"`
	Op    string `json:"op"`
	Value string `json:"value"`
}

// ExpressionCatalogEntry defines one indexable expression entry.
type ExpressionCatalogEntry struct {
	ID                 string                     `json:"id"`
	Metric             string                     `json:"metric,omitempty"`
	MaterializedMetric string                     `json:"materialized_metric,omitempty"`
	Matchers           []ExpressionCatalogMatcher `json:"matchers,omitempty"`
}

// ExpressionCatalogPromQLQuery defines a PromQL query that should be compiled
// into one or more metric leaf expressions.
type ExpressionCatalogPromQLQuery struct {
	ID    string `json:"id,omitempty"`
	Query string `json:"query"`
}

// ExpressionCatalogLogQLQuery defines a LogQL query that should be compiled
// into one or more log/trace leaf expressions.
type ExpressionCatalogLogQLQuery struct {
	ID     string `json:"id,omitempty"`
	Query  string `json:"query"`
	Signal string `json:"signal,omitempty"` // logs|traces (default: logs)
}

// ExpressionCatalogDerivedMetric defines a derived metric emission rule.
type ExpressionCatalogDerivedMetric struct {
	ID           string `json:"id,omitempty"`
	SourceSignal string `json:"source_signal"` // logs|metrics|traces
	MetricName   string `json:"metric_name"`
}

// ExpressionCatalogConfig contains active expression definitions by signal.
type ExpressionCatalogConfig struct {
	Metrics []ExpressionCatalogEntry `json:"metrics,omitempty"`
	Logs    []ExpressionCatalogEntry `json:"logs,omitempty"`
	Traces  []ExpressionCatalogEntry `json:"traces,omitempty"`

	// Derived metric emission rules keyed by source signal.
	DerivedMetrics []ExpressionCatalogDerivedMetric `json:"derived_metrics,omitempty"`

	// Optional high-level query inputs.
	PromQL []ExpressionCatalogPromQLQuery `json:"promql,omitempty"`
	LogQL  []ExpressionCatalogLogQLQuery  `json:"logql,omitempty"`
}

func defaultExpressionCatalogConfig() ExpressionCatalogConfig {
	return ExpressionCatalogConfig{}
}

func parseExpressionCatalogConfig(val json.RawMessage) (ExpressionCatalogConfig, bool) {
	if len(val) == 0 {
		return ExpressionCatalogConfig{}, false
	}

	var cfg ExpressionCatalogConfig
	if json.Unmarshal(val, &cfg) != nil {
		return ExpressionCatalogConfig{}, false
	}

	return normalizeExpressionCatalogConfig(cfg), true
}

func normalizeExpressionCatalogConfig(cfg ExpressionCatalogConfig) ExpressionCatalogConfig {
	cfg.Metrics = normalizeExpressionCatalogEntries(cfg.Metrics)
	cfg.Logs = normalizeExpressionCatalogEntries(cfg.Logs)
	cfg.Traces = normalizeExpressionCatalogEntries(cfg.Traces)
	cfg.DerivedMetrics = normalizeExpressionCatalogDerivedMetrics(cfg.DerivedMetrics)
	cfg.PromQL = normalizeExpressionCatalogPromQL(cfg.PromQL)
	cfg.LogQL = normalizeExpressionCatalogLogQL(cfg.LogQL)
	return cfg
}

func normalizeExpressionCatalogDerivedMetrics(in []ExpressionCatalogDerivedMetric) []ExpressionCatalogDerivedMetric {
	if len(in) == 0 {
		return nil
	}

	out := make([]ExpressionCatalogDerivedMetric, 0, len(in))
	for i := range in {
		dm := in[i]
		dm.SourceSignal = strings.ToLower(strings.TrimSpace(dm.SourceSignal))
		dm.MetricName = strings.TrimSpace(dm.MetricName)
		if dm.MetricName == "" {
			continue
		}
		switch dm.SourceSignal {
		case "logs", "metrics", "traces":
		default:
			continue
		}
		out = append(out, dm)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeExpressionCatalogEntries(in []ExpressionCatalogEntry) []ExpressionCatalogEntry {
	if len(in) == 0 {
		return nil
	}

	out := make([]ExpressionCatalogEntry, 0, len(in))
	for i := range in {
		entry := in[i]
		entry.ID = strings.TrimSpace(entry.ID)
		entry.Metric = strings.TrimSpace(entry.Metric)
		entry.MaterializedMetric = strings.TrimSpace(entry.MaterializedMetric)
		if entry.ID == "" {
			continue
		}
		entry.Matchers = normalizeExpressionCatalogMatchers(entry.Matchers)
		out = append(out, entry)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeExpressionCatalogMatchers(in []ExpressionCatalogMatcher) []ExpressionCatalogMatcher {
	if len(in) == 0 {
		return nil
	}

	out := make([]ExpressionCatalogMatcher, 0, len(in))
	for i := range in {
		m := in[i]
		if m.Label == "" {
			continue
		}
		out = append(out, m)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeExpressionCatalogPromQL(in []ExpressionCatalogPromQLQuery) []ExpressionCatalogPromQLQuery {
	if len(in) == 0 {
		return nil
	}

	out := make([]ExpressionCatalogPromQLQuery, 0, len(in))
	for i := range in {
		q := in[i]
		if q.Query == "" {
			continue
		}
		out = append(out, q)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func normalizeExpressionCatalogLogQL(in []ExpressionCatalogLogQLQuery) []ExpressionCatalogLogQLQuery {
	if len(in) == 0 {
		return nil
	}

	out := make([]ExpressionCatalogLogQLQuery, 0, len(in))
	for i := range in {
		q := in[i]
		if q.Query == "" {
			continue
		}
		out = append(out, q)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func marshalExpressionCatalogConfig(cfg ExpressionCatalogConfig) (json.RawMessage, error) {
	cfg = normalizeExpressionCatalogConfig(cfg)
	return json.Marshal(cfg)
}

// GetExpressionCatalogConfig returns the expression catalog config for an organization.
// Fallback order: org-specific -> system default -> hardcoded empty config.
func (s *Service) GetExpressionCatalogConfig(ctx context.Context, orgID uuid.UUID) ExpressionCatalogConfig {
	val, _ := s.getConfigCached(ctx, orgID, configKeyExpressionCatalog)
	if cfg, ok := parseExpressionCatalogConfig(val); ok {
		return cfg
	}

	if orgID != DefaultOrgID {
		val, _ = s.getConfigCached(ctx, DefaultOrgID, configKeyExpressionCatalog)
		if cfg, ok := parseExpressionCatalogConfig(val); ok {
			return cfg
		}
	}

	return defaultExpressionCatalogConfig()
}

// GetExpressionCatalogConfigResult contains direct lookup result info.
type GetExpressionCatalogConfigResult struct {
	Config    ExpressionCatalogConfig
	IsDefault bool // True when system default or hardcoded fallback is used.
}

// GetExpressionCatalogConfigDirect fetches expression catalog config directly from DB.
func GetExpressionCatalogConfigDirect(ctx context.Context, querier OrgConfigQuerier, orgID uuid.UUID) (GetExpressionCatalogConfigResult, error) {
	val, _ := querier.GetOrgConfig(ctx, configdb.GetOrgConfigParams{
		OrganizationID: orgID,
		Key:            configKeyExpressionCatalog,
	})
	if cfg, ok := parseExpressionCatalogConfig(val); ok {
		return GetExpressionCatalogConfigResult{Config: cfg, IsDefault: false}, nil
	}

	val, _ = querier.GetOrgConfig(ctx, configdb.GetOrgConfigParams{
		OrganizationID: DefaultOrgID,
		Key:            configKeyExpressionCatalog,
	})
	if cfg, ok := parseExpressionCatalogConfig(val); ok {
		return GetExpressionCatalogConfigResult{Config: cfg, IsDefault: true}, nil
	}

	return GetExpressionCatalogConfigResult{
		Config:    defaultExpressionCatalogConfig(),
		IsDefault: true,
	}, nil
}

// SetExpressionCatalogConfigDirect stores expression catalog config directly in DB.
func SetExpressionCatalogConfigDirect(ctx context.Context, querier OrgConfigQuerier, orgID uuid.UUID, cfg ExpressionCatalogConfig) error {
	val, err := marshalExpressionCatalogConfig(cfg)
	if err != nil {
		return err
	}
	return querier.UpsertOrgConfig(ctx, configdb.UpsertOrgConfigParams{
		OrganizationID: orgID,
		Key:            configKeyExpressionCatalog,
		Value:          val,
	})
}

// DeleteExpressionCatalogConfigDirect deletes expression catalog config directly from DB.
func DeleteExpressionCatalogConfigDirect(ctx context.Context, querier OrgConfigQuerier, orgID uuid.UUID) error {
	return querier.DeleteOrgConfig(ctx, configdb.DeleteOrgConfigParams{
		OrganizationID: orgID,
		Key:            configKeyExpressionCatalog,
	})
}
