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
	"fmt"
	"strings"

	"github.com/cardinalhq/lakerunner/promql"
	"github.com/cardinalhq/lakerunner/queryapi"
)

// QueryKind categorizes the type of query for routing and metadata extraction.
type QueryKind string

const (
	QueryKindMetrics   QueryKind = "metrics"
	QueryKindLogs      QueryKind = "logs"
	QueryKindSpans     QueryKind = "spans"
	QueryKindTagValues QueryKind = "tag_values"
	QueryKindSummary   QueryKind = "summary"
)

// PlannedQuery holds the resolved query plan for a PushDownRequest.
type PlannedQuery struct {
	Kind         QueryKind
	SQL          string
	AggSQL       string // only set for simple log aggregation with agg_ files
	CacheManager *CacheManager
	GlobSize     int

	// For agg_ file split optimization.
	HasAggSplit   bool
	GroupBy       []string
	MatcherFields []string

	// TimestampCol is the column used for min/max metadata extraction.
	// Empty for tag values, tag names, and summary queries.
	TimestampCol string
}

// PlanQuery determines the SQL, cache manager, and query kind for a PushDownRequest.
// This extracts the routing logic into a reusable helper for the executor.
func (ws *WorkerService) PlanQuery(req queryapi.PushDownRequest) (*PlannedQuery, error) {
	plan := &PlannedQuery{}

	if req.BaseExpr != nil {
		if req.TagNames {
			plan.Kind = QueryKindTagValues
			plan.SQL = req.BaseExpr.ToWorkerSQLForTagNames()
			plan.CacheManager = ws.MetricsCM
			plan.GlobSize = ws.MetricsGlobSize
		} else if req.TagName != "" {
			plan.Kind = QueryKindTagValues
			plan.SQL = req.BaseExpr.ToWorkerSQLForTagValues(req.Step, req.TagName)
			plan.CacheManager = ws.MetricsCM
			plan.GlobSize = ws.MetricsGlobSize
		} else if req.IsSummary {
			plan.Kind = QueryKindSummary
			plan.SQL = req.BaseExpr.ToWorkerSummarySQL()
			plan.CacheManager = ws.MetricsCM
			plan.GlobSize = ws.MetricsGlobSize
		} else {
			plan.Kind = QueryKindMetrics
			plan.TimestampCol = "bucket_ts"
			plan.SQL = req.BaseExpr.ToWorkerSQL(req.Step)
			if req.BaseExpr.LogLeaf != nil {
				plan.CacheManager = ws.LogsCM
				plan.GlobSize = ws.LogsGlobSize
				if req.BaseExpr.LogLeaf.IsSimpleAggregation() {
					plan.HasAggSplit = true
					plan.AggSQL = promql.BuildAggFileSQL(req.BaseExpr, req.Step)
					plan.GroupBy = req.BaseExpr.GroupBy
					var matcherFields []string
					for _, m := range req.BaseExpr.LogLeaf.Matchers {
						matcherFields = append(matcherFields, m.Label)
					}
					plan.MatcherFields = matcherFields
				}
			} else {
				plan.CacheManager = ws.MetricsCM
				plan.GlobSize = ws.MetricsGlobSize
			}
		}
	} else if req.LogLeaf != nil {
		if req.TagNames {
			plan.Kind = QueryKindTagValues
			if req.IsSpans {
				plan.SQL = req.LogLeaf.ToSpansWorkerSQLForTagNames()
				plan.CacheManager = ws.TracesCM
				plan.GlobSize = ws.TracesGlobSize
			} else {
				plan.SQL = req.LogLeaf.ToWorkerSQLForTagNames()
				plan.CacheManager = ws.LogsCM
				plan.GlobSize = ws.LogsGlobSize
			}
		} else if req.TagName != "" {
			plan.Kind = QueryKindTagValues
			plan.SQL = req.LogLeaf.ToWorkerSQLForTagValues(req.TagName)
			if req.IsSpans {
				plan.CacheManager = ws.TracesCM
				plan.GlobSize = ws.TracesGlobSize
			} else {
				plan.CacheManager = ws.LogsCM
				plan.GlobSize = ws.LogsGlobSize
			}
		} else {
			if req.IsSpans {
				plan.Kind = QueryKindSpans
				plan.TimestampCol = "chq_timestamp"
				plan.SQL = req.LogLeaf.ToSpansWorkerSQLWithLimit(req.Limit, req.ToOrderString(), req.Fields)
				plan.CacheManager = ws.TracesCM
				plan.GlobSize = ws.TracesGlobSize
			} else {
				plan.Kind = QueryKindLogs
				plan.TimestampCol = "chq_timestamp"
				plan.SQL = req.LogLeaf.ToWorkerSQLWithLimit(req.Limit, req.ToOrderString(), req.Fields)
				plan.CacheManager = ws.LogsCM
				plan.GlobSize = ws.LogsGlobSize
			}
		}
	} else {
		return nil, fmt.Errorf("no leaf to evaluate")
	}

	// Fill time placeholders. {table} stays in for EvaluatePushDownToFile to replace.
	plan.SQL = strings.ReplaceAll(plan.SQL, "{start}", fmt.Sprintf("%d", req.StartTs))
	plan.SQL = strings.ReplaceAll(plan.SQL, "{end}", fmt.Sprintf("%d", req.EndTs))
	if plan.AggSQL != "" {
		plan.AggSQL = strings.ReplaceAll(plan.AggSQL, "{start}", fmt.Sprintf("%d", req.StartTs))
		plan.AggSQL = strings.ReplaceAll(plan.AggSQL, "{end}", fmt.Sprintf("%d", req.EndTs))
	}

	if plan.CacheManager == nil {
		return nil, fmt.Errorf("cache manager not initialized for this query type")
	}

	return plan, nil
}
