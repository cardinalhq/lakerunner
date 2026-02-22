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
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/promql"
)

// hasEffectiveMetricSelectors returns true if the query plan has any label matchers
// that actually filter results. A selector like {tagName=~".+"} is considered a no-op.
func hasEffectiveMetricSelectors(leaves []promql.BaseExpr, tagName string) bool {
	for _, leaf := range leaves {
		for _, m := range leaf.Matchers {
			// If matcher is for a different label, it's a real filter
			if m.Label != tagName {
				return true
			}
			// If it's an exact match or negative match, it's a real filter
			if m.Op != promql.MatchRe {
				return true
			}
			// For regex match on the queried tag, check if it's a "match all" pattern
			if !isMatchAllRegex(m.Value) {
				return true
			}
			// Pattern like {tagName=~".+"} - this is a no-op, continue checking
		}
	}
	return false
}

// tryMetricNameShortcut attempts to get metric names directly from PostgreSQL
// when the requested tag is "metric_name". Returns the tag values and true if
// the shortcut succeeded, or nil and false if we should fall back to workers.
func (q *QuerierService) tryMetricNameShortcut(
	ctx context.Context,
	orgID uuid.UUID,
	startTs int64,
	endTs int64,
	tagName string,
	leaves []promql.BaseExpr,
) ([]string, bool) {
	// Only handle metric_name tag
	if tagName != "metric_name" {
		return nil, false
	}

	// If there are effective selectors in the query (not just match-all patterns),
	// we can't use the shortcut because we need to filter by those selectors
	if hasEffectiveMetricSelectors(leaves, tagName) {
		slog.Debug("metric_name shortcut skipped due to selectors in query")
		return nil, false
	}

	// Convert timestamps to dateint range for partition pruning
	startDateint, _ := helpers.MSToDateintHour(startTs)
	endDateint, _ := helpers.MSToDateintHour(endTs)

	values, err := q.mdb.ListMetricNames(ctx, lrdb.ListMetricNamesParams{
		OrganizationID: orgID,
		StartDateint:   startDateint,
		EndDateint:     endDateint,
		StartTs:        startTs,
		EndTs:          endTs,
	})
	if err != nil {
		slog.Warn("metric_name shortcut query failed, falling back to workers", "err", err)
		return nil, false
	}

	// Empty result means no segments have metric_names populated,
	// so fall back to the full query path
	if len(values) == 0 {
		slog.Debug("metric_name shortcut returned no results, falling back to workers")
		return nil, false
	}

	slog.Info("metric_name shortcut succeeded", "valueCount", len(values))
	return values, true
}

func (q *QuerierService) EvaluateMetricTagValuesQuery(
	ctx context.Context,
	orgID uuid.UUID,
	startTs int64,
	endTs int64,
	queryPlan promql.QueryPlan,
) (<-chan promql.TagValue, <-chan error, error) {
	// Try the fast path: check if the requested tag is "metric_name"
	// and get values directly from PostgreSQL segment metadata
	if values, ok := q.tryMetricNameShortcut(ctx, orgID, startTs, endTs, queryPlan.TagName, queryPlan.Leaves); ok {
		out := make(chan promql.TagValue, len(values))
		go func() {
			defer close(out)
			for _, v := range values {
				select {
				case <-ctx.Done():
					return
				case out <- promql.TagValue{Value: v}:
				}
			}
		}()
		return out, nil, nil
	}

	// Fall back to the full query worker path
	workers, err := q.workerDiscovery.GetAllWorkers()
	if err != nil {
		slog.Error("failed to get all workers", "err", err)
		return nil, nil, fmt.Errorf("failed to get all workers: %w", err)
	}
	stepDuration := StepForQueryDuration(startTs, endTs)
	tvQueryID := uuid.New().String()

	out := make(chan promql.TagValue, 1024)
	queryErrc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(queryErrc)

		seenTagValues := make(map[string]bool)
		groupCounter := 0

		dateIntHours := dateIntHoursRange(startTs, endTs, time.UTC, false)

		for _, leaf := range queryPlan.Leaves {
			for _, dih := range dateIntHours {
				segments, err := q.lookupMetricsSegments(ctx, dih, leaf, startTs, endTs, stepDuration, orgID)
				slog.Info("lookupMetricsSegments", "dih", dih, "leaf", leaf, "found", len(segments))
				if err != nil {
					slog.Error("failed to lookup metrics segments", "err", err, "dih", dih, "leaf", leaf)
					queryErrc <- fmt.Errorf("lookup metrics segments: %w", err)
					return
				}
				if len(segments) == 0 {
					continue
				}
				groups := ComputeReplayBatchesWithWorkers(segments, stepDuration, startTs, endTs, len(workers), false)
				for _, group := range groups {
					select {
					case <-ctx.Done():
						return
					default:
					}

					req := PushDownRequest{
						OrganizationID: orgID,
						BaseExpr:       &leaf,
						TagName:        queryPlan.TagName,
						StartTs:        group.StartTs,
						EndTs:          group.EndTs,
						Segments:       group.Segments,
						Step:           stepDuration,
					}

					workLeafID := fmt.Sprintf("mtv-%d", groupCounter)
					groupCounter++
					chs, errc, err := q.tagValuesPushDownArtifact(ctx, tvQueryID, workLeafID, req)
					if err != nil {
						slog.Error("pushdown failed", "err", err)
						continue
					}

					for _, ch := range chs {
						for res := range ch {
							if !seenTagValues[res.Value] {
								seenTagValues[res.Value] = true
								select {
								case out <- res:
								case <-ctx.Done():
									return
								}
							}
						}
					}
					if segErr := drainErrors(errc); segErr != nil {
						slog.Error("segment failures in metric tag values query", "err", segErr)
						queryErrc <- segErr
						return
					}
				}
			}
		}
	}()
	return out, queryErrc, nil
}

// isMatchAllRegex returns true if the regex pattern effectively matches all non-empty values.
// Patterns like ".+", ".*", "^.*$", "^.+$" are match-all patterns.
func isMatchAllRegex(pattern string) bool {
	// Common "match all" patterns
	matchAllPatterns := []string{
		".+",
		".*",
		"^.*$",
		"^.+$",
		"^.*",
		".*$",
		"^.+",
		".+$",
	}
	for _, p := range matchAllPatterns {
		if pattern == p {
			return true
		}
	}
	return false
}

// hasEffectiveLogSelectors returns true if the query plan has any label matchers,
// line filters, or label filters that actually filter results.
// A selector like {tagName=~".+"} for the queried tag is considered a no-op.
func hasEffectiveLogSelectors(leaves []logql.LogLeaf, tagName string) bool {
	for _, leaf := range leaves {
		// Line filters always filter
		if len(leaf.LineFilters) > 0 {
			return true
		}
		// Label filters always filter
		if len(leaf.LabelFilters) > 0 {
			return true
		}
		// Check matchers - skip "match all" patterns on the queried tag
		for _, m := range leaf.Matchers {
			// If matcher is for a different label, it's a real filter
			if m.Label != tagName {
				return true
			}
			// If it's an exact match or negative match, it's a real filter
			if m.Op != logql.MatchRe {
				return true
			}
			// For regex match on the queried tag, check if it's a "match all" pattern
			if !isMatchAllRegex(m.Value) {
				return true
			}
			// Pattern like {tagName=~".+"} - this is a no-op, continue checking
		}
	}
	return false
}

// logLeafSelectorsDebug returns a string representation of selectors for logging
func logLeafSelectorsDebug(leaves []logql.LogLeaf) string {
	if len(leaves) == 0 {
		return "no leaves"
	}
	var parts []string
	for i, leaf := range leaves {
		var leafParts []string
		for _, m := range leaf.Matchers {
			leafParts = append(leafParts, fmt.Sprintf("%s%s%q", m.Label, m.Op, m.Value))
		}
		for _, lf := range leaf.LineFilters {
			leafParts = append(leafParts, fmt.Sprintf("line%s%q", lf.Op, lf.Match))
		}
		for _, lf := range leaf.LabelFilters {
			leafParts = append(leafParts, fmt.Sprintf("label:%s%s%q", lf.Label, lf.Op, lf.Value))
		}
		if len(leafParts) > 0 {
			parts = append(parts, fmt.Sprintf("leaf[%d]:{%s}", i, strings.Join(leafParts, ", ")))
		}
	}
	if len(parts) == 0 {
		return "no selectors"
	}
	return strings.Join(parts, "; ")
}

// tryLogStreamIdShortcut attempts to get tag values directly from PostgreSQL
// when the requested tag matches the stream_id_field stored in log_seg metadata.
// Returns the tag values and true if the shortcut succeeded, or nil and false
// if we should fall back to the full query worker path.
func (q *QuerierService) tryLogStreamIdShortcut(
	ctx context.Context,
	orgID uuid.UUID,
	startTs int64,
	endTs int64,
	tagName string,
	leaves []logql.LogLeaf,
) ([]string, bool) {
	// Convert timestamps to dateint range for partition pruning
	startDateint, _ := helpers.MSToDateintHour(startTs)
	endDateint, _ := helpers.MSToDateintHour(endTs)

	// If there are effective selectors in the query (not just match-all patterns),
	// we can't use the shortcut because we need to filter by those selectors
	if hasEffectiveLogSelectors(leaves, tagName) {
		slog.Info("log tag values: stream_id shortcut skipped due to selectors",
			"tagName", tagName,
			"selectors", logLeafSelectorsDebug(leaves),
			"orgID", orgID,
			"startTs", startTs,
			"endTs", endTs,
			"startDateint", startDateint,
			"endDateint", endDateint,
		)
		return nil, false
	}

	slog.Info("log tag values: trying stream_id shortcut via PostgreSQL",
		"tagName", tagName,
		"orgID", orgID,
		"startTs", startTs,
		"endTs", endTs,
		"startDateint", startDateint,
		"endDateint", endDateint,
	)

	values, err := q.mdb.GetLogStreamIdValues(ctx, lrdb.GetLogStreamIdValuesParams{
		OrganizationID: orgID,
		StartDateint:   startDateint,
		EndDateint:     endDateint,
		StartTs:        startTs,
		EndTs:          endTs,
		TagName:        &tagName,
	})
	if err != nil {
		slog.Warn("log tag values: stream_id shortcut query failed, falling back to workers",
			"err", err,
			"tagName", tagName,
			"orgID", orgID,
		)
		return nil, false
	}

	// Empty result means no segments have this tag as stream_id_field,
	// so fall back to the full query path
	if len(values) == 0 {
		slog.Info("log tag values: stream_id shortcut returned no results, falling back to workers",
			"tagName", tagName,
			"orgID", orgID,
			"startDateint", startDateint,
			"endDateint", endDateint,
		)
		return nil, false
	}

	slog.Info("log tag values: stream_id shortcut succeeded",
		"tagName", tagName,
		"valueCount", len(values),
		"orgID", orgID,
	)
	return values, true
}

func (q *QuerierService) EvaluateLogTagValuesQuery(
	ctx context.Context,
	orgID uuid.UUID,
	startTs int64,
	endTs int64,
	queryPlan logql.LQueryPlan,
) (<-chan promql.Timestamped, <-chan error, error) {
	// Try the fast path: check if the requested tag is the stream_id_field
	// and get values directly from PostgreSQL segment metadata
	if values, ok := q.tryLogStreamIdShortcut(ctx, orgID, startTs, endTs, queryPlan.TagName, queryPlan.Leaves); ok {
		out := make(chan promql.Timestamped, len(values))
		go func() {
			defer close(out)
			for _, v := range values {
				select {
				case <-ctx.Done():
					return
				case out <- promql.TagValue{Value: v}:
				}
			}
		}()
		return out, nil, nil
	}

	// Fall back to the full query worker path
	workers, err := q.workerDiscovery.GetAllWorkers()
	if err != nil {
		slog.Error("failed to get all workers", "err", err)
		return nil, nil, fmt.Errorf("failed to get all workers: %w", err)
	}
	stepDuration := StepForQueryDuration(startTs, endTs)
	ltvQueryID := uuid.New().String()

	dateIntHours := dateIntHoursRange(startTs, endTs, time.UTC, true)
	slog.Info("log tag values: using worker fallback path",
		"tagName", queryPlan.TagName,
		"selectors", logLeafSelectorsDebug(queryPlan.Leaves),
		"orgID", orgID,
		"startTs", startTs,
		"endTs", endTs,
		"workerCount", len(workers),
		"dateIntHoursCount", len(dateIntHours),
		"leafCount", len(queryPlan.Leaves),
	)

	out := make(chan promql.Timestamped, 1024)
	queryErrc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(queryErrc)

		seenTagValues := make(map[string]bool)
		groupCounter := 0

		for _, leaf := range queryPlan.Leaves {
			for _, dih := range dateIntHours {
				segments, err := q.lookupLogsSegments(ctx, dih, leaf, startTs, endTs, orgID, q.mdb.ListLogSegmentsForQuery)
				if err != nil {
					slog.Error("log tag values: failed to lookup log segments", "err", err, "dih", dih, "leaf", leaf)
					queryErrc <- fmt.Errorf("lookup log segments: %w", err)
					return
				}
				slog.Info("log tag values: segment lookup",
					"tagName", queryPlan.TagName,
					"dateIntHour", dih,
					"segmentCount", len(segments),
					"selectors", logLeafSelectorsDebug([]logql.LogLeaf{leaf}),
				)
				if len(segments) == 0 {
					continue
				}
				groups := ComputeReplayBatchesWithWorkers(segments, DefaultLogStep, startTs, endTs, len(workers), true)
				for _, group := range groups {
					select {
					case <-ctx.Done():
						return
					default:
					}

					req := PushDownRequest{
						OrganizationID: orgID,
						LogLeaf:        &leaf,
						TagName:        queryPlan.TagName,
						StartTs:        group.StartTs,
						EndTs:          group.EndTs,
						Segments:       group.Segments,
						Step:           stepDuration,
					}

					workLeafID := fmt.Sprintf("ltv-%d", groupCounter)
					groupCounter++
					chs, errc, err := q.tagValuesPushDownArtifact(ctx, ltvQueryID, workLeafID, req)
					if err != nil {
						slog.Error("pushdown failed", "err", err)
						continue
					}

					for _, ch := range chs {
						for res := range ch {
							if !seenTagValues[res.Value] {
								seenTagValues[res.Value] = true
								select {
								case out <- res:
								case <-ctx.Done():
									return
								}
							}
						}
					}
					if segErr := drainErrors(errc); segErr != nil {
						slog.Error("segment failures in log tag values query", "err", segErr)
						queryErrc <- segErr
						return
					}
				}
			}
		}
	}()
	return out, queryErrc, nil
}

// tagNamesQueryConfig holds configuration for the runTagNamesQuery helper.
type tagNamesQueryConfig struct {
	logPrefix      string
	dateIntIsLog   bool // passed to dateIntHoursRange
	batchStep      time.Duration
	batchIsLog     bool // passed to ComputeReplayBatchesWithWorkers
	stepDuration   time.Duration
	lookupSegments func(dih DateIntHours) ([]SegmentInfo, error)
	buildRequest   func(group SegmentGroup, workerSegments []SegmentInfo) PushDownRequest
	logSegmentInfo func(dih DateIntHours, segmentCount int)
}

// runTagNamesQuery is a generic helper that runs the tag names query loop.
// It handles worker discovery, segment batching, pushdown, and deduplication.
func (q *QuerierService) runTagNamesQuery(
	ctx context.Context,
	orgID uuid.UUID,
	startTs int64,
	endTs int64,
	leafCount int,
	iterateLeaves func(yield func() bool),
	cfg tagNamesQueryConfig,
) (<-chan promql.Timestamped, <-chan error, error) {
	workers, err := q.workerDiscovery.GetAllWorkers()
	if err != nil {
		slog.Error("failed to get all workers", "err", err)
		return nil, nil, fmt.Errorf("failed to get all workers: %w", err)
	}

	dateIntHours := dateIntHoursRange(startTs, endTs, time.UTC, cfg.dateIntIsLog)
	slog.Info(cfg.logPrefix+": using worker path",
		"orgID", orgID,
		"startTs", startTs,
		"endTs", endTs,
		"workerCount", len(workers),
		"dateIntHoursCount", len(dateIntHours),
		"leafCount", leafCount,
	)

	tnQueryID := uuid.New().String()
	out := make(chan promql.Timestamped, 1024)
	queryErrc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(queryErrc)

		seenTagNames := make(map[string]bool)
		groupCounter := 0
		var queryError error

		iterateLeaves(func() bool {
			for _, dih := range dateIntHours {
				segments, err := cfg.lookupSegments(dih)
				if err != nil {
					slog.Error(cfg.logPrefix+": failed to lookup segments", "err", err, "dih", dih)
					queryError = fmt.Errorf("lookup segments: %w", err)
					return false
				}
				cfg.logSegmentInfo(dih, len(segments))
				if len(segments) == 0 {
					continue
				}

				groups := ComputeReplayBatchesWithWorkers(segments, cfg.batchStep, startTs, endTs, len(workers), cfg.batchIsLog)
				for _, group := range groups {
					select {
					case <-ctx.Done():
						return false
					default:
					}

					req := cfg.buildRequest(group, group.Segments)
					workLeafID := fmt.Sprintf("tn-%d", groupCounter)
					groupCounter++

					chs, errc, err := q.tagValuesPushDownArtifact(ctx, tnQueryID, workLeafID, req)
					if err != nil {
						slog.Error("pushdown failed", "err", err)
						continue
					}

					for _, ch := range chs {
						for res := range ch {
							if !seenTagNames[res.Value] {
								seenTagNames[res.Value] = true
								select {
								case out <- res:
								case <-ctx.Done():
									return false
								}
							}
						}
					}
					if segErr := drainErrors(errc); segErr != nil {
						slog.Error("segment failures in tag names query", "err", segErr)
						queryError = segErr
						return false
					}
				}
			}
			return true
		})

		if queryError != nil {
			queryErrc <- queryError
		}
	}()
	return out, queryErrc, nil
}

// EvaluateLogTagNamesQuery queries workers to find distinct tag names (column names)
// that have at least one non-null value in logs matching the filter criteria.
// This is used for scoped tag discovery.
func (q *QuerierService) EvaluateLogTagNamesQuery(
	ctx context.Context,
	orgID uuid.UUID,
	startTs int64,
	endTs int64,
	queryPlan logql.LQueryPlan,
) (<-chan promql.Timestamped, <-chan error, error) {
	stepDuration := StepForQueryDuration(startTs, endTs)

	var currentLeaf logql.LogLeaf
	return q.runTagNamesQuery(ctx, orgID, startTs, endTs, len(queryPlan.Leaves),
		func(yield func() bool) {
			for _, leaf := range queryPlan.Leaves {
				currentLeaf = leaf
				if !yield() {
					return
				}
			}
		},
		tagNamesQueryConfig{
			logPrefix:    "log tag names",
			dateIntIsLog: true,
			batchStep:    DefaultLogStep,
			batchIsLog:   true,
			stepDuration: stepDuration,
			lookupSegments: func(dih DateIntHours) ([]SegmentInfo, error) {
				return q.lookupLogsSegments(ctx, dih, currentLeaf, startTs, endTs, orgID, q.mdb.ListLogSegmentsForQuery)
			},
			buildRequest: func(group SegmentGroup, workerSegments []SegmentInfo) PushDownRequest {
				leaf := currentLeaf
				return PushDownRequest{
					OrganizationID: orgID,
					LogLeaf:        &leaf,
					TagNames:       true,
					StartTs:        group.StartTs,
					EndTs:          group.EndTs,
					Segments:       workerSegments,
					Step:           stepDuration,
				}
			},
			logSegmentInfo: func(dih DateIntHours, segmentCount int) {
				slog.Info("log tag names: segment lookup",
					"dateIntHour", dih,
					"segmentCount", segmentCount,
					"selectors", logLeafSelectorsDebug([]logql.LogLeaf{currentLeaf}),
				)
			},
		})
}

// EvaluateMetricTagNamesQuery queries workers to find distinct tag names (column names)
// that have at least one non-null value in metrics matching the filter criteria.
// This is used for scoped tag discovery.
func (q *QuerierService) EvaluateMetricTagNamesQuery(
	ctx context.Context,
	orgID uuid.UUID,
	startTs int64,
	endTs int64,
	queryPlan promql.QueryPlan,
) (<-chan promql.Timestamped, <-chan error, error) {
	stepDuration := StepForQueryDuration(startTs, endTs)

	var currentLeaf promql.BaseExpr
	return q.runTagNamesQuery(ctx, orgID, startTs, endTs, len(queryPlan.Leaves),
		func(yield func() bool) {
			for _, leaf := range queryPlan.Leaves {
				currentLeaf = leaf
				if !yield() {
					return
				}
			}
		},
		tagNamesQueryConfig{
			logPrefix:    "metric tag names",
			dateIntIsLog: false,
			batchStep:    stepDuration,
			batchIsLog:   false,
			stepDuration: stepDuration,
			lookupSegments: func(dih DateIntHours) ([]SegmentInfo, error) {
				return q.lookupMetricsSegments(ctx, dih, currentLeaf, startTs, endTs, stepDuration, orgID)
			},
			buildRequest: func(group SegmentGroup, workerSegments []SegmentInfo) PushDownRequest {
				leaf := currentLeaf
				return PushDownRequest{
					OrganizationID: orgID,
					BaseExpr:       &leaf,
					TagNames:       true,
					StartTs:        group.StartTs,
					EndTs:          group.EndTs,
					Segments:       workerSegments,
					Step:           stepDuration,
				}
			},
			logSegmentInfo: func(dih DateIntHours, segmentCount int) {
				slog.Info("metric tag names: segment lookup",
					"dateIntHour", dih,
					"segmentCount", segmentCount,
				)
			},
		})
}

// EvaluateSpanTagNamesQuery queries workers to find distinct tag names (column names)
// that have at least one non-null value in spans matching the filter criteria.
// This is used for scoped tag discovery.
func (q *QuerierService) EvaluateSpanTagNamesQuery(
	ctx context.Context,
	orgID uuid.UUID,
	startTs int64,
	endTs int64,
	queryPlan logql.LQueryPlan,
) (<-chan promql.Timestamped, <-chan error, error) {
	stepDuration := StepForQueryDuration(startTs, endTs)

	var currentLeaf logql.LogLeaf
	return q.runTagNamesQuery(ctx, orgID, startTs, endTs, len(queryPlan.Leaves),
		func(yield func() bool) {
			for _, leaf := range queryPlan.Leaves {
				currentLeaf = leaf
				if !yield() {
					return
				}
			}
		},
		tagNamesQueryConfig{
			logPrefix:    "span tag names",
			dateIntIsLog: true,
			batchStep:    DefaultLogStep,
			batchIsLog:   true,
			stepDuration: stepDuration,
			lookupSegments: func(dih DateIntHours) ([]SegmentInfo, error) {
				return q.lookupSpansSegments(ctx, dih, currentLeaf, startTs, endTs, orgID, q.mdb.ListTraceSegmentsForQuery)
			},
			buildRequest: func(group SegmentGroup, workerSegments []SegmentInfo) PushDownRequest {
				leaf := currentLeaf
				return PushDownRequest{
					OrganizationID: orgID,
					LogLeaf:        &leaf,
					TagNames:       true,
					StartTs:        group.StartTs,
					EndTs:          group.EndTs,
					Segments:       workerSegments,
					Step:           stepDuration,
					IsSpans:        true,
				}
			},
			logSegmentInfo: func(dih DateIntHours, segmentCount int) {
				slog.Info("span tag names: segment lookup",
					"dateIntHour", dih,
					"segmentCount", segmentCount,
					"selectors", logLeafSelectorsDebug([]logql.LogLeaf{currentLeaf}),
				)
			},
		})
}

func (q *QuerierService) EvaluateSpanTagValuesQuery(
	ctx context.Context,
	orgID uuid.UUID,
	startTs int64,
	endTs int64,
	queryPlan logql.LQueryPlan,
) (<-chan promql.Timestamped, <-chan error, error) {
	workers, err := q.workerDiscovery.GetAllWorkers()
	if err != nil {
		slog.Error("failed to get all workers", "err", err)
		return nil, nil, fmt.Errorf("failed to get all workers: %w", err)
	}

	stepDuration := StepForQueryDuration(startTs, endTs)
	stvQueryID := uuid.New().String()

	out := make(chan promql.Timestamped, 1024)
	queryErrc := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(queryErrc)

		seenTagValues := make(map[string]bool)
		groupCounter := 0

		dateIntHours := dateIntHoursRange(startTs, endTs, time.UTC, true)

		for _, leaf := range queryPlan.Leaves {
			for _, dih := range dateIntHours {
				segments, err := q.lookupSpansSegments(ctx, dih, leaf, startTs, endTs, orgID, q.mdb.ListTraceSegmentsForQuery)
				if err != nil {
					slog.Error("failed to lookup spans segments", "err", err, "dih", dih, "leaf", leaf)
					queryErrc <- fmt.Errorf("lookup spans segments: %w", err)
					return
				}
				if len(segments) == 0 {
					continue
				}
				groups := ComputeReplayBatchesWithWorkers(segments, DefaultLogStep, startTs, endTs, len(workers), true)
				for _, group := range groups {
					select {
					case <-ctx.Done():
						return
					default:
					}

					req := PushDownRequest{
						OrganizationID: orgID,
						LogLeaf:        &leaf,
						TagName:        queryPlan.TagName,
						StartTs:        group.StartTs,
						EndTs:          group.EndTs,
						Segments:       group.Segments,
						Step:           stepDuration,
						IsSpans:        true,
					}

					workLeafID := fmt.Sprintf("stv-%d", groupCounter)
					groupCounter++
					chs, errc, err := q.tagValuesPushDownArtifact(ctx, stvQueryID, workLeafID, req)
					if err != nil {
						slog.Error("pushdown failed", "err", err)
						continue
					}

					for _, ch := range chs {
						for res := range ch {
							if !seenTagValues[res.Value] {
								seenTagValues[res.Value] = true
								select {
								case out <- res:
								case <-ctx.Done():
									return
								}
							}
						}
					}
					if segErr := drainErrors(errc); segErr != nil {
						slog.Error("segment failures in span tag values query", "err", segErr)
						queryErrc <- segErr
						return
					}
				}
			}
		}
	}()
	return out, queryErrc, nil
}
