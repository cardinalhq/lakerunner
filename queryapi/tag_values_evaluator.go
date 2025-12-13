// Copyright (C) 2025 CardinalHQ, Inc
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
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/promql"
)

// hasMetricSelectors returns true if the query plan has any label matchers
// beyond just the metric name. If selectors are present, we can't use shortcuts.
func hasMetricSelectors(leaves []promql.BaseExpr) bool {
	for _, leaf := range leaves {
		if len(leaf.Matchers) > 0 {
			return true
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

	// If there are selectors in the query, we can't use the shortcut
	// because we need to filter by those selectors
	if hasMetricSelectors(leaves) {
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
) (<-chan promql.TagValue, error) {
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
		return out, nil
	}

	// Fall back to the full query worker path
	workers, err := q.workerDiscovery.GetAllWorkers()
	if err != nil {
		slog.Error("failed to get all workers", "err", err)
		return nil, fmt.Errorf("failed to get all workers: %w", err)
	}
	stepDuration := StepForQueryDuration(startTs, endTs)

	out := make(chan promql.TagValue, 1024)

	go func() {
		defer close(out)

		// Use a map to track unique tag values across all workers and groups
		seenTagValues := make(map[string]bool)

		dateIntHours := dateIntHoursRange(startTs, endTs, time.UTC, false)

		for _, leaf := range queryPlan.Leaves {
			for _, dih := range dateIntHours {
				segments, err := q.lookupMetricsSegments(ctx, dih, leaf, startTs, endTs, stepDuration, orgID)
				slog.Info("lookupMetricsSegments", "dih", dih, "leaf", leaf, "found", len(segments))
				if err != nil {
					slog.Error("failed to lookup metrics segments", "err", err, "dih", dih, "leaf", leaf)
					return
				}
				if len(segments) == 0 {
					continue
				}
				// Form time-contiguous batches sized for the number of workers.
				groups := ComputeReplayBatchesWithWorkers(segments, stepDuration, startTs, endTs, len(workers), false)
				for _, group := range groups {
					select {
					case <-ctx.Done():
						return
					default:
					}

					// Collect all segment IDs for worker assignment
					segmentIDs := make([]int64, 0, len(group.Segments))
					segmentMap := make(map[int64][]SegmentInfo)
					for _, segment := range group.Segments {
						segmentIDs = append(segmentIDs, segment.SegmentID)
						segmentMap[segment.SegmentID] = append(segmentMap[segment.SegmentID], segment)
					}

					// Get worker assignments for all segments
					mappings, err := q.workerDiscovery.GetWorkersForSegments(orgID, segmentIDs)
					if err != nil {
						slog.Error("failed to get worker assignments", "err", err)
						continue
					}

					// Group segments by assigned worker
					workerGroups := make(map[Worker][]SegmentInfo)
					for _, mapping := range mappings {
						segmentList := segmentMap[mapping.SegmentID]
						workerGroups[mapping.Worker] = append(workerGroups[mapping.Worker], segmentList...)
					}

					var groupLeafChans []<-chan promql.TagValue
					for worker, workerSegments := range workerGroups {
						req := PushDownRequest{
							OrganizationID: orgID,
							BaseExpr:       &leaf,
							TagName:        queryPlan.TagName,
							StartTs:        group.StartTs,
							EndTs:          group.EndTs,
							Segments:       workerSegments,
							Step:           stepDuration,
						}
						ch, err := q.tagValuesPushDown(ctx, worker, req)
						if err != nil {
							slog.Error("pushdown failed", "worker", worker, "err", err)
							continue
						}
						groupLeafChans = append(groupLeafChans, ch)
					}
					// No channels for this group — skip.
					if len(groupLeafChans) == 0 {
						continue
					}

					// Merge this group's worker streams without sorting
					mergedGroup := mergeChannels(ctx, 1024, groupLeafChans...)

					// Forward group results into final output stream as they arrive, with deduplication
					for {
						select {
						case <-ctx.Done():
							return
						case res, ok := <-mergedGroup:
							if !ok {
								// Group done, proceed to next group.
								goto nextGroup
							}
							// Only send unique tag values
							if !seenTagValues[res.Value] {
								seenTagValues[res.Value] = true
								out <- res
							}
						}
					}
				nextGroup:
				}
			}
		}
	}()
	return out, nil
}

// hasLogSelectors returns true if the query plan has any label matchers,
// line filters, or label filters. If selectors are present, we can't use shortcuts.
func hasLogSelectors(leaves []logql.LogLeaf) bool {
	for _, leaf := range leaves {
		if len(leaf.Matchers) > 0 || len(leaf.LineFilters) > 0 || len(leaf.LabelFilters) > 0 {
			return true
		}
	}
	return false
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
	// If there are selectors in the query, we can't use the shortcut
	// because we need to filter by those selectors
	if hasLogSelectors(leaves) {
		slog.Debug("stream_id shortcut skipped due to selectors in query", "tagName", tagName)
		return nil, false
	}

	// Convert timestamps to dateint range for partition pruning
	startDateint, _ := helpers.MSToDateintHour(startTs)
	endDateint, _ := helpers.MSToDateintHour(endTs)

	values, err := q.mdb.GetLogStreamIdValues(ctx, lrdb.GetLogStreamIdValuesParams{
		OrganizationID: orgID,
		StartDateint:   startDateint,
		EndDateint:     endDateint,
		StartTs:        startTs,
		EndTs:          endTs,
		TagName:        &tagName,
	})
	if err != nil {
		slog.Warn("stream_id shortcut query failed, falling back to workers", "err", err, "tagName", tagName)
		return nil, false
	}

	// Empty result means no segments have this tag as stream_id_field,
	// so fall back to the full query path
	if len(values) == 0 {
		slog.Debug("stream_id shortcut returned no results, falling back to workers", "tagName", tagName)
		return nil, false
	}

	slog.Info("stream_id shortcut succeeded", "tagName", tagName, "valueCount", len(values))
	return values, true
}

func (q *QuerierService) EvaluateLogTagValuesQuery(
	ctx context.Context,
	orgID uuid.UUID,
	startTs int64,
	endTs int64,
	queryPlan logql.LQueryPlan,
) (<-chan promql.Timestamped, error) {
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
		return out, nil
	}

	// Fall back to the full query worker path
	workers, err := q.workerDiscovery.GetAllWorkers()
	if err != nil {
		slog.Error("failed to get all workers", "err", err)
		return nil, fmt.Errorf("failed to get all workers: %w", err)
	}
	stepDuration := StepForQueryDuration(startTs, endTs)

	out := make(chan promql.Timestamped, 1024)

	go func() {
		defer close(out)

		// Use a map to track unique tag values across all workers and groups
		seenTagValues := make(map[string]bool)

		dateIntHours := dateIntHoursRange(startTs, endTs, time.UTC, true)

		for _, leaf := range queryPlan.Leaves {
			for _, dih := range dateIntHours {
				segments, err := q.lookupLogsSegments(ctx, dih, leaf, startTs, endTs, orgID, q.mdb.ListLogSegmentsForQuery)
				if err != nil {
					slog.Error("failed to lookup log segments", "err", err, "dih", dih, "leaf", leaf)
					return
				}
				if len(segments) == 0 {
					continue
				}
				// Form time-contiguous batches sized for the number of workers.
				groups := ComputeReplayBatchesWithWorkers(segments, DefaultLogStep, startTs, endTs, len(workers), true)
				for _, group := range groups {
					select {
					case <-ctx.Done():
						return
					default:
					}

					// Collect all segment IDs for worker assignment
					segmentIDs := make([]int64, 0, len(group.Segments))
					segmentMap := make(map[int64][]SegmentInfo)
					for _, segment := range group.Segments {
						segmentIDs = append(segmentIDs, segment.SegmentID)
						segmentMap[segment.SegmentID] = append(segmentMap[segment.SegmentID], segment)
					}

					// Get worker assignments for all segments
					mappings, err := q.workerDiscovery.GetWorkersForSegments(orgID, segmentIDs)
					if err != nil {
						slog.Error("failed to get worker assignments", "err", err)
						continue
					}

					// Group segments by assigned worker
					workerGroups := make(map[Worker][]SegmentInfo)
					for _, mapping := range mappings {
						segmentList := segmentMap[mapping.SegmentID]
						workerGroups[mapping.Worker] = append(workerGroups[mapping.Worker], segmentList...)
					}

					var groupLeafChans []<-chan promql.TagValue
					for worker, workerSegments := range workerGroups {
						req := PushDownRequest{
							OrganizationID: orgID,
							LogLeaf:        &leaf,
							TagName:        queryPlan.TagName,
							StartTs:        group.StartTs,
							EndTs:          group.EndTs,
							Segments:       workerSegments,
							Step:           stepDuration,
						}
						ch, err := q.tagValuesPushDown(ctx, worker, req)
						if err != nil {
							slog.Error("pushdown failed", "worker", worker, "err", err)
							continue
						}
						groupLeafChans = append(groupLeafChans, ch)
					}
					// No channels for this group — skip.
					if len(groupLeafChans) == 0 {
						continue
					}

					// Merge this group's worker streams without sorting
					mergedGroup := mergeChannels(ctx, 1024, groupLeafChans...)

					// Forward group results into final output stream as they arrive, with deduplication
					for {
						select {
						case <-ctx.Done():
							return
						case res, ok := <-mergedGroup:
							if !ok {
								// Group done, proceed to next group.
								goto nextGroup
							}
							// Only send unique tag values
							if !seenTagValues[res.Value] {
								seenTagValues[res.Value] = true
								out <- res
							}
						}
					}
				nextGroup:
				}
			}
		}
	}()
	return out, nil
}

func (q *QuerierService) EvaluateSpanTagValuesQuery(
	ctx context.Context,
	orgID uuid.UUID,
	startTs int64,
	endTs int64,
	queryPlan logql.LQueryPlan,
) (<-chan promql.Timestamped, error) {
	workers, err := q.workerDiscovery.GetAllWorkers()
	if err != nil {
		slog.Error("failed to get all workers", "err", err)
		return nil, fmt.Errorf("failed to get all workers: %w", err)
	}

	stepDuration := StepForQueryDuration(startTs, endTs)

	out := make(chan promql.Timestamped, 1024)

	go func() {
		defer close(out)

		// Use a map to track unique tag values across all workers and groups
		seenTagValues := make(map[string]bool)

		dateIntHours := dateIntHoursRange(startTs, endTs, time.UTC, true)

		for _, leaf := range queryPlan.Leaves {
			for _, dih := range dateIntHours {
				segments, err := q.lookupSpansSegments(ctx, dih, leaf, startTs, endTs, orgID, q.mdb.ListTraceSegmentsForQuery)
				if err != nil {
					slog.Error("failed to lookup spans segments", "err", err, "dih", dih, "leaf", leaf)
					return
				}
				if len(segments) == 0 {
					continue
				}
				// Form time-contiguous batches sized for the number of workers.
				groups := ComputeReplayBatchesWithWorkers(segments, DefaultLogStep, startTs, endTs, len(workers), true)
				for _, group := range groups {
					select {
					case <-ctx.Done():
						return
					default:
					}

					// Collect all segment IDs for worker assignment
					segmentIDs := make([]int64, 0, len(group.Segments))
					segmentMap := make(map[int64][]SegmentInfo)
					for _, segment := range group.Segments {
						segmentIDs = append(segmentIDs, segment.SegmentID)
						segmentMap[segment.SegmentID] = append(segmentMap[segment.SegmentID], segment)
					}

					// Get worker assignments for all segments
					mappings, err := q.workerDiscovery.GetWorkersForSegments(orgID, segmentIDs)
					if err != nil {
						slog.Error("failed to get worker assignments", "err", err)
						continue
					}

					// Group segments by assigned worker
					workerGroups := make(map[Worker][]SegmentInfo)
					for _, mapping := range mappings {
						segmentList := segmentMap[mapping.SegmentID]
						workerGroups[mapping.Worker] = append(workerGroups[mapping.Worker], segmentList...)
					}

					var groupLeafChans []<-chan promql.TagValue
					for worker, workerSegments := range workerGroups {
						req := PushDownRequest{
							OrganizationID: orgID,
							LogLeaf:        &leaf,
							TagName:        queryPlan.TagName,
							StartTs:        group.StartTs,
							EndTs:          group.EndTs,
							Segments:       workerSegments,
							Step:           stepDuration,
							IsSpans:        true,
						}
						ch, err := q.tagValuesPushDown(ctx, worker, req)
						if err != nil {
							slog.Error("pushdown failed", "worker", worker, "err", err)
							continue
						}
						groupLeafChans = append(groupLeafChans, ch)
					}
					// No channels for this group — skip.
					if len(groupLeafChans) == 0 {
						continue
					}

					// Merge this group's worker streams without sorting
					mergedGroup := mergeChannels(ctx, 1024, groupLeafChans...)

					// Forward group results into final output stream as they arrive, with deduplication
					for {
						select {
						case <-ctx.Done():
							return
						case res, ok := <-mergedGroup:
							if !ok {
								// Group done, proceed to next group.
								goto nextGroup
							}
							// Only send unique tag values
							if !seenTagValues[res.Value] {
								seenTagValues[res.Value] = true
								out <- res
							}
						}
					}
				nextGroup:
				}
			}
		}
	}()
	return out, nil
}

func (q *QuerierService) tagValuesPushDown(
	ctx context.Context,
	worker Worker,
	request PushDownRequest,
) (<-chan promql.TagValue, error) {
	return PushDownStream(ctx, worker, request,
		func(typ string, data json.RawMessage) (promql.TagValue, bool, error) {
			var zero promql.TagValue
			if typ != "result" {
				return zero, false, nil
			}
			var si promql.TagValue
			if err := json.Unmarshal(data, &si); err != nil {
				return zero, false, err
			}
			return si, true, nil
		})
}

// mergeChannels merges multiple channels into one without sorting.
// Values are forwarded as they arrive from any of the input channels.
func mergeChannels[T any](ctx context.Context, outBuf int, chans ...<-chan T) <-chan T {
	out := make(chan T, outBuf)
	if len(chans) == 0 {
		close(out)
		return out
	}

	go func() {
		defer close(out)

		// Create a slice to track which channels are still active
		activeChans := make([]<-chan T, len(chans))
		copy(activeChans, chans)

		for len(activeChans) > 0 {
			// Create cases for all active channels
			cases := make([]reflect.SelectCase, 0, len(activeChans)+1)

			// Add context cancellation case
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ctx.Done()),
			})

			// Add all active channel cases
			for _, ch := range activeChans {
				cases = append(cases, reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(ch),
				})
			}

			// Select from all channels
			chosen, value, ok := reflect.Select(cases)

			if chosen == 0 {
				// Context cancelled
				return
			}

			// Adjust chosen index for active channels (subtract 1 for context case)
			chosen--

			if !ok {
				// Channel closed, remove it from active channels
				activeChans = append(activeChans[:chosen], activeChans[chosen+1:]...)
				continue
			}

			// Forward the value
			select {
			case <-ctx.Done():
				return
			case out <- value.Interface().(T):
			}
		}
	}()

	return out
}
