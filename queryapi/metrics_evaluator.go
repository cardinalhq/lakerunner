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
	"sort"
	"sync"
	"time"

	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/promql"
	"github.com/google/uuid"
	"github.com/prometheus/common/model"
)

// EvaluateMetricsQuery plans pushdowns, fans requests out to workers, merges their streams,
// and returns a single chronologically merged stream of SketchInput.
// The merged stream’s timestamps are aligned to the evaluation window.
func (q *QuerierService) EvaluateMetricsQuery(
	ctx context.Context,
	orgID uuid.UUID,
	startTs, endTs int64,
	queryPlan promql.QueryPlan,
) (<-chan map[string]promql.EvalResult, error) {
	stepDuration := StepForQueryDuration(startTs, endTs)

	workers, err := q.workerDiscovery.GetAllWorkers()
	if err != nil {
		slog.Error("failed to get all workers", "err", err)
		return nil, fmt.Errorf("failed to get all workers: %w", err)
	}

	// Final streaming output (EvalFlow output). Closed when all groups are done (or ctx cancelled).
	out := make(chan map[string]promql.EvalResult, 1024)

	go func() {
		defer close(out)

		// For each leaf/base-expr, compute effective window (offset-aware), then push down per grouped segments.
		for _, leaf := range queryPlan.Leaves {
			offMs, err := parseOffsetMs(leaf.Offset)
			if err != nil {
				slog.Error("invalid offset on leaf; ignoring offset", "offset", leaf.Offset, "err", err)
				offMs = 0
			}

			// Effective range to *read* from storage.
			effStart := startTs - offMs
			effEnd := endTs - offMs

			// Partition by dateInt hours for storage listing.
			dateIntHours := dateIntHoursRange(effStart, effEnd, time.UTC)

			for _, dateIntHour := range dateIntHours {
				segments, err := q.lookupMetricsSegments(ctx, dateIntHour, leaf, effStart, effEnd, stepDuration, orgID)
				if err != nil {
					slog.Error("failed to get segment infos", "dateInt", dateIntHour.DateInt, "err", err)
					continue
				}
				slog.Info("Found segments for leaf",
					"leafID", leaf.ID, "dateIntHour", dateIntHour,
					"numSegments", len(segments))

				// Tag segments with this leaf id so worker knows which expr it is serving.
				for i := range segments {
					segments[i].ExprID = leaf.ID
				}
				if len(segments) == 0 {
					continue
				}

				// Form time-contiguous batches sized for the number of workers.
				groups := ComputeReplayBatchesWithWorkers(
					segments, stepDuration, effStart, effEnd, len(workers), true,
				)

				// Process groups in parallel with controlled concurrency
				const maxConcurrentGroups = 4 // Limit concurrent groups to prevent memory issues
				sem := make(chan struct{}, maxConcurrentGroups)
				var wg sync.WaitGroup

				// Channel to collect results from all groups in order
				type groupResult struct {
					index  int
					result map[string]promql.EvalResult
				}
				groupResultsChan := make(chan groupResult, len(groups))

				// Process each group concurrently
				for i, group := range groups {
					select {
					case <-ctx.Done():
						return
					default:
					}

					wg.Add(1)
					go func(groupIndex int, group SegmentGroup) {
						defer wg.Done()

						// Acquire semaphore
						sem <- struct{}{}
						defer func() { <-sem }()

						slog.Info("Pushing down segments", "groupSize", len(group.Segments), "groupIndex", groupIndex)

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
							return
						}

						// Group segments by assigned worker
						workerGroups := make(map[Worker][]SegmentInfo)
						for _, mapping := range mappings {
							segmentList := segmentMap[mapping.SegmentID]
							workerGroups[mapping.Worker] = append(workerGroups[mapping.Worker], segmentList...)
						}

						// Build per-group leaf channels (one per worker assignment).
						var groupLeafChans []<-chan promql.SketchInput
						for worker, workerSegments := range workerGroups {
							req := PushDownRequest{
								OrganizationID: orgID,
								BaseExpr:       &leaf,
								StartTs:        group.StartTs,
								EndTs:          group.EndTs,
								Segments:       workerSegments,
								Step:           stepDuration,
							}

							ch, err := q.metricsPushDown(ctx, worker, req)
							if err != nil {
								slog.Error("pushdown failed", "worker", worker, "err", err)
								continue
							}
							if offMs != 0 {
								ch = shiftTimestamps(ctx, ch, offMs, 256)
							}
							groupLeafChans = append(groupLeafChans, ch)
						}

						// No channels for this group — skip.
						if len(groupLeafChans) == 0 {
							return
						}

						// Merge this group's worker streams by timestamp (ascending).
						mergedGroup := promql.MergeSorted(ctx, 1024, false, 0, groupLeafChans...)

						// Run EvalFlow for THIS group and collect results.
						flow := NewEvalFlow(queryPlan.Root, queryPlan.Leaves, stepDuration, EvalFlowOptions{
							NumBuffers: 2,
							OutBuffer:  1024,
						})
						groupResults := flow.Run(ctx, mergedGroup)

						// Collect all results from this group
						var groupResultsList []map[string]promql.EvalResult
						for {
							select {
							case <-ctx.Done():
								return
							case res, ok := <-groupResults:
								if !ok {
									// Group done, send results in order
									for _, result := range groupResultsList {
										select {
										case <-ctx.Done():
											return
										case groupResultsChan <- groupResult{index: groupIndex, result: result}:
										}
									}
									return
								}
								groupResultsList = append(groupResultsList, res)
							}
						}
					}(i, group)
				}

				// Close the results channel when all groups are done
				go func() {
					wg.Wait()
					close(groupResultsChan)
				}()

				// Collect and sort results by group index to maintain order
				var allGroupResults []groupResult
				for result := range groupResultsChan {
					allGroupResults = append(allGroupResults, result)
				}

				// Sort by group index to maintain chronological order
				sort.Slice(allGroupResults, func(i, j int) bool {
					return allGroupResults[i].index < allGroupResults[j].index
				})

				// Forward results to output in order
				for _, gr := range allGroupResults {
					select {
					case <-ctx.Done():
						return
					case out <- gr.result:
					}
				}
			}
		}
	}()

	return out, nil
}

// metricsPushDown should POST req to the worker’s /pushdown and return a channel that yields SketchInput
// decoded from the worker’s SSE (or chunked JSON) stream. You can keep your existing stub here.
// Implement the HTTP/SSE client and decoding where you wire up workers.
func (q *QuerierService) metricsPushDown(
	ctx context.Context,
	worker Worker,
	request PushDownRequest,
) (<-chan promql.SketchInput, error) {
	return PushDownStream[promql.SketchInput](ctx, worker, request,
		func(typ string, data json.RawMessage) (promql.SketchInput, bool, error) {
			var zero promql.SketchInput
			if typ != "result" {
				return zero, false, nil
			}
			var si promql.SketchInput
			if err := json.Unmarshal(data, &si); err != nil {
				return zero, false, err
			}
			return si, true, nil
		})
}

// shiftTimestamps returns a channel that forwards every SketchInput from `in`
// with its Timestamp shifted by +deltaMs. Non-blocking via buffered output.
func shiftTimestamps(ctx context.Context, in <-chan promql.SketchInput, deltaMs int64, outBuf int) <-chan promql.SketchInput {
	out := make(chan promql.SketchInput, outBuf)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case si, ok := <-in:
				if !ok {
					return
				}
				si.Timestamp += deltaMs
				select {
				case <-ctx.Done():
					return
				case out <- si:
				}
			}
		}
	}()
	return out
}

// parseOffsetMs parses a PromQL offset string (e.g., "5m", "1h") into milliseconds.
// Empty strings return 0 with nil error.
func parseOffsetMs(offset string) (int64, error) {
	if offset == "" {
		return 0, nil
	}
	d, err := model.ParseDuration(offset)
	if err != nil {
		return 0, err
	}
	return int64(time.Duration(d) / time.Millisecond), nil
}

func (q *QuerierService) lookupMetricsSegments(ctx context.Context,
	dih DateIntHours,
	be promql.BaseExpr,
	startTs int64, endTs int64,
	stepDuration time.Duration,
	orgUUID uuid.UUID) ([]SegmentInfo, error) {

	var allSegments []SegmentInfo

	slog.Info("Issuing query for segments",
		"dateInt", dih.DateInt,
		"startTs", startTs,
		"endTs", endTs,
		"frequencyMs", stepDuration.Milliseconds(),
		"orgUUID", orgUUID)

	fingerprint := buffet.ComputeFingerprint("_cardinalhq.name", be.Metric)
	slog.Info("Computed fingerprint for baseExpr", "fingerprint", fingerprint, "metric", be.Metric)
	rows, err := q.mdb.ListMetricSegmentsForQuery(ctx, lrdb.ListMetricSegmentsForQueryParams{
		Int8range:      startTs,
		Int8range_2:    endTs,
		Dateint:        int32(dih.DateInt),
		FrequencyMs:    int32(stepDuration.Milliseconds()),
		OrganizationID: orgUUID,
		Fingerprints:   []int64{fingerprint},
	})
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		endHour := zeroFilledHour(time.UnixMilli(row.EndTs).UTC().Hour())
		allSegments = append(allSegments, SegmentInfo{
			DateInt:        dih.DateInt,
			Hour:           endHour,
			SegmentID:      row.SegmentID,
			StartTs:        row.StartTs,
			EndTs:          row.EndTs,
			Dataset:        "metrics",
			OrganizationID: orgUUID,
			InstanceNum:    row.InstanceNum,
			Frequency:      stepDuration.Milliseconds(),
		})
	}

	return allSegments, nil
}
