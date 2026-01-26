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
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/promql"
)

// EvaluateTimeseriesQueryDirect evaluates a timeseries aggregation query using the direct legacy AST path.
// This generates count_over_time style results by counting log events in time buckets.
func (q *QuerierService) EvaluateTimeseriesQueryDirect(
	ctx context.Context,
	orgID uuid.UUID,
	startTs, endTs int64,
	filter QueryClause,
	exprID string,
) (<-chan map[string]promql.EvalResult, error) {
	workers, err := q.workerDiscovery.GetAllWorkers()
	if err != nil {
		slog.Error("failed to get all workers", "err", err)
		return nil, fmt.Errorf("failed to get all workers: %w", err)
	}

	stepDuration := StepForQueryDuration(startTs, endTs)
	out := make(chan map[string]promql.EvalResult, 1024)

	go func() {
		defer close(out)

		ctxAll, cancelAll := context.WithCancel(ctx)
		defer cancelAll()

		// Partition by dateInt hours for storage listing
		dateIntHours := dateIntHoursRange(startTs, endTs, time.UTC, false)

		// Accumulate counts by time bucket
		bucketCounts := make(map[int64]int64)

		for _, dih := range dateIntHours {
			// Use direct segment selection from legacy filter
			segments, fpToSegments, err := SelectSegmentsFromLegacyFilter(
				ctxAll, dih, filter, startTs, endTs, orgID, q.mdb.ListLogSegmentsForQuery,
			)
			if err != nil {
				slog.Error("failed to lookup log segments for timeseries", "err", err, "dih", dih)
				return
			}
			if len(segments) == 0 {
				continue
			}

			// Prune the filter to remove references to non-existent fields
			prunedFilter := PruneFilterForMissingFields(filter, fpToSegments)

			groups := ComputeReplayBatchesWithWorkers(segments, DefaultLogStep, startTs, endTs, len(workers), false)
			for _, group := range groups {
				select {
				case <-ctxAll.Done():
					return
				default:
				}

				slog.Info("Pushing down segments for timeseries", "groupSize", len(group.Segments))

				// Collect all segment IDs for worker assignment
				segmentIDs := make([]int64, 0, len(group.Segments))
				segmentMap := make(map[int64][]SegmentInfo)
				for _, segment := range group.Segments {
					segmentIDs = append(segmentIDs, segment.SegmentID)
					segmentMap[segment.SegmentID] = append(segmentMap[segment.SegmentID], segment)
				}

				// Get worker assignments
				mappings, err := q.workerDiscovery.GetWorkersForSegments(orgID, segmentIDs)
				if err != nil {
					slog.Error("failed to get worker assignments", "err", err)
					continue
				}

				workerGroups := make(map[Worker][]SegmentInfo)
				for _, mapping := range mappings {
					segmentList := segmentMap[mapping.SegmentID]
					workerGroups[mapping.Worker] = append(workerGroups[mapping.Worker], segmentList...)
				}

				// Create LegacyLeaf for SQL generation with pruned filter
				legacyLeaf := &LegacyLeaf{Filter: prunedFilter}

				for worker, workerSegments := range workerGroups {
					req := PushDownRequest{
						OrganizationID: orgID,
						LegacyLeaf:     legacyLeaf,
						StartTs:        group.StartTs,
						EndTs:          group.EndTs,
						Segments:       workerSegments,
						Step:           stepDuration,
						Limit:          0, // No limit for counting
						Reverse:        false,
						Fields:         []string{"chq_timestamp"}, // Only need timestamp for counting
					}

					resultsCh, err := q.executeWorkerQuery(ctxAll, worker, req)
					if err != nil {
						slog.Error("pushdown failed", "worker", worker, "err", err)
						continue
					}

					// Count events in time buckets
					for ts := range resultsCh {
						exemplar, ok := ts.(promql.Exemplar)
						if !ok {
							continue
						}

						timestamp := exemplar.Timestamp
						// Round down to bucket boundary
						bucketMs := stepDuration.Milliseconds()
						bucket := (timestamp / bucketMs) * bucketMs

						bucketCounts[bucket]++
					}
				}
			}
		}

		// Convert bucket counts to EvalResults
		for bucket, count := range bucketCounts {
			// Skip buckets outside the time range
			if bucket < startTs || bucket >= endTs {
				continue
			}

			result := promql.EvalResult{
				Timestamp: bucket,
				Value: promql.Value{
					Num: float64(count),
				},
				Tags: map[string]any{
					"name": "log_count",
				},
			}

			select {
			case <-ctxAll.Done():
				return
			case out <- map[string]promql.EvalResult{exprID: result}:
			}
		}
	}()

	return out, nil
}

// executeWorkerQuery is a helper to execute a query on a single worker
func (q *QuerierService) executeWorkerQuery(
	ctx context.Context,
	worker Worker,
	req PushDownRequest,
) (<-chan promql.Timestamped, error) {
	return PushDownStream(ctx, worker, req, func(typ string, data json.RawMessage) (promql.Timestamped, bool, error) {
		var zero promql.Timestamped
		if typ != "result" {
			return zero, false, nil
		}
		var si promql.Exemplar
		if err := json.Unmarshal(data, &si); err != nil {
			return zero, false, err
		}
		return si, true, nil
	})
}
