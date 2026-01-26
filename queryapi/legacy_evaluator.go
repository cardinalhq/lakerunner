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
	"math"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/promql"
)

// EvaluateLogsQueryDirect evaluates a logs query using the direct legacy AST path.
// It bypasses LogQL translation and uses LegacyLeaf for segment selection and SQL generation.
func (q *QuerierService) EvaluateLogsQueryDirect(
	ctx context.Context,
	orgID uuid.UUID,
	startTs, endTs int64,
	reverse bool,
	limit int,
	filter QueryClause,
	fields []string,
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

		// If limit <= 0, treat as unlimited.
		unlimited := limit <= 0

		// Cancellation for the entire pushdown tree.
		ctxAll, cancelAll := context.WithCancel(ctx)
		defer cancelAll()

		emitted := 0

		// Partition by dateInt hours for storage listing.
		dateIntHours := dateIntHoursRange(startTs, endTs, time.UTC, reverse)

	outer:
		for _, dih := range dateIntHours {
			// Use direct segment selection from legacy filter
			segments, fpToSegments, err := SelectSegmentsFromLegacyFilter(
				ctxAll, dih, filter, startTs, endTs, orgID, q.mdb.ListLogSegmentsForQuery,
			)
			if err != nil {
				slog.Error("failed to lookup log segments", "err", err, "dih", dih)
				return
			}
			if len(segments) == 0 {
				continue
			}

			// Prune the filter to remove references to non-existent fields
			prunedFilter := PruneFilterForMissingFields(filter, fpToSegments)

			// Create LegacyLeaf for SQL generation with pruned filter
			legacyLeaf := &LegacyLeaf{Filter: prunedFilter}

			groups := ComputeReplayBatchesWithWorkers(segments, DefaultLogStep, startTs, endTs, len(workers), reverse)
			for _, group := range groups {
				select {
				case <-ctxAll.Done():
					return
				default:
				}

				// Respect global limit across groups
				remaining := math.MaxInt // large default
				if !unlimited {
					remaining = limit - emitted
					if remaining <= 0 {
						cancelAll()
						break outer
					}
				}

				slog.Info("Pushing down segments", "groupSize", len(group.Segments), "remaining", remaining)

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

				var groupLeafChans []<-chan promql.Timestamped
				for worker, workerSegments := range workerGroups {
					reqLimit := 0
					if !unlimited {
						reqLimit = remaining
					}
					req := PushDownRequest{
						OrganizationID: orgID,
						LegacyLeaf:     legacyLeaf, // Use LegacyLeaf instead of LogLeaf
						StartTs:        group.StartTs,
						EndTs:          group.EndTs,
						Segments:       workerSegments,
						Step:           stepDuration,
						Limit:          reqLimit,
						Reverse:        reverse,
						Fields:         fields,
					}
					ch, err := PushDownStream(ctxAll, worker, req, func(typ string, data json.RawMessage) (promql.Timestamped, bool, error) {
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
					if err != nil {
						slog.Error("pushdown failed", "worker", worker, "err", err)
						continue
					}
					groupLeafChans = append(groupLeafChans, ch)
				}

				if len(groupLeafChans) == 0 {
					continue
				}

				// Merge this group's worker streams by timestamp
				mergeLimit := 0 // unlimited for the merge by default
				if !unlimited {
					mergeLimit = remaining
				}
				mergedGroup := promql.MergeSorted(ctxAll, 1024, reverse, mergeLimit, groupLeafChans...)

				// Forward results (and stop globally when limit hit)
				for {
					select {
					case <-ctxAll.Done():
						return
					case res, ok := <-mergedGroup:
						if !ok {
							// Group done
							goto nextGroup
						}
						out <- res
						if !unlimited {
							emitted++
							if emitted >= limit {
								cancelAll() // stop all remaining work
								break outer
							}
						}
					}
				}
			nextGroup:
			}
		}
	}()

	return out, nil
}
