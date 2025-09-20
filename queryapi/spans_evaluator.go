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
	"math"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/promql"

	"github.com/google/codesearch/index"
)

const (
	DefaultSpansStep = 10 * time.Second
)

func (q *QuerierService) EvaluateSpansQuery(
	ctx context.Context,
	orgID uuid.UUID,
	startTs, endTs int64,
	reverse bool,
	limit int,
	queryPlan logql.LQueryPlan,
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
		for _, leaf := range queryPlan.Leaves {
			for _, dih := range dateIntHours {
				segments, err := q.lookupSpansSegments(ctxAll, dih, leaf, startTs, endTs, orgID, q.mdb.ListTraceSegmentsForQuery)
				slog.Info("lookupSpansSegments", "dih", dih, "leaf", leaf, "found", len(segments))
				if err != nil {
					slog.Error("failed to lookup spans segments", "err", err, "dih", dih, "leaf", leaf)
					return
				}
				if len(segments) == 0 {
					continue
				}

				groups := ComputeReplayBatchesWithWorkers(segments, DefaultSpansStep, startTs, endTs, len(workers), reverse)
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
							LogLeaf:        &leaf,
							StartTs:        group.StartTs,
							EndTs:          group.EndTs,
							Segments:       workerSegments,
							Step:           stepDuration,
							Limit:          reqLimit,
							Reverse:        reverse,
							Fields:         fields,
							IsSpans:        true,
						}
						ch, err := q.spansPushDown(ctxAll, worker, req)
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
		}
	}()
	return out, nil
}

func (q *QuerierService) spansPushDown(
	ctx context.Context,
	worker Worker,
	request PushDownRequest,
) (<-chan promql.Timestamped, error) {
	// Modify the request to use spans-specific SQL generation
	spansRequest := request
	if spansRequest.LogLeaf != nil {
		// Create a copy of the LogLeaf and modify it to use spans SQL generation
		leaf := *spansRequest.LogLeaf
		spansRequest.LogLeaf = &leaf
	}

	return PushDownStream[promql.Timestamped](ctx, worker, spansRequest,
		func(typ string, data json.RawMessage) (promql.Timestamped, bool, error) {
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

type TraceSegmentLookupFunc func(context.Context, lrdb.ListTraceSegmentsForQueryParams) ([]lrdb.ListTraceSegmentsForQueryRow, error)

const spansNameField = "_cardinalhq.name"

func (q *QuerierService) lookupSpansSegments(
	ctx context.Context,
	dih DateIntHours,
	leaf logql.LogLeaf,
	startTs, endTs int64,
	orgUUID uuid.UUID,
	lookupFunc TraceSegmentLookupFunc,
) ([]SegmentInfo, error) {
	root := &TrigramQuery{Op: index.QAll}
	fpsToFetch := make(map[int64]struct{})

	for _, lm := range leaf.Matchers {
		label, val := lm.Label, lm.Value
		if !slices.Contains(dimensionsToIndex, label) {
			addExistsNode(label, fpsToFetch, &root)
			continue
		}
		switch lm.Op {
		case logql.MatchEq, logql.MatchRe:
			addAndNodeFromPattern(label, val, fpsToFetch, &root)
		default:
			addExistsNode(label, fpsToFetch, &root)
		}
	}

	for _, lf := range leaf.LabelFilters {
		if lf.ParserIdx != nil {
			continue
		}
		label, val := lf.Label, lf.Value
		if !slices.Contains(dimensionsToIndex, label) {
			addExistsNode(label, fpsToFetch, &root)
			continue
		}
		switch lf.Op {
		case logql.MatchEq, logql.MatchRe:
			addAndNodeFromPattern(label, val, fpsToFetch, &root)
		default:
			addExistsNode(label, fpsToFetch, &root)
		}
	}

	slog.Info("lookupSpansSegments", "dih", dih, "startTs", startTs, "endTs", endTs, "fps", len(fpsToFetch))
	if len(fpsToFetch) == 0 {
		// For spans, default to _cardinalhq.name field instead of _cardinalhq.message
		addExistsNode(spansNameField, fpsToFetch, &root)
	}

	// 2) Fetch candidate segments for the UNION of all fingerprints.
	fpList := make([]int64, 0, len(fpsToFetch))
	for fp := range fpsToFetch {
		fpList = append(fpList, fp)
	}
	slices.Sort(fpList)

	rows, err := lookupFunc(ctx, lrdb.ListTraceSegmentsForQueryParams{
		OrganizationID: orgUUID,
		Dateint:        int32(dih.DateInt),
		Fingerprints:   fpList,
		S:              startTs,
		E:              endTs,
	})
	if err != nil {
		return nil, fmt.Errorf("list trace segments for query: %w", err)
	}

	if len(rows) == 0 {
		slog.Info("lookupSpansSegments: no segments found", "dih", dih, "startTs", startTs, "endTs", endTs, slog.Any("fps", fpsToFetch))
	}

	fpToSegments := make(map[int64][]SegmentInfo, len(rows))
	for _, row := range rows {
		startHour := zeroFilledHour(time.UnixMilli(row.StartTs).UTC().Hour())
		seg := SegmentInfo{
			DateInt:        dih.DateInt,
			Hour:           startHour,
			SegmentID:      row.SegmentID,
			StartTs:        row.StartTs,
			EndTs:          row.EndTs,
			OrganizationID: orgUUID,
			InstanceNum:    row.InstanceNum,
			Frequency:      10000,
		}
		fpToSegments[row.Fingerprint] = append(fpToSegments[row.Fingerprint], seg)
	}

	finalSet := computeSegmentSet(root, fpToSegments)

	out := make([]SegmentInfo, 0, len(finalSet))
	for s := range finalSet {
		out = append(out, s)
	}
	return out, nil
}
