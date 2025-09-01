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
	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/promql"
	"github.com/google/uuid"
	"github.com/prometheus/common/model"
	"log/slog"
	"sync"
	"time"
)

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

	out := make(chan map[string]promql.EvalResult, 1024)

	go func() {
		var groupsWG sync.WaitGroup
		// bound group-level parallelism to num workers (override if you want with an env/flag)
		maxParallel := len(workers)
		if maxParallel <= 0 {
			maxParallel = 1
		}
		sem := make(chan struct{}, maxParallel)

	launchAll:
		for _, leaf := range queryPlan.Leaves {
			offMs, err := parseOffsetMs(leaf.Offset)
			if err != nil {
				slog.Error("invalid offset on leaf; ignoring offset", "offset", leaf.Offset, "err", err)
				offMs = 0
			}

			effStart := startTs - offMs
			effEnd := endTs - offMs
			dateIntHours := dateIntHoursRange(effStart, effEnd, time.UTC)

			for _, dateIntHour := range dateIntHours {
				select {
				case <-ctx.Done():
					break launchAll
				default:
				}

				segments, err := q.lookupMetricsSegments(ctx, dateIntHour, leaf, effStart, effEnd, stepDuration, orgID)
				if err != nil {
					slog.Error("failed to get segment infos", "dateInt", dateIntHour.DateInt, "err", err)
					continue
				}
				for i := range segments {
					segments[i].ExprID = leaf.ID
				}
				if len(segments) == 0 {
					continue
				}

				groups := ComputeReplayBatchesWithWorkers(
					segments, stepDuration, effStart, effEnd, len(workers), true,
				)

				for _, group := range groups {
					g := group
					l := leaf
					localOff := offMs

					groupsWG.Add(1)
					sem <- struct{}{} // acquire slot

					go func() {
						defer func() {
							<-sem // release slot
							groupsWG.Done()
						}()

						select {
						case <-ctx.Done():
							return
						default:
						}

						slog.Info("Pushing down segments", "groupSize", len(g.Segments))

						// Worker assignment
						segmentIDs := make([]int64, 0, len(g.Segments))
						segmentMap := make(map[int64]SegmentInfo, len(g.Segments))
						for _, s := range g.Segments {
							segmentIDs = append(segmentIDs, s.SegmentID)
							segmentMap[s.SegmentID] = s
						}
						mappings, err := q.workerDiscovery.GetWorkersForSegments(orgID, segmentIDs)
						if err != nil {
							slog.Error("failed to get worker assignments", "err", err)
							return
						}

						workerGroups := make(map[Worker][]SegmentInfo)
						for _, m := range mappings {
							workerGroups[m.Worker] = append(workerGroups[m.Worker], segmentMap[m.SegmentID])
						}

						// Per-worker pushdowns
						var groupLeafChans []<-chan promql.SketchInput
						for wkr, segs := range workerGroups {
							req := PushDownRequest{
								OrganizationID: orgID,
								BaseExpr:       &l,
								StartTs:        g.StartTs,
								EndTs:          g.EndTs,
								Segments:       segs,
								Step:           stepDuration,
							}
							ch, err := q.metricsPushDown(ctx, wkr, req)
							if err != nil {
								slog.Error("pushdown failed", "worker", wkr, "err", err)
								continue
							}
							if localOff != 0 {
								ch = shiftTimestamps(ctx, ch, localOff, 256)
							}
							groupLeafChans = append(groupLeafChans, ch)
						}
						if len(groupLeafChans) == 0 {
							return
						}

						// Keep per-group stream time-sorted
						mergedGroup := promql.MergeSorted(ctx, 1024, false, 0, groupLeafChans...)

						// EvalFlow for this group
						flow := NewEvalFlow(queryPlan.Root, queryPlan.Leaves, stepDuration, EvalFlowOptions{
							NumBuffers: 2,
							OutBuffer:  1024,
						})
						groupResults := flow.Run(ctx, mergedGroup)

						// Fan results into the shared 'out' as they arrive (interleaved across groups)
						for {
							select {
							case <-ctx.Done():
								return
							case res, ok := <-groupResults:
								if !ok {
									return
								}
								select {
								case <-ctx.Done():
									return
								case out <- res: // backpressure propagates here
								}
							}
						}
					}()
				}
			}
		}

		// wait for all groups to finish, then close
		groupsWG.Wait()
		close(out)
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
