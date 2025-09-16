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
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/common/model"

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/promql"
)

// registration for the coordinator
type groupReg struct {
	idx     int
	startTs int64
	endTs   int64
	ch      <-chan promql.SketchInput
}

// max(3, numWorkers); never below 1
func computeMaxParallel(numWorkers int) int {
	if numWorkers < 1 {
		return 1
	}
	if numWorkers < 3 {
		return 3
	}
	return numWorkers
}

// concat groups in index order; streams as soon as idx=0 registers.
func runOrderedCoordinator(ctx context.Context, regs <-chan groupReg) <-chan promql.SketchInput {
	out := make(chan promql.SketchInput, 4096)
	go func() {
		defer close(out)

		pending := map[int]groupReg{}
		want := 0
		var cur <-chan promql.SketchInput
		closed := false

		for {
			// Try to start the next expected group when we're idle.
			if cur == nil {
				if gr, ok := pending[want]; ok {
					delete(pending, want)
					slog.Info("Group starting emit", "idx", want, "groupStart", gr.startTs, "groupEnd", gr.endTs)
					want++
					cur = gr.ch
				} else if closed {
					// No current, nothing pending, registry is closed → done.
					return
				}
			}

			select {
			case v, ok := <-cur:
				if !ok {
					cur = nil
					continue
				}
				select {
				case out <- v:
				case <-ctx.Done():
					return
				}

			case gr, ok := <-regs:
				if !ok {
					// Registry closed; keep draining current and any pending.
					closed = true
					regs = nil // remove this select arm
					continue
				}
				pending[gr.idx] = gr

			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

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
		defer close(out)

		// ---------- Stage 0: start coordinator & EvalFlow upfront ----------
		regs := make(chan groupReg, 256)
		coordinated := runOrderedCoordinator(ctx, regs)

		flow := NewEvalFlow(queryPlan.Root, queryPlan.Leaves, stepDuration, EvalFlowOptions{
			NumBuffers: 2,
			OutBuffer:  1024,
		})
		results := flow.Run(ctx, coordinated)

		// fan-out EvalFlow results immediately
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				select {
				case <-ctx.Done():
					return
				case res, ok := <-results:
					if !ok {
						return
					}
					select {
					case out <- res:
					case <-ctx.Done():
						return
					}
				}
			}
		}()

		// ---------- Stage 1: build segment universe ----------
		segmentUniverse := make([]SegmentInfo, 0)
		globalStart := startTs
		globalEnd := endTs
		leavesByID := make(map[string]promql.BaseExpr, len(queryPlan.Leaves))

		for _, leaf := range queryPlan.Leaves {
			leavesByID[leaf.ID] = leaf

			offMs, err := parseOffsetMs(leaf.Offset)
			if err != nil {
				slog.Error("invalid offset on leaf; ignoring offset", "offset", leaf.Offset, "err", err)
				offMs = 0
			}
			baseStart := startTs
			if leaf.Range != "" {
				baseStart -= promql.RangeMsFromRange(leaf.Range)
			}
			effStart := baseStart - offMs
			effEnd := endTs - offMs

			if effStart < globalStart {
				globalStart = effStart
			}
			if effEnd > globalEnd {
				globalEnd = effEnd
			}

			for _, dih := range dateIntHoursRange(effStart, effEnd, time.UTC, false) {
				var segments []SegmentInfo
				if leaf.LogLeaf != nil {
					// logs query
					segments, err = q.lookupLogsSegments(ctx, dih, *leaf.LogLeaf, effStart, effEnd, orgID, q.mdb.ListLogSegmentsForQuery)
					slog.Info("Logs Metadata Query for segments", "numSegments", len(segments))
				} else {
					// metrics query
					segments, err = q.lookupMetricsSegments(ctx, dih, leaf, effStart, effEnd, stepDuration, orgID)
				}
				if err != nil {
					slog.Error("failed to get segment infos", "dateInt", dih.DateInt, "err", err)
					continue
				}
				for i := range segments {
					segments[i].ExprID = leaf.ID
				}
				if len(segments) > 0 {
					segmentUniverse = append(segmentUniverse, segments...)
				}
			}
		}

		// ---------- Stage 2: group globally (time-disjoint) ----------
		groups := ComputeReplayBatchesWithWorkers(
			segmentUniverse, stepDuration, globalStart, globalEnd, len(workers), false,
		)

		// helper: pick one worker per group (round-robin)
		pickWorkerForGroup := func(groupIdx int) (Worker, bool) {
			if len(workers) == 0 {
				return Worker{}, false
			}
			return workers[groupIdx%len(workers)], true
		}

		// ---------- Stage 3: launch groups concurrently & register ----------
		maxParallel := len(workers)
		if maxParallel < 1 {
			maxParallel = 1
		}
		sem := make(chan struct{}, maxParallel)
		var regWG sync.WaitGroup

		for gi, group := range groups {
			regWG.Add(1)
			sem <- struct{}{}
			go func(gi int, group SegmentGroup) {
				defer func() { <-sem }()
				defer regWG.Done()

				// choose ONE worker for the entire group
				worker, ok := pickWorkerForGroup(gi)
				if !ok {
					slog.Error("no workers available; registering empty group stream", "idx", gi)
					empty := make(chan promql.SketchInput)
					close(empty)
					select {
					case regs <- groupReg{idx: gi, startTs: group.StartTs, endTs: group.EndTs, ch: empty}:
					case <-ctx.Done():
					}
					return
				}
				slog.Info("Assigning group to worker",
					"idx", gi, "groupStart", group.StartTs, "groupEnd", group.EndTs,
					"worker", fmt.Sprintf("%s:%d", worker.IP, worker.Port))

				// split group by leaf
				segmentsByLeaf := make(map[string][]SegmentInfo)
				for _, s := range group.Segments {
					segmentsByLeaf[s.ExprID] = append(segmentsByLeaf[s.ExprID], s)
				}

				// per-leaf pushdowns → all to the SAME worker
				leafChans := make([]<-chan promql.SketchInput, 0, len(segmentsByLeaf))
				for leafID, segmentsForLeaf := range segmentsByLeaf {
					leaf := leavesByID[leafID]
					offMs, err := parseOffsetMs(leaf.Offset)
					if err != nil {
						slog.Error("invalid offset on leaf; ignoring offset", "offset", leaf.Offset, "err", err)
						offMs = 0
					}

					slog.Info("Pushing down segments",
						"groupIndex", gi, "leafID", leafID, "leafSegments", len(segmentsForLeaf),
						"groupStart", group.StartTs, "groupEnd", group.EndTs,
						"worker", fmt.Sprintf("%s:%d", worker.IP, worker.Port))

					var reqStart int64 = math.MaxInt64
					reqEnd := int64(0)

					for _, s := range segmentsForLeaf {
						if s.StartTs < reqStart {
							reqStart = s.StartTs
						}
						if s.EndTs > reqEnd {
							reqEnd = s.EndTs
						}
					}

					req := PushDownRequest{
						OrganizationID: orgID,
						BaseExpr:       &leaf,
						StartTs:        reqStart,
						EndTs:          reqEnd + 10000,
						Segments:       segmentsForLeaf,
						Step:           stepDuration,
					}

					ch, err := q.metricsPushDown(ctx, worker, req)
					if err != nil {
						slog.Error("pushdown failed", "worker", worker, "leafID", leafID, "err", err)
						continue
					}
					if offMs != 0 {
						ch = shiftTimestamps(ctx, ch, offMs, 256)
					}

					// tap for visibility
					tag := fmt.Sprintf("g=%d leaf=%s %s:%d", gi, leafID, worker.IP, worker.Port)
					ch = tapStream(ctx, ch, tag)

					leafChans = append(leafChans, ch)
				}

				// if nothing survived, register a closed stream to keep ordering happy
				if len(leafChans) == 0 {
					empty := make(chan promql.SketchInput)
					close(empty)
					select {
					case regs <- groupReg{idx: gi, startTs: group.StartTs, endTs: group.EndTs, ch: empty}:
					case <-ctx.Done():
					}
					return
				}

				// merge across leaves within this group and register immediately
				groupChan := promql.MergeSorted(ctx, 1024, false, 0, leafChans...)
				slog.Info("Registering group stream", "idx", gi, "groupStart", group.StartTs, "groupEnd", group.EndTs)
				select {
				case regs <- groupReg{idx: gi, startTs: group.StartTs, endTs: group.EndTs, ch: groupChan}:
				case <-ctx.Done():
					// if cancelled, still register a closed stream so coordinator advances
					empty := make(chan promql.SketchInput)
					close(empty)
					select {
					case regs <- groupReg{idx: gi, startTs: group.StartTs, endTs: group.EndTs, ch: empty}:
					case <-ctx.Done():
					}
				}
			}(gi, group)
		}

		// close registry when all groups have registered
		go func() { regWG.Wait(); close(regs) }()

		// wait for EvalFlow to finish, then return
		<-done
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

func tapStream(ctx context.Context, in <-chan promql.SketchInput, tag string) <-chan promql.SketchInput {
	out := make(chan promql.SketchInput, 256)
	go func() {
		defer close(out)
		var n int
		var tmin, tmax int64
		first := true
		for {
			select {
			case <-ctx.Done():
				slog.Info("pushdown stream closed (ctx)", "tag", tag, "count", n, "tmin", tmin, "tmax", tmax)
				return
			case v, ok := <-in:
				if !ok {
					slog.Info("pushdown stream closed (eof)", "tag", tag, "count", n, "tmin", tmin, "tmax", tmax)
					return
				}
				if first {
					tmin, tmax = v.GetTimestamp(), v.GetTimestamp()
					first = false
				} else {
					if ts := v.GetTimestamp(); ts < tmin {
						tmin = ts
					} else if ts > tmax {
						tmax = ts
					}
				}
				n++
				select {
				case out <- v:
				case <-ctx.Done():
					slog.Info("pushdown stream closed (ctx while forward)", "tag", tag, "count", n, "tmin", tmin, "tmax", tmax)
					return
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

	fingerprint := computeFingerprint("_cardinalhq.name", be.Metric)

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

	slog.Info("Metrics Metadata Query for segments",
		"dateInt", dih.DateInt,
		"startTs", startTs,
		"endTs", endTs,
		"frequencyMs", stepDuration.Milliseconds(),
		"orgUUID", orgUUID,
		"fingerprint", fingerprint,
		"numSegments", len(rows))
	for _, row := range rows {
		startHour := zeroFilledHour(time.UnixMilli(row.StartTs).UTC().Hour())
		allSegments = append(allSegments, SegmentInfo{
			DateInt:        dih.DateInt,
			Hour:           startHour,
			SegmentID:      row.SegmentID,
			StartTs:        row.StartTs,
			EndTs:          row.EndTs,
			OrganizationID: orgUUID,
			InstanceNum:    row.InstanceNum,
			Frequency:      stepDuration.Milliseconds(),
		})
	}

	return allSegments, nil
}
