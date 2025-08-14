package promql

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/google/uuid"
	"github.com/prometheus/common/model"
)

// PushDownRequest is sent to a worker.
type PushDownRequest struct {
	BaseExpr BaseExpr      `json:"baseExpr"`
	StartTs  int64         `json:"startTs"`
	EndTs    int64         `json:"endTs"`
	Segments []SegmentInfo `json:"segments"`
}

// Evaluate plans pushdowns, fans requests out to workers, merges their streams,
// and returns a single chronologically merged stream of SketchInput.
// The merged stream’s timestamps are aligned to the evaluation window.
func (q *QuerierService) Evaluate(
	ctx context.Context,
	orgID uuid.UUID,
	startTs, endTs int64,
	queryPlan QueryPlan,
	reverseSort bool,
) (<-chan map[string]EvalResult, error) {
	stepDuration := stepForQueryDuration(startTs, endTs)

	var allLeafChans []<-chan SketchInput

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
			segments, err := q.lookupSegments(ctx, dateIntHour, effStart, effEnd, stepDuration, orgID)
			if err != nil {
				slog.Error("failed to get segment infos", "dateInt", dateIntHour.DateInt, "err", err)
				continue
			}
			// Tag segments with this leaf id so worker knows which expr it is serving.
			for i := range segments {
				segments[i].ExprID = leaf.ID
			}

			if len(segments) == 0 {
				continue
			}

			// Collect all segment IDs for worker assignment
			segmentIDs := make([]string, 0, len(segments))
			segmentMap := make(map[string][]SegmentInfo)
			for _, segment := range segments {
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
				segs := segmentMap[mapping.SegmentID]
				workerGroups[mapping.Worker] = append(workerGroups[mapping.Worker], segs...)
			}

			// Create pushdown requests for each worker
			for worker, workerSegments := range workerGroups {
				req := PushDownRequest{
					BaseExpr: leaf,
					StartTs:  effStart,
					EndTs:    effEnd,
					Segments: workerSegments,
				}

				// Push down to worker; get its stream back.
				ch, err := q.pushDown(ctx, worker, req)
				if err != nil {
					slog.Error("pushdown failed", "worker", worker, "err", err)
					continue
				}

				if offMs != 0 {
					ch = shiftTimestamps(ctx, ch, offMs, 256)
				}

				allLeafChans = append(allLeafChans, ch)
			}
		}
	}

	// Nothing to merge → return closed chan.
	if len(allLeafChans) == 0 {
		slog.Info("no pushdowns produced any channels")
		out := make(chan SketchInput)
		close(out)
		return nil, fmt.Errorf("no pushdowns produced any channels")
	}

	// Merge all worker streams by timestamp (ascending).
	merged := MergeSorted[SketchInput](ctx, reverseSort, 1024, allLeafChans...)
	// Pipe through EvalFlow (aggregator -> root.Eval)
	flow := NewEvalFlow(queryPlan.Root, queryPlan.Leaves, stepDuration, EvalFlowOptions{
		NumBuffers: 2,
		OutBuffer:  1024,
	})
	results := flow.Run(ctx, merged)
	return results, nil
}

// pushDown should POST req to the worker’s /pushdown and return a channel that yields SketchInput
// decoded from the worker’s SSE (or chunked JSON) stream. You can keep your existing stub here.
// Implement the HTTP/SSE client and decoding where you wire up workers.
func (q *QuerierService) pushDown(ctx context.Context, worker Worker, request PushDownRequest) (<-chan SketchInput, error) {
	// TODO: implement: POST http://{worker.IP}:{worker.Port}/pushdown, stream response -> decode -> chan SketchInput
	// Return a channel that closes when the stream ends or ctx is canceled.

	// List Of Segments:
	// Execute sub-batches of segments, and then calling "MergeSorted" on the n-channels.
	// while(resultSet.next) {
	// --> send to channel
	// }
	return nil, fmt.Errorf("pushDown not implemented")
}

// shiftTimestamps returns a channel that forwards every SketchInput from `in`
// with its Timestamp shifted by +deltaMs. Non-blocking via buffered output.
func shiftTimestamps(ctx context.Context, in <-chan SketchInput, deltaMs int64, outBuf int) <-chan SketchInput {
	out := make(chan SketchInput, outBuf)
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

// lookupSegments is unchanged from your version.
func (q *QuerierService) lookupSegments(ctx context.Context,
	dih DateIntHours,
	startTs int64, endTs int64,
	stepDuration time.Duration,
	orgUUID uuid.UUID) ([]SegmentInfo, error) {

	var allSegments []SegmentInfo
	rows, err := q.mdb.ListSegmentsForQuery(ctx, lrdb.ListSegmentsForQueryParams{
		Int8range:      startTs,
		Int8range_2:    endTs,
		Dateint:        int32(dih.DateInt),
		FrequencyMs:    int32(stepDuration.Milliseconds()),
		OrganizationID: orgUUID,
	})
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		endHour := zeroFilledHour(time.UnixMilli(row.EndTs).UTC().Hour())
		allSegments = append(allSegments, SegmentInfo{
			DateInt:     dih.DateInt,
			Hour:        endHour,
			SegmentID:   fmt.Sprintf("tbl_%d", row.SegmentID),
			StartTs:     row.StartTs,
			EndTs:       row.EndTs,
			ExprID:      "",
			Dataset:     "metrics",
			BucketName:  "bucket",
			CustomerID:  orgUUID.String(),
			CollectorID: "collectorId",
			Frequency:   stepDuration.Milliseconds(),
		})
	}
	return allSegments, nil
}
