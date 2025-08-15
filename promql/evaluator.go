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

package promql

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/google/uuid"
	"github.com/prometheus/common/model"
)

// PushDownRequest is sent to a worker.
type PushDownRequest struct {
	OrganizationID uuid.UUID     `json:"orgId"`
	BaseExpr       BaseExpr      `json:"baseExpr"`
	StartTs        int64         `json:"startTs"`
	EndTs          int64         `json:"endTs"`
	Step           time.Duration `json:"step"`
	Segments       []SegmentInfo `json:"segments"`
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

	workers, err := q.workerDiscovery.GetAllWorkers()
	if err != nil {
		slog.Error("failed to get all workers", "err", err)
		return nil, fmt.Errorf("failed to get all workers: %w", err)
	}

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

			// Form time-contiguous batches sized for the number of workers.
			groups := ComputeReplayBatchesWithWorkers(segments, stepDuration, effStart, effEnd, len(workers), true)

			for _, group := range groups {
				// Collect all segment IDs for worker assignment
				segmentIDs := make([]string, 0, len(group.Segments))
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
					segmentList := segmentMap[mapping.SegmentID]
					workerGroups[mapping.Worker] = append(workerGroups[mapping.Worker], segmentList...)
				}

				for worker, workerSegments := range workerGroups {
					req := PushDownRequest{
						OrganizationID: orgID,
						BaseExpr:       leaf,
						StartTs:        group.StartTs,
						EndTs:          group.EndTs,
						Segments:       workerSegments,
						Step:           stepDuration,
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
	}

	// Nothing to merge → return closed chan.
	if len(allLeafChans) == 0 {
		slog.Info("no pushdowns produced any channels")
		out := make(chan SketchInput)
		close(out)
		return nil, fmt.Errorf("no pushdowns produced any channels")
	}

	// Merge all worker streams by timestamp (ascending).
	merged := MergeSorted(ctx, reverseSort, 1024, allLeafChans...)
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
	sql := request.BaseExpr.ToWorkerSQL(request.Step)
	if sql == "" {
		return nil, fmt.Errorf("no SQL generated for expression")
	}

	if IsLocalDev() {
		sql = strings.ReplaceAll(sql, "{start}", fmt.Sprintf("%d", 0))
		sql = strings.ReplaceAll(sql, "{end}", fmt.Sprintf("%d", time.Now().UnixMilli()))
		sql = strings.ReplaceAll(sql, "{table}", "read_parquet('./db/*.parquet')")
	} else {
		sql = strings.ReplaceAll(sql, "{start}", fmt.Sprintf("%d", request.StartTs))
		sql = strings.ReplaceAll(sql, "{end}", fmt.Sprintf("%d", request.EndTs))
		sql = strings.ReplaceAll(sql, "{table}", fmt.Sprintf("'%s'", "worker.ParquetPath"))
	}
	slog.Info("Executing SQL on worker", "sql", sql)

	rows, err := q.ddb.Query(ctx, sql)
	if err != nil {
		slog.Error("failed to query worker", "worker", worker, "err", err.Error())
		return nil, fmt.Errorf("failed to query worker %w", err)
	}

	out := make(chan SketchInput, 1024)
	go func() {
		defer close(out)
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			slog.Error("failed to get columns", "err", err)
			return
		}

		for rows.Next() {
			vals := make([]interface{}, len(cols))
			ptrs := make([]interface{}, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}

			if err := rows.Scan(ptrs...); err != nil {
				slog.Error("failed to scan row", "err", err)
				continue
			}

			var ts int64
			agg := map[string]float64{}
			tags := map[string]any{}

			for i, col := range cols {
				switch col {
				case "bucket_ts":
					switch v := vals[i].(type) {
					case int64:
						ts = v
					case int32:
						ts = int64(v)
					case int:
						ts = int64(v)
					default:
						slog.Error("unexpected type for bucket_ts", "value", vals[i])
						continue
					}
				case SUM, COUNT, MIN, MAX:
					if vals[i] == nil {
						continue
					}
					switch v := vals[i].(type) {
					case float64:
						agg[col] = v
					case float32:
						agg[col] = float64(v)
					case int64:
						agg[col] = float64(v)
					case int32:
						agg[col] = float64(v)
					case int:
						agg[col] = float64(v)
					default:
						slog.Warn("unexpected numeric type in agg", "col", col, "value", vals[i])
					}
				default:
					if vals[i] != nil {
						tags[col] = vals[i]
					}
				}
			}

			slog.Info("making sketch input", "ts", ts)
			out <- SketchInput{
				ExprID:         request.BaseExpr.ID,
				OrganizationID: request.OrganizationID.String(),
				Timestamp:      ts,
				Frequency:      int64(request.Step.Seconds()),
				SketchTags: SketchTags{
					Tags:       tags,
					SketchType: SketchMAP,
					Agg:        agg,
				},
			}
		}

		if err := rows.Err(); err != nil {
			slog.Error("row iteration error", "err", err)
		}
	}()

	return out, nil
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

func (q *QuerierService) lookupSegments(ctx context.Context,
	dih DateIntHours,
	startTs int64, endTs int64,
	stepDuration time.Duration,
	orgUUID uuid.UUID) ([]SegmentInfo, error) {

	var allSegments []SegmentInfo

	if IsLocalDev() {
		files, err := os.ReadDir("./db")
		if err != nil {
			return nil, fmt.Errorf("failed to read local db dir: %w", err)
		}
		for _, f := range files {
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".parquet") {
				continue
			}

			allSegments = append(allSegments, SegmentInfo{
				SegmentID:  f.Name(),
				StartTs:    startTs,
				EndTs:      endTs,
				CustomerID: orgUUID.String(),
				Frequency:  stepDuration.Milliseconds(),
			})
		}
		return allSegments, nil
	}

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
			Dataset:     "metrics",
			BucketName:  "bucket",
			CustomerID:  orgUUID.String(),
			CollectorID: "collectorId",
			Frequency:   stepDuration.Milliseconds(),
		})
	}

	return allSegments, nil
}
