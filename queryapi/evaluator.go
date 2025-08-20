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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cardinalhq/lakerunner/promql"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/common/model"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// PushDownRequest is sent to a worker.
type PushDownRequest struct {
	OrganizationID uuid.UUID            `json:"orgId"`
	BaseExpr       promql.BaseExpr      `json:"baseExpr"`
	StartTs        int64                `json:"startTs"`
	EndTs          int64                `json:"endTs"`
	Step           time.Duration        `json:"step"`
	Segments       []promql.SegmentInfo `json:"segments"`
}

// Evaluate plans pushdowns, fans requests out to workers, merges their streams,
// and returns a single chronologically merged stream of SketchInput.
// The merged stream’s timestamps are aligned to the evaluation window.
func (q *QuerierService) Evaluate(
	ctx context.Context,
	orgID uuid.UUID,
	startTs, endTs int64,
	queryPlan promql.QueryPlan,
) (<-chan map[string]promql.EvalResult, error) {
	stepDuration := StepForQueryDuration(startTs, endTs)

	var allLeafChans []<-chan promql.SketchInput

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
			groups := promql.ComputeReplayBatchesWithWorkers(segments, stepDuration, effStart, effEnd, len(workers), true)

			for _, group := range groups {
				// Collect all segment IDs for worker assignment
				segmentIDs := make([]int64, 0, len(group.Segments))
				segmentMap := make(map[int64][]promql.SegmentInfo)
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
				workerGroups := make(map[Worker][]promql.SegmentInfo)
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
		out := make(chan promql.SketchInput)
		close(out)
		return nil, fmt.Errorf("no pushdowns produced any channels")
	}

	// Merge all worker streams by timestamp (ascending).
	merged := promql.MergeSorted(ctx, 1024, allLeafChans...)
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
func (q *QuerierService) pushDown(
	ctx context.Context,
	worker Worker,
	request PushDownRequest,
) (<-chan promql.SketchInput, error) {
	// --- Build request ---
	u := fmt.Sprintf("http://%s:%d/api/v1/pushDown", worker.IP, worker.Port)

	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal PushDownRequest: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	// Use a client you already have if available (e.g., q.httpClient)
	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("worker request failed: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				slog.Error("failed to close response body", "err", err)
			}
		}(resp.Body)
		return nil, fmt.Errorf("worker returned %s", resp.Status)
	}

	out := make(chan promql.SketchInput, 1024)

	go func() {
		defer close(out)
		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				slog.Error("failed to close response body", "err", err)
			}
		}(resp.Body)

		type envelope struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}

		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 2*1024*1024) // grow if needed

		var dataBuf strings.Builder

		flush := func() bool {
			if dataBuf.Len() == 0 {
				return true
			}
			payload := dataBuf.String()
			dataBuf.Reset()

			var env envelope
			if err := json.Unmarshal([]byte(payload), &env); err != nil {
				slog.Error("SSE json unmarshal failed", "err", err)
				return false
			}

			switch env.Type {
			case "result":
				var si promql.SketchInput
				if err := json.Unmarshal(env.Data, &si); err != nil {
					slog.Error("SSE data unmarshal failed", "err", err)
					return false
				}
				select {
				case <-ctx.Done():
					return false
				case out <- si:
				}
			case "done":
				return false
			default:
				// ignore unknown types
			}
			return true
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if !sc.Scan() {
				// Scanner ended (EOF or error). Flush any pending data once.
				_ = flush()
				if err := sc.Err(); err != nil && !errors.Is(err, io.EOF) {
					slog.Error("SSE scanner error", "err", err)
				}
				return
			}

			line := sc.Text()
			if line == "" {
				// blank line → event boundary
				if !flush() {
					return
				}
				continue
			}

			// Only collect "data:" lines (ignore other SSE fields)
			if strings.HasPrefix(line, "data:") {
				// Trim "data:" prefix & space
				chunk := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
				if dataBuf.Len() > 0 {
					dataBuf.WriteByte('\n')
				}
				dataBuf.WriteString(chunk)
			}
		}
	}()

	return out, nil
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

func (q *QuerierService) lookupSegments(ctx context.Context,
	dih DateIntHours,
	startTs int64, endTs int64,
	stepDuration time.Duration,
	orgUUID uuid.UUID) ([]promql.SegmentInfo, error) {

	var allSegments []promql.SegmentInfo

	if IsLocalDev() {
		files, err := os.ReadDir("./db")
		if err != nil {
			return nil, fmt.Errorf("failed to read local db dir: %w", err)
		}
		for _, f := range files {
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".parquet") {
				continue
			}
			numericSegmentId := strings.Replace(strings.Replace(f.Name(), ".parquet", "", 1), "tbl_", "", 1)
			segmentID, err := strconv.ParseInt(numericSegmentId, 10, 64)
			if err != nil {
				slog.Error("failed to parse segment ID from filename", "filename", f.Name(), "err", err)
				continue
			}
			allSegments = append(allSegments, promql.SegmentInfo{
				SegmentID:      segmentID,
				StartTs:        startTs,
				EndTs:          endTs,
				OrganizationID: orgUUID,
				InstanceNum:    0,
				Frequency:      stepDuration.Milliseconds(),
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
		allSegments = append(allSegments, promql.SegmentInfo{
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
