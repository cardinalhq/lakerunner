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
	"net/http"
	"time"

	"github.com/cardinalhq/oteltools/pkg/dateutils"

	"github.com/cardinalhq/lakerunner/promql"
)

// handleGraphQuery handles the legacy /api/v1/graph endpoint.
func (q *QuerierService) handleGraphQuery(w http.ResponseWriter, r *http.Request) {
	// Parse request
	var req GraphRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Error("Failed to parse legacy graph query request",
			slog.String("error", err.Error()))
		writeAPIError(w, http.StatusBadRequest, InvalidJSON, "invalid JSON: "+err.Error())
		return
	}

	// Get org from context
	orgID, ok := GetOrgIDFromContext(r.Context())
	if !ok {
		writeAPIError(w, http.StatusUnauthorized, ErrUnauthorized, "missing organization")
		return
	}

	// Parse time range from query parameters
	s := r.URL.Query().Get("s")
	e := r.URL.Query().Get("e")
	startTs, endTs, err := dateutils.ToStartEnd(s, e)
	if err != nil {
		slog.Error("Invalid time range in legacy graph query",
			slog.String("start", s),
			slog.String("end", e),
			slog.String("error", err.Error()))
		writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "invalid time range: "+err.Error())
		return
	}

	slog.Info("Received legacy graph query request",
		slog.String("orgID", orgID.String()),
		slog.Int64("startTs", startTs),
		slog.Int64("endTs", endTs),
		slog.Int("expressionCount", len(req.BaseExpressions)))

	// Setup SSE manually (without the envelope wrapper)
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Helper to write SSE events directly (optimized to reduce system calls)
	writeSSE := func(data any) error {
		jsonData, err := json.Marshal(data)
		if err != nil {
			return err
		}
		// Use fmt.Fprintf to write in a single call
		if _, err := fmt.Fprintf(w, "data: %s\n\n", jsonData); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	// Start heartbeat to keep connection alive
	heartbeatCtx, cancelHeartbeat := context.WithCancel(r.Context())
	defer cancelHeartbeat()
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		heartbeat := map[string]string{"type": "heartbeat"}
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				_ = writeSSE(heartbeat)
			}
		}
	}()

	// Process each base expression
	for exprID, baseExpr := range req.BaseExpressions {
		if !baseExpr.ReturnResults {
			continue // Skip non-returning expressions
		}

		slog.Info("Processing legacy graph query expression",
			slog.String("exprID", exprID),
			slog.String("dataset", baseExpr.Dataset),
			slog.Int("limit", baseExpr.Limit),
			slog.String("order", baseExpr.Order))

		// Execute timeseries query using direct legacy path
		slog.Info("Executing direct legacy timeseries query (bypassing LogQL)",
			slog.String("exprID", exprID),
			slog.Int64("startTs", startTs),
			slog.Int64("endTs", endTs))

		evalResultsCh, err := q.EvaluateTimeseriesQueryDirect(
			r.Context(),
			orgID,
			startTs,
			endTs,
			baseExpr.Filter,
			exprID,
		)
		if err != nil {
			slog.Error("Failed to execute direct timeseries query",
				slog.String("exprID", exprID),
				slog.String("error", err.Error()))
			writeAPIError(w, http.StatusInternalServerError, ErrInternalError, "timeseries query execution failed: "+err.Error())
			return
		}

		// Load label maps for denormalization (use empty LogQL plan as we don't have one)
		// The denormalizer will fall back to heuristics
		denormalizer := NewLabelDenormalizer(nil)

		// Stream timeseries results
		timeseriesCount := 0
		for evalResults := range evalResultsCh {
			for _, evalResult := range evalResults {
				// Extract the count value
				count := evalResult.Value.Num

				// Skip NaN values (JSON doesn't support NaN)
				if math.IsNaN(count) {
					continue
				}

				// Extract segment_id from tags if available
				segmentID := int64(0)
				if sid, ok := evalResult.Tags["_segment_id"]; ok {
					if sidInt, ok := sid.(int64); ok {
						segmentID = sidInt
					}
				}

				event := ToLegacyTimeseriesEvent(exprID, segmentID, evalResult.Timestamp, count, evalResult.Tags, denormalizer)

				if err := writeSSE(event); err != nil {
					slog.Error("failed to write timeseries SSE event", slog.String("error", err.Error()))
					return
				}
				timeseriesCount++
			}
		}

		slog.Info("Completed streaming timeseries events for expression",
			slog.String("exprID", exprID),
			slog.Int("timeseriesCount", timeseriesCount))

		// Now execute raw events query using direct legacy path
		slog.Info("Using direct legacy AST path for raw events (bypassing LogQL)",
			slog.String("exprID", exprID))

		// Reuse denormalizer from timeseries query (label maps already loaded)

		// Execute query using direct legacy path
		reverse := baseExpr.Order == "DESC"
		limit := baseExpr.Limit
		if limit == 0 {
			limit = 1000 // Default
		}

		slog.Info("Executing direct legacy query",
			slog.String("exprID", exprID),
			slog.Int64("startTs", startTs),
			slog.Int64("endTs", endTs),
			slog.Bool("reverse", reverse),
			slog.Int("limit", limit))

		resultsCh, err := q.EvaluateLogsQueryDirect(
			r.Context(),
			orgID,
			startTs,
			endTs,
			reverse,
			limit,
			baseExpr.Filter,
			nil, // fields
		)
		if err != nil {
			slog.Error("Failed to execute direct legacy query",
				slog.String("exprID", exprID),
				slog.String("error", err.Error()))
			writeAPIError(w, http.StatusInternalServerError, ErrInternalError, "query execution failed: "+err.Error())
			return
		}

		// Stream results
		eventCount := 0
		for ts := range resultsCh {
			// For logs, we use promql.Exemplar which has Timestamp and Tags
			// The Value field is always 1.0 for log events (indicating presence)
			exemplar, ok := ts.(promql.Exemplar)
			if !ok {
				slog.Error("unexpected type in resultsCh; expected promql.Exemplar",
					slog.String("exprID", exprID))
				continue
			}

			// Extract segment_id from tags if available (added by query execution)
			segmentID := int64(0)
			if sid, ok := exemplar.Tags["_segment_id"]; ok {
				if sidInt, ok := sid.(int64); ok {
					segmentID = sidInt
				}
			}

			event := ToLegacySSEEvent(exprID, segmentID, ts.GetTimestamp(), 1.0, exemplar.Tags, denormalizer)

			if err := writeSSE(event); err != nil {
				slog.Error("failed to write SSE event", slog.String("error", err.Error()))
				return
			}
			eventCount++
		}

		slog.Info("Completed streaming events for expression",
			slog.String("exprID", exprID),
			slog.Int("eventCount", eventCount))
	}

	// Send done event
	slog.Debug("Sending done event")
	doneEvent := NewLegacyDoneEvent("query", "ok")
	_ = writeSSE(doneEvent)
}
