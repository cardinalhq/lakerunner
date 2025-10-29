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
	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/lrdb"
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

		// First, execute timeseries query with count_over_time
		timeseriesQuery, _, err := TranslateToLogQLWithTimeseries(baseExpr, startTs, endTs)
		if err != nil {
			slog.Error("Failed to translate legacy query to LogQL with timeseries",
				slog.String("exprID", exprID),
				slog.String("error", err.Error()),
				slog.Any("baseExpr", baseExpr))
			writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "timeseries translation failed: "+err.Error())
			return
		}

		slog.Info("Translated legacy query to LogQL with timeseries",
			slog.String("exprID", exprID),
			slog.String("logql", timeseriesQuery))

		// Parse and compile timeseries LogQL
		timeseriesAst, err := logql.FromLogQL(timeseriesQuery)
		if err != nil {
			slog.Error("Failed to parse timeseries LogQL query",
				slog.String("exprID", exprID),
				slog.String("logql", timeseriesQuery),
				slog.String("error", err.Error()))
			writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "invalid timeseries LogQL: "+err.Error())
			return
		}

		lplan, err := logql.CompileLog(timeseriesAst)
		if err != nil {
			slog.Error("Failed to compile timeseries LogQL query",
				slog.String("exprID", exprID),
				slog.String("logql", timeseriesQuery),
				slog.String("error", err.Error()))
			writeAPIError(w, http.StatusUnprocessableEntity, ErrCompileError, "timeseries compilation error: "+err.Error())
			return
		}

		// Rewrite LogQL to PromQL for aggregation
		rr, err := promql.RewriteToPromQL(lplan.Root)
		if err != nil {
			slog.Error("Failed to rewrite timeseries query to PromQL",
				slog.String("exprID", exprID),
				slog.String("error", err.Error()))
			writeAPIError(w, http.StatusNotImplemented, ErrRewriteUnsupported, "cannot rewrite to PromQL: "+err.Error())
			return
		}

		promExpr, err := promql.FromPromQL(rr.PromQL)
		if err != nil {
			slog.Error("Failed to parse rewritten PromQL",
				slog.String("exprID", exprID),
				slog.String("error", err.Error()))
			writeAPIError(w, http.StatusInternalServerError, ErrInternalError, "cannot parse rewritten PromQL: "+err.Error())
			return
		}

		plan, err := promql.Compile(promExpr)
		if err != nil {
			slog.Error("Failed to compile PromQL plan",
				slog.String("exprID", exprID),
				slog.String("error", err.Error()))
			writeAPIError(w, http.StatusInternalServerError, ErrInternalError, "cannot compile rewritten PromQL: "+err.Error())
			return
		}
		plan.AttachLogLeaves(rr)

		// Execute timeseries query
		slog.Info("Executing timeseries metrics query",
			slog.String("exprID", exprID),
			slog.String("logql", timeseriesQuery),
			slog.String("promql", rr.PromQL),
			slog.Int64("startTs", startTs),
			slog.Int64("endTs", endTs))

		evalResultsCh, err := q.EvaluateMetricsQuery(
			r.Context(),
			orgID,
			startTs,
			endTs,
			plan,
		)
		if err != nil {
			slog.Error("Failed to execute timeseries metrics query",
				slog.String("exprID", exprID),
				slog.String("error", err.Error()))
			writeAPIError(w, http.StatusInternalServerError, ErrInternalError, "timeseries query execution failed: "+err.Error())
			return
		}

		// Load label maps once for both queries
		labelMaps, err := q.loadLabelMapsForTimeRange(r.Context(), orgID, startTs, endTs, lplan)
		if err != nil {
			slog.Warn("failed to load label name maps, using fallback denormalization",
				slog.String("error", err.Error()))
		}
		denormalizer := NewLabelDenormalizer(labelMaps)

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

		// Now execute raw events query
		logqlQuery, _, err := TranslateToLogQL(baseExpr)
		if err != nil {
			slog.Error("Failed to translate legacy query to LogQL",
				slog.String("exprID", exprID),
				slog.String("error", err.Error()),
				slog.Any("baseExpr", baseExpr))
			writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "translation failed: "+err.Error())
			return
		}

		slog.Info("Translated legacy query to LogQL for raw events",
			slog.String("exprID", exprID),
			slog.String("logql", logqlQuery))

		// Parse and compile LogQL
		logAst, err := logql.FromLogQL(logqlQuery)
		if err != nil {
			slog.Error("Failed to parse LogQL query",
				slog.String("exprID", exprID),
				slog.String("logql", logqlQuery),
				slog.String("error", err.Error()))
			writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "invalid LogQL: "+err.Error())
			return
		}

		rawPlan, err := logql.CompileLog(logAst)
		if err != nil {
			slog.Error("Failed to compile LogQL query",
				slog.String("exprID", exprID),
				slog.String("logql", logqlQuery),
				slog.String("error", err.Error()))
			writeAPIError(w, http.StatusUnprocessableEntity, ErrCompileError, "compilation error: "+err.Error())
			return
		}

		// Reuse denormalizer from timeseries query (label maps already loaded)

		// Execute query
		reverse := baseExpr.Order == "DESC"
		limit := baseExpr.Limit
		if limit == 0 {
			limit = 1000 // Default
		}

		slog.Info("Executing LogQL query",
			slog.String("exprID", exprID),
			slog.String("logql", logqlQuery),
			slog.Int64("startTs", startTs),
			slog.Int64("endTs", endTs),
			slog.Bool("reverse", reverse),
			slog.Int("limit", limit))

		resultsCh, err := q.EvaluateLogsQuery(
			r.Context(),
			orgID,
			startTs,
			endTs,
			reverse,
			limit,
			rawPlan,
			nil, // fields
		)
		if err != nil {
			slog.Error("Failed to execute LogQL query",
				slog.String("exprID", exprID),
				slog.String("logql", logqlQuery),
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

// loadLabelMapsForTimeRange loads label name maps for all segments in the query time range.
// Returns a map from segment_id to label name mapping.
func (q *QuerierService) loadLabelMapsForTimeRange(
	ctx context.Context,
	orgID uuid.UUID,
	startTs, endTs int64,
	queryPlan logql.LQueryPlan,
) (map[int64]map[string]string, error) {
	// Calculate dateints for the time range
	dateIntHours := dateIntHoursRange(startTs, endTs, time.UTC, false)

	// Collect unique segment IDs grouped by dateint in a single pass
	// Use a map of sets to track uniqueness per dateint
	segmentSetsByDateint := make(map[int32]map[int64]bool)

	for _, leaf := range queryPlan.Leaves {
		for _, dih := range dateIntHours {
			segments, err := q.lookupLogsSegments(ctx, dih, leaf, startTs, endTs, orgID, q.mdb.ListLogSegmentsForQuery)
			if err != nil {
				slog.Debug("failed to lookup log segments for label maps", slog.String("error", err.Error()))
				continue
			}

			dateint := int32(dih.DateInt)
			if segmentSetsByDateint[dateint] == nil {
				segmentSetsByDateint[dateint] = make(map[int64]bool)
			}

			for _, seg := range segments {
				segmentSetsByDateint[dateint][seg.SegmentID] = true
			}
		}
	}

	if len(segmentSetsByDateint) == 0 {
		return nil, nil
	}

	// Convert sets to slices for database query
	segmentsByDateint := make(map[int32][]int64, len(segmentSetsByDateint))
	for dateint, segmentSet := range segmentSetsByDateint {
		segments := make([]int64, 0, len(segmentSet))
		for segmentID := range segmentSet {
			segments = append(segments, segmentID)
		}
		segmentsByDateint[dateint] = segments
	}

	// Load label maps for each dateint
	result := make(map[int64]map[string]string)
	for dateint, segmentIDs := range segmentsByDateint {
		if len(segmentIDs) == 0 {
			continue
		}

		// Segment IDs are already unique from the collection phase
		rows, err := q.mdb.GetLabelNameMaps(ctx, lrdb.GetLabelNameMapsParams{
			OrganizationID: orgID,
			Dateint:        dateint,
			SegmentIds:     segmentIDs,
		})
		if err != nil {
			slog.Warn("failed to get label name maps for dateint",
				slog.Int("dateint", int(dateint)),
				slog.String("error", err.Error()))
			continue
		}

		// Parse JSONB label maps
		for _, row := range rows {
			if len(row.LabelNameMap) == 0 {
				continue
			}

			var labelMap map[string]string
			if err := json.Unmarshal(row.LabelNameMap, &labelMap); err != nil {
				slog.Warn("failed to parse label name map",
					slog.Int64("segment_id", row.SegmentID),
					slog.String("error", err.Error()))
				continue
			}

			result[row.SegmentID] = labelMap
		}
	}

	if len(result) > 0 {
		slog.Debug("loaded label name maps for legacy query",
			slog.Int("segment_count", len(result)))
	}

	return result, nil
}
