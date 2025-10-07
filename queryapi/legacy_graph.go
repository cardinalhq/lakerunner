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
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/promql"
	"github.com/cardinalhq/oteltools/pkg/dateutils"
)

// handleGraphQuery handles the legacy /api/v1/graph endpoint.
func (q *QuerierService) handleGraphQuery(w http.ResponseWriter, r *http.Request) {
	// Parse request
	var req GraphRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
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
		writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "invalid time range: "+err.Error())
		return
	}

	// Setup SSE
	writeSSE, ok := q.sseWriter(w)
	if !ok {
		return
	}

	// Process each base expression
	for exprID, baseExpr := range req.BaseExpressions {
		if !baseExpr.ReturnResults {
			continue // Skip non-returning expressions
		}

		// Translate to LogQL
		logqlQuery, _, err := TranslateToLogQL(baseExpr)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "translation failed: "+err.Error())
			return
		}

		slog.Debug("Translated legacy query to LogQL",
			slog.String("exprID", exprID),
			slog.String("logql", logqlQuery))

		// Parse and compile LogQL
		logAst, err := logql.FromLogQL(logqlQuery)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "invalid LogQL: "+err.Error())
			return
		}

		lplan, err := logql.CompileLog(logAst)
		if err != nil {
			writeAPIError(w, http.StatusUnprocessableEntity, ErrCompileError, "compilation error: "+err.Error())
			return
		}

		// Pre-load label name maps for the query time range
		labelMaps, err := q.loadLabelMapsForTimeRange(r.Context(), orgID, startTs, endTs, lplan)
		if err != nil {
			slog.Warn("failed to load label name maps, using fallback denormalization",
				slog.String("error", err.Error()))
		}
		denormalizer := NewLabelDenormalizer(labelMaps)

		// Execute query
		reverse := baseExpr.Order == "DESC"
		limit := baseExpr.Limit
		if limit == 0 {
			limit = 1000 // Default
		}

		resultsCh, err := q.EvaluateLogsQuery(
			r.Context(),
			orgID,
			startTs,
			endTs,
			reverse,
			limit,
			lplan,
			nil, // fields
		)
		if err != nil {
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

			if err := writeSSE("event", event); err != nil {
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
	_ = writeSSE("done", doneEvent.Message)
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

	// Collect all segment IDs across all dateints and query leaves
	var allSegmentIDs []int64
	segmentIDSet := make(map[int64]bool)

	for _, leaf := range queryPlan.Leaves {
		for _, dih := range dateIntHours {
			segments, err := q.lookupLogsSegments(ctx, dih, leaf, startTs, endTs, orgID, q.mdb.ListLogSegmentsForQuery)
			if err != nil {
				slog.Debug("failed to lookup log segments for label maps", slog.String("error", err.Error()))
				continue
			}

			for _, seg := range segments {
				if !segmentIDSet[seg.SegmentID] {
					segmentIDSet[seg.SegmentID] = true
					allSegmentIDs = append(allSegmentIDs, seg.SegmentID)
				}
			}
		}
	}

	if len(allSegmentIDs) == 0 {
		return nil, nil
	}

	// Load label maps for all unique segments
	// We need to query by dateint, so group segments by dateint
	segmentsByDateint := make(map[int32][]int64)
	for _, dih := range dateIntHours {
		segmentsByDateint[int32(dih.DateInt)] = []int64{}
	}

	// Re-scan to group by dateint (we need dateint for the query)
	for _, leaf := range queryPlan.Leaves {
		for _, dih := range dateIntHours {
			segments, err := q.lookupLogsSegments(ctx, dih, leaf, startTs, endTs, orgID, q.mdb.ListLogSegmentsForQuery)
			if err != nil {
				continue
			}

			for _, seg := range segments {
				segmentsByDateint[int32(dih.DateInt)] = append(segmentsByDateint[int32(dih.DateInt)], seg.SegmentID)
			}
		}
	}

	// Load label maps for each dateint
	result := make(map[int64]map[string]string)
	for dateint, segmentIDs := range segmentsByDateint {
		if len(segmentIDs) == 0 {
			continue
		}

		// Remove duplicates
		uniqueIDs := make([]int64, 0, len(segmentIDs))
		seen := make(map[int64]bool)
		for _, id := range segmentIDs {
			if !seen[id] {
				seen[id] = true
				uniqueIDs = append(uniqueIDs, id)
			}
		}

		rows, err := q.mdb.GetLabelNameMaps(ctx, lrdb.GetLabelNameMapsParams{
			OrganizationID: orgID,
			Dateint:        dateint,
			SegmentIds:     uniqueIDs,
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
