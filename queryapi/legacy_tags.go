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
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/promql"
	"github.com/cardinalhq/oteltools/pkg/dateutils"
)

// handleTagsQuery handles the legacy /api/v1/tags/logs endpoint.
// This endpoint returns sample log records to discover available tag names.
func (q *QuerierService) handleTagsQuery(w http.ResponseWriter, r *http.Request) {
	// Get org from context
	orgID, ok := GetOrgIDFromContext(r.Context())
	if !ok {
		writeAPIError(w, http.StatusUnauthorized, ErrUnauthorized, "missing organization")
		return
	}

	// Parse query parameters
	limit := int64(100) // Default limit for tag discovery
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if parsedLimit, err := strconv.ParseInt(limitStr, 10, 64); err == nil {
			limit = parsedLimit
		}
	}

	s := r.URL.Query().Get("s")
	e := r.URL.Query().Get("e")
	startTs, endTs, err := dateutils.ToStartEnd(s, e)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "invalid time range: "+err.Error())
		return
	}

	slog.Debug("Tags query",
		slog.Int64("startTs", startTs),
		slog.Int64("endTs", endTs),
		slog.Int64("limit", limit))

	// Use a query that matches logs with any fingerprint (always exists)
	// This avoids the empty matcher validation error
	logqlQuery := "{chq_fingerprint=~\".+\"}"

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

	// Setup SSE manually (without the envelope wrapper)
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Execute query to get sample logs
	resultsCh, err := q.EvaluateLogsQuery(
		r.Context(),
		orgID,
		startTs,
		endTs,
		false, // not reversed
		int(limit),
		lplan,
		nil, // fields
	)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, ErrInternalError, "query execution failed: "+err.Error())
		return
	}

	// Pre-load label maps for denormalization
	labelMaps, err := q.loadLabelMapsForTimeRange(r.Context(), orgID, startTs, endTs, lplan)
	if err != nil {
		slog.Warn("failed to load label name maps for tags query",
			slog.String("error", err.Error()))
	}
	denormalizer := NewLabelDenormalizer(labelMaps)

	// Stream sample log records
	for ts := range resultsCh {
		exemplar, ok := ts.(promql.Exemplar)
		if !ok {
			continue
		}

		// Extract segment_id for proper denormalization
		segmentID := int64(0)
		if sid, ok := exemplar.Tags["_segment_id"]; ok {
			if sidInt, ok := sid.(int64); ok {
				segmentID = sidInt
			}
		}

		// Denormalize tag names
		denormalizedTags := denormalizer.DenormalizeMap(segmentID, exemplar.Tags)

		// Format as legacy response
		event := map[string]interface{}{
			"id":      "_",
			"type":    "data",
			"message": denormalizedTags,
		}

		// Write SSE event directly
		jsonData, err := json.Marshal(event)
		if err != nil {
			slog.Error("failed to marshal event", slog.String("error", err.Error()))
			return
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", jsonData); err != nil {
			slog.Error("failed to write SSE event", slog.String("error", err.Error()))
			return
		}
		flusher.Flush()
	}

	// Send done event
	doneEvent := NewLegacyDoneEvent("tags", "ok")
	jsonData, _ := json.Marshal(doneEvent)
	_, _ = fmt.Fprintf(w, "data: %s\n\n", jsonData)
	flusher.Flush()
}
