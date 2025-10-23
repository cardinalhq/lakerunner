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
	"net/http"
	"strings"
	"time"

	"github.com/cardinalhq/oteltools/pkg/dateutils"

	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/promql"
)

// handleTagsQuery handles the legacy /api/v1/tags/logs endpoint.
// This endpoint returns distinct values for a specific tag name.
func (q *QuerierService) handleTagsQuery(w http.ResponseWriter, r *http.Request) {
	// Get org from context
	orgID, ok := GetOrgIDFromContext(r.Context())
	if !ok {
		writeAPIError(w, http.StatusUnauthorized, ErrUnauthorized, "missing organization")
		return
	}

	// Parse request body to get the filter
	var req BaseExpression
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		slog.Error("Failed to parse legacy tags query request",
			slog.String("error", err.Error()))
		writeAPIError(w, http.StatusBadRequest, InvalidJSON, "invalid JSON: "+err.Error())
		return
	}

	// Override dataset to "logs" if not set
	if req.Dataset == "" {
		req.Dataset = "logs"
	}

	// Validate dataset
	if req.Dataset != "logs" {
		writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "only 'logs' dataset is supported")
		return
	}

	// Get the tag name from query parameters (optional)
	// If provided: returns distinct VALUES for the specified tag that match the filter
	// If not provided: returns distinct tag NAMES (field names) that match the filter
	tagName := r.URL.Query().Get("tagName")

	// Parse time range
	s := r.URL.Query().Get("s")
	e := r.URL.Query().Get("e")
	startTs, endTs, err := dateutils.ToStartEnd(s, e)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "invalid time range: "+err.Error())
		return
	}

	if tagName != "" {
		slog.Debug("Tags query - getting distinct values for tag",
			slog.String("tagName", tagName),
			slog.Int64("startTs", startTs),
			slog.Int64("endTs", endTs))
	} else {
		slog.Debug("Tags query - getting all tag names",
			slog.Int64("startTs", startTs),
			slog.Int64("endTs", endTs))
	}

	// Translate the filter to LogQL, just like /api/v1/graph does
	logqlQuery, _, err := TranslateToLogQL(req)
	if err != nil {
		slog.Error("Failed to translate legacy tags query to LogQL",
			slog.String("error", err.Error()),
			slog.Any("baseExpr", req))
		writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "translation failed: "+err.Error())
		return
	}

	slog.Debug("Translated legacy tags query to LogQL",
		slog.String("logql", logqlQuery))

	// Parse and compile LogQL
	logAst, err := logql.FromLogQL(logqlQuery)
	if err != nil {
		slog.Error("Failed to parse LogQL query",
			slog.String("logql", logqlQuery),
			slog.String("error", err.Error()))
		writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "invalid LogQL: "+err.Error())
		return
	}

	lplan, err := logql.CompileLog(logAst)
	if err != nil {
		slog.Error("Failed to compile LogQL query",
			slog.String("logql", logqlQuery),
			slog.String("error", err.Error()))
		writeAPIError(w, http.StatusUnprocessableEntity, ErrCompileError, "compilation error: "+err.Error())
		return
	}

	// Set the tag name for tag value extraction (if provided)
	// If tagName is empty, we'll be doing tag name discovery instead
	if tagName != "" {
		lplan.TagName = strings.ReplaceAll(tagName, ".", "_")
	} else {
		lplan.NeedTagNames = true
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
				jsonData, _ := json.Marshal(heartbeat)
				_, _ = fmt.Fprintf(w, "data: %s\n\n", jsonData)
				flusher.Flush()
			}
		}
	}()

	// Execute tag values query to get distinct values for the requested tag
	resultsCh, err := q.EvaluateLogTagValuesQuery(r.Context(), orgID, startTs, endTs, lplan)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, ErrInternalError, "query execution failed: "+err.Error())
		return
	}

	// Stream tag value results
	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			slog.Info("client disconnected; stopping stream")
			return
		case res, more := <-resultsCh:
			if !more {
				// Send done event
				doneEvent := NewLegacyDoneEvent("tags", "ok")
				jsonData, _ := json.Marshal(doneEvent)
				_, _ = fmt.Fprintf(w, "data: %s\n\n", jsonData)
				flusher.Flush()
				return
			}

			// If we're doing tag name discovery (tagName is empty), the result will be
			// a comma-separated list of column names. Split and emit each separately.
			var values []string
			if tagName == "" {
				// Tag names mode: split the comma-separated list
				tv, ok := res.(promql.TagValue)
				if !ok {
					slog.Error("expected TagValue for tag names", slog.Any("res", res))
					continue
				}
				values = strings.Split(tv.Value, ",")
			} else {
				// Tag values mode: emit single value
				tv, ok := res.(promql.TagValue)
				if !ok {
					slog.Error("expected TagValue for tag values", slog.Any("res", res))
					continue
				}
				values = []string{tv.Value}
			}

			// Write each value as a separate SSE event
			for _, val := range values {
				// Format as legacy response - just the tag value in the message field
				event := map[string]interface{}{
					"id":      "_",
					"type":    "data",
					"message": val,
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
		}
	}
}
