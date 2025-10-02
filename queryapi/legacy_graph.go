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
	"log/slog"
	"net/http"

	"github.com/cardinalhq/lakerunner/logql"
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

		// TODO: Load segment label maps for proper denormalization
		// For now, use fallback-only denormalizer
		denormalizer := NewLabelDenormalizer(nil)

		// Stream results
		for ts := range resultsCh {
			// TODO: Get actual segment ID from result
			// For now, use 0 (will use fallback denormalization)
			segmentID := int64(0)

			// For logs, we use promql.Exemplar which has Timestamp and Tags
			// The Value field is always 1.0 for log events (indicating presence)
			event := ToLegacySSEEvent(exprID, segmentID, ts.GetTimestamp(), 1.0, ts.(promql.Exemplar).Tags, denormalizer)

			if err := writeSSE("event", event); err != nil {
				slog.Error("failed to write SSE event", slog.String("error", err.Error()))
				return
			}
		}
	}

	// Send done event
	doneEvent := NewLegacyDoneEvent("query", "ok")
	_ = writeSSE("done", doneEvent.Message)
}
