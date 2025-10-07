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
	"strconv"

	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/promql"
	"github.com/cardinalhq/oteltools/pkg/dateutils"
)

// handleTagsQuery handles the legacy /api/v1/tags/logs endpoint.
// This endpoint returns unique tag names (if tagName not provided) or unique tag values (if tagName provided).
func (q *QuerierService) handleTagsQuery(w http.ResponseWriter, r *http.Request) {
	// Get org from context
	orgID, ok := GetOrgIDFromContext(r.Context())
	if !ok {
		writeAPIError(w, http.StatusUnauthorized, ErrUnauthorized, "missing organization")
		return
	}

	// Parse query parameters
	tagName := r.URL.Query().Get("tagName")
	limit := int64(1000) // Default limit
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

	// Parse request body (filter expression)
	var qBody string
	if r.Body != nil {
		bodyBytes := make([]byte, r.ContentLength)
		if _, err := r.Body.Read(bodyBytes); err == nil {
			qBody = string(bodyBytes)
		}
	}

	// Build LogQL query
	var logqlQuery string
	if qBody != "" {
		// Parse the filter and convert to LogQL
		baseExpr, err := parseBaseExpr(qBody)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "invalid filter: "+err.Error())
			return
		}
		logqlQuery, _, err = TranslateToLogQL(baseExpr)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "translation failed: "+err.Error())
			return
		}
	} else {
		// Default: match all logs
		logqlQuery = "{}"
	}

	slog.Debug("Tags query",
		slog.String("tagName", tagName),
		slog.String("logql", logqlQuery),
		slog.Int64("limit", limit))

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

	// Setup SSE
	writeSSE, ok := q.sseWriter(w)
	if !ok {
		return
	}

	// Execute query with a reasonable limit for tag discovery
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

	// Collect unique tag names or tag values
	seenTags := make(map[string]bool)
	count := int64(0)

	for ts := range resultsCh {
		if count >= limit {
			break
		}

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

		if tagName == "" {
			// Return unique tag names
			for key := range denormalizedTags {
				if !seenTags[key] && count < limit {
					seenTags[key] = true
					payload := map[string]string{"tag": key}
					jsonBytes, _ := json.Marshal(payload)
					if err := writeSSE("tag", string(jsonBytes)); err != nil {
						slog.Error("failed to write SSE event", slog.String("error", err.Error()))
						return
					}
					count++
				}
			}
		} else {
			// Return unique values for the specified tag
			if value, ok := denormalizedTags[tagName]; ok {
				valueStr := ""
				switch v := value.(type) {
				case string:
					valueStr = v
				case int64:
					valueStr = strconv.FormatInt(v, 10)
				case float64:
					valueStr = strconv.FormatFloat(v, 'f', -1, 64)
				case bool:
					valueStr = strconv.FormatBool(v)
				default:
					valueStr = ""
				}

				if valueStr != "" && !seenTags[valueStr] && count < limit {
					seenTags[valueStr] = true
					payload := map[string]string{"value": valueStr}
					jsonBytes, _ := json.Marshal(payload)
					if err := writeSSE("value", string(jsonBytes)); err != nil {
						slog.Error("failed to write SSE event", slog.String("error", err.Error()))
						return
					}
					count++
				}
			}
		}
	}

	// Send done event
	doneEvent := NewLegacyDoneEvent("tags", "ok")
	_ = writeSSE("done", doneEvent.Message)
}

// parseBaseExpr parses a legacy query body into a BaseExpression.
// This is a simplified parser for the tags endpoint.
func parseBaseExpr(qBody string) (BaseExpression, error) {
	var baseExpr BaseExpression
	if err := json.Unmarshal([]byte(qBody), &baseExpr); err != nil {
		return baseExpr, err
	}
	baseExpr.Dataset = "logs" // Ensure dataset is set
	return baseExpr, nil
}
