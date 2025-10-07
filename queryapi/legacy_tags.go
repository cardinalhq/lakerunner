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
)

// handleTagsQuery handles the legacy /api/v1/tags/logs endpoint.
// This endpoint returns unique tag names available for querying.
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

	slog.Debug("Tags query",
		slog.String("tagName", tagName),
		slog.Int64("limit", limit))

	// Setup SSE
	writeSSE, ok := q.sseWriter(w)
	if !ok {
		return
	}

	if tagName != "" {
		// If tagName is specified, this is a tag values query - not supported in this simplified endpoint
		writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "tag values query not supported, only tag names")
		return
	}

	// Query database for available tag names
	tags, err := q.mdb.ListLogQLTags(r.Context(), orgID)
	if err != nil {
		slog.Error("ListLogQLTags failed", "org", orgID, "error", err)
		writeAPIError(w, http.StatusInternalServerError, ErrInternalError, "failed to list tags: "+err.Error())
		return
	}

	// Denormalize tag names (underscore to dotted format)
	denormalizer := NewLabelDenormalizer(nil) // No segment-specific maps needed for tag names
	count := int64(0)

	for _, tag := range tags {
		if count >= limit {
			break
		}

		// Denormalize the tag name
		denormalized := denormalizer.Denormalize(0, tag)

		payload := map[string]string{"tag": denormalized}
		jsonBytes, _ := json.Marshal(payload)
		if err := writeSSE("tag", string(jsonBytes)); err != nil {
			slog.Error("failed to write SSE event", slog.String("error", err.Error()))
			return
		}
		count++
	}

	// Send done event
	doneEvent := NewLegacyDoneEvent("tags", "ok")
	_ = writeSSE("done", doneEvent.Message)
}
