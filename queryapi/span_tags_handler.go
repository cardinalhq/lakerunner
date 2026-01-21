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
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/cardinalhq/oteltools/pkg/dateutils"
	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/promql"
)

type spanTagsPayload struct {
	S string `json:"s"`
	E string `json:"e"`
	Q string `json:"q,omitempty"` // Optional LogQL-style selector for scoping

	OrgUUID uuid.UUID `json:"-"`
	StartTs int64     `json:"-"`
	EndTs   int64     `json:"-"`
}

func readSpanTagsPayload(w http.ResponseWriter, r *http.Request) *spanTagsPayload {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST method is allowed", http.StatusMethodNotAllowed)
		return nil
	}
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		http.Error(w, "unsupported content type", http.StatusBadRequest)
		return nil
	}
	body, _ := io.ReadAll(r.Body)
	defer func() { _ = r.Body.Close() }()

	var p spanTagsPayload
	if err := json.Unmarshal(body, &p); err != nil {
		http.Error(w, "invalid JSON body: "+err.Error(), http.StatusBadRequest)
		return nil
	}

	// Get orgId from context (set by middleware)
	orgUUID, ok := GetOrgIDFromContext(r.Context())
	if !ok {
		http.Error(w, "organization ID not found in context", http.StatusInternalServerError)
		return nil
	}
	p.OrgUUID = orgUUID

	st, en, err := dateutils.ToStartEnd(p.S, p.E)
	if err != nil {
		http.Error(w, "invalid start/end time: "+err.Error(), http.StatusBadRequest)
		return nil
	}
	p.StartTs, p.EndTs = st, en
	return &p
}

// handleListSpanTags returns distinct tag names (label keys) for spans.
// Supports optional 'q' parameter for scoping results to tags that exist on spans matching a LogQL-style selector.
// - No filter: Fast DB-only path using segment metadata
// - With filter: Query workers to find tags that exist in matching rows
func (q *QuerierService) handleListSpanTags(w http.ResponseWriter, r *http.Request) {
	p := readSpanTagsPayload(w, r)
	if p == nil {
		return
	}

	// Calculate dateint from the request time range
	startDateint, _ := helpers.MSToDateintHour(p.StartTs)
	endDateint, _ := helpers.MSToDateintHour(p.EndTs)

	if p.Q == "" {
		// No filter - use fast DB-only path
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		tags, err := q.mdb.ListSpanTags(ctx, lrdb.ListSpanTagsParams{
			OrganizationID: p.OrgUUID,
			StartDateint:   startDateint,
			EndDateint:     endDateint,
		})
		if err != nil {
			slog.Error("ListSpanTags failed", "org", p.OrgUUID, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(tagsResponse{Tags: tags})
		return
	}

	// With filter - use worker query path for accurate scoped results
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Parse the LogQL-style selector (spans use same syntax as logs)
	logAst, parseErr := logql.FromLogQL(p.Q)
	if parseErr != nil {
		http.Error(w, "invalid query expression: "+parseErr.Error(), http.StatusBadRequest)
		return
	}
	lplan, compileErr := logql.CompileLog(logAst)
	if compileErr != nil {
		http.Error(w, "compile error: "+compileErr.Error(), http.StatusBadRequest)
		return
	}

	// Query workers for tag names matching the filter
	resultsCh, err := q.EvaluateSpanTagNamesQuery(ctx, p.OrgUUID, p.StartTs, p.EndTs, lplan)
	if err != nil {
		slog.Error("EvaluateSpanTagNamesQuery failed", "org", p.OrgUUID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Collect all tag names from the channel
	var tags []string
	for res := range resultsCh {
		if tv, ok := res.(promql.TagValue); ok {
			tags = append(tags, tv.Value)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(tagsResponse{Tags: tags})
}
