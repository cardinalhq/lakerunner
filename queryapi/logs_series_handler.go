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
	"regexp"
	"strings"
	"time"

	"github.com/cardinalhq/oteltools/pkg/dateutils"
	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// logsSeriesPayload is the request payload for /api/v1/logs/series
type logsSeriesPayload struct {
	S string `json:"s"`
	E string `json:"e"`
	Q string `json:"q,omitempty"` // Optional LogQL selector for scoping

	OrgUUID uuid.UUID `json:"-"`
	StartTs int64     `json:"-"`
	EndTs   int64     `json:"-"`
}

// LokiSeriesResponse mimics Loki's /loki/api/v1/series response format.
// This allows clients to reuse existing Loki-compatible code.
type LokiSeriesResponse struct {
	Status string              `json:"status"`
	Data   []map[string]string `json:"data"`
}

func readLogsSeriesPayload(w http.ResponseWriter, r *http.Request) *logsSeriesPayload {
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

	var p logsSeriesPayload
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

// handleListLogSeries returns distinct log streams for a time range.
// Response format is Loki-compatible: {"status": "success", "data": [{field_name: "value"}, ...]}
// The field_name is the actual source field (resource_customer_domain, resource_service_name, or stream_id for legacy).
// Supports optional 'q' parameter for scoping results to streams matching a LogQL selector.
func (q *QuerierService) handleListLogSeries(w http.ResponseWriter, r *http.Request) {
	p := readLogsSeriesPayload(w, r)
	if p == nil {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Parse the optional LogQL selector for filtering
	// Each leaf represents an OR branch; matchers within a leaf are AND'd together
	var matcherGroups [][]logql.LabelMatch
	if p.Q != "" {
		logAst, err := logql.FromLogQL(p.Q)
		if err != nil {
			http.Error(w, "invalid query expression: "+err.Error(), http.StatusBadRequest)
			return
		}
		lplan, err := logql.CompileLog(logAst)
		if err != nil {
			http.Error(w, "compile error: "+err.Error(), http.StatusBadRequest)
			return
		}
		// Keep matchers grouped by leaf to preserve OR semantics
		for _, leaf := range lplan.Leaves {
			matcherGroups = append(matcherGroups, leaf.Matchers)
		}
	}

	// Convert timestamps to dateint for partition pruning
	startDateint, _ := helpers.MSToDateintHour(p.StartTs)
	endDateint, _ := helpers.MSToDateintHour(p.EndTs)

	streams, err := q.mdb.ListLogStreams(ctx, lrdb.ListLogStreamsParams{
		OrganizationID: p.OrgUUID,
		StartDateint:   startDateint,
		EndDateint:     endDateint,
		StartTs:        p.StartTs,
		EndTs:          p.EndTs,
	})
	if err != nil {
		slog.Error("ListLogStreams failed", "org", p.OrgUUID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Convert to Loki-compatible format using the actual field name as the key
	// Apply matchers filter if provided
	data := make([]map[string]string, 0, len(streams))
	for _, stream := range streams {
		// Skip entries with nil FieldName - these are broken data that can't be queried
		if stream.FieldName == nil {
			continue
		}

		// Apply matchers filter if provided (OR across groups, AND within each group)
		if len(matcherGroups) > 0 && !matchesSeriesGroups(*stream.FieldName, stream.StreamValue, matcherGroups) {
			continue
		}

		data = append(data, map[string]string{*stream.FieldName: stream.StreamValue})
	}

	resp := LokiSeriesResponse{
		Status: "success",
		Data:   data,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// matchesSeriesGroups checks if a series matches any of the matcher groups (OR semantics).
// Each group represents a LogQL leaf; within each group, all matchers must match (AND).
func matchesSeriesGroups(fieldName, value string, groups [][]logql.LabelMatch) bool {
	for _, matchers := range groups {
		if matchesSeries(fieldName, value, matchers) {
			return true
		}
	}
	return false
}

// matchesSeries checks if a series (field_name, value) matches the given matchers.
// Returns true if all matchers that apply to this field_name match.
func matchesSeries(fieldName, value string, matchers []logql.LabelMatch) bool {
	for _, m := range matchers {
		// Only apply matchers that match this field name
		if m.Label != fieldName {
			continue
		}

		matched := false
		switch m.Op {
		case logql.MatchEq:
			matched = value == m.Value
		case logql.MatchNe:
			matched = value != m.Value
		case logql.MatchRe:
			re, err := regexp.Compile("^(?:" + m.Value + ")$")
			if err != nil {
				slog.Error("failed to compile regex for series matcher", "field", fieldName, "pattern", m.Value, "error", err)
				return false
			}
			matched = re.MatchString(value)
		case logql.MatchNre:
			re, err := regexp.Compile("^(?:" + m.Value + ")$")
			if err != nil {
				slog.Error("failed to compile regex for series matcher", "field", fieldName, "pattern", m.Value, "error", err)
				return false
			}
			matched = !re.MatchString(value)
		}

		if !matched {
			return false
		}
	}
	return true
}
