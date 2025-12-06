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
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// logsSeriesPayload is the request payload for /api/v1/logs/series
type logsSeriesPayload struct {
	S string `json:"s"`
	E string `json:"e"`

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

// handleListLogSeries returns distinct log streams (stream_ids) for a time range.
// Response format is Loki-compatible: {"status": "success", "data": [{"stream_id": "value"}, ...]}
func (q *QuerierService) handleListLogSeries(w http.ResponseWriter, r *http.Request) {
	p := readLogsSeriesPayload(w, r)
	if p == nil {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	// Convert timestamps to dateint for partition pruning
	startDateint, _ := helpers.MSToDateintHour(p.StartTs)
	endDateint, _ := helpers.MSToDateintHour(p.EndTs)

	streamIDs, err := q.mdb.ListLogStreamIDs(ctx, lrdb.ListLogStreamIDsParams{
		OrganizationID: p.OrgUUID,
		StartDateint:   startDateint,
		EndDateint:     endDateint,
		StartTs:        p.StartTs,
		EndTs:          p.EndTs,
	})
	if err != nil {
		slog.Error("ListLogStreamIDs failed", "org", p.OrgUUID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Convert to Loki-compatible format
	streamIDKey := string(wkk.RowKeyCStreamID.Value())
	data := make([]map[string]string, 0, len(streamIDs))
	for _, streamID := range streamIDs {
		data = append(data, map[string]string{streamIDKey: streamID})
	}

	resp := LokiSeriesResponse{
		Status: "success",
		Data:   data,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
