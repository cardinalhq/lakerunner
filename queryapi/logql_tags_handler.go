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
)

type logqlTagsPayload struct {
	S string `json:"s"`
	E string `json:"e"`

	OrgUUID uuid.UUID `json:"-"`
	StartTs int64     `json:"-"`
	EndTs   int64     `json:"-"`
}

type tagsResponse struct {
	Tags []string `json:"tags"`
}

func readLogQLTagsPayload(w http.ResponseWriter, r *http.Request) *logqlTagsPayload {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST method is allowed", http.StatusMethodNotAllowed)
		return nil
	}
	if ct := r.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		http.Error(w, "unsupported content type", http.StatusBadRequest)
		return nil
	}
	body, _ := io.ReadAll(r.Body)
	defer r.Body.Close()

	var p logqlTagsPayload
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

func (q *QuerierService) handleListLogQLTags(w http.ResponseWriter, r *http.Request) {
	p := readLogQLTagsPayload(w, r)
	if p == nil {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	tags, err := q.mdb.ListLogQLTags(ctx, p.OrgUUID)

	if err != nil {
		slog.Error("ListLogQLTags failed", "org", p.OrgUUID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(tagsResponse{Tags: tags})
}
