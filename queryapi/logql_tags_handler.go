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
	OrgID string `json:"orgId"`
	S     string `json:"s"`
	E     string `json:"e"`

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
	if p.OrgID == "" {
		http.Error(w, "missing orgId", http.StatusBadRequest)
		return nil
	}
	u, err := uuid.Parse(p.OrgID)
	if err != nil {
		http.Error(w, "invalid orgId: "+err.Error(), http.StatusBadRequest)
		return nil
	}
	p.OrgUUID = u

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
