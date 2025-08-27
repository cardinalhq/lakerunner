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
	"errors"
	"fmt"
	"github.com/cardinalhq/lakerunner/promql"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/cardinalhq/oteltools/pkg/dateutils"
	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/lrdb"
)

type QuerierService struct {
	mdb             lrdb.StoreFull
	workerDiscovery WorkerDiscovery
}

// NewQuerierService creates a new QuerierService with the given database store and worker discovery.
func NewQuerierService(mdb lrdb.StoreFull, workerDiscovery WorkerDiscovery) (*QuerierService, error) {
	return &QuerierService{
		mdb:             mdb,
		workerDiscovery: workerDiscovery,
	}, nil
}

// queryPayload is accepted for POST application/json bodies.
type queryPayload struct {
	OrgID   string `json:"orgId"`
	S       string `json:"s"`       // start
	E       string `json:"e"`       // end
	Q       string `json:"q"`       // promql
	Reverse *bool  `json:"reverse"` // optional; defaults to true
}

func (q *QuerierService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Defaults / fallbacks (GET-compatible)
	orgID := r.URL.Query().Get("orgId")
	s := r.URL.Query().Get("s")
	e := r.URL.Query().Get("e")
	prom := r.URL.Query().Get("q")

	// Allow POST with body (JSON or text/plain)
	if r.Method == http.MethodPost {
		ct := r.Header.Get("Content-Type")
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()

		switch {
		case strings.HasPrefix(ct, "application/json"):
			var p queryPayload
			if err := json.Unmarshal(body, &p); err != nil {
				http.Error(w, "invalid JSON body: "+err.Error(), http.StatusBadRequest)
				return
			}
			// Body values override query params when provided
			if p.OrgID != "" {
				orgID = p.OrgID
			}
			if p.S != "" {
				s = p.S
			}
			if p.E != "" {
				e = p.E
			}
			if p.Q != "" {
				prom = p.Q
			}

		case strings.HasPrefix(ct, "text/plain"):
			// Treat entire body as the PromQL expression; other fields can still
			// come via query params (or you can pass them in JSON if you prefer).
			if len(body) > 0 {
				prom = strings.TrimSpace(string(body))
			}

		default:
			// Best-effort: if body is non-empty, try to use it as PromQL.
			if len(body) > 0 && prom == "" {
				prom = strings.TrimSpace(string(body))
			}
		}
	}

	// Validate inputs
	if orgID == "" {
		http.Error(w, "missing orgId", http.StatusBadRequest)
		return
	}
	if s == "" || e == "" {
		http.Error(w, "missing s/e", http.StatusBadRequest)
		return
	}
	startTs, endTs, err := dateutils.ToStartEnd(s, e)
	if err != nil {
		http.Error(w, "invalid s/e: "+err.Error(), http.StatusBadRequest)
		return
	}
	if startTs >= endTs {
		http.Error(w, "start must be < end", http.StatusBadRequest)
		return
	}
	if prom == "" {
		http.Error(w, "missing query expression (q)", http.StatusBadRequest)
		return
	}
	orgUUID, err := uuid.Parse(orgID)
	if err != nil {
		http.Error(w, "invalid orgId: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Parse & compile PromQL
	promExpr, err := promql.FromPromQL(prom)
	if err != nil {
		http.Error(w, "invalid query expression: "+err.Error(), http.StatusBadRequest)
		return
	}
	plan, err := promql.Compile(promExpr)
	if err != nil {
		http.Error(w, "compile error: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Kick off evaluation; reverseSort can be toggled via input
	resultsCh, err := q.Evaluate(r.Context(), orgUUID, startTs, endTs, plan)
	if err != nil {
		http.Error(w, "evaluate error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// SSE setup (same response shape for GET and POST)
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	writeSSE := func(event string, v any) error {
		type envelope struct {
			Type string `json:"type"`
			Data any    `json:"data,omitempty"`
		}
		env := envelope{Type: event, Data: v}

		data, err := json.Marshal(env)
		if err != nil {
			return err
		}
		if _, err := w.Write([]byte("data: ")); err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
		if _, err := w.Write([]byte("\n\n")); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	// Stream results until channel closes or client disconnects.
	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			slog.Info("client disconnected; stopping stream")
			return
		case res, ok := <-resultsCh:
			if !ok {
				_ = writeSSE("done", map[string]string{"status": "ok"})
				return
			}
			if err := writeSSE("result", res); err != nil {
				slog.Error("write SSE failed", "error", err)
				return
			}
		}
	}
}

func (q *QuerierService) Run(doneCtx context.Context) error {
	slog.Info("Starting querier service")

	mux := http.NewServeMux()
	mux.Handle("/api/v1/query", q)          // supports GET + POST
	mux.Handle("/api/v1/comparePeriods", q) // supports GET + POST

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Failed to start HTTP server", slog.Any("error", err))
		}
	}()

	<-doneCtx.Done()

	slog.Info("Shutting down querier service")
	if err := srv.Shutdown(context.Background()); err != nil {
		slog.Error("Failed to shutdown HTTP server", slog.Any("error", err))
		return fmt.Errorf("failed to shutdown HTTP server: %w", err)
	}

	return nil
}
