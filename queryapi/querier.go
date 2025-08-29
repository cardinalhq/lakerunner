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
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/cardinalhq/oteltools/pkg/dateutils"
	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/promql"
	// import your logql package (adjust path if different)
	"github.com/cardinalhq/lakerunner/logql"
)

type queryPayload struct {
	OrgID   string `json:"orgId"`
	S       string `json:"s"`
	E       string `json:"e"`
	Q       string `json:"q"`
	Reverse *bool  `json:"reverse,omitempty"`
}

// ---------------- Shared helpers ----------------

func readQueryPayload(w http.ResponseWriter, r *http.Request) (orgID uuid.UUID, startTs, endTs int64, expr string, ok bool) {
	// Defaults from query string
	org := r.URL.Query().Get("orgId")
	s := r.URL.Query().Get("s")
	e := r.URL.Query().Get("e")
	q := r.URL.Query().Get("q")

	// Optional POST body
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
			if p.OrgID != "" {
				org = p.OrgID
			}
			if p.S != "" {
				s = p.S
			}
			if p.E != "" {
				e = p.E
			}
			if p.Q != "" {
				q = p.Q
			}
		case strings.HasPrefix(ct, "text/plain"):
			if len(body) > 0 {
				q = strings.TrimSpace(string(body))
			}
		default:
			if len(body) > 0 && q == "" {
				q = strings.TrimSpace(string(body))
			}
		}
	}

	if org == "" {
		http.Error(w, "missing orgId", http.StatusBadRequest)
		return
	}
	ou, err := uuid.Parse(org)
	if err != nil {
		http.Error(w, "invalid orgId: "+err.Error(), http.StatusBadRequest)
		return
	}

	if s == "" || e == "" {
		http.Error(w, "missing s/e", http.StatusBadRequest)
		return
	}
	st, en, err := dateutils.ToStartEnd(s, e)
	if err != nil {
		http.Error(w, "invalid s/e: "+err.Error(), http.StatusBadRequest)
		return
	}
	if st >= en {
		http.Error(w, "start must be < end", http.StatusBadRequest)
		return
	}
	if q == "" {
		http.Error(w, "missing query expression (q)", http.StatusBadRequest)
		return
	}
	return ou, st, en, q, true
}

func (q *QuerierService) sseWriter(w http.ResponseWriter) (func(event string, v any) error, bool) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return nil, false
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	write := func(event string, v any) error {
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
	return write, true
}

// ---------------- Handlers ----------------

// PromQL endpoint: /api/v1/query
func (q *QuerierService) handlePromQuery(w http.ResponseWriter, r *http.Request) {
	orgID, startTs, endTs, expr, ok := readQueryPayload(w, r)
	if !ok {
		return
	}
	// Parse & compile PromQL
	promExpr, err := promql.FromPromQL(expr)
	if err != nil {
		http.Error(w, "invalid query expression: "+err.Error(), http.StatusBadRequest)
		return
	}
	plan, err := promql.Compile(promExpr)
	if err != nil {
		http.Error(w, "compile error: "+err.Error(), http.StatusBadRequest)
		return
	}

	writeSSE, ok := q.sseWriter(w)
	if !ok {
		return
	}

	resultsCh, err := q.EvaluateMetricsQuery(r.Context(), orgID, startTs, endTs, plan)
	if err != nil {
		http.Error(w, "evaluate error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			slog.Info("client disconnected; stopping stream")
			return
		case res, more := <-resultsCh:
			if !more {
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

func (q *QuerierService) handleLogQuery(w http.ResponseWriter, r *http.Request) {
	orgID, startTs, endTs, expr, ok := readQueryPayload(w, r)
	if !ok {
		return
	}
	logAst, err := logql.FromLogQL(expr)
	if err != nil {
		http.Error(w, "invalid log query expression: "+err.Error(), http.StatusBadRequest)
		return
	}
	lplan, err := logql.CompileLog(logAst)
	if err != nil {
		http.Error(w, "compile error: "+err.Error(), http.StatusBadRequest)
		return
	}

	writeSSE, ok := q.sseWriter(w)
	if !ok {
		return
	}

	resultsCh, err := q.EvaluateLogsQuery(r.Context(), orgID, startTs, endTs, lplan)
	if err != nil {
		http.Error(w, "evaluate error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			slog.Info("client disconnected; stopping log stream")
			return
		case res, more := <-resultsCh:
			if !more {
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
	mux.HandleFunc("/api/v1/metrics/query", q.handlePromQuery)
	mux.HandleFunc("/api/v1/logs/query", q.handleLogQuery)

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
