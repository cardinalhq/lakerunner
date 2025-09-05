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
	Reverse bool   `json:"reverse,omitempty"`
	Limit   int    `json:"limit,omitempty"`

	// derived fields
	OrgUUID uuid.UUID `json:"-"`
	StartTs int64     `json:"-"`
	EndTs   int64     `json:"-"`
}

func readQueryPayload(w http.ResponseWriter, r *http.Request) *queryPayload {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST method is allowed", http.StatusMethodNotAllowed)
		return nil
	}
	ct := r.Header.Get("Content-Type")
	body, _ := io.ReadAll(r.Body)
	defer r.Body.Close()

	switch {
	case strings.HasPrefix(ct, "application/json"):
		var p queryPayload
		if err := json.Unmarshal(body, &p); err != nil {
			http.Error(w, "invalid JSON body: "+err.Error(), http.StatusBadRequest)
			return nil
		}
		if p.OrgID == "" {
			http.Error(w, "missing orgId", http.StatusBadRequest)
			return nil
		}
		if p.Q == "" {
			http.Error(w, "missing query expression", http.StatusBadRequest)
			return nil
		}
		orgId, err := uuid.Parse(p.OrgID)
		if err != nil {
			http.Error(w, "invalid orgId: "+err.Error(), http.StatusBadRequest)
			return nil
		}
		p.OrgUUID = orgId
		st, en, err := dateutils.ToStartEnd(p.S, p.E)
		if err != nil {
			http.Error(w, "invalid start/end time: "+err.Error(), http.StatusBadRequest)
			return nil
		}
		p.StartTs = st
		p.EndTs = en
		return &p
	default:
		http.Error(w, "unsupported content type", http.StatusBadRequest)
		return nil
	}
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

type evalData struct {
	Key       string         `json:"key,omitempty"`
	Tags      map[string]any `json:"tags"`
	Value     float64        `json:"value"`
	Timestamp int64          `json:"timestamp"`
	Label     string         `json:"label"`
}

func (q *QuerierService) handlePromQuery(w http.ResponseWriter, r *http.Request) {
	qPayload := readQueryPayload(w, r)
	if qPayload == nil {
		return
	}

	// Parse & compile PromQL
	promExpr, err := promql.FromPromQL(qPayload.Q)
	if err != nil {
		http.Error(w, "invalid query expression: "+err.Error(), http.StatusBadRequest)
		return
	}
	plan, err := promql.Compile(promExpr)
	if err != nil {
		http.Error(w, "compile error: "+err.Error(), http.StatusBadRequest)
		return
	}

	resultsCh, err := q.EvaluateMetricsQuery(r.Context(), qPayload.OrgUUID, qPayload.StartTs, qPayload.EndTs, plan)
	if err != nil {
		http.Error(w, "evaluate error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	q.sendEvalResults(r, w, resultsCh, plan)
}

func (q *QuerierService) sendEvalResults(r *http.Request, w http.ResponseWriter, resultsCh <-chan map[string]promql.EvalResult, plan promql.QueryPlan) {
	writeSSE, ok := q.sseWriter(w)
	if !ok {
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
			for k, v := range res {
				label := plan.Root.Label(v.Tags)
				ed := evalData{
					Key:       k,
					Tags:      v.Tags,
					Value:     v.Value.Num,
					Timestamp: v.Timestamp,
					Label:     label,
				}
				if err := writeSSE("result", ed); err != nil {
					slog.Error("write SSE failed", "error", err)
					return
				}
			}

		}
	}
}

func (q *QuerierService) handleLogQuery(w http.ResponseWriter, r *http.Request) {
	qp := readQueryPayload(w, r)
	if qp == nil {
		return
	}

	logAst, err := logql.FromLogQL(qp.Q)
	if err != nil {
		http.Error(w, "invalid log query expression: "+qp.Q+" "+err.Error(), http.StatusBadRequest)
		return
	}

	lplan, err := logql.CompileLog(logAst)
	if err != nil {
		http.Error(w, "cannot compile LogQL: "+err.Error(), http.StatusBadRequest)
		return
	}

	if logAst.NeedsRewrite() {
		rr, err := promql.RewriteToPromQL(lplan.Root)
		if err != nil {
			http.Error(w, "cannot rewrite to PromQL: "+err.Error(), http.StatusBadRequest)
			return
		}

		promExpr, err := promql.FromPromQL(rr.PromQL)
		if err != nil {
			http.Error(w, "cannot parse rewritten PromQL: "+err.Error(), http.StatusBadRequest)
			return
		}

		plan, err := promql.Compile(promExpr)
		if err != nil {
			http.Error(w, "cannot compile rewritten PromQL: "+err.Error(), http.StatusBadRequest)
			return
		}

		logLeafByBaseExprID := make(map[string]logql.LogLeaf, len(rr.Leaves))
		for i := range plan.Leaves {
			// Take address so we mutate the element in the slice.
			be := &plan.Leaves[i]

			found := false
			kept := make([]promql.LabelMatch, 0, len(be.Matchers))

			for _, m := range be.Matchers {
				if m.Label == promql.LeafMatcher {
					leaf, ok := rr.Leaves[m.Value]
					if !ok {
						http.Error(w, "unknown __leaf id: "+m.Value, http.StatusBadRequest)
						return
					}
					lcopy := leaf
					logLeafByBaseExprID[be.ID] = lcopy
					be.LogLeaf = &lcopy
					found = true
					slog.Info("Found leaf matcher", "leaf", leaf)
					// Skip adding __leaf back into matchers.
					continue
				}
				kept = append(kept, m)
			}

			if !found {
				http.Error(w, "internal: base expr missing __leaf matcher", http.StatusBadRequest)
				return
			}
			// Remove __leaf matcher so it doesn’t appear in downstream SQL.
			be.Matchers = kept
		}
		plan.AttachLogLeaves(logLeafByBaseExprID)

		evalResults, err := q.EvaluateMetricsQuery(r.Context(), qp.OrgUUID, qp.StartTs, qp.EndTs, plan)
		if err != nil {
			http.Error(w, "evaluate error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		q.sendEvalResults(r, w, evalResults, plan)
		return
	}

	// ---- Raw logs path (no rewrite) ----
	writeSSE, ok := q.sseWriter(w)
	if !ok {
		return
	}

	resultsCh, err := q.EvaluateLogsQuery(
		r.Context(), qp.OrgUUID, qp.StartTs, qp.EndTs, qp.Reverse, qp.Limit, lplan,
	)
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

	mux.HandleFunc("/api/v1/metrics/metadata", q.handleListPromQLMetricsMetadata)
	mux.HandleFunc("/api/v1/metrics/tags", q.handleListPromQLTags)
	mux.HandleFunc("/api/v1/metrics/tagvalues", q.handleGetMetricTagValues)
	mux.HandleFunc("/api/v1/metrics/query", q.handlePromQuery)

	mux.HandleFunc("/api/v1/logs/tags", q.handleListLogQLTags)
	mux.HandleFunc("/api/v1/logs/tagvalues", q.handleGetLogTagValues)
	mux.HandleFunc("/api/v1/logs/query", q.handleLogQuery)

	mux.HandleFunc("/api/v1/promql/validate", q.handlePromQLValidate)

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
