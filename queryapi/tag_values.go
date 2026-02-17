// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"

	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/promql"
)

var validTagNameRe = regexp.MustCompile(`^[a-zA-Z0-9_.]+$`)

// normalizeTagName converts dots to underscores to match Parquet column naming.
func normalizeTagName(tagName string) string {
	return strings.ReplaceAll(tagName, ".", "_")
}

func (q *QuerierService) handleGetMetricTagValues(w http.ResponseWriter, r *http.Request) {
	qPayload := readQueryPayload(w, r, true)
	if qPayload == nil {
		return
	}

	tagName := r.URL.Query().Get("tagName")
	if tagName == "" {
		http.Error(w, "missing tagName parameter", http.StatusBadRequest)
		return
	}
	if !validTagNameRe.MatchString(tagName) {
		http.Error(w, "invalid tagName parameter", http.StatusBadRequest)
		return
	}

	columnName := normalizeTagName(tagName)

	if qPayload.Q == "" {
		// If no query expression, use a default query that does an exists check for the requested tag
		qPayload.Q = fmt.Sprintf("{%s=~\".+\"}", columnName)
	}

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
	plan.TagName = columnName

	writeSSE, ok := q.sseWriter(w)
	if !ok {
		return
	}

	resultsCh, err := q.EvaluateMetricTagValuesQuery(r.Context(), qPayload.OrgUUID, qPayload.StartTs, qPayload.EndTs, plan)
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

func (q *QuerierService) handleGetLogTagValues(w http.ResponseWriter, r *http.Request) {
	qp := readQueryPayload(w, r, true)
	if qp == nil {
		return
	}

	tagName := r.URL.Query().Get("tagName")
	if tagName == "" {
		http.Error(w, "missing tagName parameter", http.StatusBadRequest)
		return
	}
	if !validTagNameRe.MatchString(tagName) {
		http.Error(w, "invalid tagName parameter", http.StatusBadRequest)
		return
	}

	columnName := normalizeTagName(tagName)

	if qp.Q == "" {
		// If no query expression, use a default query that does an exists check for the requested tag
		qp.Q = fmt.Sprintf("{%s=~\".+\"}", columnName)
	}

	logAst, err := logql.FromLogQL(qp.Q)
	if err != nil {
		http.Error(w, "invalid log query expression: "+qp.Q+" "+err.Error(), http.StatusBadRequest)
		return
	}
	lplan, err := logql.CompileLog(logAst)
	if err != nil {
		http.Error(w, "compile error: "+err.Error(), http.StatusBadRequest)
		return
	}
	lplan.TagName = columnName

	writeSSE, ok := q.sseWriter(w)
	if !ok {
		return
	}

	resultsCh, err := q.EvaluateLogTagValuesQuery(r.Context(), qp.OrgUUID, qp.StartTs, qp.EndTs, lplan)
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

func (q *QuerierService) handleGetSpanTagValues(w http.ResponseWriter, r *http.Request) {
	qp := readQueryPayload(w, r, true)
	if qp == nil {
		return
	}

	tagName := r.URL.Query().Get("tagName")
	if tagName == "" {
		http.Error(w, "missing tagName parameter", http.StatusBadRequest)
		return
	}
	if !validTagNameRe.MatchString(tagName) {
		http.Error(w, "invalid tagName parameter", http.StatusBadRequest)
		return
	}

	columnName := normalizeTagName(tagName)

	if qp.Q == "" {
		// If no query expression, use a default query that does an exists check for the requested tag
		qp.Q = fmt.Sprintf("{%s=~\".+\"}", columnName)
	}

	logAst, err := logql.FromLogQL(qp.Q)
	if err != nil {
		http.Error(w, "invalid span query expression: "+qp.Q+" "+err.Error(), http.StatusBadRequest)
		return
	}
	lplan, err := logql.CompileLog(logAst)
	if err != nil {
		http.Error(w, "compile error: "+err.Error(), http.StatusBadRequest)
		return
	}
	lplan.TagName = columnName

	writeSSE, ok := q.sseWriter(w)
	if !ok {
		return
	}

	resultsCh, err := q.EvaluateSpanTagValuesQuery(r.Context(), qp.OrgUUID, qp.StartTs, qp.EndTs, lplan)
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
