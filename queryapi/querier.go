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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/cardinalhq/oteltools/pkg/dateutils"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/promql"
)

type queryPayload struct {
	S       string   `json:"s"`
	E       string   `json:"e"`
	Q       string   `json:"q"`
	Reverse bool     `json:"reverse,omitempty"`
	Limit   int      `json:"limit,omitempty"`
	Fields  []string `json:"fields,omitempty"`
	Summary bool     `json:"summary,omitempty"` // Return per-series aggregate stats instead of time series

	// derived fields
	OrgUUID uuid.UUID `json:"-"`
	StartTs int64     `json:"-"`
	EndTs   int64     `json:"-"`
}

func readQueryPayload(w http.ResponseWriter, r *http.Request, allowEmptyQuery bool) *queryPayload {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST method is allowed", http.StatusMethodNotAllowed)
		return nil
	}
	ct := r.Header.Get("Content-Type")
	body, _ := io.ReadAll(r.Body)
	defer func() { _ = r.Body.Close() }()

	switch {
	case strings.HasPrefix(ct, "application/json"):
		var p queryPayload
		if err := json.Unmarshal(body, &p); err != nil {
			http.Error(w, "invalid JSON body: "+err.Error(), http.StatusBadRequest)
			return nil
		}

		// Get orgId from context (set by middleware)
		orgId, ok := GetOrgIDFromContext(r.Context())
		if !ok {
			http.Error(w, "organization ID not found in context", http.StatusInternalServerError)
			return nil
		}
		p.OrgUUID = orgId

		if p.Q == "" && !allowEmptyQuery {
			http.Error(w, "missing query expression", http.StatusBadRequest)
			return nil
		}
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

// finishSSEWithStatus drains the query error channel and sends the final
// "done" SSE event with status "ok" or "error".
func finishSSEWithStatus(writeSSE func(string, any) error, queryErrc <-chan error) {
	status := "ok"
	if qErr := drainErrors(queryErrc); qErr != nil {
		slog.Error("query completed with segment errors", "error", qErr)
		status = "error"
	}
	_ = writeSSE("done", map[string]string{"status": status})
}

// TagMap is a map[string]any with a custom JSON marshaler that emits
// int64 and uint64 values as JSON integers with sorted keys for
// deterministic output.
type TagMap map[string]any

func (t TagMap) MarshalJSON() ([]byte, error) {
	keys := make([]string, 0, len(t))
	for k := range t {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		key, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		buf.Write(key)
		buf.WriteByte(':')
		switch val := t[k].(type) {
		case int64:
			fmt.Fprintf(&buf, "%d", val)
		case uint64:
			fmt.Fprintf(&buf, "%d", val)
		default:
			b, err := json.Marshal(val)
			if err != nil {
				return nil, err
			}
			buf.Write(b)
		}
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func (t *TagMap) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var raw map[string]any
	if err := dec.Decode(&raw); err != nil {
		return err
	}
	result := make(TagMap, len(raw))
	for k, v := range raw {
		if n, ok := v.(json.Number); ok {
			if i, err := n.Int64(); err == nil {
				result[k] = i
				continue
			}
			if f, err := n.Float64(); err == nil {
				result[k] = f
				continue
			}
		}
		result[k] = v
	}
	*t = result
	return nil
}

type evalData struct {
	Tags      TagMap  `json:"tags"`
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
	Label     string  `json:"label"`
}

func (q *QuerierService) handlePromQuery(w http.ResponseWriter, r *http.Request) {
	tracer := otel.Tracer("github.com/cardinalhq/lakerunner/queryapi")
	ctx, requestSpan := tracer.Start(r.Context(), "query.api.metrics_query")
	defer requestSpan.End()

	qPayload := readQueryPayload(w, r, false)
	if qPayload == nil {
		requestSpan.SetStatus(codes.Error, "invalid payload")
		return
	}

	requestSpan.SetAttributes(
		attribute.String("organization_id", qPayload.OrgUUID.String()),
		attribute.Int64("start_ts", qPayload.StartTs),
		attribute.Int64("end_ts", qPayload.EndTs),
		attribute.String("query", qPayload.Q),
	)

	// Parse & compile PromQL
	ctx, parseSpan := tracer.Start(ctx, "query.api.parse_promql")
	promExpr, err := promql.FromPromQL(qPayload.Q)
	if err != nil {
		parseSpan.RecordError(err)
		parseSpan.SetStatus(codes.Error, "invalid expression")
		parseSpan.End()
		requestSpan.RecordError(err)
		requestSpan.SetStatus(codes.Error, "invalid expression")
		http.Error(w, "invalid query expression: "+err.Error(), http.StatusBadRequest)
		return
	}
	plan, err := promql.Compile(promExpr)
	if err != nil {
		parseSpan.RecordError(err)
		parseSpan.SetStatus(codes.Error, "compile error")
		parseSpan.End()
		requestSpan.RecordError(err)
		requestSpan.SetStatus(codes.Error, "compile error")
		http.Error(w, "compile error: "+err.Error(), http.StatusBadRequest)
		return
	}
	parseSpan.SetAttributes(attribute.Int("leaf_count", len(plan.Leaves)))
	parseSpan.End()

	// Summary mode: return per-series aggregate stats instead of time series
	if qPayload.Summary {
		q.handlePromQuerySummary(ctx, w, qPayload, plan)
		return
	}

	resultsCh, queryErrc, err := q.EvaluateMetricsQuery(ctx, qPayload.OrgUUID, qPayload.StartTs, qPayload.EndTs, plan)
	if err != nil {
		requestSpan.RecordError(err)
		requestSpan.SetStatus(codes.Error, "evaluate error")
		http.Error(w, "evaluate error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	q.sendEvalResults(ctx, w, resultsCh, queryErrc, plan)
}

func (q *QuerierService) sendEvalResults(ctx context.Context, w http.ResponseWriter, resultsCh <-chan map[string]promql.EvalResult, queryErrc <-chan error, plan promql.QueryPlan) {
	writeSSE, ok := q.sseWriter(w)
	if !ok {
		return
	}

	notify := ctx.Done()
	for {
		select {
		case <-notify:
			slog.Info("client disconnected; stopping stream")
			return
		case res, more := <-resultsCh:
			if !more {
				finishSSEWithStatus(writeSSE, queryErrc)
				return
			}
			for _, v := range res {
				label := v.Label
				if label == "" {
					label = plan.Root.Label(v.Tags)
				}
				if math.IsNaN(v.Value.Num) {
					continue
				}
				ed := evalData{
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

// SeriesSummary contains aggregate statistics for a single time series.
type SeriesSummary struct {
	Label string   `json:"label"`
	Tags  TagMap   `json:"tags"`
	Min   float64  `json:"min"`
	Max   float64  `json:"max"`
	Avg   float64  `json:"avg"`
	Sum   float64  `json:"sum"`
	Count int64    `json:"count"`
	P50   *float64 `json:"p50,omitempty"`
	P90   *float64 `json:"p90,omitempty"`
	P95   *float64 `json:"p95,omitempty"`
	P99   *float64 `json:"p99,omitempty"`
}

func (q *QuerierService) handlePromQuerySummary(ctx context.Context, w http.ResponseWriter, qp *queryPayload, plan promql.QueryPlan) {
	tracer := otel.Tracer("github.com/cardinalhq/lakerunner/queryapi")
	ctx, summarySpan := tracer.Start(ctx, "query.api.metrics_summary")
	defer summarySpan.End()

	writeSSE, ok := q.sseWriter(w)
	if !ok {
		return
	}

	summaries, err := q.EvaluateMetricsSummary(ctx, qp.OrgUUID, qp.StartTs, qp.EndTs, plan)
	if err != nil {
		summarySpan.RecordError(err)
		summarySpan.SetStatus(codes.Error, "summary evaluation error")
		_ = writeSSE("error", map[string]string{
			"message": err.Error(),
			"code":    string(ErrInternalError),
		})
		return
	}

	for _, s := range summaries {
		if err := writeSSE("summary", s); err != nil {
			slog.Error("write SSE summary failed", "error", err)
			return
		}
	}
	_ = writeSSE("done", map[string]string{"status": "ok"})
}

type APIErrorCode string

const (
	InvalidJSON           APIErrorCode = "INVALID_JSON"
	ErrInvalidExpr        APIErrorCode = "INVALID_EXPR"
	ValidationFailed      APIErrorCode = "VALIDATION_FAILED"
	ErrCompileError       APIErrorCode = "COMPILE_ERROR"
	ErrRewriteUnsupported APIErrorCode = "REWRITE_UNSUPPORTED"
	ErrInternalError      APIErrorCode = "INTERNAL_ERROR"
	ErrClientClosed       APIErrorCode = "CLIENT_CLOSED"
	ErrDeadlineExceeded   APIErrorCode = "DEADLINE_EXCEEDED"
	ErrServiceUnavailable APIErrorCode = "SERVICE_UNAVAILABLE"
	ErrForbidden          APIErrorCode = "FORBIDDEN"
	ErrUnauthorized       APIErrorCode = "UNAUTHORIZED"
	ErrNotFound           APIErrorCode = "NOT_FOUND"
	ErrRateLimited        APIErrorCode = "RATE_LIMITED"
	ErrSSEUnsupported     APIErrorCode = "SSE_UNSUPPORTED"
)

type APIError struct {
	Status  int          `json:"status"`
	Code    APIErrorCode `json:"code"`
	Message string       `json:"message"`
}

func writeAPIError(w http.ResponseWriter, status int, code APIErrorCode, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(APIError{
		Status:  status,
		Code:    code,
		Message: msg,
	})
}

// Non-standard but used by many proxies for client disconnects.
const statusClientClosedRequest = 499

func statusAndCodeForRuntimeError(err error) (int, APIErrorCode) {
	if err == nil {
		return http.StatusOK, ""
	}
	if errors.Is(err, context.Canceled) {
		return statusClientClosedRequest, ErrClientClosed
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout, ErrDeadlineExceeded
	}

	var ne net.Error
	if errors.As(err, &ne) {
		if ne.Timeout() {
			return http.StatusGatewayTimeout, ErrDeadlineExceeded
		}
		// Treat temporary/transient as 503
		type temporary interface{ Temporary() bool }
		if t, ok := any(ne).(temporary); ok && t.Temporary() {
			return http.StatusServiceUnavailable, ErrServiceUnavailable
		}
	}

	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "permission") || strings.Contains(msg, "forbidden"):
		return http.StatusForbidden, ErrForbidden
	case strings.Contains(msg, "unauthorized") || strings.Contains(msg, "unauthenticated"):
		return http.StatusUnauthorized, ErrUnauthorized
	case strings.Contains(msg, "not found"):
		return http.StatusNotFound, ErrNotFound
	case strings.Contains(msg, "too many requests") || strings.Contains(msg, "rate limit"):
		return http.StatusTooManyRequests, ErrRateLimited
	}

	return http.StatusInternalServerError, ErrInternalError
}

func (q *QuerierService) handleLogQuery(w http.ResponseWriter, r *http.Request) {
	tracer := otel.Tracer("github.com/cardinalhq/lakerunner/queryapi")
	ctx, requestSpan := tracer.Start(r.Context(), "query.api.logs_query")
	defer requestSpan.End()

	qp := readQueryPayload(w, r, false)
	if qp == nil {
		requestSpan.SetStatus(codes.Error, "invalid payload")
		return
	}

	requestSpan.SetAttributes(
		attribute.String("organization_id", qp.OrgUUID.String()),
		attribute.Int64("start_ts", qp.StartTs),
		attribute.Int64("end_ts", qp.EndTs),
		attribute.String("query", qp.Q),
		attribute.Bool("reverse", qp.Reverse),
		attribute.Int("limit", qp.Limit),
	)

	ctx, parseSpan := tracer.Start(ctx, "query.api.parse_logql")
	logAst, err := logql.FromLogQL(qp.Q)
	if err != nil {
		parseSpan.RecordError(err)
		parseSpan.SetStatus(codes.Error, "invalid expression")
		parseSpan.End()
		requestSpan.RecordError(err)
		requestSpan.SetStatus(codes.Error, "invalid expression")
		writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "invalid log query expression: "+err.Error())
		return
	}

	lplan, err := logql.CompileLog(logAst)
	if err != nil {
		parseSpan.RecordError(err)
		parseSpan.SetStatus(codes.Error, "compile error")
		parseSpan.End()
		requestSpan.RecordError(err)
		requestSpan.SetStatus(codes.Error, "compile error")
		writeAPIError(w, http.StatusUnprocessableEntity, ErrCompileError, "cannot compile LogQL: "+err.Error())
		return
	}
	isAggregate := logAst.IsAggregateExpr()
	parseSpan.SetAttributes(
		attribute.Bool("is_aggregate", isAggregate),
		attribute.Int("leaf_count", len(lplan.Leaves)),
	)
	parseSpan.End()

	requestSpan.SetAttributes(attribute.Bool("is_aggregate", isAggregate))

	if isAggregate {
		rr, err := promql.RewriteToPromQL(lplan.Root)
		if err != nil {
			requestSpan.RecordError(err)
			requestSpan.SetStatus(codes.Error, "rewrite unsupported")
			writeAPIError(w, http.StatusNotImplemented, ErrRewriteUnsupported, "cannot rewrite to PromQL: "+err.Error())
			return
		}

		promExpr, err := promql.FromPromQL(rr.PromQL)
		if err != nil {
			requestSpan.RecordError(err)
			requestSpan.SetStatus(codes.Error, "parse rewritten failed")
			writeAPIError(w, http.StatusInternalServerError, ErrInternalError, "cannot parse rewritten PromQL: "+err.Error())
			return
		}

		plan, err := promql.Compile(promExpr)
		if err != nil {
			requestSpan.RecordError(err)
			requestSpan.SetStatus(codes.Error, "compile rewritten failed")
			writeAPIError(w, http.StatusInternalServerError, ErrInternalError, "cannot compile rewritten PromQL: "+err.Error())
			return
		}
		plan.AttachLogLeaves(rr)

		evalResults, queryErrc, err := q.EvaluateMetricsQuery(ctx, qp.OrgUUID, qp.StartTs, qp.EndTs, plan)
		if err != nil {
			requestSpan.RecordError(err)
			requestSpan.SetStatus(codes.Error, "evaluate error")
			status, code := statusAndCodeForRuntimeError(err)
			writeAPIError(w, status, code, "evaluate error: "+err.Error())
			return
		}
		q.sendEvalResults(ctx, w, evalResults, queryErrc, plan)
		return
	}

	// ---- Raw logs path (no rewrite) ----
	writeSSE, ok := q.sseWriter(w)
	if !ok {
		requestSpan.SetStatus(codes.Error, "SSE unsupported")
		writeAPIError(w, http.StatusNotAcceptable, ErrSSEUnsupported, "server-sent events not supported by client")
		return
	}

	resultsCh, queryErrc, err := q.EvaluateLogsQuery(
		ctx, qp.OrgUUID, qp.StartTs, qp.EndTs, qp.Reverse, qp.Limit, lplan, qp.Fields,
	)
	if err != nil {
		requestSpan.RecordError(err)
		requestSpan.SetStatus(codes.Error, "evaluate error")
		status, code := statusAndCodeForRuntimeError(err)
		writeAPIError(w, status, code, "evaluate error: "+err.Error())
		return
	}

	notify := ctx.Done()
	for {
		select {
		case <-notify:
			slog.Info("client disconnected; stopping log stream")
			return
		case res, more := <-resultsCh:
			if !more {
				finishSSEWithStatus(writeSSE, queryErrc)
				return
			}
			if err := writeSSE("result", res); err != nil {
				slog.Error("write SSE failed", "error", err)
				return
			}
		}
	}
}

func (q *QuerierService) handleSpansQuery(w http.ResponseWriter, r *http.Request) {
	tracer := otel.Tracer("github.com/cardinalhq/lakerunner/queryapi")
	ctx, requestSpan := tracer.Start(r.Context(), "query.api.spans_query")
	defer requestSpan.End()

	qp := readQueryPayload(w, r, false)
	if qp == nil {
		requestSpan.SetStatus(codes.Error, "invalid payload")
		return
	}

	requestSpan.SetAttributes(
		attribute.String("organization_id", qp.OrgUUID.String()),
		attribute.Int64("start_ts", qp.StartTs),
		attribute.Int64("end_ts", qp.EndTs),
		attribute.String("query", qp.Q),
		attribute.Bool("reverse", qp.Reverse),
		attribute.Int("limit", qp.Limit),
	)

	ctx, parseSpan := tracer.Start(ctx, "query.api.parse_logql")
	logAst, err := logql.FromLogQL(qp.Q)
	if err != nil {
		parseSpan.RecordError(err)
		parseSpan.SetStatus(codes.Error, "invalid expression")
		parseSpan.End()
		requestSpan.RecordError(err)
		requestSpan.SetStatus(codes.Error, "invalid expression")
		http.Error(w, "invalid spans query expression: "+qp.Q+" "+err.Error(), http.StatusBadRequest)
		return
	}

	lplan, err := logql.CompileLog(logAst)
	if err != nil {
		parseSpan.RecordError(err)
		parseSpan.SetStatus(codes.Error, "compile error")
		parseSpan.End()
		requestSpan.RecordError(err)
		requestSpan.SetStatus(codes.Error, "compile error")
		http.Error(w, "cannot compile LogQL: "+err.Error(), http.StatusBadRequest)
		return
	}
	isAggregate := logAst.IsAggregateExpr()
	parseSpan.SetAttributes(
		attribute.Bool("is_aggregate", isAggregate),
		attribute.Int("leaf_count", len(lplan.Leaves)),
	)
	parseSpan.End()

	requestSpan.SetAttributes(attribute.Bool("is_aggregate", isAggregate))

	if isAggregate {
		rr, err := promql.RewriteToPromQL(lplan.Root)
		if err != nil {
			requestSpan.RecordError(err)
			requestSpan.SetStatus(codes.Error, "rewrite error")
			http.Error(w, "cannot rewrite to PromQL: "+err.Error(), http.StatusBadRequest)
			return
		}

		promExpr, err := promql.FromPromQL(rr.PromQL)
		if err != nil {
			requestSpan.RecordError(err)
			requestSpan.SetStatus(codes.Error, "parse rewritten failed")
			http.Error(w, "cannot parse rewritten PromQL: "+err.Error(), http.StatusBadRequest)
			return
		}

		plan, err := promql.Compile(promExpr)
		if err != nil {
			requestSpan.RecordError(err)
			requestSpan.SetStatus(codes.Error, "compile rewritten failed")
			http.Error(w, "cannot compile rewritten PromQL: "+err.Error(), http.StatusBadRequest)
			return
		}
		plan.AttachLogLeaves(rr)

		evalResults, queryErrc, err := q.EvaluateMetricsQuery(ctx, qp.OrgUUID, qp.StartTs, qp.EndTs, plan)
		if err != nil {
			requestSpan.RecordError(err)
			requestSpan.SetStatus(codes.Error, "evaluate error")
			http.Error(w, "evaluate error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		q.sendEvalResults(ctx, w, evalResults, queryErrc, plan)
		return
	}

	// ---- Raw spans path (no rewrite) ----
	writeSSE, ok := q.sseWriter(w)
	if !ok {
		requestSpan.SetStatus(codes.Error, "SSE unsupported")
		return
	}

	resultsCh, queryErrc, err := q.EvaluateSpansQuery(
		ctx, qp.OrgUUID, qp.StartTs, qp.EndTs, qp.Reverse, qp.Limit, lplan, qp.Fields,
	)
	if err != nil {
		requestSpan.RecordError(err)
		requestSpan.SetStatus(codes.Error, "evaluate error")
		http.Error(w, "evaluate error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	notify := ctx.Done()
	for {
		select {
		case <-notify:
			slog.Info("client disconnected; stopping spans stream")
			return
		case res, more := <-resultsCh:
			if !more {
				finishSSEWithStatus(writeSSE, queryErrc)
				return
			}
			if err := writeSSE("result", res); err != nil {
				slog.Error("write SSE failed", "error", err)
				return
			}
		}
	}
}

func (q *QuerierService) handlePing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (q *QuerierService) handleListServices(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}

	orgUUID, ok := GetOrgIDFromContext(r.Context())
	if !ok {
		http.Error(w, "organization ID not found in context", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	services, err := q.mdb.ListServiceNames(ctx, orgUUID)
	if err != nil {
		slog.Error("ListServiceNames failed", slog.Any("error", err))
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string][]string{"services": services})
}

type queryAPIFeatures struct {
	MetricsSummarySSE bool `json:"metricsSummarySSE"`
}

type queryAPIFeaturesResp struct {
	Features queryAPIFeatures `json:"features"`
}

func (q *QuerierService) handleFeatures(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "only GET or POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(queryAPIFeaturesResp{
		Features: queryAPIFeatures{
			MetricsSummarySSE: true,
		},
	})
}

func (q *QuerierService) Run(doneCtx context.Context) error {
	slog.Info("Starting querier service")

	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/ping", q.apiKeyMiddleware(q.handlePing))
	mux.HandleFunc("/api/v1/services", q.apiKeyMiddleware(q.handleListServices))
	mux.HandleFunc("/api/v1/features", q.apiKeyMiddleware(q.handleFeatures))

	mux.HandleFunc("/api/v1/metrics/metadata", q.apiKeyMiddleware(q.handleListPromQLMetricsMetadata))
	mux.HandleFunc("/api/v1/metrics/tags", q.apiKeyMiddleware(q.handleListPromQLTags))
	mux.HandleFunc("/api/v1/metrics/tagvalues", q.apiKeyMiddleware(q.handleGetMetricTagValues))
	mux.HandleFunc("/api/v1/metrics/query", q.apiKeyMiddleware(q.handlePromQuery))

	mux.HandleFunc("/api/v1/logs/tags", q.apiKeyMiddleware(q.handleListLogQLTags))
	mux.HandleFunc("/api/v1/logs/tagvalues", q.apiKeyMiddleware(q.handleGetLogTagValues))
	mux.HandleFunc("/api/v1/logs/query", q.apiKeyMiddleware(q.handleLogQuery))
	mux.HandleFunc("/api/v1/logs/series", q.apiKeyMiddleware(q.handleListLogSeries))

	mux.HandleFunc("/api/v1/spans/tags", q.apiKeyMiddleware(q.handleListSpanTags))
	mux.HandleFunc("/api/v1/spans/tagvalues", q.apiKeyMiddleware(q.handleGetSpanTagValues))
	mux.HandleFunc("/api/v1/spans/query", q.apiKeyMiddleware(q.handleSpansQuery))

	mux.HandleFunc("/api/v1/promql/validate", q.apiKeyMiddleware(q.handlePromQLValidate))
	mux.HandleFunc("/api/v1/logql/validate", q.apiKeyMiddleware(q.handleLogQLValidate))

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

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
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Warn("Graceful shutdown timed out, forcing close", slog.Any("error", err))
		_ = srv.Close()
	}
	return nil
}
