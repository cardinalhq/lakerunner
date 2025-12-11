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
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/cardinalhq/oteltools/pkg/dateutils"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/promql"
)

type queryPayload struct {
	S       string   `json:"s"`
	E       string   `json:"e"`
	Q       string   `json:"q"`
	Reverse bool     `json:"reverse,omitempty"`
	Limit   int      `json:"limit,omitempty"`
	Fields  []string `json:"fields,omitempty"`

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

type evalData struct {
	Key       string         `json:"key,omitempty"`
	Tags      map[string]any `json:"tags"`
	Value     float64        `json:"value"`
	Timestamp int64          `json:"timestamp"`
	Label     string         `json:"label"`
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

	resultsCh, err := q.EvaluateMetricsQuery(ctx, qPayload.OrgUUID, qPayload.StartTs, qPayload.EndTs, plan)
	if err != nil {
		requestSpan.RecordError(err)
		requestSpan.SetStatus(codes.Error, "evaluate error")
		http.Error(w, "evaluate error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	q.sendEvalResults(ctx, w, resultsCh, plan)
}

func (q *QuerierService) sendEvalResults(ctx context.Context, w http.ResponseWriter, resultsCh <-chan map[string]promql.EvalResult, plan promql.QueryPlan) {
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
				_ = writeSSE("done", map[string]string{"status": "ok"})
				return
			}
			for k, v := range res {
				label := plan.Root.Label(v.Tags)
				if math.IsNaN(v.Value.Num) {
					continue
				}
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

		evalResults, err := q.EvaluateMetricsQuery(ctx, qp.OrgUUID, qp.StartTs, qp.EndTs, plan)
		if err != nil {
			requestSpan.RecordError(err)
			requestSpan.SetStatus(codes.Error, "evaluate error")
			status, code := statusAndCodeForRuntimeError(err)
			writeAPIError(w, status, code, "evaluate error: "+err.Error())
			return
		}
		q.sendEvalResults(ctx, w, evalResults, plan)
		return
	}

	// ---- Raw logs path (no rewrite) ----
	writeSSE, ok := q.sseWriter(w)
	if !ok {
		requestSpan.SetStatus(codes.Error, "SSE unsupported")
		writeAPIError(w, http.StatusNotAcceptable, ErrSSEUnsupported, "server-sent events not supported by client")
		return
	}

	resultsCh, err := q.EvaluateLogsQuery(
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

		evalResults, err := q.EvaluateMetricsQuery(ctx, qp.OrgUUID, qp.StartTs, qp.EndTs, plan)
		if err != nil {
			requestSpan.RecordError(err)
			requestSpan.SetStatus(codes.Error, "evaluate error")
			http.Error(w, "evaluate error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		q.sendEvalResults(ctx, w, evalResults, plan)
		return
	}

	// ---- Raw spans path (no rewrite) ----
	writeSSE, ok := q.sseWriter(w)
	if !ok {
		requestSpan.SetStatus(codes.Error, "SSE unsupported")
		return
	}

	resultsCh, err := q.EvaluateSpansQuery(
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

func (q *QuerierService) handleListServiceMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "only GET method is allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse URL parameters
	queryParams := r.URL.Query()
	serviceName := queryParams.Get("serviceName")
	startTimeStr := queryParams.Get("startTime")
	endTimeStr := queryParams.Get("endTime")

	// Validate required parameters
	if serviceName == "" {
		http.Error(w, "missing required parameter: serviceName", http.StatusBadRequest)
		return
	}
	if startTimeStr == "" {
		http.Error(w, "missing required parameter: startTime", http.StatusBadRequest)
		return
	}
	if endTimeStr == "" {
		http.Error(w, "missing required parameter: endTime", http.StatusBadRequest)
		return
	}

	// Parse time parameters
	startTime, err := strconv.ParseInt(startTimeStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid startTime: must be a number", http.StatusBadRequest)
		return
	}
	endTime, err := strconv.ParseInt(endTimeStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid endTime: must be a number", http.StatusBadRequest)
		return
	}

	// Get organization ID from context
	orgUUID, ok := GetOrgIDFromContext(r.Context())
	if !ok {
		http.Error(w, "organization ID not found in context", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	metrics, err := q.mdb.ListServiceMetrics(ctx, lrdb.ListServiceMetricsParams{
		OrganizationID: orgUUID,
		ServiceName:    serviceName,
		StartTime:      float64(startTime),
		EndTime:        float64(endTime),
	})
	if err != nil {
		slog.Error("ListServiceMetrics failed",
			"org", orgUUID,
			"service", serviceName,
			"startTime", startTime,
			"endTime", endTime,
			"error", err)
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string][]string{"metrics": metrics})
}

type exemplarLogsRequest struct {
	Fingerprints []int64 `json:"fingerprints"`
}

func (q *QuerierService) handleGetExemplarLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	orgUUID, ok := GetOrgIDFromContext(r.Context())
	if !ok {
		http.Error(w, "organization ID not found in context", http.StatusInternalServerError)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	defer func() { _ = r.Body.Close() }()

	var req exemplarLogsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid JSON body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Fingerprints) == 0 {
		http.Error(w, "fingerprints array is required and cannot be empty", http.StatusBadRequest)
		return
	}

	exemplars, err := q.mdb.GetExemplarLogsByFingerprints(r.Context(), lrdb.GetExemplarLogsByFingerprintsParams{
		OrganizationID: orgUUID,
		Fingerprints:   req.Fingerprints,
	})
	if err != nil {
		slog.Error("GetExemplarLogsByFingerprints failed", slog.Any("error", err))
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"exemplars": exemplars})
}

func (q *QuerierService) Run(doneCtx context.Context) error {
	slog.Info("Starting querier service")

	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/services", q.apiKeyMiddleware(q.handleListServices))
	mux.HandleFunc("/api/v1/services/metrics", q.apiKeyMiddleware(q.handleListServiceMetrics))

	mux.HandleFunc("/api/v1/metrics/metadata", q.apiKeyMiddleware(q.handleListPromQLMetricsMetadata))
	mux.HandleFunc("/api/v1/metrics/tags", q.apiKeyMiddleware(q.handleListPromQLTags))
	mux.HandleFunc("/api/v1/metrics/tagvalues", q.apiKeyMiddleware(q.handleGetMetricTagValues))
	mux.HandleFunc("/api/v1/metrics/query", q.apiKeyMiddleware(q.handlePromQuery))

	mux.HandleFunc("/api/v1/logs/tags", q.apiKeyMiddleware(q.handleListLogQLTags))
	mux.HandleFunc("/api/v1/logs/tagvalues", q.apiKeyMiddleware(q.handleGetLogTagValues))
	mux.HandleFunc("/api/v1/logs/query", q.apiKeyMiddleware(q.handleLogQuery))
	mux.HandleFunc("/api/v1/logs/series", q.apiKeyMiddleware(q.handleListLogSeries))
	mux.HandleFunc("/api/v1/logs/exemplars", q.apiKeyMiddleware(q.handleGetExemplarLogs))

	mux.HandleFunc("/api/v1/spans/tags", q.apiKeyMiddleware(q.handleListSpanTags))
	mux.HandleFunc("/api/v1/spans/tagvalues", q.apiKeyMiddleware(q.handleGetSpanTagValues))
	mux.HandleFunc("/api/v1/spans/query", q.apiKeyMiddleware(q.handleSpansQuery))

	mux.HandleFunc("/api/v1/promql/validate", q.apiKeyMiddleware(q.handlePromQLValidate))
	mux.HandleFunc("/api/v1/logql/validate", q.apiKeyMiddleware(q.handleLogQLValidate))

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Legacy Scala API compatibility endpoints
	mux.HandleFunc("/api/v1/graph", q.apiKeyMiddleware(q.handleGraphQuery))
	mux.HandleFunc("/api/v1/tags/logs", q.apiKeyMiddleware(q.handleTagsQuery))

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
