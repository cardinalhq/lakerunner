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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/promql"
)

// --- finishSSEWithStatus unit tests ---

// flushRecorder wraps httptest.ResponseRecorder with Flush support for SSE.
type flushRecorder struct {
	*httptest.ResponseRecorder
}

func (f *flushRecorder) Flush() {}

func newSSEWriter(t *testing.T) (func(string, any) error, *httptest.ResponseRecorder) {
	t.Helper()
	rec := httptest.NewRecorder()
	q := &QuerierService{}
	fr := &flushRecorder{rec}
	writeSSE, ok := q.sseWriter(fr)
	require.True(t, ok)
	return writeSSE, rec
}

// parseSSEEvents extracts all SSE data lines from a response body.
func parseSSEEvents(body string) []map[string]any {
	var events []map[string]any
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		var m map[string]any
		if err := json.Unmarshal([]byte(data), &m); err != nil {
			continue
		}
		events = append(events, m)
	}
	return events
}

func lastSSEEventStatus(events []map[string]any) string {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i]["type"] == "done" {
			if d, ok := events[i]["data"].(map[string]any); ok {
				if s, ok := d["status"].(string); ok {
					return s
				}
			}
		}
	}
	return ""
}

func TestFinishSSEWithStatus_Ok(t *testing.T) {
	writeSSE, rec := newSSEWriter(t)
	errc := make(chan error)
	close(errc)

	finishSSEWithStatus(writeSSE, errc)

	events := parseSSEEvents(rec.Body.String())
	assert.Equal(t, "ok", lastSSEEventStatus(events))
}

func TestFinishSSEWithStatus_Error(t *testing.T) {
	writeSSE, rec := newSSEWriter(t)
	errc := make(chan error, 1)
	errc <- fmt.Errorf("segment 42 dispatch: connection refused")
	close(errc)

	finishSSEWithStatus(writeSSE, errc)

	events := parseSSEEvents(rec.Body.String())
	assert.Equal(t, "error", lastSSEEventStatus(events))
}

func TestFinishSSEWithStatus_NilChannel(t *testing.T) {
	writeSSE, rec := newSSEWriter(t)
	finishSSEWithStatus(writeSSE, nil)

	events := parseSSEEvents(rec.Body.String())
	assert.Equal(t, "ok", lastSSEEventStatus(events))
}

// --- End-to-end: segment lookup failure → evaluator error → SSE done:error ---

// failingStore returns an error from segment listing methods.
type failingStore struct {
	lrdb.StoreFull
	err error
}

func (s failingStore) ListLogSegmentsForQuery(_ context.Context, _ lrdb.ListLogSegmentsForQueryParams) ([]lrdb.ListLogSegmentsForQueryRow, error) {
	return nil, s.err
}

func (s failingStore) ListMetricSegmentsForQuery(_ context.Context, _ lrdb.ListMetricSegmentsForQueryParams) ([]lrdb.ListMetricSegmentsForQueryRow, error) {
	return nil, s.err
}

func (s failingStore) ListTraceSegmentsForQuery(_ context.Context, _ lrdb.ListTraceSegmentsForQueryParams) ([]lrdb.ListTraceSegmentsForQueryRow, error) {
	return nil, s.err
}

func (s failingStore) GetLogStreamIdValues(_ context.Context, _ lrdb.GetLogStreamIdValuesParams) ([]string, error) {
	return nil, nil
}

func newFailingQuerier(err error) *QuerierService {
	return &QuerierService{
		mdb:             failingStore{err: err},
		workerDiscovery: singleWorkerDiscovery{},
	}
}

func TestLogsQuery_SegmentLookupError_PropagatesToSSE(t *testing.T) {
	q := newFailingQuerier(fmt.Errorf("connection refused"))
	plan := logql.LQueryPlan{
		Leaves: []logql.LogLeaf{{ID: "l0", Matchers: []logql.LabelMatch{{Label: "svc", Op: logql.MatchEq, Value: "x"}}}},
	}
	dataCh, queryErrc, err := q.EvaluateLogsQuery(t.Context(), uuid.New(), contractStart, contractEnd, false, 0, plan, nil)
	require.NoError(t, err)

	// Drain data.
	for range dataCh {
	}

	// Write SSE done event using the helper.
	writeSSE, rec := newSSEWriter(t)
	finishSSEWithStatus(writeSSE, queryErrc)

	events := parseSSEEvents(rec.Body.String())
	assert.Equal(t, "error", lastSSEEventStatus(events))
}

func TestSpansQuery_SegmentLookupError_PropagatesToSSE(t *testing.T) {
	q := newFailingQuerier(fmt.Errorf("connection refused"))
	plan := logql.LQueryPlan{
		Leaves: []logql.LogLeaf{{ID: "s0", Matchers: []logql.LabelMatch{{Label: "svc", Op: logql.MatchEq, Value: "x"}}}},
	}
	dataCh, queryErrc, err := q.EvaluateSpansQuery(t.Context(), uuid.New(), contractStart, contractEnd, false, 0, plan, nil)
	require.NoError(t, err)

	for range dataCh {
	}

	writeSSE, rec := newSSEWriter(t)
	finishSSEWithStatus(writeSSE, queryErrc)

	events := parseSSEEvents(rec.Body.String())
	assert.Equal(t, "error", lastSSEEventStatus(events))
}

func TestMetricsQuery_SegmentLookupError_PropagatesToSSE(t *testing.T) {
	q := newFailingQuerier(fmt.Errorf("connection refused"))
	plan := promql.QueryPlan{
		Root:   &promql.LeafNode{BE: promql.BaseExpr{ID: "m0", Metric: "cpu"}},
		Leaves: []promql.BaseExpr{{ID: "m0", Metric: "cpu"}},
	}
	dataCh, queryErrc, err := q.EvaluateMetricsQuery(t.Context(), uuid.New(), contractStart, contractEnd, plan)
	require.NoError(t, err)

	for range dataCh {
	}

	writeSSE, rec := newSSEWriter(t)
	finishSSEWithStatus(writeSSE, queryErrc)

	events := parseSSEEvents(rec.Body.String())
	assert.Equal(t, "error", lastSSEEventStatus(events))
}

func TestLogTagValues_SegmentLookupError_PropagatesToSSE(t *testing.T) {
	q := newFailingQuerier(fmt.Errorf("connection refused"))
	plan := logql.LQueryPlan{
		TagName: "host",
		Leaves:  []logql.LogLeaf{{ID: "ltv0", Matchers: []logql.LabelMatch{{Label: "svc", Op: logql.MatchEq, Value: "x"}}}},
	}
	dataCh, queryErrc, err := q.EvaluateLogTagValuesQuery(t.Context(), uuid.New(), contractStart, contractEnd, plan)
	require.NoError(t, err)

	for range dataCh {
	}

	writeSSE, rec := newSSEWriter(t)
	finishSSEWithStatus(writeSSE, queryErrc)

	events := parseSSEEvents(rec.Body.String())
	assert.Equal(t, "error", lastSSEEventStatus(events))
}

// TestHandlerSSE_RawLogsErrorPath exercises the full handleLogQuery raw logs
// path with a failing store, verifying the SSE stream ends with done:error.
func TestHandlerSSE_RawLogsErrorPath(t *testing.T) {
	q := newFailingQuerier(fmt.Errorf("db connection refused"))
	orgID := uuid.New()

	body := `{"s":"1700000000000","e":"1700003600000","q":"{service=\"api\"}"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/logs/query", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(WithOrgID(req.Context(), orgID))

	rec := httptest.NewRecorder()
	q.handleLogQuery(rec, req)

	events := parseSSEEvents(rec.Body.String())
	status := lastSSEEventStatus(events)
	assert.Equal(t, "error", status, "handler should send done:error when segment lookup fails")
}
