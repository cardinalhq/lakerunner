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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/promql"
)

// emptyStore embeds lrdb.StoreFull so it satisfies the interface. Only
// the segment-listing methods called in the zero-segments path are
// overridden; everything else panics if called (indicating a test gap).
type emptyStore struct {
	lrdb.StoreFull
}

func (emptyStore) ListLogSegmentsForQuery(context.Context, lrdb.ListLogSegmentsForQueryParams) ([]lrdb.ListLogSegmentsForQueryRow, error) {
	return nil, nil
}

func (emptyStore) ListMetricSegmentsForQuery(context.Context, lrdb.ListMetricSegmentsForQueryParams) ([]lrdb.ListMetricSegmentsForQueryRow, error) {
	return nil, nil
}

func (emptyStore) ListTraceSegmentsForQuery(context.Context, lrdb.ListTraceSegmentsForQueryParams) ([]lrdb.ListTraceSegmentsForQueryRow, error) {
	return nil, nil
}

func (emptyStore) GetLogStreamIdValues(context.Context, lrdb.GetLogStreamIdValuesParams) ([]string, error) {
	return nil, nil
}

// singleWorkerDiscovery satisfies WorkerDiscovery with one dummy worker.
type singleWorkerDiscovery struct{}

func (singleWorkerDiscovery) Start(context.Context) error { return nil }
func (singleWorkerDiscovery) Stop() error                 { return nil }
func (singleWorkerDiscovery) GetAllWorkers() ([]Worker, error) {
	return []Worker{{IP: "127.0.0.1", Port: 9999}}, nil
}
func (singleWorkerDiscovery) GetWorkersForSegments(uuid.UUID, []int64) ([]SegmentWorkerMapping, error) {
	return nil, nil
}

// newEmptyQuerier returns a QuerierService wired to stubs that return no segments.
func newEmptyQuerier() *QuerierService {
	return &QuerierService{
		mdb:             emptyStore{},
		workerDiscovery: singleWorkerDiscovery{},
	}
}

// ts helpers – one-hour window that won't hit any real segments.
const (
	contractStart = int64(1700000000000)
	contractEnd   = int64(1700003600000)
)

// drainAndAssertClosed drains the data channel and then asserts the error
// channel is closed (receives ok == false).
func drainAndAssertClosed[T any](t *testing.T, dataCh <-chan T, errCh <-chan error) {
	t.Helper()
	for range dataCh {
	}
	if errCh == nil {
		return
	}
	_, ok := <-errCh
	assert.False(t, ok, "error channel should be closed after data channel is drained")
}

func TestEvaluateLogsQuery_ClosesChannels(t *testing.T) {
	q := newEmptyQuerier()
	plan := logql.LQueryPlan{
		Leaves: []logql.LogLeaf{{ID: "l0", Matchers: []logql.LabelMatch{{Label: "service", Op: logql.MatchEq, Value: "x"}}}},
	}
	dataCh, errCh, err := q.EvaluateLogsQuery(t.Context(), uuid.New(), contractStart, contractEnd, false, 0, plan, nil)
	require.NoError(t, err)
	drainAndAssertClosed(t, dataCh, errCh)
}

func TestEvaluateSpansQuery_ClosesChannels(t *testing.T) {
	q := newEmptyQuerier()
	plan := logql.LQueryPlan{
		Leaves: []logql.LogLeaf{{ID: "s0", Matchers: []logql.LabelMatch{{Label: "service", Op: logql.MatchEq, Value: "x"}}}},
	}
	dataCh, errCh, err := q.EvaluateSpansQuery(t.Context(), uuid.New(), contractStart, contractEnd, false, 0, plan, nil)
	require.NoError(t, err)
	drainAndAssertClosed(t, dataCh, errCh)
}

func TestEvaluateMetricsQuery_ClosesChannels(t *testing.T) {
	q := newEmptyQuerier()
	plan := promql.QueryPlan{
		Root:   &promql.LeafNode{BE: promql.BaseExpr{ID: "m0", Metric: "cpu_usage"}},
		Leaves: []promql.BaseExpr{{ID: "m0", Metric: "cpu_usage"}},
	}
	dataCh, errCh, err := q.EvaluateMetricsQuery(t.Context(), uuid.New(), contractStart, contractEnd, plan)
	require.NoError(t, err)
	drainAndAssertClosed(t, dataCh, errCh)
}

func TestEvaluateMetricTagValuesQuery_ClosesChannels(t *testing.T) {
	q := newEmptyQuerier()
	plan := promql.QueryPlan{
		TagName: "host",
		Leaves:  []promql.BaseExpr{{ID: "tv0", Metric: "cpu_usage"}},
	}
	dataCh, errCh, err := q.EvaluateMetricTagValuesQuery(t.Context(), uuid.New(), contractStart, contractEnd, plan)
	require.NoError(t, err)
	drainAndAssertClosed(t, dataCh, errCh)
}

func TestEvaluateLogTagValuesQuery_ClosesChannels(t *testing.T) {
	q := newEmptyQuerier()
	plan := logql.LQueryPlan{
		TagName: "host",
		Leaves:  []logql.LogLeaf{{ID: "ltv0", Matchers: []logql.LabelMatch{{Label: "service", Op: logql.MatchEq, Value: "x"}}}},
	}
	dataCh, errCh, err := q.EvaluateLogTagValuesQuery(t.Context(), uuid.New(), contractStart, contractEnd, plan)
	require.NoError(t, err)
	drainAndAssertClosed(t, dataCh, errCh)
}

func TestEvaluateSpanTagValuesQuery_ClosesChannels(t *testing.T) {
	q := newEmptyQuerier()
	plan := logql.LQueryPlan{
		TagName: "host",
		Leaves:  []logql.LogLeaf{{ID: "stv0", Matchers: []logql.LabelMatch{{Label: "service", Op: logql.MatchEq, Value: "x"}}}},
	}
	dataCh, errCh, err := q.EvaluateSpanTagValuesQuery(t.Context(), uuid.New(), contractStart, contractEnd, plan)
	require.NoError(t, err)
	drainAndAssertClosed(t, dataCh, errCh)
}

func TestEvaluateLogTagNamesQuery_ClosesChannels(t *testing.T) {
	q := newEmptyQuerier()
	plan := logql.LQueryPlan{
		Leaves: []logql.LogLeaf{{ID: "ltn0", Matchers: []logql.LabelMatch{{Label: "service", Op: logql.MatchEq, Value: "x"}}}},
	}
	dataCh, errCh, err := q.EvaluateLogTagNamesQuery(t.Context(), uuid.New(), contractStart, contractEnd, plan)
	require.NoError(t, err)
	drainAndAssertClosed(t, dataCh, errCh)
}

func TestEvaluateMetricTagNamesQuery_ClosesChannels(t *testing.T) {
	q := newEmptyQuerier()
	plan := promql.QueryPlan{
		Leaves: []promql.BaseExpr{{ID: "mtn0", Metric: "cpu_usage"}},
	}
	dataCh, errCh, err := q.EvaluateMetricTagNamesQuery(t.Context(), uuid.New(), contractStart, contractEnd, plan)
	require.NoError(t, err)
	drainAndAssertClosed(t, dataCh, errCh)
}

func TestEvaluateSpanTagNamesQuery_ClosesChannels(t *testing.T) {
	q := newEmptyQuerier()
	plan := logql.LQueryPlan{
		Leaves: []logql.LogLeaf{{ID: "stn0", Matchers: []logql.LabelMatch{{Label: "service", Op: logql.MatchEq, Value: "x"}}}},
	}
	dataCh, errCh, err := q.EvaluateSpanTagNamesQuery(t.Context(), uuid.New(), contractStart, contractEnd, plan)
	require.NoError(t, err)
	drainAndAssertClosed(t, dataCh, errCh)
}
