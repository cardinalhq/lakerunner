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

package exemplarreceiver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/orgapikey"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// mockDB implements the ExemplarStore interface for testing
type mockDB struct {
	mock.Mock
}

func (m *mockDB) BatchUpsertExemplarLogs(ctx context.Context, params []lrdb.BatchUpsertExemplarLogsParams) *lrdb.BatchUpsertExemplarLogsBatchResults {
	args := m.Called(ctx, params)
	return args.Get(0).(*lrdb.BatchUpsertExemplarLogsBatchResults)
}

func (m *mockDB) BatchUpsertExemplarMetrics(ctx context.Context, params []lrdb.BatchUpsertExemplarMetricsParams) *lrdb.BatchUpsertExemplarMetricsBatchResults {
	args := m.Called(ctx, params)
	return args.Get(0).(*lrdb.BatchUpsertExemplarMetricsBatchResults)
}

func (m *mockDB) BatchUpsertExemplarTraces(ctx context.Context, params []lrdb.BatchUpsertExemplarTracesParams) *lrdb.BatchUpsertExemplarTracesBatchResults {
	args := m.Called(ctx, params)
	return args.Get(0).(*lrdb.BatchUpsertExemplarTracesBatchResults)
}

func (m *mockDB) UpsertServiceIdentifier(ctx context.Context, params lrdb.UpsertServiceIdentifierParams) (lrdb.UpsertServiceIdentifierRow, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(lrdb.UpsertServiceIdentifierRow), args.Error(1)
}

// mockAPIKeyProvider implements orgapikey.OrganizationAPIKeyProvider for testing
type mockAPIKeyProvider struct {
	mock.Mock
}

func (m *mockAPIKeyProvider) ValidateAPIKey(ctx context.Context, apiKey string) (*uuid.UUID, error) {
	args := m.Called(ctx, apiKey)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*uuid.UUID), args.Error(1)
}

func (m *mockAPIKeyProvider) GetAPIKeyInfo(ctx context.Context, apiKey string) (*orgapikey.OrganizationAPIKey, error) {
	args := m.Called(ctx, apiKey)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*orgapikey.OrganizationAPIKey), args.Error(1)
}

func TestHealthCheck(t *testing.T) {
	db := &mockDB{}
	apiKeyProvider := &mockAPIKeyProvider{}

	service := &ReceiverService{
		db:             db,
		apiKeyProvider: apiKeyProvider,
		port:           8080,
	}

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()

	service.healthCheck(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var response map[string]string
	err := json.NewDecoder(w.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, "ok", response["status"])
}

func TestHandleExemplar_EmptySource(t *testing.T) {
	db := &mockDB{}
	apiKeyProvider := &mockAPIKeyProvider{}
	orgID := uuid.New()

	service := &ReceiverService{
		db:             db,
		apiKeyProvider: apiKeyProvider,
		port:           8080,
	}

	serviceName := "test-service"
	clusterName := "test-cluster"
	namespace := "test-namespace"

	batchReq := LogsBatchRequest{
		Source: "",
		Exemplars: []LogsExemplar{
			{
				ServiceName: &serviceName,
				ClusterName: &clusterName,
				Namespace:   &namespace,
				Attributes:  map[wkk.RowKey]any{wkk.NewRowKey("message"): "test"},
			},
		},
	}

	body, err := json.Marshal(batchReq)
	require.NoError(t, err)

	req := httptest.NewRequest("POST", "/api/v1/exemplar/logs", bytes.NewReader(body))
	req.SetPathValue("signal", "logs")
	req = req.WithContext(WithOrgID(req.Context(), orgID))

	w := httptest.NewRecorder()

	service.handleExemplar(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "source is required")
}

func TestHandleExemplar_EmptyExemplars(t *testing.T) {
	db := &mockDB{}
	apiKeyProvider := &mockAPIKeyProvider{}
	orgID := uuid.New()

	service := &ReceiverService{
		db:             db,
		apiKeyProvider: apiKeyProvider,
		port:           8080,
	}

	batchReq := LogsBatchRequest{
		Source:    "test",
		Exemplars: []LogsExemplar{},
	}

	body, err := json.Marshal(batchReq)
	require.NoError(t, err)

	req := httptest.NewRequest("POST", "/api/v1/exemplar/logs", bytes.NewReader(body))
	req.SetPathValue("signal", "logs")
	req = req.WithContext(WithOrgID(req.Context(), orgID))

	w := httptest.NewRecorder()

	service.handleExemplar(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "exemplars array cannot be empty")
}

func TestHandleExemplar_InvalidSignal(t *testing.T) {
	db := &mockDB{}
	apiKeyProvider := &mockAPIKeyProvider{}
	orgID := uuid.New()

	service := &ReceiverService{
		db:             db,
		apiKeyProvider: apiKeyProvider,
		port:           8080,
	}

	req := httptest.NewRequest("POST", "/api/v1/exemplar/invalid", bytes.NewReader([]byte("{}")))
	req.SetPathValue("signal", "invalid")
	req = req.WithContext(WithOrgID(req.Context(), orgID))

	w := httptest.NewRecorder()

	service.handleExemplar(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "unknown signal type")
}

func TestHandleExemplar_MissingOrgID(t *testing.T) {
	db := &mockDB{}
	apiKeyProvider := &mockAPIKeyProvider{}

	service := &ReceiverService{
		db:             db,
		apiKeyProvider: apiKeyProvider,
		port:           8080,
	}

	req := httptest.NewRequest("POST", "/api/v1/exemplar/logs", bytes.NewReader([]byte("{}")))
	req.SetPathValue("signal", "logs")

	w := httptest.NewRecorder()

	service.handleExemplar(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "organization ID not found")
}

func TestHandleExemplar_InvalidJSON(t *testing.T) {
	db := &mockDB{}
	apiKeyProvider := &mockAPIKeyProvider{}
	orgID := uuid.New()

	service := &ReceiverService{
		db:             db,
		apiKeyProvider: apiKeyProvider,
		port:           8080,
	}

	req := httptest.NewRequest("POST", "/api/v1/exemplar/logs", bytes.NewReader([]byte("not json")))
	req.SetPathValue("signal", "logs")
	req = req.WithContext(WithOrgID(req.Context(), orgID))

	w := httptest.NewRecorder()

	service.handleExemplar(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid JSON")
}

func TestProcessMetricsBatch_MissingFields(t *testing.T) {
	db := &mockDB{}
	apiKeyProvider := &mockAPIKeyProvider{}
	orgID := uuid.New()

	service := &ReceiverService{
		db:             db,
		apiKeyProvider: apiKeyProvider,
		port:           8080,
	}

	// Missing metric_name
	response, err := service.processMetricsBatch(context.Background(), orgID, "test", []MetricsExemplar{
		{
			ServiceName: stringPtr("service"),
			ClusterName: stringPtr("cluster"),
			Namespace:   stringPtr("namespace"),
			MetricName:  "",
			MetricType:  "gauge",
			Attributes:  map[wkk.RowKey]any{wkk.NewRowKey("value"): 42},
		},
	})

	require.NoError(t, err)
	assert.Equal(t, "ok", response.Status)
	assert.Equal(t, 0, response.Accepted)
	assert.Equal(t, 1, response.Failed)
	assert.Len(t, response.Errors, 1)
	assert.Contains(t, response.Errors[0], "metric_name and metric_type are required")
}

func TestProcessTracesBatch_MissingFields(t *testing.T) {
	db := &mockDB{}
	apiKeyProvider := &mockAPIKeyProvider{}
	orgID := uuid.New()

	service := &ReceiverService{
		db:             db,
		apiKeyProvider: apiKeyProvider,
		port:           8080,
	}

	// Missing span_name
	response, err := service.processTracesBatch(context.Background(), orgID, "test", []TracesExemplar{
		{
			ServiceName: stringPtr("service"),
			ClusterName: stringPtr("cluster"),
			Namespace:   stringPtr("namespace"),
			SpanName:    "",
			SpanKind:    "2",
			Attributes:  map[wkk.RowKey]any{wkk.NewRowKey("trace_id"): "abc123"},
		},
	})

	require.NoError(t, err)
	assert.Equal(t, "ok", response.Status)
	assert.Equal(t, 0, response.Accepted)
	assert.Equal(t, 1, response.Failed)
	assert.Len(t, response.Errors, 1)
	assert.Contains(t, response.Errors[0], "span_name is required")
}

func TestHandleExemplar_MetricsPath(t *testing.T) {
	db := &mockDB{}
	apiKeyProvider := &mockAPIKeyProvider{}
	orgID := uuid.New()

	service := &ReceiverService{
		db:             db,
		apiKeyProvider: apiKeyProvider,
		port:           8080,
	}

	batchReq := MetricsBatchRequest{
		Source: "test",
		Exemplars: []MetricsExemplar{
			{
				ServiceName: stringPtr("service"),
				ClusterName: stringPtr("cluster"),
				Namespace:   stringPtr("namespace"),
				MetricName:  "",
				MetricType:  "gauge",
				Attributes:  map[wkk.RowKey]any{},
			},
		},
	}

	body, err := json.Marshal(batchReq)
	require.NoError(t, err)

	req := httptest.NewRequest("POST", "/api/v1/exemplar/metrics", bytes.NewReader(body))
	req.SetPathValue("signal", "metrics")
	req = req.WithContext(WithOrgID(req.Context(), orgID))

	w := httptest.NewRecorder()

	service.handleExemplar(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response ExemplarBatchResponse
	err = json.NewDecoder(w.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, "ok", response.Status)
	assert.Equal(t, 1, response.Failed)
}

func TestHandleExemplar_TracesPath(t *testing.T) {
	db := &mockDB{}
	apiKeyProvider := &mockAPIKeyProvider{}
	orgID := uuid.New()

	service := &ReceiverService{
		db:             db,
		apiKeyProvider: apiKeyProvider,
		port:           8080,
	}

	batchReq := TracesBatchRequest{
		Source: "test",
		Exemplars: []TracesExemplar{
			{
				ServiceName: stringPtr("service"),
				ClusterName: stringPtr("cluster"),
				Namespace:   stringPtr("namespace"),
				SpanName:    "",
				SpanKind:    "2",
				Attributes:  map[wkk.RowKey]any{},
			},
		},
	}

	body, err := json.Marshal(batchReq)
	require.NoError(t, err)

	req := httptest.NewRequest("POST", "/api/v1/exemplar/traces", bytes.NewReader(body))
	req.SetPathValue("signal", "traces")
	req = req.WithContext(WithOrgID(req.Context(), orgID))

	w := httptest.NewRecorder()

	service.handleExemplar(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response ExemplarBatchResponse
	err = json.NewDecoder(w.Body).Decode(&response)
	require.NoError(t, err)
	assert.Equal(t, "ok", response.Status)
	assert.Equal(t, 1, response.Failed)
}

func stringPtr(s string) *string {
	return &s
}
