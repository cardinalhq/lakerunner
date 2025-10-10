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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogsBatchRequest_Unmarshal(t *testing.T) {
	jsonData := `{
		"source": "datadog",
		"exemplars": [
			{
				"service_name": "api-gateway",
				"cluster_name": "prod-us-east",
				"namespace": "production",
				"attributes": {
					"message": "HTTP request failed",
					"tags": ["error", "retry"]
				}
			},
			{
				"service_name": "checkout-service",
				"cluster_name": "prod-eu-west",
				"namespace": "production",
				"attributes": {
					"message": "Payment processed"
				}
			}
		]
	}`

	var req LogsBatchRequest
	err := json.Unmarshal([]byte(jsonData), &req)
	require.NoError(t, err)

	assert.Equal(t, "datadog", req.Source)
	assert.Len(t, req.Exemplars, 2)
	require.NotNil(t, req.Exemplars[0].ServiceName)
	assert.Equal(t, "api-gateway", *req.Exemplars[0].ServiceName)
	require.NotNil(t, req.Exemplars[1].ServiceName)
	assert.Equal(t, "checkout-service", *req.Exemplars[1].ServiceName)
}

func TestLogsExemplar_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		validate func(t *testing.T, exemplar LogsExemplar)
	}{
		{
			name: "basic logs exemplar",
			jsonData: `{
				"service_name": "api-gateway",
				"cluster_name": "prod-us-east",
				"namespace": "production",
				"attributes": {
					"message": "HTTP request failed",
					"tags": ["error", "retry"],
					"host": "pod-xyz"
				}
			}`,
			validate: func(t *testing.T, exemplar LogsExemplar) {
				require.NotNil(t, exemplar.ServiceName)
				assert.Equal(t, "api-gateway", *exemplar.ServiceName)
				require.NotNil(t, exemplar.ClusterName)
				assert.Equal(t, "prod-us-east", *exemplar.ClusterName)
				require.NotNil(t, exemplar.Namespace)
				assert.Equal(t, "production", *exemplar.Namespace)
				assert.Len(t, exemplar.Attributes, 3)
			},
		},
		{
			name: "logs exemplar with old_fingerprint",
			jsonData: `{
				"service_name": "test-service",
				"cluster_name": "test-cluster",
				"namespace": "test-namespace",
				"attributes": {
					"message": "Test message",
					"old_fingerprint": 123456789
				}
			}`,
			validate: func(t *testing.T, exemplar LogsExemplar) {
				require.NotNil(t, exemplar.ServiceName)
				assert.Equal(t, "test-service", *exemplar.ServiceName)
				assert.Len(t, exemplar.Attributes, 2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var exemplar LogsExemplar
			err := json.Unmarshal([]byte(tt.jsonData), &exemplar)
			require.NoError(t, err)
			tt.validate(t, exemplar)
		})
	}
}

func TestMetricsExemplar_UnmarshalJSON(t *testing.T) {
	jsonData := `{
		"service_name": "checkout-service",
		"cluster_name": "prod-eu-west",
		"namespace": "production",
		"metric_name": "http_request_duration_seconds",
		"metric_type": "histogram",
		"attributes": {
			"labels": {
				"method": "POST",
				"endpoint": "/api/v1/checkout"
			},
			"value": 0.245
		}
	}`

	var exemplar MetricsExemplar
	err := json.Unmarshal([]byte(jsonData), &exemplar)
	require.NoError(t, err)

	require.NotNil(t, exemplar.ServiceName)
	assert.Equal(t, "checkout-service", *exemplar.ServiceName)
	require.NotNil(t, exemplar.ClusterName)
	assert.Equal(t, "prod-eu-west", *exemplar.ClusterName)
	require.NotNil(t, exemplar.Namespace)
	assert.Equal(t, "production", *exemplar.Namespace)
	assert.Equal(t, "http_request_duration_seconds", exemplar.MetricName)
	assert.Equal(t, "histogram", exemplar.MetricType)
	assert.Len(t, exemplar.Attributes, 2)
}

func TestTracesExemplar_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		validate func(t *testing.T, exemplar TracesExemplar)
	}{
		{
			name: "traces exemplar with string span_kind",
			jsonData: `{
				"service_name": "payment-processor",
				"cluster_name": "prod-us-west",
				"namespace": "production",
				"span_name": "process_payment",
				"span_kind": "2",
				"attributes": {
					"trace_id": "abc123",
					"duration_ms": 145.23
				}
			}`,
			validate: func(t *testing.T, exemplar TracesExemplar) {
				require.NotNil(t, exemplar.ServiceName)
				assert.Equal(t, "payment-processor", *exemplar.ServiceName)
				require.NotNil(t, exemplar.ClusterName)
				assert.Equal(t, "prod-us-west", *exemplar.ClusterName)
				require.NotNil(t, exemplar.Namespace)
				assert.Equal(t, "production", *exemplar.Namespace)
				assert.Equal(t, "process_payment", exemplar.SpanName)
				assert.Equal(t, "2", exemplar.SpanKind)
				assert.Len(t, exemplar.Attributes, 2)
			},
		},
		{
			name: "traces exemplar with named span_kind",
			jsonData: `{
				"service_name": "payment-processor",
				"cluster_name": "prod-us-west",
				"namespace": "production",
				"span_name": "process_payment",
				"span_kind": "SPAN_KIND_SERVER",
				"attributes": {
					"trace_id": "abc123"
				}
			}`,
			validate: func(t *testing.T, exemplar TracesExemplar) {
				require.NotNil(t, exemplar.ServiceName)
				assert.Equal(t, "payment-processor", *exemplar.ServiceName)
				assert.Equal(t, "SPAN_KIND_SERVER", exemplar.SpanKind)
				assert.Len(t, exemplar.Attributes, 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var exemplar TracesExemplar
			err := json.Unmarshal([]byte(tt.jsonData), &exemplar)
			require.NoError(t, err)
			tt.validate(t, exemplar)
		})
	}
}
