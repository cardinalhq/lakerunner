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

func TestExemplarBatchRequest_Unmarshal(t *testing.T) {
	jsonData := `{
		"source": "datadog",
		"exemplars": [
			{
				"service_name": "api-gateway",
				"cluster_name": "prod-us-east",
				"namespace": "production",
				"message": "HTTP request failed",
				"tags": ["error", "retry"]
			},
			{
				"service_name": "checkout-service",
				"cluster_name": "prod-eu-west",
				"namespace": "production",
				"message": "Payment processed"
			}
		]
	}`

	var req ExemplarBatchRequest
	err := json.Unmarshal([]byte(jsonData), &req)
	require.NoError(t, err)

	assert.Equal(t, "datadog", req.Source)
	assert.Len(t, req.Exemplars, 2)
	assert.Equal(t, "api-gateway", req.Exemplars[0]["service_name"])
	assert.Equal(t, "checkout-service", req.Exemplars[1]["service_name"])
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
				"message": "HTTP request failed",
				"tags": ["error", "retry"],
				"host": "pod-xyz"
			}`,
			validate: func(t *testing.T, exemplar LogsExemplar) {
				assert.Equal(t, "api-gateway", exemplar.ServiceName)
				assert.Equal(t, "prod-us-east", exemplar.ClusterName)
				assert.Equal(t, "production", exemplar.Namespace)
				assert.Equal(t, "HTTP request failed", exemplar.Data["message"])
				assert.Equal(t, "pod-xyz", exemplar.Data["host"])
				// Verify explicit fields are also in Data (preserved)
				assert.Equal(t, "api-gateway", exemplar.Data["service_name"])
			},
		},
		{
			name: "logs exemplar with old_fingerprint",
			jsonData: `{
				"service_name": "test-service",
				"cluster_name": "test-cluster",
				"namespace": "test-namespace",
				"message": "Test message",
				"old_fingerprint": 123456789
			}`,
			validate: func(t *testing.T, exemplar LogsExemplar) {
				assert.Equal(t, "test-service", exemplar.ServiceName)
				assert.Contains(t, exemplar.Data, "old_fingerprint")
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
		"labels": {
			"method": "POST",
			"endpoint": "/api/v1/checkout"
		},
		"value": 0.245
	}`

	var exemplar MetricsExemplar
	err := json.Unmarshal([]byte(jsonData), &exemplar)
	require.NoError(t, err)

	assert.Equal(t, "checkout-service", exemplar.ServiceName)
	assert.Equal(t, "prod-eu-west", exemplar.ClusterName)
	assert.Equal(t, "production", exemplar.Namespace)
	assert.Equal(t, "http_request_duration_seconds", exemplar.MetricName)
	assert.Equal(t, "histogram", exemplar.MetricType)
	assert.Equal(t, 0.245, exemplar.Data["value"])
	// Verify explicit fields are also in Data
	assert.Equal(t, "http_request_duration_seconds", exemplar.Data["metric_name"])
	assert.Equal(t, "histogram", exemplar.Data["metric_type"])
}

func TestTracesExemplar_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		validate func(t *testing.T, exemplar TracesExemplar)
	}{
		{
			name: "traces exemplar with int span_kind",
			jsonData: `{
				"service_name": "payment-processor",
				"cluster_name": "prod-us-west",
				"namespace": "production",
				"span_name": "process_payment",
				"span_kind": 2,
				"trace_id": "abc123",
				"duration_ms": 145.23
			}`,
			validate: func(t *testing.T, exemplar TracesExemplar) {
				assert.Equal(t, "payment-processor", exemplar.ServiceName)
				assert.Equal(t, "prod-us-west", exemplar.ClusterName)
				assert.Equal(t, "production", exemplar.Namespace)
				assert.Equal(t, "process_payment", exemplar.SpanName)
				assert.Equal(t, float64(2), exemplar.SpanKind) // JSON unmarshals numbers as float64
				assert.Equal(t, "abc123", exemplar.Data["trace_id"])
			},
		},
		{
			name: "traces exemplar with string span_kind",
			jsonData: `{
				"service_name": "payment-processor",
				"cluster_name": "prod-us-west",
				"namespace": "production",
				"span_name": "process_payment",
				"span_kind": "SPAN_KIND_SERVER",
				"trace_id": "abc123"
			}`,
			validate: func(t *testing.T, exemplar TracesExemplar) {
				assert.Equal(t, "SPAN_KIND_SERVER", exemplar.SpanKind)
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
