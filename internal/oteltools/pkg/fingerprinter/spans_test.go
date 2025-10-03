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

package fingerprinter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func TestCalculateSpanFingerprintFromRow(t *testing.T) {
	tests := []struct {
		name     string
		row      map[wkk.RowKey]any
		expected int64 // We'll check that it's non-zero and consistent
		desc     string
	}{
		{
			name: "basic span with service info",
			row: map[wkk.RowKey]any{
				wkk.NewRowKey("resource_k8s_cluster_name"):   "test-cluster",
				wkk.NewRowKey("resource_k8s_namespace_name"): "test-namespace",
				wkk.NewRowKey("resource_service_name"):       "test-service",
				wkk.NewRowKey("span_kind"):                   "SPAN_KIND_SERVER",
				wkk.NewRowKey("span_name"):                   "GET /api/users",
			},
			desc: "should generate fingerprint with resource and span info",
		},
		{
			name: "http span",
			row: map[wkk.RowKey]any{
				wkk.NewRowKey("resource_k8s_cluster_name"):   "prod-cluster",
				wkk.NewRowKey("resource_k8s_namespace_name"): "prod-namespace",
				wkk.NewRowKey("resource_service_name"):       "api-service",
				wkk.NewRowKey("span_kind"):                   "SPAN_KIND_CLIENT",
				wkk.NewRowKey("attr_http_request_method"):    "POST",
				wkk.NewRowKey("attr_url_template"):           "/api/users/{id}",
			},
			desc: "should generate fingerprint with HTTP attributes",
		},
		{
			name: "database span",
			row: map[wkk.RowKey]any{
				wkk.NewRowKey("resource_k8s_cluster_name"):   "prod-cluster",
				wkk.NewRowKey("resource_k8s_namespace_name"): "prod-namespace",
				wkk.NewRowKey("resource_service_name"):       "db-service",
				wkk.NewRowKey("span_kind"):                   "SPAN_KIND_CLIENT",
				wkk.NewRowKey("span_name"):                   "SELECT FROM users",
				wkk.NewRowKey("attr_db_system_name"):         "postgresql",
				wkk.NewRowKey("attr_db_namespace"):           "mydb",
				wkk.NewRowKey("attr_db_operation_name"):      "SELECT",
				wkk.NewRowKey("attr_server_address"):         "postgres.svc.cluster.local",
			},
			desc: "should generate fingerprint with database attributes",
		},
		{
			name: "messaging span",
			row: map[wkk.RowKey]any{
				wkk.NewRowKey("resource_k8s_cluster_name"):       "prod-cluster",
				wkk.NewRowKey("resource_k8s_namespace_name"):     "prod-namespace",
				wkk.NewRowKey("resource_service_name"):           "messaging-service",
				wkk.NewRowKey("span_kind"):                       "SPAN_KIND_PRODUCER",
				wkk.NewRowKey("attr_messaging_system"):           "kafka",
				wkk.NewRowKey("attr_messaging_operation_type"):   "publish",
				wkk.NewRowKey("attr_messaging_destination_name"): "events-topic",
			},
			desc: "should generate fingerprint with messaging attributes",
		},
		{
			name: "span with missing resource attributes",
			row: map[wkk.RowKey]any{
				wkk.NewRowKey("span_kind"): "SPAN_KIND_INTERNAL",
				wkk.NewRowKey("span_name"): "processRequest",
			},
			desc: "should use 'unknown' for missing resource attributes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fingerprint := CalculateSpanFingerprintFromRow(tt.row)

			// Verify fingerprint is non-zero
			assert.NotZero(t, fingerprint, tt.desc)

			// Verify consistency: same input produces same output
			fingerprint2 := CalculateSpanFingerprintFromRow(tt.row)
			assert.Equal(t, fingerprint, fingerprint2, "fingerprint should be consistent")
		})
	}
}

func TestCalculateSpanFingerprintFromRow_Distinctness(t *testing.T) {
	// Test that different span types produce different fingerprints
	httpSpan := map[wkk.RowKey]any{
		wkk.NewRowKey("resource_k8s_cluster_name"):   "cluster",
		wkk.NewRowKey("resource_k8s_namespace_name"): "namespace",
		wkk.NewRowKey("resource_service_name"):       "service",
		wkk.NewRowKey("span_kind"):                   "SPAN_KIND_CLIENT",
		wkk.NewRowKey("attr_http_request_method"):    "GET",
		wkk.NewRowKey("attr_url_template"):           "/api/users",
	}

	dbSpan := map[wkk.RowKey]any{
		wkk.NewRowKey("resource_k8s_cluster_name"):   "cluster",
		wkk.NewRowKey("resource_k8s_namespace_name"): "namespace",
		wkk.NewRowKey("resource_service_name"):       "service",
		wkk.NewRowKey("span_kind"):                   "SPAN_KIND_CLIENT",
		wkk.NewRowKey("span_name"):                   "SELECT",
		wkk.NewRowKey("attr_db_system_name"):         "postgresql",
	}

	httpFingerprint := CalculateSpanFingerprintFromRow(httpSpan)
	dbFingerprint := CalculateSpanFingerprintFromRow(dbSpan)

	assert.NotEqual(t, httpFingerprint, dbFingerprint, "HTTP and DB spans should have different fingerprints")
}

func TestGetStringFromRow(t *testing.T) {
	tests := []struct {
		name     string
		row      map[wkk.RowKey]any
		key      wkk.RowKey
		expected string
	}{
		{
			name: "existing string key",
			row: map[wkk.RowKey]any{
				wkk.NewRowKey("test_key"): "test_value",
			},
			key:      wkk.NewRowKey("test_key"),
			expected: "test_value",
		},
		{
			name:     "missing key",
			row:      map[wkk.RowKey]any{},
			key:      wkk.NewRowKey("missing_key"),
			expected: "",
		},
		{
			name: "non-string value",
			row: map[wkk.RowKey]any{
				wkk.NewRowKey("int_key"): 123,
			},
			key:      wkk.NewRowKey("int_key"),
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetStringFromRow(tt.row, tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}
