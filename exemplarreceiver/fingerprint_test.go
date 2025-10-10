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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func TestComputeTracesFingerprint(t *testing.T) {
	tests := []struct {
		name string
		data pipeline.Row
	}{
		{
			name: "basic trace with span name",
			data: pipeline.Row{
				wkk.NewRowKey("service_name"): "test-service",
				wkk.NewRowKey("cluster_name"): "prod",
				wkk.NewRowKey("namespace"):    "default",
				wkk.NewRowKey("span_kind"):    int32(2), // SERVER
				wkk.NewRowKey("span_name"):    "GET /api/users",
			},
		},
		{
			name: "HTTP trace",
			data: pipeline.Row{
				wkk.NewRowKey("service_name"):        "test-service",
				wkk.NewRowKey("cluster_name"):        "prod",
				wkk.NewRowKey("namespace"):           "default",
				wkk.NewRowKey("span_kind"):           int32(2),
				wkk.NewRowKey("http_request_method"): "GET",
				wkk.NewRowKey("url_template"):        "/api/users/{id}",
			},
		},
		{
			name: "database trace",
			data: pipeline.Row{
				wkk.NewRowKey("service_name"):      "test-service",
				wkk.NewRowKey("cluster_name"):      "prod",
				wkk.NewRowKey("namespace"):         "default",
				wkk.NewRowKey("span_kind"):         int32(3), // CLIENT
				wkk.NewRowKey("db_system"):         "postgresql",
				wkk.NewRowKey("db_namespace"):      "mydb",
				wkk.NewRowKey("db_operation_name"): "SELECT",
				wkk.NewRowKey("server_address"):    "localhost:5432",
				wkk.NewRowKey("span_name"):         "SELECT users",
			},
		},
		{
			name: "messaging trace",
			data: pipeline.Row{
				wkk.NewRowKey("service_name"):               "test-service",
				wkk.NewRowKey("cluster_name"):               "prod",
				wkk.NewRowKey("namespace"):                  "default",
				wkk.NewRowKey("span_kind"):                  int32(4), // PRODUCER
				wkk.NewRowKey("messaging_system"):           "kafka",
				wkk.NewRowKey("messaging_operation_type"):   "publish",
				wkk.NewRowKey("messaging_destination_name"): "user-events",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fp := computeTracesFingerprint(tt.data)
			assert.NotZero(t, fp, "fingerprint should not be zero")

			// Computing same data twice should give same fingerprint
			fp2 := computeTracesFingerprint(tt.data)
			assert.Equal(t, fp, fp2, "fingerprint should be deterministic")
		})
	}
}

func TestComputeTracesFingerprintConsistency(t *testing.T) {
	// Same data should always produce same fingerprint
	data := pipeline.Row{
		wkk.NewRowKey("service_name"): "test-service",
		wkk.NewRowKey("cluster_name"): "prod",
		wkk.NewRowKey("namespace"):    "default",
		wkk.NewRowKey("span_kind"):    int32(2),
		wkk.NewRowKey("span_name"):    "GET /api/users",
	}

	fp1 := computeTracesFingerprint(data)
	fp2 := computeTracesFingerprint(data)

	assert.Equal(t, fp1, fp2, "fingerprints should be consistent")
}

func TestComputeLogsFingerprintConsistency(t *testing.T) {
	// Same data should always produce same fingerprint
	data := pipeline.Row{
		wkk.NewRowKey("service_name"): "test-service",
		wkk.NewRowKey("cluster_name"): "prod",
		wkk.NewRowKey("namespace"):    "default",
		wkk.NewRowKey("message"):      "Error processing request",
	}

	fp1 := computeLogsFingerprint(data)
	fp2 := computeLogsFingerprint(data)

	assert.Equal(t, fp1, fp2, "fingerprints should be consistent")
	assert.NotZero(t, fp1, "fingerprint should not be zero")
}

func TestComputeLogsFingerprintWithBody(t *testing.T) {
	// Test with "body" field instead of "message"
	data := pipeline.Row{
		wkk.NewRowKey("service_name"): "test-service",
		wkk.NewRowKey("cluster_name"): "prod",
		wkk.NewRowKey("namespace"):    "default",
		wkk.NewRowKey("body"):         "Error processing request",
	}

	fp := computeLogsFingerprint(data)
	assert.NotZero(t, fp, "fingerprint should not be zero")
}

func TestSpanKindToString(t *testing.T) {
	tests := []struct {
		kind     int32
		expected string
	}{
		{0, "SPAN_KIND_UNSPECIFIED"},
		{1, "SPAN_KIND_INTERNAL"},
		{2, "SPAN_KIND_SERVER"},
		{3, "SPAN_KIND_CLIENT"},
		{4, "SPAN_KIND_PRODUCER"},
		{5, "SPAN_KIND_CONSUMER"},
		{99, "SPAN_KIND_UNSPECIFIED"}, // Unknown kind
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := spanKindToString(tt.kind)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestComputeTracesFingerprintWithDefaults(t *testing.T) {
	// Test with missing fields - should use defaults
	data := pipeline.Row{
		wkk.NewRowKey("span_name"): "test-span",
	}

	fp := computeTracesFingerprint(data)
	assert.NotZero(t, fp, "fingerprint should not be zero even with missing fields")
}

func TestComputeLogsFingerprintWithDefaults(t *testing.T) {
	// Test with missing fields - should use defaults
	data := pipeline.Row{
		wkk.NewRowKey("message"): "test message",
	}

	fp := computeLogsFingerprint(data)
	assert.NotZero(t, fp, "fingerprint should not be zero even with missing fields")
}
