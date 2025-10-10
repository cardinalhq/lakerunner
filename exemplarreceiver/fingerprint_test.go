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

	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
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
	message := "Error processing request"
	level := "error"
	data := pipeline.Row{
		wkk.NewRowKey("service_name"): "test-service",
		wkk.NewRowKey("cluster_name"): "prod",
		wkk.NewRowKey("namespace"):    "default",
	}

	// Create test fixtures
	tenantMgr := fingerprint.NewTenantManager(0.8)
	fp := fingerprinter.NewFingerprinter()
	orgID := "test-org-id"

	fp1, err := computeLogsFingerprint(orgID, message, level, data, tenantMgr, fp)
	assert.NoError(t, err, "fingerprinting should not fail")
	fp2, err := computeLogsFingerprint(orgID, message, level, data, tenantMgr, fp)
	assert.NoError(t, err, "fingerprinting should not fail")

	assert.Equal(t, fp1, fp2, "fingerprints should be consistent")
	assert.NotZero(t, fp1, "fingerprint should not be zero")
}

func TestComputeLogsFingerprintDifferentMessages(t *testing.T) {
	// Different messages should produce different fingerprints
	level := "error"
	data := pipeline.Row{
		wkk.NewRowKey("service_name"): "test-service",
		wkk.NewRowKey("cluster_name"): "prod",
		wkk.NewRowKey("namespace"):    "default",
	}

	// Create test fixtures
	tenantMgr := fingerprint.NewTenantManager(0.8)
	fp := fingerprinter.NewFingerprinter()
	orgID := "test-org-id"

	fp1, err := computeLogsFingerprint(orgID, "Error processing request", level, data, tenantMgr, fp)
	assert.NoError(t, err, "fingerprinting should not fail")
	fp2, err := computeLogsFingerprint(orgID, "Success processing request", level, data, tenantMgr, fp)
	assert.NoError(t, err, "fingerprinting should not fail")

	assert.NotEqual(t, fp1, fp2, "different messages should produce different fingerprints")
}

func TestComputeLogsFingerprintDifferentLevels(t *testing.T) {
	// Different levels should produce different fingerprints
	message := "Processing request"
	data := pipeline.Row{
		wkk.NewRowKey("service_name"): "test-service",
	}

	// Create test fixtures
	tenantMgr := fingerprint.NewTenantManager(0.8)
	fp := fingerprinter.NewFingerprinter()
	orgID := "test-org-id"

	fp1, err := computeLogsFingerprint(orgID, message, "error", data, tenantMgr, fp)
	assert.NoError(t, err, "fingerprinting should not fail")
	fp2, err := computeLogsFingerprint(orgID, message, "info", data, tenantMgr, fp)
	assert.NoError(t, err, "fingerprinting should not fail")

	assert.NotEqual(t, fp1, fp2, "different levels should produce different fingerprints")
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

func TestComputeLogsFingerprintEmptyAttributes(t *testing.T) {
	// Test with empty attributes row
	message := "test message"
	level := "info"
	data := pipeline.Row{}

	// Create test fixtures
	tenantMgr := fingerprint.NewTenantManager(0.8)
	fp := fingerprinter.NewFingerprinter()
	orgID := "test-org-id"

	result, err := computeLogsFingerprint(orgID, message, level, data, tenantMgr, fp)
	assert.NoError(t, err, "fingerprinting should not fail")
	assert.NotZero(t, result, "fingerprint should not be zero even with empty attributes")
}

func TestComputeLogsFingerprintWithAttributes(t *testing.T) {
	tests := []struct {
		name    string
		message string
		level   string
		row     pipeline.Row
	}{
		{
			name:    "with attributes",
			message: "Error processing request",
			level:   "error",
			row: pipeline.Row{
				wkk.NewRowKey("status"):     "500",
				wkk.NewRowKey("request_id"): "abc123",
			},
		},
		{
			name:    "with different attributes",
			message: "Error processing request",
			level:   "error",
			row: pipeline.Row{
				wkk.NewRowKey("status"):  "500",
				wkk.NewRowKey("user_id"): "user456",
			},
		},
		{
			name:    "no attributes",
			message: "Simple message",
			level:   "info",
			row:     pipeline.Row{},
		},
	}

	// Create test fixtures
	tenantMgr := fingerprint.NewTenantManager(0.8)
	fp := fingerprinter.NewFingerprinter()
	orgID := "test-org-id"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := computeLogsFingerprint(orgID, tt.message, tt.level, tt.row, tenantMgr, fp)
			assert.NoError(t, err, "fingerprinting should not fail")
			// All cases should produce a fingerprint
			// Verify consistency
			result2, err := computeLogsFingerprint(orgID, tt.message, tt.level, tt.row, tenantMgr, fp)
			assert.NoError(t, err, "fingerprinting should not fail")
			assert.Equal(t, result, result2, "fingerprint should be deterministic")
			assert.NotZero(t, result, "fingerprint should not be zero")
		})
	}
}

func TestComputeLogsFingerprintAttributeOrder(t *testing.T) {
	// Different insertion order should produce same fingerprint
	message := "test"
	level := "info"
	row1 := pipeline.Row{
		wkk.NewRowKey("attr_a"): "value_a",
		wkk.NewRowKey("attr_b"): "value_b",
		wkk.NewRowKey("attr_c"): "value_c",
	}

	row2 := pipeline.Row{
		wkk.NewRowKey("attr_c"): "value_c",
		wkk.NewRowKey("attr_a"): "value_a",
		wkk.NewRowKey("attr_b"): "value_b",
	}

	// Create test fixtures
	tenantMgr := fingerprint.NewTenantManager(0.8)
	fp := fingerprinter.NewFingerprinter()
	orgID := "test-org-id"

	fp1, err := computeLogsFingerprint(orgID, message, level, row1, tenantMgr, fp)
	assert.NoError(t, err, "fingerprinting should not fail")
	fp2, err := computeLogsFingerprint(orgID, message, level, row2, tenantMgr, fp)
	assert.NoError(t, err, "fingerprinting should not fail")

	assert.Equal(t, fp1, fp2, "attribute order should not affect fingerprint")
}

func TestComputeLogsFingerprintDifferentAttributes(t *testing.T) {
	// Different attribute keys should produce different fingerprints
	message := "test"
	level := "info"
	row1 := pipeline.Row{
		wkk.NewRowKey("attr_a"): "value",
	}

	row2 := pipeline.Row{
		wkk.NewRowKey("attr_b"): "value",
	}

	// Create test fixtures
	tenantMgr := fingerprint.NewTenantManager(0.8)
	fp := fingerprinter.NewFingerprinter()
	orgID := "test-org-id"

	fp1, err := computeLogsFingerprint(orgID, message, level, row1, tenantMgr, fp)
	assert.NoError(t, err, "fingerprinting should not fail")
	fp2, err := computeLogsFingerprint(orgID, message, level, row2, tenantMgr, fp)
	assert.NoError(t, err, "fingerprinting should not fail")

	assert.NotEqual(t, fp1, fp2, "different attribute keys should produce different fingerprints")
}

func TestComputeLogsFingerprintSameKeysDifferentValues(t *testing.T) {
	// Same keys with different values should produce same fingerprint (values not hashed)
	message := "test"
	level := "info"
	row1 := pipeline.Row{
		wkk.NewRowKey("attr"): "value_a",
	}

	row2 := pipeline.Row{
		wkk.NewRowKey("attr"): "value_b",
	}

	// Create test fixtures
	tenantMgr := fingerprint.NewTenantManager(0.8)
	fp := fingerprinter.NewFingerprinter()
	orgID := "test-org-id"

	fp1, err := computeLogsFingerprint(orgID, message, level, row1, tenantMgr, fp)
	assert.NoError(t, err, "fingerprinting should not fail")
	fp2, err := computeLogsFingerprint(orgID, message, level, row2, tenantMgr, fp)
	assert.NoError(t, err, "fingerprinting should not fail")

	assert.Equal(t, fp1, fp2, "same keys with different values should produce same fingerprint")
}

func TestComputeLogsFingerprintDifferentKeys(t *testing.T) {
	// Different sets of keys should produce different fingerprints
	message := "Processing request"
	level := "info"
	row1 := pipeline.Row{
		wkk.NewRowKey("status"): "500",
	}

	row2 := pipeline.Row{
		wkk.NewRowKey("request_id"): "abc123",
	}

	// Create test fixtures
	tenantMgr := fingerprint.NewTenantManager(0.8)
	fp := fingerprinter.NewFingerprinter()
	orgID := "test-org-id"

	fp1, err := computeLogsFingerprint(orgID, message, level, row1, tenantMgr, fp)
	assert.NoError(t, err, "fingerprinting should not fail")
	fp2, err := computeLogsFingerprint(orgID, message, level, row2, tenantMgr, fp)
	assert.NoError(t, err, "fingerprinting should not fail")

	assert.NotEqual(t, fp1, fp2, "different sets of keys should produce different fingerprints")
}

func TestComputeLogsFingerprintValueTypes(t *testing.T) {
	// Test different value types
	message := "test"
	level := "debug"
	row := pipeline.Row{
		wkk.NewRowKey("int_val"):    42,
		wkk.NewRowKey("float_val"):  3.14,
		wkk.NewRowKey("bool_val"):   true,
		wkk.NewRowKey("string_val"): "text",
		wkk.NewRowKey("bytes_val"):  []byte("data"),
	}

	// Create test fixtures
	tenantMgr := fingerprint.NewTenantManager(0.8)
	fp := fingerprinter.NewFingerprinter()
	orgID := "test-org-id"

	result, err := computeLogsFingerprint(orgID, message, level, row, tenantMgr, fp)
	assert.NoError(t, err, "fingerprinting should not fail")
	assert.NotZero(t, result, "fingerprint should handle different value types")

	// Verify consistency with same types
	result2, err := computeLogsFingerprint(orgID, message, level, row, tenantMgr, fp)
	assert.NoError(t, err, "fingerprinting should not fail")
	assert.Equal(t, result, result2, "fingerprint should be consistent with mixed types")
}
