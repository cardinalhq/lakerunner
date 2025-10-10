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

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func TestCalculateSpanFingerprintFromRow(t *testing.T) {
	tests := []struct {
		name     string
		row      pipeline.Row
		expected int64 // We'll check that it's non-zero and consistent
		desc     string
	}{
		{
			name: "basic span with service info",
			row: pipeline.Row{
				wkk.RowKeyResourceK8sClusterName:   "test-cluster",
				wkk.RowKeyResourceK8sNamespaceName: "test-namespace",
				wkk.RowKeyResourceServiceName:      "test-service",
				wkk.RowKeySpanKind:                 "SPAN_KIND_SERVER",
				wkk.RowKeySpanName:                 "GET /api/users",
			},
			desc: "should generate fingerprint with resource and span info",
		},
		{
			name: "http span",
			row: pipeline.Row{
				wkk.RowKeyResourceK8sClusterName:   "prod-cluster",
				wkk.RowKeyResourceK8sNamespaceName: "prod-namespace",
				wkk.RowKeyResourceServiceName:      "api-service",
				wkk.RowKeySpanKind:                 "SPAN_KIND_CLIENT",
				wkk.RowKeyAttrHTTPRequestMethod:    "POST",
				wkk.RowKeyAttrURLTemplate:          "/api/users/{id}",
			},
			desc: "should generate fingerprint with HTTP attributes",
		},
		{
			name: "database span",
			row: pipeline.Row{
				wkk.RowKeyResourceK8sClusterName:   "prod-cluster",
				wkk.RowKeyResourceK8sNamespaceName: "prod-namespace",
				wkk.RowKeyResourceServiceName:      "db-service",
				wkk.RowKeySpanKind:                 "SPAN_KIND_CLIENT",
				wkk.RowKeySpanName:                 "SELECT FROM users",
				wkk.RowKeyAttrDBSystemName:         "postgresql",
				wkk.RowKeyAttrDBNamespace:          "mydb",
				wkk.RowKeyAttrDBOperationName:      "SELECT",
				wkk.RowKeyAttrServerAddress:        "postgres.svc.cluster.local",
			},
			desc: "should generate fingerprint with database attributes",
		},
		{
			name: "messaging span",
			row: pipeline.Row{
				wkk.RowKeyResourceK8sClusterName:       "prod-cluster",
				wkk.RowKeyResourceK8sNamespaceName:     "prod-namespace",
				wkk.RowKeyResourceServiceName:          "messaging-service",
				wkk.RowKeySpanKind:                     "SPAN_KIND_PRODUCER",
				wkk.RowKeyAttrMessagingSystem:          "kafka",
				wkk.RowKeyAttrMessagingOperationType:   "publish",
				wkk.RowKeyAttrMessagingDestinationName: "events-topic",
			},
			desc: "should generate fingerprint with messaging attributes",
		},
		{
			name: "span with missing resource attributes",
			row: pipeline.Row{
				wkk.RowKeySpanKind: "SPAN_KIND_INTERNAL",
				wkk.RowKeySpanName: "processRequest",
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
	httpSpan := pipeline.Row{
		wkk.RowKeyResourceK8sClusterName:   "cluster",
		wkk.RowKeyResourceK8sNamespaceName: "namespace",
		wkk.RowKeyResourceServiceName:      "service",
		wkk.RowKeySpanKind:                 "SPAN_KIND_CLIENT",
		wkk.RowKeyAttrHTTPRequestMethod:    "GET",
		wkk.RowKeyAttrURLTemplate:          "/api/users",
	}

	dbSpan := pipeline.Row{
		wkk.RowKeyResourceK8sClusterName:   "cluster",
		wkk.RowKeyResourceK8sNamespaceName: "namespace",
		wkk.RowKeyResourceServiceName:      "service",
		wkk.RowKeySpanKind:                 "SPAN_KIND_CLIENT",
		wkk.RowKeySpanName:                 "SELECT",
		wkk.RowKeyAttrDBSystemName:         "postgresql",
	}

	httpFingerprint := CalculateSpanFingerprintFromRow(httpSpan)
	dbFingerprint := CalculateSpanFingerprintFromRow(dbSpan)

	assert.NotEqual(t, httpFingerprint, dbFingerprint, "HTTP and DB spans should have different fingerprints")
}
