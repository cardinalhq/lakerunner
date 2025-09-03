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

package ingestion

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func TestMetricTranslator(t *testing.T) {
	translator := &MetricTranslator{
		OrgID:    "test-org",
		Bucket:   "test-bucket",
		ObjectID: "metrics/test.json.gz",
	}

	row := filereader.Row{
		wkk.RowKeyCName:                              "cpu.usage",
		wkk.RowKeyCTimestamp:                         int64(1756049235874),
		wkk.NewRowKey("resource.service.name"):       "web-server-1",
		wkk.NewRowKey("resource.k8s.namespace.name"): "default",
		wkk.NewRowKey("resource.should.be.filtered"): "value",
	}

	err := translator.TranslateRow(&row)
	require.NoError(t, err)
	require.Equal(t, "test-org", row[wkk.RowKeyCCustomerID])
	require.Equal(t, "metrics", row[wkk.RowKeyCTelemetryType])
	require.Equal(t, "cpu.usage", row[wkk.RowKeyCName])                            // Original field preserved
	require.Equal(t, "web-server-1", row[wkk.NewRowKey("resource.service.name")])  // Kept attribute
	require.Equal(t, "default", row[wkk.NewRowKey("resource.k8s.namespace.name")]) // Kept attribute
	require.Nil(t, row[wkk.NewRowKey("resource.should.be.filtered")])              // Filtered attribute
	require.Equal(t, int64(1756049230000), row[wkk.RowKeyCTimestamp])              // Timestamp truncated to 10s boundary

	// Check that TID was computed and added
	tid, ok := row[wkk.RowKeyCTID].(int64)
	require.True(t, ok, "TID should be computed and added as int64")
	require.NotZero(t, tid, "TID should be non-zero")
}

func TestMetricTranslator_TimestampTruncation(t *testing.T) {
	translator := &MetricTranslator{
		OrgID:    "test-org",
		Bucket:   "test-bucket",
		ObjectID: "metrics/test.json.gz",
	}

	testCases := []struct {
		name              string
		inputTimestamp    int64
		expectedTruncated int64
	}{
		{
			name:              "exact 10s boundary",
			inputTimestamp:    1640995200000, // 2022-01-01 00:00:00.000
			expectedTruncated: 1640995200000,
		},
		{
			name:              "needs truncation down",
			inputTimestamp:    1640995203456, // 2022-01-01 00:00:03.456
			expectedTruncated: 1640995200000, // 2022-01-01 00:00:00.000
		},
		{
			name:              "needs truncation from 9.999s",
			inputTimestamp:    1640995209999, // 2022-01-01 00:00:09.999
			expectedTruncated: 1640995200000, // 2022-01-01 00:00:00.000
		},
		{
			name:              "next 10s boundary",
			inputTimestamp:    1640995210000, // 2022-01-01 00:00:10.000
			expectedTruncated: 1640995210000,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			row := filereader.Row{
				wkk.RowKeyCName:      "test.metric",
				wkk.RowKeyCTimestamp: tc.inputTimestamp,
			}

			err := translator.TranslateRow(&row)
			require.NoError(t, err)
			require.Equal(t, tc.expectedTruncated, row[wkk.RowKeyCTimestamp])
		})
	}
}

func TestMetricTranslator_AttributeFiltering(t *testing.T) {
	translator := &MetricTranslator{
		OrgID:    "test-org",
		Bucket:   "test-bucket",
		ObjectID: "metrics/test.json.gz",
	}

	row := filereader.Row{
		wkk.RowKeyCName:      "test.metric",
		wkk.RowKeyCTimestamp: int64(1640995200000),
		// Resource attributes that should be kept
		wkk.NewRowKey("resource.app"):                  "myapp",
		wkk.NewRowKey("resource.container.image.name"): "nginx",
		wkk.NewRowKey("resource.container.image.tag"):  "latest",
		wkk.NewRowKey("resource.k8s.cluster.name"):     "prod-cluster",
		wkk.NewRowKey("resource.k8s.daemonset.name"):   "fluentd",
		wkk.NewRowKey("resource.k8s.deployment.name"):  "web-app",
		wkk.NewRowKey("resource.k8s.namespace.name"):   "default",
		wkk.NewRowKey("resource.k8s.pod.ip"):           "10.0.0.1",
		wkk.NewRowKey("resource.k8s.pod.name"):         "web-app-123",
		wkk.NewRowKey("resource.k8s.statefulset.name"): "database",
		wkk.NewRowKey("resource.service.name"):         "frontend",
		wkk.NewRowKey("resource.service.version"):      "v1.2.3",
		// Resource attributes that should be filtered
		wkk.NewRowKey("resource.k8s.pod.uid"):    "abc-123-def",
		wkk.NewRowKey("resource.k8s.node.name"):  "node-1",
		wkk.NewRowKey("resource.process.pid"):    "12345",
		wkk.NewRowKey("resource.host.name"):      "server-1",
		wkk.NewRowKey("resource.cloud.provider"): "aws",
		wkk.NewRowKey("resource.cloud.region"):   "us-east-1",
		// Non-resource attributes (should not be touched)
		wkk.NewRowKey("metric.unit"):  "bytes",
		wkk.NewRowKey("scope.name"):   "io.opentelemetry",
		wkk.NewRowKey("custom.field"): "value",
	}

	err := translator.TranslateRow(&row)
	require.NoError(t, err)

	// Check that kept resource attributes are still present
	require.Equal(t, "myapp", row[wkk.NewRowKey("resource.app")])
	require.Equal(t, "nginx", row[wkk.NewRowKey("resource.container.image.name")])
	require.Equal(t, "latest", row[wkk.NewRowKey("resource.container.image.tag")])
	require.Equal(t, "prod-cluster", row[wkk.NewRowKey("resource.k8s.cluster.name")])
	require.Equal(t, "fluentd", row[wkk.NewRowKey("resource.k8s.daemonset.name")])
	require.Equal(t, "web-app", row[wkk.NewRowKey("resource.k8s.deployment.name")])
	require.Equal(t, "default", row[wkk.NewRowKey("resource.k8s.namespace.name")])
	require.Equal(t, "10.0.0.1", row[wkk.NewRowKey("resource.k8s.pod.ip")])
	require.Equal(t, "web-app-123", row[wkk.NewRowKey("resource.k8s.pod.name")])
	require.Equal(t, "database", row[wkk.NewRowKey("resource.k8s.statefulset.name")])
	require.Equal(t, "frontend", row[wkk.NewRowKey("resource.service.name")])
	require.Equal(t, "v1.2.3", row[wkk.NewRowKey("resource.service.version")])

	// Check that filtered resource attributes were removed
	require.Nil(t, row[wkk.NewRowKey("resource.k8s.pod.uid")])
	require.Nil(t, row[wkk.NewRowKey("resource.k8s.node.name")])
	require.Nil(t, row[wkk.NewRowKey("resource.process.pid")])
	require.Nil(t, row[wkk.NewRowKey("resource.host.name")])
	require.Nil(t, row[wkk.NewRowKey("resource.cloud.provider")])
	require.Nil(t, row[wkk.NewRowKey("resource.cloud.region")])

	// Check that non-resource attributes were not touched
	require.Equal(t, "bytes", row[wkk.NewRowKey("metric.unit")])
	require.Equal(t, "io.opentelemetry", row[wkk.NewRowKey("scope.name")])
	require.Equal(t, "value", row[wkk.NewRowKey("custom.field")])
}
