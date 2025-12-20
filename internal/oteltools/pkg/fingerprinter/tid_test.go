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
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func TestComputeTID_NewBehavior(t *testing.T) {
	// Test that TID changes when specific fields change
	t.Run("TID changes with metric_name", func(t *testing.T) {
		tags1 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):   "metric1",
			wkk.NewRowKey("resource_host"): "server1",
		}
		tags2 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):   "metric2",
			wkk.NewRowKey("resource_host"): "server1",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.NotEqual(t, tid1, tid2, "TID should change when metric_name changes")
	})

	t.Run("TID changes with chq_metric_type", func(t *testing.T) {
		tags1 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):     "metric1",
			wkk.NewRowKey("chq_metric_type"): "gauge",
			wkk.NewRowKey("resource_host"):   "server1",
		}
		tags2 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):     "metric1",
			wkk.NewRowKey("chq_metric_type"): "counter",
			wkk.NewRowKey("resource_host"):   "server1",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.NotEqual(t, tid1, tid2, "TID should change when chq_metric_type changes")
	})

	t.Run("TID changes with resource.* fields", func(t *testing.T) {
		// Test adding a resource field
		tags1 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):   "metric1",
			wkk.NewRowKey("resource_host"): "server1",
		}
		tags2 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):     "metric1",
			wkk.NewRowKey("resource_host"):   "server1",
			wkk.NewRowKey("resource_region"): "us-east",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.NotEqual(t, tid1, tid2, "TID should change when resource field is added")

		// Test changing a resource field value
		tags3 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):   "metric1",
			wkk.NewRowKey("resource_host"): "server2",
		}
		tid3 := ComputeTID(tags3)
		assert.NotEqual(t, tid1, tid3, "TID should change when resource field value changes")

		// Test removing a resource field
		tags4 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"): "metric1",
		}
		tid4 := ComputeTID(tags4)
		assert.NotEqual(t, tid1, tid4, "TID should change when resource field is removed")
	})

	t.Run("TID changes with attr_* fields", func(t *testing.T) {
		// Test adding an attr field
		tags1 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"): "metric1",
			wkk.NewRowKey("attr_label1"): "value1",
		}
		tags2 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"): "metric1",
			wkk.NewRowKey("attr_label1"): "value1",
			wkk.NewRowKey("attr_label2"): "value2",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.NotEqual(t, tid1, tid2, "TID should change when attr field is added")

		// Test changing an attr field value
		tags3 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"): "metric1",
			wkk.NewRowKey("attr_label1"): "value3",
		}
		tid3 := ComputeTID(tags3)
		assert.NotEqual(t, tid1, tid3, "TID should change when attr field value changes")
	})

	t.Run("TID ignores non-string resource_* and attr_* fields", func(t *testing.T) {
		tags1 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):   "metric1",
			wkk.NewRowKey("resource_host"): "server1",
		}
		tags2 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):    "metric1",
			wkk.NewRowKey("resource_host"):  "server1",
			wkk.NewRowKey("resource_count"): 123,   // non-string, should be ignored
			wkk.NewRowKey("attr_value"):     45.67, // non-string, should be ignored
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.Equal(t, tid1, tid2, "TID should not change when non-string resource/attr fields are added")
	})

	t.Run("TID does not change with scope.* fields", func(t *testing.T) {
		tags1 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):   "metric1",
			wkk.NewRowKey("resource_host"): "server1",
		}
		tags2 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):   "metric1",
			wkk.NewRowKey("resource_host"): "server1",
			wkk.NewRowKey("scope_name"):    "my-scope",
			wkk.NewRowKey("scope_version"): "1.0.0",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.Equal(t, tid1, tid2, "TID should not change when scope fields are added")
	})

	t.Run("TID does not change with arbitrary fields", func(t *testing.T) {
		tags1 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):   "metric1",
			wkk.NewRowKey("resource_host"): "server1",
		}
		tags2 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):   "metric1",
			wkk.NewRowKey("resource_host"): "server1",
			wkk.NewRowKey("alice"):         "value",
			wkk.NewRowKey("bob"):           "another",
			wkk.NewRowKey("random_field"):  "ignored",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.Equal(t, tid1, tid2, "TID should not change when arbitrary fields are added")
	})

	t.Run("TID ignores other chq_* fields", func(t *testing.T) {
		tags1 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):     "metric1",
			wkk.NewRowKey("chq_metric_type"): "gauge",
			wkk.NewRowKey("resource_host"):   "server1",
		}
		tags2 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):     "metric1",
			wkk.NewRowKey("chq_metric_type"): "gauge",
			wkk.NewRowKey("resource_host"):   "server1",
			wkk.NewRowKey("chq_timestamp"):   123456789,
			wkk.NewRowKey("chq_description"): "some description",
			wkk.NewRowKey("chq_unit"):        "bytes",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.Equal(t, tid1, tid2, "TID should not change when other chq_* fields are added")
	})

	t.Run("TID is deterministic", func(t *testing.T) {
		tags := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):     "metric1",
			wkk.NewRowKey("chq_metric_type"): "gauge",
			wkk.NewRowKey("resource_host"):   "server1",
			wkk.NewRowKey("resource_region"): "us-east",
			wkk.NewRowKey("metric_label1"):   "value1",
			wkk.NewRowKey("metric_label2"):   "value2",
		}
		tid1 := ComputeTID(tags)
		tid2 := ComputeTID(tags)
		tid3 := ComputeTID(tags)
		assert.Equal(t, tid1, tid2, "TID should be deterministic")
		assert.Equal(t, tid1, tid3, "TID should be deterministic")
	})

	t.Run("Empty values are filtered", func(t *testing.T) {
		tags1 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):     "metric1",
			wkk.NewRowKey("resource_host"):   "server1",
			wkk.NewRowKey("resource_region"): "", // empty string should be filtered
		}
		tags2 := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):   "metric1",
			wkk.NewRowKey("resource_host"): "server1",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.Equal(t, tid1, tid2, "Empty string values should be filtered out")
	})
}

// TestComputeTIDFromOTEL_MatchesComputeTID verifies that ComputeTIDFromOTEL produces
// the same TID as ComputeTID when given equivalent data. This is critical for the
// SortingIngestProtoMetricsReader which uses ComputeTIDFromOTEL for sorting but
// ComputeTID for the final row.
func TestComputeTIDFromOTEL_MatchesComputeTID(t *testing.T) {
	testCases := []struct {
		name           string
		metricName     string
		metricType     pmetric.MetricType
		resourceAttrs  map[string]string
		datapointAttrs map[string]string
	}{
		{
			name:       "gauge with all KeepResourceKeys",
			metricName: "test_metric",
			metricType: pmetric.MetricTypeGauge,
			resourceAttrs: map[string]string{
				"service.name":    "my-service", // OTEL uses dots
				"service.version": "1.0.0",
				"k8s.pod.name":    "pod-123",
			},
			datapointAttrs: map[string]string{
				"http.method": "GET", // OTEL uses dots
				"http.status": "200",
			},
		},
		{
			name:       "sum/count metric",
			metricName: "http.requests.total",
			metricType: pmetric.MetricTypeSum,
			resourceAttrs: map[string]string{
				"service.name":        "api-gateway",
				"k8s.namespace.name":  "production",
				"k8s.deployment.name": "api-gateway-v2",
			},
			datapointAttrs: map[string]string{
				"endpoint": "/api/users",
				"code":     "200",
			},
		},
		{
			name:       "histogram metric",
			metricName: "request.duration",
			metricType: pmetric.MetricTypeHistogram,
			resourceAttrs: map[string]string{
				"service.name":     "backend",
				"k8s.cluster.name": "prod-us-east",
			},
			datapointAttrs: map[string]string{
				"operation": "query",
			},
		},
		{
			name:       "metric with dots in name (normalized to underscores)",
			metricName: "http.server.request.duration",
			metricType: pmetric.MetricTypeGauge,
			resourceAttrs: map[string]string{
				"service.name": "web",
			},
			datapointAttrs: map[string]string{},
		},
		{
			name:       "metric with no resource attrs (only allowed keys)",
			metricName: "simple_counter",
			metricType: pmetric.MetricTypeSum,
			resourceAttrs: map[string]string{
				"host.name": "server01", // Not in KeepResourceKeys, will be filtered
			},
			datapointAttrs: map[string]string{
				"label": "value",
			},
		},
		{
			name:       "metric with empty datapoint attrs",
			metricName: "mem_usage",
			metricType: pmetric.MetricTypeGauge,
			resourceAttrs: map[string]string{
				"service.name": "cache",
			},
			datapointAttrs: map[string]string{},
		},
		{
			name:           "minimal metric - name only",
			metricName:     "bare_metric",
			metricType:     pmetric.MetricTypeGauge,
			resourceAttrs:  map[string]string{},
			datapointAttrs: map[string]string{},
		},
		{
			name:       "underscore-prefixed datapoint attrs are excluded from TID",
			metricName: "stats_metric",
			metricType: pmetric.MetricTypeGauge,
			resourceAttrs: map[string]string{
				"service.name": "collector",
			},
			datapointAttrs: map[string]string{
				"_cardinalhq.collector_id": "chq-123",    // underscore-prefixed, excluded
				"_cardinalhq.customer_id":  "customer-1", // underscore-prefixed, excluded
				"normal.attr":              "value",      // normal, included
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Build OTEL structures
			metrics := pmetric.NewMetrics()
			rm := metrics.ResourceMetrics().AppendEmpty()
			for k, v := range tc.resourceAttrs {
				rm.Resource().Attributes().PutStr(k, v)
			}

			sm := rm.ScopeMetrics().AppendEmpty()
			metric := sm.Metrics().AppendEmpty()
			metric.SetName(tc.metricName)

			// Create a datapoint with attributes based on metric type
			var dpAttrs = pmetric.NewMetric()
			switch tc.metricType {
			case pmetric.MetricTypeGauge:
				metric.SetEmptyGauge()
				dp := metric.Gauge().DataPoints().AppendEmpty()
				for k, v := range tc.datapointAttrs {
					dp.Attributes().PutStr(k, v)
				}
				dpAttrs = metric
			case pmetric.MetricTypeSum:
				metric.SetEmptySum()
				dp := metric.Sum().DataPoints().AppendEmpty()
				for k, v := range tc.datapointAttrs {
					dp.Attributes().PutStr(k, v)
				}
				dpAttrs = metric
			case pmetric.MetricTypeHistogram:
				metric.SetEmptyHistogram()
				dp := metric.Histogram().DataPoints().AppendEmpty()
				for k, v := range tc.datapointAttrs {
					dp.Attributes().PutStr(k, v)
				}
				dpAttrs = metric
			}

			// Compute TID from OTEL
			dpAttrMap := GetDatapointAttributes(dpAttrs, 0)
			tidFromOTEL := ComputeTIDFromOTEL(
				rm.Resource().Attributes(),
				dpAttrMap,
				tc.metricName,
				tc.metricType,
			)

			// Build equivalent row for ComputeTID
			// This must exactly match how the real code builds rows from OTEL data
			row := make(map[wkk.RowKey]any)

			// Normalize metric name using canonical function
			row[wkk.NewRowKey("metric_name")] = wkk.NormalizeName(tc.metricName)

			// Add metric type as chq_metric_type
			row[wkk.NewRowKey("chq_metric_type")] = metricTypeToString(tc.metricType)

			// Add resource attributes (with "resource_" prefix, filtered by KeepResourceKeys)
			// Normalize the key BEFORE checking KeepResourceKeys (OTEL uses dots, we use underscores)
			for k, v := range tc.resourceAttrs {
				normalizedKey := wkk.NormalizeName(k)
				if KeepResourceKeys[normalizedKey] {
					row[wkk.NewRowKey("resource_"+normalizedKey)] = v
				}
			}

			// Add datapoint attributes (with "attr_" prefix, unless underscore-prefixed)
			// This matches the behavior of prefixAttributeRowKey in otel_attributes.go
			for k, v := range tc.datapointAttrs {
				normalizedKey := wkk.NormalizeName(k)
				if len(k) > 0 && k[0] == '_' {
					// Underscore-prefixed: no prefix, just normalized name
					row[wkk.NewRowKey(normalizedKey)] = v
				} else {
					row[wkk.NewRowKey("attr_"+normalizedKey)] = v
				}
			}

			// Compute TID from row
			tidFromRow := ComputeTID(row)

			// They must match!
			require.Equal(t, tidFromOTEL, tidFromRow,
				"ComputeTIDFromOTEL and ComputeTID must produce identical TIDs for equivalent data.\n"+
					"Metric: %s, Type: %v\n"+
					"Resource attrs: %v\n"+
					"DP attrs: %v\n"+
					"TID from OTEL: %d\n"+
					"TID from Row: %d",
				tc.metricName, tc.metricType, tc.resourceAttrs, tc.datapointAttrs, tidFromOTEL, tidFromRow)
		})
	}
}

// TestComputeTIDFromOTEL_ConsistencyWithFiltering verifies that ComputeTIDFromOTEL
// correctly filters resource keys the same way as the translator.
func TestComputeTIDFromOTEL_ConsistencyWithFiltering(t *testing.T) {
	// Test that non-KeepResourceKeys attributes don't affect TID
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()

	// Add a mix of kept and non-kept resource attributes
	rm.Resource().Attributes().PutStr("service_name", "test-svc")   // kept
	rm.Resource().Attributes().PutStr("host_name", "server01")      // NOT kept
	rm.Resource().Attributes().PutStr("k8s_pod_name", "pod-abc")    // kept
	rm.Resource().Attributes().PutStr("custom_label", "some-value") // NOT kept
	rm.Resource().Attributes().PutStr("process_pid", "12345")       // NOT kept

	sm := rm.ScopeMetrics().AppendEmpty()
	metric := sm.Metrics().AppendEmpty()
	metric.SetName("test_metric")
	metric.SetEmptyGauge()
	dp := metric.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("method", "GET")

	// Compute TID from OTEL
	tidFromOTEL := ComputeTIDFromOTEL(
		rm.Resource().Attributes(),
		dp.Attributes(),
		"test_metric",
		pmetric.MetricTypeGauge,
	)

	// Build row with ONLY kept resource keys
	row := make(map[wkk.RowKey]any)
	row[wkk.NewRowKey("metric_name")] = "test_metric"
	row[wkk.NewRowKey("chq_metric_type")] = "gauge"
	row[wkk.NewRowKey("resource_service_name")] = "test-svc"
	row[wkk.NewRowKey("resource_k8s_pod_name")] = "pod-abc"
	row[wkk.NewRowKey("attr_method")] = "GET"

	tidFromRow := ComputeTID(row)

	assert.Equal(t, tidFromOTEL, tidFromRow,
		"ComputeTIDFromOTEL should filter non-KeepResourceKeys the same way as the translator")
}
