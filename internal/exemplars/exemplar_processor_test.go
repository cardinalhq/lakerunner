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

package exemplars

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestConvertLogsToMap(t *testing.T) {
	processor := NewProcessor(DefaultConfig())

	// Create a test log record
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test-service")
	rl.Resource().Attributes().PutStr("k8s.cluster.name", "test-cluster")

	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("test-scope")

	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("test log message")
	lr.Attributes().PutStr("level", "info")
	lr.SetTimestamp(1234567890000000000)

	// Convert to map
	result, err := processor.convertLogsToMap(logs)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify the result is a proper map structure
	assert.IsType(t, map[string]any{}, result)

	// Verify it contains expected top-level structure
	resourceLogs, ok := result["resourceLogs"]
	require.True(t, ok, "should contain resourceLogs")
	assert.NotNil(t, resourceLogs)

	// Verify it's a slice of resource logs
	resourceLogsSlice, ok := resourceLogs.([]any)
	require.True(t, ok, "resourceLogs should be a slice")
	require.Len(t, resourceLogsSlice, 1)

	// Verify resource attributes are present
	rl0, ok := resourceLogsSlice[0].(map[string]any)
	require.True(t, ok)

	resource, ok := rl0["resource"].(map[string]any)
	require.True(t, ok)

	attributes, ok := resource["attributes"].([]any)
	require.True(t, ok)
	require.Len(t, attributes, 2) // service.name and k8s.cluster.name

	// Find service.name attribute
	var serviceNameAttr map[string]any
	for _, attr := range attributes {
		attrMap := attr.(map[string]any)
		if attrMap["key"] == "service.name" {
			serviceNameAttr = attrMap
			break
		}
	}
	require.NotNil(t, serviceNameAttr)

	value, ok := serviceNameAttr["value"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "test-service", value["stringValue"])
}

func TestConvertMetricsToMap(t *testing.T) {
	processor := NewProcessor(DefaultConfig())

	// Create a test metric
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test-service")
	rm.Resource().Attributes().PutStr("k8s.cluster.name", "test-cluster")

	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("test-scope")

	m := sm.Metrics().AppendEmpty()
	m.SetName("test_counter")
	m.SetDescription("A test counter metric")
	m.SetUnit("1")

	// Create a sum metric with data point
	sum := m.SetEmptySum()
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	sum.SetIsMonotonic(true)

	dp := sum.DataPoints().AppendEmpty()
	dp.SetIntValue(42)
	dp.SetTimestamp(1234567890000000000)
	dp.Attributes().PutStr("label1", "value1")

	// Convert to map
	result, err := processor.convertMetricsToMap(metrics)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify the result is a proper map structure
	assert.IsType(t, map[string]any{}, result)

	// Verify it contains expected top-level structure
	resourceMetrics, ok := result["resourceMetrics"]
	require.True(t, ok, "should contain resourceMetrics")
	assert.NotNil(t, resourceMetrics)

	// Verify it's a slice of resource metrics
	resourceMetricsSlice, ok := resourceMetrics.([]any)
	require.True(t, ok, "resourceMetrics should be a slice")
	require.Len(t, resourceMetricsSlice, 1)

	// Verify resource attributes are present
	rm0, ok := resourceMetricsSlice[0].(map[string]any)
	require.True(t, ok)

	resource, ok := rm0["resource"].(map[string]any)
	require.True(t, ok)

	attributes, ok := resource["attributes"].([]any)
	require.True(t, ok)
	require.Len(t, attributes, 2) // service.name and k8s.cluster.name

	// Find service.name attribute
	var serviceNameAttr map[string]any
	for _, attr := range attributes {
		attrMap := attr.(map[string]any)
		if attrMap["key"] == "service.name" {
			serviceNameAttr = attrMap
			break
		}
	}
	require.NotNil(t, serviceNameAttr)

	value, ok := serviceNameAttr["value"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "test-service", value["stringValue"])

	// Verify metrics structure
	scopeMetrics, ok := rm0["scopeMetrics"].([]any)
	require.True(t, ok)
	require.Len(t, scopeMetrics, 1)

	sm0, ok := scopeMetrics[0].(map[string]any)
	require.True(t, ok)

	metricsArray, ok := sm0["metrics"].([]any)
	require.True(t, ok)
	require.Len(t, metricsArray, 1)

	metric0, ok := metricsArray[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "test_counter", metric0["name"])
	assert.Equal(t, "A test counter metric", metric0["description"])
	assert.Equal(t, "1", metric0["unit"])
}

func TestConvertEmptyLogsToMap(t *testing.T) {
	processor := NewProcessor(DefaultConfig())

	// Create empty logs
	logs := plog.NewLogs()

	// Convert to map
	result, err := processor.convertLogsToMap(logs)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should be empty structure but valid JSON
	assert.IsType(t, map[string]any{}, result)
	// Empty logs just return an empty JSON object
	assert.Len(t, result, 0, "Empty logs should result in empty map")
}

func TestConvertEmptyMetricsToMap(t *testing.T) {
	processor := NewProcessor(DefaultConfig())

	// Create empty metrics
	metrics := pmetric.NewMetrics()

	// Convert to map
	result, err := processor.convertMetricsToMap(metrics)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Should be empty structure but valid JSON
	assert.IsType(t, map[string]any{}, result)
	// Empty metrics just return an empty JSON object
	assert.Len(t, result, 0, "Empty metrics should result in empty map")
}
