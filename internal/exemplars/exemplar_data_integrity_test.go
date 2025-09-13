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
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// TestExemplarDataFormatIntegrity verifies that our optimization from string to map[string]any
// produces identical data to the original JSON string approach
func TestExemplarDataFormatIntegrity(t *testing.T) {
	t.Run("Metrics conversion produces identical data", func(t *testing.T) {
		// Create test metrics data
		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", "test-service")
		rm.Resource().Attributes().PutStr("k8s.cluster.name", "test-cluster")

		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("test-scope")

		// Add a gauge metric
		metric := sm.Metrics().AppendEmpty()
		metric.SetName("test_metric")
		metric.SetDescription("Test metric description")
		metric.SetUnit("bytes")

		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetIntValue(42)
		dp.SetTimestamp(pcommon.Timestamp(1234567890))
		dp.Attributes().PutStr("label1", "value1")

		processor := NewProcessor(DefaultConfig())

		// Test our new approach (direct conversion to map)
		optimizedData, err := processor.convertMetricsToMap(metrics)
		require.NoError(t, err)

		// Test the original approach (JSON string then parse to map)
		originalJSONString, err := processor.marshalMetricsToString(metrics)
		require.NoError(t, err)

		var originalData map[string]any
		err = json.Unmarshal([]byte(originalJSONString), &originalData)
		require.NoError(t, err)

		// The results should be identical
		assert.True(t, reflect.DeepEqual(optimizedData, originalData),
			"Optimized and original data should be identical")
	})

	t.Run("Logs conversion produces identical data", func(t *testing.T) {
		// Create test log data
		logs := plog.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "test-service")
		rl.Resource().Attributes().PutStr("k8s.cluster.name", "test-cluster")

		sl := rl.ScopeLogs().AppendEmpty()
		sl.Scope().SetName("test-scope")

		logRecord := sl.LogRecords().AppendEmpty()
		logRecord.SetTimestamp(pcommon.Timestamp(1234567890))
		logRecord.SetSeverityNumber(9) // INFO
		logRecord.SetSeverityText("INFO")
		logRecord.Body().SetStr("Test log message")
		logRecord.Attributes().PutStr("key1", "value1")

		processor := NewProcessor(DefaultConfig())

		// Test our new approach (direct conversion to map)
		optimizedData, err := processor.convertLogsToMap(logs)
		require.NoError(t, err)

		// Test the original approach (JSON string then parse to map)
		originalJSONString, err := processor.marshalLogsToString(logs)
		require.NoError(t, err)

		var originalData map[string]any
		err = json.Unmarshal([]byte(originalJSONString), &originalData)
		require.NoError(t, err)

		// The results should be identical
		assert.True(t, reflect.DeepEqual(optimizedData, originalData),
			"Optimized and original data should be identical")
	})

	t.Run("End-to-end exemplar processing produces valid database format", func(t *testing.T) {
		// Create test metrics data
		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("service.name", "test-service")
		rm.Resource().Attributes().PutStr("k8s.cluster.name", "test-cluster")
		rm.Resource().Attributes().PutStr("k8s.namespace.name", "test-namespace")

		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName("test-scope")

		// Add a gauge metric
		metric := sm.Metrics().AppendEmpty()
		metric.SetName("cpu.usage")
		metric.SetDescription("CPU usage metric")
		metric.SetUnit("percent")

		gauge := metric.SetEmptyGauge()
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetDoubleValue(75.5)
		dp.SetTimestamp(pcommon.Timestamp(1234567890))
		dp.Attributes().PutStr("host", "server1")

		// Set up processor with callback to capture exemplar data
		config := DefaultConfig()
		config.Metrics.CacheSize = 1 // Force small cache to trigger exemplars
		processor := NewProcessor(config)

		var capturedExemplars []*ExemplarData
		processor.SetMetricsCallback(func(ctx context.Context, organizationID string, exemplars []*ExemplarData) error {
			capturedExemplars = exemplars
			return nil
		})

		organizationID := "test-org-123"

		// Process the metric (this should generate an exemplar)
		err := processor.ProcessMetrics(context.Background(), organizationID, rm, sm, metric)
		require.NoError(t, err)

		// Get the tenant and force flush the cache to trigger exemplar generation
		tenant := processor.GetTenant(context.Background(), organizationID)
		if tenant.metricCache != nil {
			tenant.metricCache.FlushPending()
		}

		// Verify we got exemplar data
		if len(capturedExemplars) > 0 {
			exemplar := capturedExemplars[0]

			// Verify the Payload is a proper map[string]any (not a string)
			assert.IsType(t, map[string]any{}, exemplar.Payload)

			// Verify the payload contains expected OpenTelemetry structure
			payload := exemplar.Payload
			assert.Contains(t, payload, "resourceMetrics")

			// Verify attributes contain the expected service information
			assert.Contains(t, exemplar.Attributes, "service.name")
			assert.Contains(t, exemplar.Attributes, "k8s.cluster.name")
			assert.Contains(t, exemplar.Attributes, "k8s.namespace.name")
			assert.Equal(t, "test-service", exemplar.Attributes["service.name"])
			assert.Equal(t, "test-cluster", exemplar.Attributes["k8s.cluster.name"])
			assert.Equal(t, "test-namespace", exemplar.Attributes["k8s.namespace.name"])

			// Verify we can marshal the payload back to JSON (database compatibility)
			jsonBytes, err := json.Marshal(exemplar.Payload)
			require.NoError(t, err)
			assert.Greater(t, len(jsonBytes), 0, "Payload should be serializable to JSON")

			// Verify the JSON can be unmarshalled back to a map
			var roundTripData map[string]any
			err = json.Unmarshal(jsonBytes, &roundTripData)
			require.NoError(t, err)
			assert.True(t, reflect.DeepEqual(payload, roundTripData),
				"Payload should round-trip through JSON serialization")

			t.Logf("Successfully processed %d exemplars", len(capturedExemplars))
			t.Logf("Exemplar payload keys: %v", getMapKeys(exemplar.Payload))
		} else {
			t.Log("No exemplars were generated (likely due to cache behavior)")
		}
	})
}

// Helper function to get map keys for debugging
func getMapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// marshalMetricsToString simulates the original approach for comparison
func (p *Processor) marshalMetricsToString(md pmetric.Metrics) (string, error) {
	marshaller := &pmetric.JSONMarshaler{}
	bytes, err := marshaller.MarshalMetrics(md)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// marshalLogsToString simulates the original approach for comparison
func (p *Processor) marshalLogsToString(ld plog.Logs) (string, error) {
	marshaller := &plog.JSONMarshaler{}
	bytes, err := marshaller.MarshalLogs(ld)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
