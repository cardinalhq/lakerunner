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

package filereader

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/cardinalhq/oteltools/signalbuilder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Helper function to create float64 pointer
func floatPtr(f float64) *float64 {
	return &f
}

// Helper function to create simple synthetic metrics data for basic tests
func createSimpleSyntheticMetrics() []byte {
	builder := signalbuilder.NewMetricsBuilder()
	resourceMetrics := &signalbuilder.ResourceMetrics{
		Resource: map[string]any{
			"service.name": "simple-test-service",
		},
		ScopeMetrics: []signalbuilder.ScopeMetrics{
			{
				Name: "simple-test-meter",
				Metrics: []signalbuilder.Metric{
					{
						Name: "test_gauge",
						Type: "gauge",
						Gauge: &signalbuilder.GaugeMetric{
							DataPoints: []signalbuilder.NumberDataPoint{
								{
									Value:     42.0,
									Timestamp: time.Now().UnixNano(),
								},
							},
						},
					},
				},
			},
		},
	}

	err := builder.Add(resourceMetrics)
	if err != nil {
		panic(err)
	}

	metrics := builder.Build()
	marshaler := &pmetric.ProtoMarshaler{}
	data, err := marshaler.MarshalMetrics(metrics)
	if err != nil {
		panic(err)
	}

	return data
}

func TestNewProtoMetricsReader_InvalidData(t *testing.T) {
	// Test with invalid protobuf data
	invalidData := []byte("not a protobuf")
	reader := bytes.NewReader(invalidData)

	_, err := NewProtoMetricsReader(reader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse proto to OTEL metrics")
}

func TestNewProtoMetricsReader_EmptyData(t *testing.T) {
	// Test with empty data
	emptyReader := bytes.NewReader([]byte{})

	reader, err := NewProtoMetricsReader(emptyReader)
	// Empty data may create a valid but empty metrics object
	if err != nil {
		assert.Contains(t, err.Error(), "failed to parse proto to OTEL metrics")
	} else {
		// If no error, should still be able to use the reader
		require.NotNil(t, reader)
		defer reader.Close()

		// Reading from empty metrics should return EOF immediately
		rows := make([]Row, 1)
		rows[0] = make(Row)
		n, readErr := reader.Read(rows)
		assert.Equal(t, 0, n)
		assert.True(t, errors.Is(readErr, io.EOF))
	}
}

func TestProtoMetricsReader_EmptySlice(t *testing.T) {
	syntheticData := createSimpleSyntheticMetrics()
	reader, err := NewProtoMetricsReader(bytes.NewReader(syntheticData))
	require.NoError(t, err)
	defer reader.Close()

	// Read with empty slice
	n, err := reader.Read([]Row{})
	assert.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestProtoMetricsReader_Close(t *testing.T) {
	syntheticData := createSimpleSyntheticMetrics()
	reader, err := NewProtoMetricsReader(bytes.NewReader(syntheticData))
	require.NoError(t, err)

	// Should be able to read before closing
	rows := make([]Row, 1)
	rows[0] = make(Row)
	n, err := reader.Read(rows)
	require.NoError(t, err)
	require.Equal(t, 1, n, "Should read exactly 1 datapoint row before closing")

	// Close should work
	err = reader.Close()
	assert.NoError(t, err)

	// Reading after close should return error
	rows[0] = make(Row)
	_, err = reader.Read(rows)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	// Close should be idempotent
	err = reader.Close()
	assert.NoError(t, err)
}

func TestProtoMetricsReader_ExponentialHistogram(t *testing.T) {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("eh_metric")
	eh := m.SetEmptyExponentialHistogram()
	dp := eh.DataPoints().AppendEmpty()

	ts := pcommon.NewTimestampFromTime(time.Unix(1, 0).UTC())
	dp.SetTimestamp(ts)
	dp.SetStartTimestamp(ts)
	dp.SetCount(6)
	dp.SetScale(1)
	dp.SetZeroCount(1)
	dp.SetSum(10)
	dp.SetMin(1)
	dp.SetMax(9)
	dp.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))

	pos := dp.Positive()
	pos.SetOffset(0)
	pos.BucketCounts().FromRaw([]uint64{1, 2})

	neg := dp.Negative()
	neg.SetOffset(-1)
	neg.BucketCounts().FromRaw([]uint64{3})

	ex := dp.Exemplars().AppendEmpty()
	ex.SetTimestamp(ts)
	ex.SetDoubleValue(5.5)
	ex.SetTraceID(pcommon.TraceID([16]byte{1}))
	ex.SetSpanID(pcommon.SpanID([8]byte{2}))
	ex.FilteredAttributes().PutStr("foo", "bar")

	reader, err := NewProtoMetricsReaderFromMetrics(&metrics)
	require.NoError(t, err)
	defer reader.Close()

	rows := make([]Row, 1)
	rows[0] = make(Row)
	n, err := reader.Read(rows)
	require.NoError(t, err)
	require.Equal(t, 1, n)

	row := rows[0]
	assert.Equal(t, uint64(6), row["count"])
	assert.Equal(t, int32(1), row["scale"])
	assert.Equal(t, uint64(1), row["zero_count"])
	assert.Equal(t, int32(0), row["positive_offset"])
	assert.Equal(t, int32(-1), row["negative_offset"])
	assert.Equal(t, []uint64{1, 2}, row["positive_bucket_counts"])
	assert.Equal(t, []uint64{3}, row["negative_bucket_counts"])
	assert.Equal(t, uint64(3), row["negative_count"])
	assert.Equal(t, uint64(3), row["positive_count"])
	assert.Contains(t, row, "exemplars")
	exemplars, ok := row["exemplars"].([]map[string]any)
	require.True(t, ok)
	require.Len(t, exemplars, 1)
	assert.Equal(t, 5.5, exemplars[0]["value"])
	assert.Equal(t, "01000000000000000000000000000000", exemplars[0]["trace_id"])
	assert.Equal(t, "0200000000000000", exemplars[0]["span_id"])
	assert.Contains(t, row, "flags")
}

// Test parsing function directly with invalid data
func TestParseProtoToOtelMetrics_InvalidData(t *testing.T) {
	invalidData := bytes.NewReader([]byte("invalid protobuf data"))
	_, err := parseProtoToOtelMetrics(invalidData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unmarshal protobuf metrics")
}

// Test parsing function with read error
func TestParseProtoToOtelMetrics_ReadError(t *testing.T) {
	errorReader := &errorReaderImpl{shouldError: true}
	_, err := parseProtoToOtelMetrics(errorReader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read data")
}

// errorReaderImpl is a test reader that can simulate read errors
type errorReaderImpl struct {
	shouldError bool
}

func (e *errorReaderImpl) Read(p []byte) (int, error) {
	if e.shouldError {
		return 0, errors.New("simulated read error")
	}
	return 0, io.EOF
}

// Test ProtoMetricsReader with comprehensive synthetic metric data
func TestProtoMetricsReader_SyntheticMultiTypeMetrics(t *testing.T) {
	builder := signalbuilder.NewMetricsBuilder()

	// Create comprehensive metrics with all major types
	resourceMetrics := &signalbuilder.ResourceMetrics{
		Resource: map[string]any{
			"service.name":    "synthetic-metrics-service",
			"service.version": "1.0.0",
			"host.name":       "test-host-01",
			"environment":     "testing",
		},
		ScopeMetrics: []signalbuilder.ScopeMetrics{
			{
				Name:    "synthetic-meter",
				Version: "1.0.0",
				Attributes: map[string]any{
					"meter.library": "synthetic",
				},
				Metrics: []signalbuilder.Metric{
					// Gauge metric
					{
						Name:        "cpu_usage_percent",
						Description: "Current CPU usage percentage",
						Unit:        "percent",
						Type:        "gauge",
						Gauge: &signalbuilder.GaugeMetric{
							DataPoints: []signalbuilder.NumberDataPoint{
								{
									Attributes: map[string]any{
										"cpu":  "cpu0",
										"host": "server-01",
									},
									Value:     85.5,
									Timestamp: time.Now().UnixNano(),
								},
								{
									Attributes: map[string]any{
										"cpu":  "cpu1",
										"host": "server-01",
									},
									Value:     72.3,
									Timestamp: time.Now().UnixNano(),
								},
							},
						},
					},
					// Sum metric (counter)
					{
						Name:        "http_requests_total",
						Description: "Total HTTP requests",
						Unit:        "1",
						Type:        "sum",
						Sum: &signalbuilder.SumMetric{
							IsMonotonic:            true,
							AggregationTemporality: "cumulative",
							DataPoints: []signalbuilder.NumberDataPoint{
								{
									Attributes: map[string]any{
										"method":   "GET",
										"status":   "200",
										"endpoint": "/api/users",
									},
									Value:     1250,
									Timestamp: time.Now().UnixNano(),
								},
								{
									Attributes: map[string]any{
										"method":   "POST",
										"status":   "201",
										"endpoint": "/api/users",
									},
									Value:     875,
									Timestamp: time.Now().UnixNano(),
								},
								{
									Attributes: map[string]any{
										"method":   "GET",
										"status":   "404",
										"endpoint": "/api/notfound",
									},
									Value:     45,
									Timestamp: time.Now().UnixNano(),
								},
							},
						},
					},
					// Histogram metric
					{
						Name:        "http_request_duration_seconds",
						Description: "HTTP request duration in seconds",
						Unit:        "s",
						Type:        "histogram",
						Histogram: &signalbuilder.HistogramMetric{
							AggregationTemporality: "cumulative",
							DataPoints: []signalbuilder.HistogramDataPoint{
								{
									Attributes: map[string]any{
										"method":   "GET",
										"endpoint": "/api/users",
									},
									Count:          100,
									Sum:            floatPtr(45.67),
									BucketCounts:   []uint64{10, 25, 35, 20, 8, 2},
									ExplicitBounds: []float64{0.1, 0.5, 1.0, 2.0, 5.0},
									Timestamp:      time.Now().UnixNano(),
								},
								{
									Attributes: map[string]any{
										"method":   "POST",
										"endpoint": "/api/orders",
									},
									Count:          75,
									Sum:            floatPtr(89.23),
									BucketCounts:   []uint64{5, 15, 25, 20, 10},
									ExplicitBounds: []float64{0.1, 0.5, 1.0, 2.0},
									Timestamp:      time.Now().UnixNano(),
								},
							},
						},
					},
				},
			},
		},
	}

	err := builder.Add(resourceMetrics)
	require.NoError(t, err, "Should successfully add synthetic metrics")

	// Build and test
	metrics := builder.Build()
	marshaler := &pmetric.ProtoMarshaler{}
	data, err := marshaler.MarshalMetrics(metrics)
	require.NoError(t, err, "Should successfully marshal metrics")

	reader := bytes.NewReader(data)
	protoReader, err := NewProtoMetricsReader(reader)
	require.NoError(t, err)
	require.NotNil(t, protoReader)
	defer protoReader.Close()

	// Read all rows
	allRows, err := readAllRows(protoReader)
	require.NoError(t, err)
	require.Equal(t, 7, len(allRows), "Should read exactly 7 datapoint rows (2 gauge + 3 sum + 2 histogram)")

	// Verify different metric types are present
	metricTypes := make(map[string]int)
	metricNames := make(map[string]int)

	for _, row := range allRows {
		if metricType, exists := row["_cardinalhq.metric_type"]; exists {
			if typeStr, ok := metricType.(string); ok {
				metricTypes[typeStr]++
			}
		}
		if metricName, exists := row["_cardinalhq.name"]; exists {
			if nameStr, ok := metricName.(string); ok {
				metricNames[nameStr]++
			}
		}

		// All rows should have resource attributes
		assert.Equal(t, "synthetic-metrics-service", row["resource.service.name"])
		assert.Equal(t, "1.0.0", row["resource.service.version"])
		assert.Equal(t, "test-host-01", row["resource.host.name"])
		assert.Equal(t, "testing", row["resource.environment"])

		// All rows should have scope attributes
		assert.Equal(t, "synthetic", row["scope.meter.library"])
	}

	// Verify metric type distribution
	assert.Equal(t, 2, metricTypes["gauge"], "Should have 2 Gauge datapoints")
	assert.Equal(t, 3, metricTypes["count"], "Should have 3 Sum datapoints")
	assert.Equal(t, 2, metricTypes["histogram"], "Should have 2 Histogram datapoints")

	// Verify metric name distribution
	assert.Equal(t, 2, metricNames["cpu_usage_percent"], "Should have 2 CPU usage datapoints")
	assert.Equal(t, 3, metricNames["http_requests_total"], "Should have 3 HTTP request datapoints")
	assert.Equal(t, 2, metricNames["http_request_duration_seconds"], "Should have 2 duration datapoints")

	// Test batched reading with a new reader instance
	protoReader2, err := NewProtoMetricsReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer protoReader2.Close()

	// Read in batches of 3
	var totalBatchedRows int
	batchSize := 3
	for {
		rows := make([]Row, batchSize)
		for i := range rows {
			rows[i] = make(Row)
		}

		n, readErr := protoReader2.Read(rows)
		totalBatchedRows += n

		// Verify each row that was read
		for i := 0; i < n; i++ {
			assert.Greater(t, len(rows[i]), 0, "Batched row %d should have data", i)
			assert.Contains(t, rows[i], "_cardinalhq.name")
			assert.Contains(t, rows[i], "_cardinalhq.metric_type")
		}

		if errors.Is(readErr, io.EOF) {
			break
		}
		require.NoError(t, readErr)
	}
	assert.Equal(t, len(allRows), totalBatchedRows, "Batched reading should read same number of rows")

	// Test single row reading
	protoReader3, err := NewProtoMetricsReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer protoReader3.Close()

	singleRows := make([]Row, 1)
	singleRows[0] = make(Row)
	n, err := protoReader3.Read(singleRows)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "Should read exactly 1 row")
	assert.Contains(t, singleRows[0], "_cardinalhq.name")
	assert.Contains(t, singleRows[0], "resource.service.name")

	// Test data exhaustion - continue reading until EOF
	var exhaustRows int
	for {
		rows := make([]Row, 2)
		for i := range rows {
			rows[i] = make(Row)
		}
		n, readErr := protoReader3.Read(rows)
		exhaustRows += n
		if errors.Is(readErr, io.EOF) {
			break
		}
		require.NoError(t, readErr)
	}
	// Should have read all remaining rows
	assert.Equal(t, len(allRows)-1, exhaustRows, "Should read all remaining rows after first single read")

	t.Logf("Successfully processed %d synthetic metric datapoints", len(allRows))
}

// Test ProtoMetricsReader with multi-resource synthetic data
func TestProtoMetricsReader_SyntheticMultiResourceMetrics(t *testing.T) {
	builder := signalbuilder.NewMetricsBuilder()

	// Add metrics from multiple services/resources
	services := []struct {
		name     string
		resource map[string]any
		metrics  []signalbuilder.Metric
	}{
		{
			name: "web-service",
			resource: map[string]any{
				"service.name":           "web-frontend",
				"service.version":        "2.1.0",
				"deployment.environment": "production",
			},
			metrics: []signalbuilder.Metric{
				{
					Name: "active_connections",
					Type: "gauge",
					Unit: "1",
					Gauge: &signalbuilder.GaugeMetric{
						DataPoints: []signalbuilder.NumberDataPoint{
							{
								Attributes: map[string]any{"port": "8080"},
								Value:      125,
								Timestamp:  time.Now().UnixNano(),
							},
						},
					},
				},
				{
					Name: "requests_per_second",
					Type: "sum",
					Unit: "1/s",
					Sum: &signalbuilder.SumMetric{
						IsMonotonic: false,
						DataPoints: []signalbuilder.NumberDataPoint{
							{
								Attributes: map[string]any{"handler": "api"},
								Value:      45.6,
								Timestamp:  time.Now().UnixNano(),
							},
						},
					},
				},
			},
		},
		{
			name: "database-service",
			resource: map[string]any{
				"service.name":           "postgres-db",
				"service.version":        "13.7",
				"deployment.environment": "production",
			},
			metrics: []signalbuilder.Metric{
				{
					Name: "query_duration_ms",
					Type: "histogram",
					Unit: "ms",
					Histogram: &signalbuilder.HistogramMetric{
						AggregationTemporality: "cumulative",
						DataPoints: []signalbuilder.HistogramDataPoint{
							{
								Attributes:     map[string]any{"table": "users"},
								Count:          1000,
								Sum:            floatPtr(15000.5),
								BucketCounts:   []uint64{100, 400, 350, 125, 25},
								ExplicitBounds: []float64{1, 10, 50, 100},
								Timestamp:      time.Now().UnixNano(),
							},
						},
					},
				},
			},
		},
	}

	// Add each service's metrics
	for _, service := range services {
		resourceMetrics := &signalbuilder.ResourceMetrics{
			Resource: service.resource,
			ScopeMetrics: []signalbuilder.ScopeMetrics{
				{
					Name:    service.name + "-meter",
					Version: "1.0.0",
					Metrics: service.metrics,
				},
			},
		}
		err := builder.Add(resourceMetrics)
		require.NoError(t, err, "Should add metrics for %s", service.name)
	}

	// Build and test
	metrics := builder.Build()
	marshaler := &pmetric.ProtoMarshaler{}
	data, err := marshaler.MarshalMetrics(metrics)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	protoReader, err := NewProtoMetricsReader(reader)
	require.NoError(t, err)
	defer protoReader.Close()

	// Should read metrics from both services (3 total datapoints)
	allRows, err := readAllRows(protoReader)
	require.NoError(t, err)
	require.Equal(t, 3, len(allRows), "Should read datapoints from both services")

	// Count datapoints by service
	webServiceCount := 0
	dbServiceCount := 0
	for _, row := range allRows {
		serviceName := row["resource.service.name"].(string)
		switch serviceName {
		case "web-frontend":
			webServiceCount++
		case "postgres-db":
			dbServiceCount++
		}
	}

	assert.Equal(t, 2, webServiceCount, "Should have 2 datapoints from web service")
	assert.Equal(t, 1, dbServiceCount, "Should have 1 datapoint from database service")

	t.Logf("Successfully processed %d datapoints from %d services", len(allRows), len(services))
}

// Test ProtoMetricsReader with edge case synthetic data
func TestProtoMetricsReader_SyntheticEdgeCases(t *testing.T) {
	builder := signalbuilder.NewMetricsBuilder()

	resourceMetrics := &signalbuilder.ResourceMetrics{
		Resource: map[string]any{
			"service.name": "edge-case-service",
		},
		ScopeMetrics: []signalbuilder.ScopeMetrics{
			{
				Name: "edge-case-meter",
				Metrics: []signalbuilder.Metric{
					// Metric with no attributes
					{
						Name: "simple_counter",
						Type: "sum",
						Sum: &signalbuilder.SumMetric{
							IsMonotonic: true,
							DataPoints: []signalbuilder.NumberDataPoint{
								{
									Value:     100,
									Timestamp: time.Now().UnixNano(),
									// No attributes - testing empty attributes
								},
							},
						},
					},
					// Metric with zero value
					{
						Name: "zero_gauge",
						Type: "gauge",
						Gauge: &signalbuilder.GaugeMetric{
							DataPoints: []signalbuilder.NumberDataPoint{
								{
									Attributes: map[string]any{
										"measurement": "idle",
									},
									Value:     0.0,
									Timestamp: time.Now().UnixNano(),
								},
							},
						},
					},
					// Histogram with single bucket
					{
						Name: "simple_histogram",
						Type: "histogram",
						Histogram: &signalbuilder.HistogramMetric{
							AggregationTemporality: "cumulative",
							DataPoints: []signalbuilder.HistogramDataPoint{
								{
									Attributes: map[string]any{
										"operation": "test",
									},
									Count:          5,
									Sum:            floatPtr(12.5),
									BucketCounts:   []uint64{5},
									ExplicitBounds: []float64{}, // Single bucket, no bounds
									Timestamp:      time.Now().UnixNano(),
								},
							},
						},
					},
				},
			},
		},
	}

	err := builder.Add(resourceMetrics)
	require.NoError(t, err, "Should successfully add edge case metrics")

	// Build and test
	metrics := builder.Build()
	marshaler := &pmetric.ProtoMarshaler{}
	data, err := marshaler.MarshalMetrics(metrics)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	protoReader, err := NewProtoMetricsReader(reader)
	require.NoError(t, err)
	defer protoReader.Close()

	// Should read all edge case datapoints
	allRows, err := readAllRows(protoReader)
	require.NoError(t, err)
	require.Equal(t, 3, len(allRows), "Should read 3 edge case datapoints")

	// Verify edge cases
	for i, row := range allRows {
		t.Run(fmt.Sprintf("edge_case_%d", i), func(t *testing.T) {
			// All should have resource attributes
			assert.Equal(t, "edge-case-service", row["resource.service.name"])

			metricName := row["_cardinalhq.name"].(string)
			switch metricName {
			case "simple_counter":
				// Should handle metrics with no attributes gracefully
				assert.Equal(t, "count", row["_cardinalhq.metric_type"])
				assert.Equal(t, 100.0, row["rollup_sum"])
			case "zero_gauge":
				// Should handle zero values
				assert.Equal(t, "gauge", row["_cardinalhq.metric_type"])
				assert.Equal(t, 0.0, row["rollup_sum"])
				assert.Equal(t, "idle", row["metric.measurement"])
			case "simple_histogram":
				// Should handle simple histograms
				assert.Equal(t, "histogram", row["_cardinalhq.metric_type"])
				assert.Equal(t, "test", row["metric.operation"])
			}
		})
	}

	t.Logf("Successfully processed %d edge case datapoints", len(allRows))
}
