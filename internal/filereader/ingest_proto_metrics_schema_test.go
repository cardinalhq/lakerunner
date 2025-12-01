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
	"compress/gzip"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/cardinalhq/oteltools/signalbuilder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// gzipOpen opens a gzip file and returns an io.ReadCloser
func gzipOpen(path string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &gzipReadCloser{gz: gz, file: f}, nil
}

type gzipReadCloser struct {
	gz   *gzip.Reader
	file *os.File
}

func (g *gzipReadCloser) Read(p []byte) (int, error) {
	return g.gz.Read(p)
}

func (g *gzipReadCloser) Close() error {
	_ = g.gz.Close()
	return g.file.Close()
}

// TestIngestProtoMetricsReader_SchemaExtraction_BasicTypes tests that the schema extractor
// correctly identifies different attribute types across all metrics.
func TestIngestProtoMetricsReader_SchemaExtraction_BasicTypes(t *testing.T) {
	builder := signalbuilder.NewMetricsBuilder()

	resourceMetrics := &signalbuilder.ResourceMetrics{
		Resource: map[string]any{
			"service.name": "test-service",
		},
		ScopeMetrics: []signalbuilder.ScopeMetrics{
			{
				Name: "test-meter",
				Metrics: []signalbuilder.Metric{
					{
						Name: "test.gauge",
						Type: "gauge",
						Gauge: &signalbuilder.GaugeMetric{
							DataPoints: []signalbuilder.NumberDataPoint{
								{
									Timestamp: time.Now().UnixNano(),
									Value:     42.0,
									Attributes: map[string]any{
										"string.attr": "hello",
										"int.attr":    int64(42),
										"float.attr":  3.14,
										"bool.attr":   true,
									},
								},
							},
						},
					},
				},
			},
		},
	}

	err := builder.Add(resourceMetrics)
	require.NoError(t, err)

	metrics := builder.Build()
	marshaler := &pmetric.ProtoMarshaler{}
	data, err := marshaler.MarshalMetrics(metrics)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	protoReader, err := NewIngestProtoMetricsReader(reader, ReaderOptions{OrgID: "1", BatchSize: 1000})
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	schema := protoReader.GetSchema()
	require.NotNil(t, schema)

	// Verify core metric fields
	assert.Equal(t, DataTypeString, schema.GetColumnType("metric_name"))
	assert.Equal(t, DataTypeInt64, schema.GetColumnType("chq_tid"))
	assert.Equal(t, DataTypeInt64, schema.GetColumnType("chq_timestamp"))
	assert.Equal(t, DataTypeString, schema.GetColumnType("chq_metric_type"))

	// Verify rollup fields
	assert.Equal(t, DataTypeBytes, schema.GetColumnType("chq_sketch"))
	assert.Equal(t, DataTypeFloat64, schema.GetColumnType("chq_rollup_avg"))
	assert.Equal(t, DataTypeFloat64, schema.GetColumnType("chq_rollup_max"))
	assert.Equal(t, DataTypeFloat64, schema.GetColumnType("chq_rollup_min"))
	assert.Equal(t, DataTypeFloat64, schema.GetColumnType("chq_rollup_count"))
	assert.Equal(t, DataTypeFloat64, schema.GetColumnType("chq_rollup_sum"))
	assert.Equal(t, DataTypeFloat64, schema.GetColumnType("chq_rollup_p25"))
	assert.Equal(t, DataTypeFloat64, schema.GetColumnType("chq_rollup_p50"))
	assert.Equal(t, DataTypeFloat64, schema.GetColumnType("chq_rollup_p75"))
	assert.Equal(t, DataTypeFloat64, schema.GetColumnType("chq_rollup_p90"))
	assert.Equal(t, DataTypeFloat64, schema.GetColumnType("chq_rollup_p95"))
	assert.Equal(t, DataTypeFloat64, schema.GetColumnType("chq_rollup_p99"))

	// Verify attribute types are preserved
	assert.Equal(t, DataTypeString, schema.GetColumnType("resource_service_name"))
	assert.Equal(t, DataTypeString, schema.GetColumnType("attr_string_attr"))
	assert.Equal(t, DataTypeInt64, schema.GetColumnType("attr_int_attr"))
	assert.Equal(t, DataTypeFloat64, schema.GetColumnType("attr_float_attr"))
	// Booleans are converted to strings to avoid OTEL data type mismatch panics
	assert.Equal(t, DataTypeString, schema.GetColumnType("attr_bool_attr"))
}

// TestIngestProtoMetricsReader_SchemaExtraction_TypePromotion tests type promotion
// when the same attribute has different types across metrics.
func TestIngestProtoMetricsReader_SchemaExtraction_TypePromotion(t *testing.T) {
	builder := signalbuilder.NewMetricsBuilder()

	resourceMetrics := &signalbuilder.ResourceMetrics{
		Resource: map[string]any{
			"service.name": "test-service",
		},
		ScopeMetrics: []signalbuilder.ScopeMetrics{
			{
				Name: "test-meter",
				Metrics: []signalbuilder.Metric{
					{
						Name: "test.gauge1",
						Type: "gauge",
						Gauge: &signalbuilder.GaugeMetric{
							DataPoints: []signalbuilder.NumberDataPoint{
								{
									Timestamp: time.Now().UnixNano(),
									Value:     42.0,
									Attributes: map[string]any{
										"mixed.attr": int64(42), // int64 first
									},
								},
							},
						},
					},
					{
						Name: "test.gauge2",
						Type: "gauge",
						Gauge: &signalbuilder.GaugeMetric{
							DataPoints: []signalbuilder.NumberDataPoint{
								{
									Timestamp: time.Now().UnixNano(),
									Value:     43.0,
									Attributes: map[string]any{
										"mixed.attr": "string value", // string later - should promote to string
									},
								},
							},
						},
					},
				},
			},
		},
	}

	err := builder.Add(resourceMetrics)
	require.NoError(t, err)

	metrics := builder.Build()
	marshaler := &pmetric.ProtoMarshaler{}
	data, err := marshaler.MarshalMetrics(metrics)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	protoReader, err := NewIngestProtoMetricsReader(reader, ReaderOptions{OrgID: "1", BatchSize: 1000})
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	schema := protoReader.GetSchema()
	require.NotNil(t, schema)

	// Type should be promoted to string (int64 + string → string)
	assert.Equal(t, DataTypeString, schema.GetColumnType("attr_mixed_attr"))
}

// TestIngestProtoMetricsReader_SchemaExtraction_MultiResource tests schema extraction
// across multiple resources.
func TestIngestProtoMetricsReader_SchemaExtraction_MultiResource(t *testing.T) {
	builder := signalbuilder.NewMetricsBuilder()

	// First resource with specific attributes
	resourceMetrics1 := &signalbuilder.ResourceMetrics{
		Resource: map[string]any{
			"service.name": "service-1",
			"env":          "production",
		},
		ScopeMetrics: []signalbuilder.ScopeMetrics{
			{
				Metrics: []signalbuilder.Metric{
					{
						Name: "test.gauge1",
						Type: "gauge",
						Gauge: &signalbuilder.GaugeMetric{
							DataPoints: []signalbuilder.NumberDataPoint{
								{
									Timestamp: time.Now().UnixNano(),
									Value:     42.0,
									Attributes: map[string]any{
										"http.method": "GET",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Second resource with different attributes
	resourceMetrics2 := &signalbuilder.ResourceMetrics{
		Resource: map[string]any{
			"service.name": "service-2",
			"region":       "us-west-2",
		},
		ScopeMetrics: []signalbuilder.ScopeMetrics{
			{
				Metrics: []signalbuilder.Metric{
					{
						Name: "test.gauge2",
						Type: "gauge",
						Gauge: &signalbuilder.GaugeMetric{
							DataPoints: []signalbuilder.NumberDataPoint{
								{
									Timestamp: time.Now().UnixNano(),
									Value:     43.0,
									Attributes: map[string]any{
										"db.system": "postgresql",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	err := builder.Add(resourceMetrics1)
	require.NoError(t, err)
	err = builder.Add(resourceMetrics2)
	require.NoError(t, err)

	metrics := builder.Build()
	marshaler := &pmetric.ProtoMarshaler{}
	data, err := marshaler.MarshalMetrics(metrics)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	protoReader, err := NewIngestProtoMetricsReader(reader, ReaderOptions{OrgID: "1", BatchSize: 1000})
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	schema := protoReader.GetSchema()
	require.NotNil(t, schema)

	// Schema should include attributes from both resources
	assert.True(t, schema.HasColumn("resource_service_name"))
	assert.True(t, schema.HasColumn("resource_env"))
	assert.True(t, schema.HasColumn("resource_region"))
	assert.True(t, schema.HasColumn("attr_http_method"))
	assert.True(t, schema.HasColumn("attr_db_system"))
}

// TestIngestProtoMetricsReader_SchemaExtraction_MultipleMetricTypes tests schema extraction
// across different metric types (Gauge, Sum, Histogram, etc.).
func TestIngestProtoMetricsReader_SchemaExtraction_MultipleMetricTypes(t *testing.T) {
	builder := signalbuilder.NewMetricsBuilder()

	resourceMetrics := &signalbuilder.ResourceMetrics{
		Resource: map[string]any{
			"service.name": "test-service",
		},
		ScopeMetrics: []signalbuilder.ScopeMetrics{
			{
				Name: "test-meter",
				Metrics: []signalbuilder.Metric{
					{
						Name: "test.gauge",
						Type: "gauge",
						Gauge: &signalbuilder.GaugeMetric{
							DataPoints: []signalbuilder.NumberDataPoint{
								{
									Timestamp: time.Now().UnixNano(),
									Value:     42.0,
									Attributes: map[string]any{
										"gauge.attr": "gauge-value",
									},
								},
							},
						},
					},
					{
						Name: "test.sum",
						Type: "sum",
						Sum: &signalbuilder.SumMetric{
							DataPoints: []signalbuilder.NumberDataPoint{
								{
									Timestamp: time.Now().UnixNano(),
									Value:     100.0,
									Attributes: map[string]any{
										"sum.attr": "sum-value",
									},
								},
							},
						},
					},
					{
						Name: "test.histogram",
						Type: "histogram",
						Histogram: &signalbuilder.HistogramMetric{
							DataPoints: []signalbuilder.HistogramDataPoint{
								{
									Timestamp: time.Now().UnixNano(),
									Count:     10,
									Sum:       floatPtr(50.0),
									Attributes: map[string]any{
										"histogram.attr": "histogram-value",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	err := builder.Add(resourceMetrics)
	require.NoError(t, err)

	metrics := builder.Build()
	marshaler := &pmetric.ProtoMarshaler{}
	data, err := marshaler.MarshalMetrics(metrics)
	require.NoError(t, err)

	reader := bytes.NewReader(data)
	protoReader, err := NewIngestProtoMetricsReader(reader, ReaderOptions{OrgID: "1", BatchSize: 1000})
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	schema := protoReader.GetSchema()
	require.NotNil(t, schema)

	// Schema should include attributes from all metric types
	assert.True(t, schema.HasColumn("attr_gauge_attr"))
	assert.True(t, schema.HasColumn("attr_sum_attr"))
	assert.True(t, schema.HasColumn("attr_histogram_attr"))
}

// TestIngestProtoMetricsReader_RealFile_BoolField tests loading the actual metrics_with_bool_field.binpb.gz
// file to investigate boolean field handling issues.
func TestIngestProtoMetricsReader_RealFile_BoolField(t *testing.T) {
	file, err := gzipOpen("testdata/metrics_with_bool_field.binpb.gz")
	require.NoError(t, err)
	defer file.Close()

	protoReader, err := NewIngestProtoMetricsReader(file, ReaderOptions{OrgID: "test-org", BatchSize: 1000})
	require.NoError(t, err)
	defer func() { _ = protoReader.Close() }()

	schema := protoReader.GetSchema()
	require.NotNil(t, schema)

	// Print all columns and their types for inspection
	columns := schema.Columns()
	t.Logf("Schema has %d columns:", len(columns))
	for _, col := range columns {
		t.Logf("  %s: %v (hasNonNull=%v)", wkk.RowKeyValue(col.Name), col.DataType, col.HasNonNull)
	}

	// Verify that attr_success is now marked as string (not bool)
	successFound := false
	for _, col := range columns {
		if wkk.RowKeyValue(col.Name) == "attr_success" {
			successFound = true
			t.Logf("attr_success type in schema: %v", col.DataType)
			if col.DataType != DataTypeString {
				t.Errorf("Expected attr_success to be string type, got %v", col.DataType)
			}
			break
		}
	}

	if !successFound {
		t.Error("attr_success field not found in schema")
	}

	// Now read some actual rows to see the boolean values
	ctx := context.Background()
	batch, err := protoReader.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, batch)

	t.Logf("Read batch with %d rows", batch.Len())

	// Scan ALL rows to find ones with attr_success value and check types
	successKey := wkk.NewRowKey("attr_success")
	foundCount := 0
	wrongTypeCount := 0
	examplesPrinted := 0
	for i := range batch.Len() {
		row := batch.Get(i)
		if val, exists := row[successKey]; exists {
			foundCount++
			// Check if the actual type is string (as expected now)
			if _, isString := val.(string); !isString {
				wrongTypeCount++
				if examplesPrinted < 10 {
					t.Logf("Row %d: attr_success = %v (type: %T) - EXPECTED string, GOT %T", i, val, val, val)
					examplesPrinted++
				}
			} else if examplesPrinted < 5 {
				// Show first few string values
				t.Logf("Row %d: attr_success = %v (type: string) ✓", i, val)
				examplesPrinted++
			}
		}
	}

	t.Logf("Summary: Found %d rows with attr_success field", foundCount)
	if wrongTypeCount > 0 {
		t.Errorf("ERROR: %d rows have attr_success with WRONG TYPE (not string)", wrongTypeCount)
	} else if foundCount > 0 {
		t.Logf("SUCCESS: All %d rows with attr_success have correct string type", foundCount)
	} else {
		t.Log("WARNING: No rows found with attr_success field despite schema having it")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
