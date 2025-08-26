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
	"fmt"
	"io"

	"github.com/DataDog/sketches-go/ddsketch"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// ProtoMetricsReader reads rows from OpenTelemetry protobuf metrics format.
type ProtoMetricsReader struct {
	closed   bool
	rowCount int64

	// Store the original OTEL metrics for exemplar processing
	otelMetrics *pmetric.Metrics

	// Streaming iterator state for metrics
	resourceIndex  int
	scopeIndex     int
	metricIndex    int
	datapointIndex int
}

// NewProtoMetricsReader creates a new ProtoMetricsReader for the given io.Reader.
// The caller is responsible for closing the underlying reader.
func NewProtoMetricsReader(reader io.Reader) (*ProtoMetricsReader, error) {
	metrics, err := parseProtoToOtelMetrics(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto to OTEL metrics: %w", err)
	}

	return NewProtoMetricsReaderFromMetrics(metrics)
}

// NewProtoMetricsReaderFromMetrics creates a new ProtoMetricsReader from pre-parsed OTEL metrics.
// This is useful when you need to access the raw OTEL structure for processing (e.g., exemplars)
// while also reading rows from the same data.
func NewProtoMetricsReaderFromMetrics(metrics *pmetric.Metrics) (*ProtoMetricsReader, error) {
	if metrics == nil {
		return nil, fmt.Errorf("metrics cannot be nil")
	}

	return &ProtoMetricsReader{
		otelMetrics: metrics,
	}, nil
}

// Read populates the provided slice with as many rows as possible.
func (r *ProtoMetricsReader) Read(rows []Row) (int, error) {
	if r.closed {
		return 0, fmt.Errorf("reader is closed")
	}

	if len(rows) == 0 {
		return 0, nil
	}

	n := 0
	for n < len(rows) {
		row, err := r.getMetricRow()
		if err != nil {
			return n, err
		}

		resetRow(&rows[n])

		// Copy data to Row
		for k, v := range row {
			rows[n][k] = v
		}

		n++
	}

	// Update row count with successfully read rows
	if n > 0 {
		r.rowCount += int64(n)
	}

	return n, nil
}

// getMetricRow handles reading the next datapoint row using streaming iteration.
func (r *ProtoMetricsReader) getMetricRow() (Row, error) {
	if r.otelMetrics == nil {
		return nil, io.EOF
	}

	// Iterator pattern: advance through resources -> scopes -> metrics -> datapoints
	for r.resourceIndex < r.otelMetrics.ResourceMetrics().Len() {
		rm := r.otelMetrics.ResourceMetrics().At(r.resourceIndex)

		for r.scopeIndex < rm.ScopeMetrics().Len() {
			sm := rm.ScopeMetrics().At(r.scopeIndex)

			for r.metricIndex < sm.Metrics().Len() {
				metric := sm.Metrics().At(r.metricIndex)
				datapointCount := r.getDatapointCount(metric)

				if r.datapointIndex < datapointCount {
					// Build row for this datapoint
					row := r.buildDatapointRow(rm, sm, metric, r.datapointIndex)

					// Advance to next datapoint
					r.datapointIndex++

					return r.processRow(row)
				}

				// Move to next metric, reset datapoint index
				r.metricIndex++
				r.datapointIndex = 0
			}

			// Move to next scope, reset metric and datapoint indices
			r.scopeIndex++
			r.metricIndex = 0
			r.datapointIndex = 0
		}

		// Move to next resource, reset scope, metric and datapoint indices
		r.resourceIndex++
		r.scopeIndex = 0
		r.metricIndex = 0
		r.datapointIndex = 0
	}

	return nil, io.EOF
}

// getDatapointCount returns the number of datapoints for a metric based on its type.
func (r *ProtoMetricsReader) getDatapointCount(metric pmetric.Metric) int {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		return metric.Gauge().DataPoints().Len()
	case pmetric.MetricTypeSum:
		return metric.Sum().DataPoints().Len()
	case pmetric.MetricTypeHistogram:
		return metric.Histogram().DataPoints().Len()
	case pmetric.MetricTypeExponentialHistogram:
		return metric.ExponentialHistogram().DataPoints().Len()
	case pmetric.MetricTypeSummary:
		return metric.Summary().DataPoints().Len()
	default:
		return 0
	}
}

// buildDatapointRow creates a row from a single datapoint and its context.
func (r *ProtoMetricsReader) buildDatapointRow(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, metric pmetric.Metric, datapointIndex int) map[string]any {
	ret := map[string]any{}

	// Add resource attributes with prefix
	rm.Resource().Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "resource")] = value
		return true
	})

	// Add scope attributes with prefix
	sm.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "scope")] = value
		return true
	})

	// Basic metric fields
	ret["_cardinalhq.name"] = metric.Name()
	ret["description"] = metric.Description()
	ret["unit"] = metric.Unit()
	ret["type"] = metric.Type().String()

	// Add CardinalHQ metric type field
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		ret["_cardinalhq.metric_type"] = "gauge"
	case pmetric.MetricTypeSum:
		ret["_cardinalhq.metric_type"] = "count"
	case pmetric.MetricTypeHistogram:
		ret["_cardinalhq.metric_type"] = "histogram"
	case pmetric.MetricTypeExponentialHistogram:
		ret["_cardinalhq.metric_type"] = "histogram"
	case pmetric.MetricTypeSummary:
		ret["_cardinalhq.metric_type"] = "histogram"
	default:
		ret["_cardinalhq.metric_type"] = "gauge"
	}

	// Add datapoint-specific fields based on metric type
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dp := metric.Gauge().DataPoints().At(datapointIndex)
		r.addNumberDatapointFields(ret, dp)
	case pmetric.MetricTypeSum:
		dp := metric.Sum().DataPoints().At(datapointIndex)
		r.addNumberDatapointFields(ret, dp)
	case pmetric.MetricTypeHistogram:
		dp := metric.Histogram().DataPoints().At(datapointIndex)
		r.addHistogramDatapointFields(ret, dp)
	case pmetric.MetricTypeExponentialHistogram:
		dp := metric.ExponentialHistogram().DataPoints().At(datapointIndex)
		r.addExponentialHistogramDatapointFields(ret, dp)
	case pmetric.MetricTypeSummary:
		dp := metric.Summary().DataPoints().At(datapointIndex)
		r.addSummaryDatapointFields(ret, dp)
	}

	return ret
}

// addNumberDatapointFields adds fields from a NumberDataPoint to the row.
func (r *ProtoMetricsReader) addNumberDatapointFields(ret map[string]any, dp pmetric.NumberDataPoint) {
	// Add datapoint attributes
	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "metric")] = value
		return true
	})

	ret["_cardinalhq.timestamp"] = dp.Timestamp().AsTime().UnixMilli()
	ret["start_timestamp"] = dp.StartTimestamp().AsTime().UnixMilli()

	// Get the actual value
	var value float64
	if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
		value = float64(dp.IntValue())
	} else {
		value = dp.DoubleValue()
	}

	// Use CardinalHQ single-value pattern for gauges/sums
	ret["_cardinalhq.value"] = float64(-1) // Marker for single values
	ret["sketch"] = []byte{}               // Empty sketch for single values
	ret["rollup_avg"] = value              // For single value, all stats are the same
	ret["rollup_max"] = value
	ret["rollup_min"] = value
	ret["rollup_count"] = float64(1)
	ret["rollup_sum"] = value // Single value: sum equals the value
	ret["rollup_p25"] = value // All percentiles are the single value
	ret["rollup_p50"] = value
	ret["rollup_p75"] = value
	ret["rollup_p90"] = value
	ret["rollup_p95"] = value
	ret["rollup_p99"] = value
}

// addHistogramDatapointFields adds fields from a HistogramDataPoint to the row.
func (r *ProtoMetricsReader) addHistogramDatapointFields(ret map[string]any, dp pmetric.HistogramDataPoint) {
	// Add datapoint attributes
	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "metric")] = value
		return true
	})

	ret["_cardinalhq.timestamp"] = dp.Timestamp().AsTime().UnixMilli()
	ret["start_timestamp"] = dp.StartTimestamp().AsTime().UnixMilli()

	// Convert bucket data to float64 slices for processing
	bucketCounts := make([]float64, dp.BucketCounts().Len())
	for i := 0; i < dp.BucketCounts().Len(); i++ {
		bucketCounts[i] = float64(dp.BucketCounts().At(i))
	}

	explicitBounds := make([]float64, dp.ExplicitBounds().Len())
	for i := 0; i < dp.ExplicitBounds().Len(); i++ {
		explicitBounds[i] = dp.ExplicitBounds().At(i)
	}

	// Use handleHistogram to convert buckets to value/count pairs
	counts, values := r.handleHistogram(bucketCounts, explicitBounds)

	if len(counts) > 0 {
		// Create sketch from histogram data
		sketch, err := ddsketch.NewDefaultDDSketch(0.01)
		if err == nil {
			// Add histogram data to sketch
			for i, count := range counts {
				if count > 0 {
					if err := sketch.AddWithCount(values[i], count); err != nil {
						// Log error but continue processing
						continue
					}
				}
			}

			// Generate rollup statistics
			if sketch.GetCount() > 0 {
				maxvalue, _ := sketch.GetMaxValue()
				minvalue, _ := sketch.GetMinValue()
				quantiles, _ := sketch.GetValuesAtQuantiles([]float64{0.25, 0.5, 0.75, 0.90, 0.95, 0.99})

				count := sketch.GetCount()
				sum := sketch.GetSum()
				avg := sum / count

				// Add rollup fields in CardinalHQ format
				ret["_cardinalhq.value"] = float64(-1) // Marker for histogram values
				ret["rollup_avg"] = avg
				ret["rollup_max"] = maxvalue
				ret["rollup_min"] = minvalue
				ret["rollup_count"] = count
				ret["rollup_sum"] = sum
				ret["rollup_p25"] = quantiles[0]
				ret["rollup_p50"] = quantiles[1]
				ret["rollup_p75"] = quantiles[2]
				ret["rollup_p90"] = quantiles[3]
				ret["rollup_p95"] = quantiles[4]
				ret["rollup_p99"] = quantiles[5]
				ret["sketch"] = encodeSketch(sketch)
			}
		}
	}
}

// addExponentialHistogramDatapointFields adds fields from an ExponentialHistogramDataPoint to the row.
// This includes positive/negative buckets, exemplars, and other metadata specific to
// exponential histograms.
func (r *ProtoMetricsReader) addExponentialHistogramDatapointFields(ret map[string]any, dp pmetric.ExponentialHistogramDataPoint) {
	// Add datapoint attributes
	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "metric")] = value
		return true
	})

	ret["_cardinalhq.timestamp"] = dp.Timestamp().AsTime().UnixMilli()
	ret["start_timestamp"] = dp.StartTimestamp().AsTime().UnixMilli()

	ret["count"] = dp.Count()
	if dp.HasSum() {
		ret["sum"] = dp.Sum()
	}
	if dp.HasMin() {
		ret["min"] = dp.Min()
	}
	if dp.HasMax() {
		ret["max"] = dp.Max()
	}

	ret["scale"] = dp.Scale()
	ret["zero_count"] = dp.ZeroCount()

	// Positive bucket data
	pos := dp.Positive()
	ret["positive_offset"] = pos.Offset()
	posCounts := make([]uint64, pos.BucketCounts().Len())
	var posCount uint64
	for i := 0; i < pos.BucketCounts().Len(); i++ {
		v := pos.BucketCounts().At(i)
		posCounts[i] = v
		posCount += v
	}
	ret["positive_bucket_counts"] = posCounts
	ret["positive_count"] = posCount

	// Negative bucket data
	neg := dp.Negative()
	ret["negative_offset"] = neg.Offset()
	negCounts := make([]uint64, neg.BucketCounts().Len())
	var negCount uint64
	for i := 0; i < neg.BucketCounts().Len(); i++ {
		v := neg.BucketCounts().At(i)
		negCounts[i] = v
		negCount += v
	}
	ret["negative_bucket_counts"] = negCounts
	ret["negative_count"] = negCount

	// Exemplars
	if dp.Exemplars().Len() > 0 {
		exemplars := make([]map[string]any, dp.Exemplars().Len())
		for i := 0; i < dp.Exemplars().Len(); i++ {
			ex := dp.Exemplars().At(i)
			exMap := map[string]any{
				"timestamp": ex.Timestamp().AsTime().UnixMilli(),
			}

			// Exemplar value (int or double)
			if ex.ValueType() == pmetric.ExemplarValueTypeInt {
				exMap["value"] = float64(ex.IntValue())
			} else {
				exMap["value"] = ex.DoubleValue()
			}

			// Trace and span IDs if present
			if tid := ex.TraceID(); !tid.IsEmpty() {
				exMap["trace_id"] = tid.String()
			}
			if sid := ex.SpanID(); !sid.IsEmpty() {
				exMap["span_id"] = sid.String()
			}

			// Filtered attributes
			attrs := map[string]string{}
			ex.FilteredAttributes().Range(func(k string, v pcommon.Value) bool {
				attrs[k] = v.AsString()
				return true
			})
			if len(attrs) > 0 {
				exMap["filtered_attributes"] = attrs
			}

			exemplars[i] = exMap
		}
		ret["exemplars"] = exemplars
	}

	// Datapoint flags
	if dp.Flags() != 0 {
		ret["flags"] = dp.Flags()
	}
}

// addSummaryDatapointFields adds fields from a SummaryDataPoint to the row.
func (r *ProtoMetricsReader) addSummaryDatapointFields(ret map[string]any, dp pmetric.SummaryDataPoint) {
	// Add datapoint attributes
	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "metric")] = value
		return true
	})

	ret["_cardinalhq.timestamp"] = dp.Timestamp().AsTime().UnixMilli()
	ret["start_timestamp"] = dp.StartTimestamp().AsTime().UnixMilli()

	ret["count"] = dp.Count()
	ret["sum"] = dp.Sum()

	// Add quantile values
	quantiles := make([]map[string]any, dp.QuantileValues().Len())
	for i := 0; i < dp.QuantileValues().Len(); i++ {
		qv := dp.QuantileValues().At(i)
		quantiles[i] = map[string]any{
			"quantile": qv.Quantile(),
			"value":    qv.Value(),
		}
	}
	ret["quantiles"] = quantiles
}

// processRow applies any processing to a row.
func (r *ProtoMetricsReader) processRow(row map[string]any) (Row, error) {
	return Row(row), nil
}

// Close closes the reader and releases resources.
func (r *ProtoMetricsReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Release references to parsed data
	r.otelMetrics = nil

	return nil
}

// RowCount returns the total number of rows that have been successfully read.
func (r *ProtoMetricsReader) RowCount() int64 {
	return r.rowCount
}

func parseProtoToOtelMetrics(reader io.Reader) (*pmetric.Metrics, error) {
	unmarshaler := &pmetric.ProtoUnmarshaler{}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	metrics, err := unmarshaler.UnmarshalMetrics(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal protobuf metrics: %w", err)
	}

	return &metrics, nil
}

// handleHistogram fills the sketch with representative values for each bucket count.
// If bucketCounts[i] > 0, it inserts the midpoint of the bucket that bucketCounts[i] represents.
func (r *ProtoMetricsReader) handleHistogram(bucketCounts []float64, bucketBounds []float64) (counts, values []float64) {
	const maxTrackableValue = 1e9

	counts = []float64{}
	values = []float64{}

	if len(bucketCounts) == 0 || len(bucketBounds) == 0 {
		return counts, values
	}
	if len(bucketCounts) > len(bucketBounds)+1 {
		return counts, values
	}

	for i, count := range bucketCounts {
		if count <= 0 {
			continue
		}
		var value float64
		if i < len(bucketBounds) {
			var lowerBound float64
			if i == 0 {
				lowerBound = 1e-10 // very small lower bound
			} else {
				lowerBound = bucketBounds[i-1]
			}
			upperBound := bucketBounds[i]
			value = (lowerBound + upperBound) / 2.0
		} else {
			value = min(bucketBounds[len(bucketBounds)-1]+1, maxTrackableValue)
		}

		counts = append(counts, count)
		values = append(values, value)
	}

	return counts, values
}

// encodeSketch encodes a DDSketch to bytes.
func encodeSketch(sketch *ddsketch.DDSketch) []byte {
	var buf []byte
	sketch.Encode(&buf, false)
	return buf
}

// GetOTELMetrics implements the OTELMetricsProvider interface.
// Returns the underlying pmetric.Metrics structure for exemplar processing.
func (r *ProtoMetricsReader) GetOTELMetrics() (any, error) {
	if r.otelMetrics == nil {
		return nil, fmt.Errorf("no OTEL metrics available")
	}
	return r.otelMetrics, nil
}
