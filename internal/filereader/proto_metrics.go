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

	// Pre-generated datapoint rows
	datapointRows []map[string]any
	currentIndex  int
}

// NewProtoMetricsReader creates a new ProtoMetricsReader for the given io.Reader.
// The caller is responsible for closing the underlying reader.
func NewProtoMetricsReader(reader io.Reader) (*ProtoMetricsReader, error) {
	metrics, err := parseProtoToOtelMetrics(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto to OTEL metrics: %w", err)
	}

	protoReader := &ProtoMetricsReader{}
	protoReader.datapointRows = protoReader.generateDatapointRows(metrics)

	return protoReader, nil
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

// getMetricRow handles reading the next datapoint row from the pre-generated slice.
func (r *ProtoMetricsReader) getMetricRow() (Row, error) {
	if r.currentIndex >= len(r.datapointRows) {
		return nil, io.EOF
	}

	row := r.datapointRows[r.currentIndex]
	r.currentIndex++

	return r.processRow(row)
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
	ret["name"] = metric.Name()
	ret["description"] = metric.Description()
	ret["unit"] = metric.Unit()
	ret["type"] = metric.Type().String()

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

	ret["timestamp"] = dp.Timestamp().AsTime().UnixMilli()
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

	ret["timestamp"] = dp.Timestamp().AsTime().UnixMilli()
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
// TODO: implement
func (r *ProtoMetricsReader) addExponentialHistogramDatapointFields(ret map[string]any, dp pmetric.ExponentialHistogramDataPoint) {
	// Add datapoint attributes
	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "metric")] = value
		return true
	})

	ret["timestamp"] = dp.Timestamp().AsTime().UnixMilli()
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
}

// addSummaryDatapointFields adds fields from a SummaryDataPoint to the row.
func (r *ProtoMetricsReader) addSummaryDatapointFields(ret map[string]any, dp pmetric.SummaryDataPoint) {
	// Add datapoint attributes
	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "metric")] = value
		return true
	})

	ret["timestamp"] = dp.Timestamp().AsTime().UnixMilli()
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

// generateDatapointRows pre-generates all datapoint rows from the metrics.
func (r *ProtoMetricsReader) generateDatapointRows(metrics *pmetric.Metrics) []map[string]any {
	var rows []map[string]any

	// Iterate through resources -> scopes -> metrics -> datapoints
	for resourceIndex := 0; resourceIndex < metrics.ResourceMetrics().Len(); resourceIndex++ {
		rm := metrics.ResourceMetrics().At(resourceIndex)

		for scopeIndex := 0; scopeIndex < rm.ScopeMetrics().Len(); scopeIndex++ {
			sm := rm.ScopeMetrics().At(scopeIndex)

			for metricIndex := 0; metricIndex < sm.Metrics().Len(); metricIndex++ {
				metric := sm.Metrics().At(metricIndex)

				// Generate rows for all datapoints in this metric
				datapointCount := r.getDatapointCount(metric)
				for datapointIndex := 0; datapointIndex < datapointCount; datapointIndex++ {
					row := r.buildDatapointRow(rm, sm, metric, datapointIndex)
					rows = append(rows, row)
				}
			}
		}
	}

	return rows
}

// Close closes the reader and releases resources.
func (r *ProtoMetricsReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Release references to parsed data
	r.datapointRows = nil

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
