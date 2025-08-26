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

// IngestProtoMetricsReader reads rows from OpenTelemetry protobuf metrics format for ingestion.
// This reader is specifically designed for metric ingestion and should not be used for
// compaction or rollup operations.
type IngestProtoMetricsReader struct {
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

// NewIngestProtoMetricsReader creates a new IngestProtoMetricsReader for the given io.Reader.
// The caller is responsible for closing the underlying reader.
func NewIngestProtoMetricsReader(reader io.Reader) (*IngestProtoMetricsReader, error) {
	metrics, err := parseProtoToOtelMetrics(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto to OTEL metrics: %w", err)
	}

	return NewIngestProtoMetricsReaderFromMetrics(metrics)
}

// NewIngestProtoMetricsReaderFromMetrics creates a new IngestProtoMetricsReader from pre-parsed OTEL metrics.
// This is useful when you need to access the raw OTEL structure for processing (e.g., exemplars)
// while also reading rows from the same data.
func NewIngestProtoMetricsReaderFromMetrics(metrics *pmetric.Metrics) (*IngestProtoMetricsReader, error) {
	if metrics == nil {
		return nil, fmt.Errorf("metrics cannot be nil")
	}

	return &IngestProtoMetricsReader{
		otelMetrics: metrics,
	}, nil
}

// Read populates the provided slice with as many rows as possible.
func (r *IngestProtoMetricsReader) Read(rows []Row) (int, error) {
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
func (r *IngestProtoMetricsReader) getMetricRow() (Row, error) {
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
					row, err := r.buildDatapointRow(rm, sm, metric, r.datapointIndex)

					// Advance to next datapoint
					r.datapointIndex++

					// If datapoint failed to build (e.g., sketch creation failed), skip it
					if err != nil {
						// Log the error but continue to next datapoint
						// TODO: Add counter to track dropped datapoints
						continue
					}

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
func (r *IngestProtoMetricsReader) getDatapointCount(metric pmetric.Metric) int {
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
func (r *IngestProtoMetricsReader) buildDatapointRow(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, metric pmetric.Metric, datapointIndex int) (map[string]any, error) {
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

	// Add scope URL and name
	ret["scope_url"] = sm.Scope().Version()
	ret["scope_name"] = sm.Scope().Name()

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
		if err := r.addHistogramDatapointFields(ret, dp); err != nil {
			return nil, fmt.Errorf("failed to process histogram datapoint: %w", err)
		}
	case pmetric.MetricTypeExponentialHistogram:
		// TODO: Implement proper exponential histogram handling with sketches and rollup fields
		// For now, drop these data points to avoid "Empty sketch without valid rollup_sum" errors
		return nil, fmt.Errorf("exponential histograms not yet implemented")
	case pmetric.MetricTypeSummary:
		// TODO: Implement proper summary handling with sketches and rollup fields
		// For now, drop these data points silently to avoid downstream processing issues
		return nil, fmt.Errorf("summary data points not yet implemented")
	}

	return ret, nil
}

// addNumberDatapointFields adds fields from a NumberDataPoint to the row.
func (r *IngestProtoMetricsReader) addNumberDatapointFields(ret map[string]any, dp pmetric.NumberDataPoint) {
	// Add datapoint attributes
	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "metric")] = value
		return true
	})

	// Use Timestamp if available, fallback to StartTimestamp
	if dp.Timestamp() != 0 {
		ret["_cardinalhq.timestamp"] = dp.Timestamp().AsTime().UnixMilli()
	} else {
		ret["_cardinalhq.timestamp"] = dp.StartTimestamp().AsTime().UnixMilli()
	}
	ret["start_timestamp"] = dp.StartTimestamp().AsTime().UnixMilli()

	// Get the actual value
	var value float64
	if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
		value = float64(dp.IntValue())
	} else {
		value = dp.DoubleValue()
	}

	// Use CardinalHQ single-value pattern for gauges/sums
	ret["sketch"] = []byte{}  // Empty sketch for single values
	ret["rollup_avg"] = value // For single value, all stats are the same
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
func (r *IngestProtoMetricsReader) addHistogramDatapointFields(ret map[string]any, dp pmetric.HistogramDataPoint) error {
	// Add datapoint attributes
	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "metric")] = value
		return true
	})

	// Use Timestamp if available, fallback to StartTimestamp
	if dp.Timestamp() != 0 {
		ret["_cardinalhq.timestamp"] = dp.Timestamp().AsTime().UnixMilli()
	} else {
		ret["_cardinalhq.timestamp"] = dp.StartTimestamp().AsTime().UnixMilli()
	}
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

	// Always create a sketch for histograms, even if empty
	sketch, err := ddsketch.NewDefaultDDSketch(0.01)
	if err != nil {
		// TODO: Add a counter to track sketch creation failures for monitoring
		// If we can't create sketch for histogram, we must drop this data point
		// since histograms without sketches would break downstream aggregation
		return fmt.Errorf("failed to create sketch for histogram: %w", err)
	}

	// Add histogram data to sketch if we have any
	if len(counts) > 0 {
		for i, count := range counts {
			if count > 0 {
				if err := sketch.AddWithCount(values[i], count); err != nil {
					// Log error but continue processing
					continue
				}
			}
		}
	}

	// Generate rollup statistics from sketch (works for both empty and populated sketches)
	if sketch.GetCount() > 0 {
		// Sketch has data - use actual values
		maxvalue, _ := sketch.GetMaxValue()
		minvalue, _ := sketch.GetMinValue()
		quantiles, _ := sketch.GetValuesAtQuantiles([]float64{0.25, 0.5, 0.75, 0.90, 0.95, 0.99})

		count := sketch.GetCount()
		sum := sketch.GetSum()
		avg := sum / count

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
	} else {
		// Empty sketch - add zero rollup fields but keep the sketch
		ret["rollup_avg"] = 0.0
		ret["rollup_max"] = 0.0
		ret["rollup_min"] = 0.0
		ret["rollup_count"] = 0.0
		ret["rollup_sum"] = 0.0
		ret["rollup_p25"] = 0.0
		ret["rollup_p50"] = 0.0
		ret["rollup_p75"] = 0.0
		ret["rollup_p90"] = 0.0
		ret["rollup_p95"] = 0.0
		ret["rollup_p99"] = 0.0
	}

	// Always encode the sketch (even if empty) for histograms
	ret["sketch"] = encodeSketch(sketch)
	return nil
}

// processRow applies any processing to a row.
func (r *IngestProtoMetricsReader) processRow(row map[string]any) (Row, error) {
	return Row(row), nil
}

// Close closes the reader and releases resources.
func (r *IngestProtoMetricsReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Release references to parsed data
	r.otelMetrics = nil

	return nil
}

// TotalRowsReturned returns the total number of rows that have been successfully returned via Read().
func (r *IngestProtoMetricsReader) TotalRowsReturned() int64 {
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
func (r *IngestProtoMetricsReader) handleHistogram(bucketCounts []float64, bucketBounds []float64) (counts, values []float64) {
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
func (r *IngestProtoMetricsReader) GetOTELMetrics() (any, error) {
	if r.otelMetrics == nil {
		return nil, fmt.Errorf("no OTEL metrics available")
	}
	return r.otelMetrics, nil
}
