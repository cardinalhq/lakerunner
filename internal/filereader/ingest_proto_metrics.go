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
	"context"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"

	"github.com/DataDog/sketches-go/ddsketch"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/metricmath"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// IngestProtoMetricsReader reads rows from OpenTelemetry protobuf metrics format for ingestion.
// This reader is specifically designed for metric ingestion and should not be used for
// compaction or rollup operations.
type IngestProtoMetricsReader struct {
	closed    bool
	rowCount  int64
	batchSize int

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
func NewIngestProtoMetricsReader(reader io.Reader, batchSize int) (*IngestProtoMetricsReader, error) {
	metrics, err := parseProtoToOtelMetrics(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto to OTEL metrics: %w", err)
	}

	return NewIngestProtoMetricsReaderFromMetrics(metrics, batchSize)
}

// NewIngestProtoMetricsReaderFromMetrics creates a new IngestProtoMetricsReader from pre-parsed OTEL metrics.
// This is useful when you need to access the raw OTEL structure for processing (e.g., exemplars)
// while also reading rows from the same data.
func NewIngestProtoMetricsReaderFromMetrics(metrics *pmetric.Metrics, batchSize int) (*IngestProtoMetricsReader, error) {
	if metrics == nil {
		return nil, fmt.Errorf("metrics cannot be nil")
	}
	if batchSize <= 0 {
		batchSize = 1000 // Default batch size
	}

	return &IngestProtoMetricsReader{
		otelMetrics: metrics,
		batchSize:   batchSize,
	}, nil
}

// Next returns the next batch of rows from the OTEL metrics.
func (r *IngestProtoMetricsReader) Next() (*Batch, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	batch := pipeline.GetBatch()

	for batch.Len() < r.batchSize {
		row := make(Row)

		if err := r.getMetricRow(row); err != nil {
			if err == io.EOF {
				if batch.Len() == 0 {
					pipeline.ReturnBatch(batch)
					return nil, io.EOF
				}
				break
			}
			pipeline.ReturnBatch(batch)
			return nil, err
		}

		batchRow := batch.AddRow()
		maps.Copy(batchRow, row)
	}

	// Update row count with successfully read rows
	if batch.Len() > 0 {
		r.rowCount += int64(batch.Len())
		return batch, nil
	}

	pipeline.ReturnBatch(batch)
	return nil, io.EOF
}

// getMetricRow handles reading the next datapoint row using streaming iteration.
// The provided row must be initialized as an empty map before calling this method.
func (r *IngestProtoMetricsReader) getMetricRow(row Row) error {
	if r.otelMetrics == nil {
		return io.EOF
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
					dropped, err := r.buildDatapointRow(row, rm, sm, metric, r.datapointIndex)
					r.datapointIndex++

					if err != nil {
						slog.Error("Failed to build datapoint row", "error", err)
						continue
					}
					if dropped {
						continue
					}

					return nil
				}

				r.metricIndex++
				r.datapointIndex = 0
			}

			r.scopeIndex++
			r.metricIndex = 0
			r.datapointIndex = 0
		}

		r.resourceIndex++
		r.scopeIndex = 0
		r.metricIndex = 0
		r.datapointIndex = 0
	}

	return io.EOF
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

// buildDatapointRow populates the provided row with data from a single datapoint and its context.
// Returns (dropped, error) where dropped indicates if the datapoint was filtered out.
func (r *IngestProtoMetricsReader) buildDatapointRow(row Row, rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, metric pmetric.Metric, datapointIndex int) (bool, error) {
	// Add resource attributes with prefix
	rm.Resource().Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		row[wkk.NewRowKey(prefixAttribute(name, "resource"))] = value
		return true
	})

	// Add scope attributes with prefix
	sm.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		row[wkk.NewRowKey(prefixAttribute(name, "scope"))] = value
		return true
	})

	// Add scope URL and name
	row[wkk.NewRowKey("scope_url")] = sm.Scope().Version()
	row[wkk.NewRowKey("scope_name")] = sm.Scope().Name()

	// Basic metric fields
	row[wkk.RowKeyCName] = metric.Name()
	row[wkk.NewRowKey("description")] = metric.Description()
	row[wkk.NewRowKey("unit")] = metric.Unit()

	// Add CardinalHQ metric type field
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		row[wkk.RowKeyCMetricType] = "gauge"
	case pmetric.MetricTypeSum:
		row[wkk.RowKeyCMetricType] = "count"
	case pmetric.MetricTypeHistogram:
		row[wkk.RowKeyCMetricType] = "histogram"
	case pmetric.MetricTypeExponentialHistogram:
		row[wkk.RowKeyCMetricType] = "histogram"
	case pmetric.MetricTypeSummary:
		row[wkk.RowKeyCMetricType] = "histogram"
	default:
		row[wkk.RowKeyCMetricType] = "gauge"
	}

	// Add datapoint-specific fields based on metric type
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dp := metric.Gauge().DataPoints().At(datapointIndex)
		if dropped, err := r.addNumberDatapointFields(row, dp); err != nil {
			return false, fmt.Errorf("failed to process gauge datapoint: %w", err)
		} else if dropped {
			return true, nil
		}
	case pmetric.MetricTypeSum:
		dp := metric.Sum().DataPoints().At(datapointIndex)
		if dropped, err := r.addNumberDatapointFields(row, dp); err != nil {
			return false, fmt.Errorf("failed to process sum datapoint: %w", err)
		} else if dropped {
			return true, nil
		}
	case pmetric.MetricTypeHistogram:
		dp := metric.Histogram().DataPoints().At(datapointIndex)
		if err := r.addHistogramDatapointFields(row, dp); err != nil {
			return false, fmt.Errorf("failed to process histogram datapoint: %w", err)
		}
	case pmetric.MetricTypeExponentialHistogram:
		dp := metric.ExponentialHistogram().DataPoints().At(datapointIndex)
		if err := r.addExponentialHistogramDatapointFields(row, dp); err != nil {
			return false, fmt.Errorf("failed to process exponential histogram datapoint: %w", err)
		}
	case pmetric.MetricTypeSummary:
		// TODO: Implement proper summary handling with sketches and rollup fields
		// For now, drop these data points silently to avoid downstream processing issues
		rowsDroppedCounter.Add(context.Background(), 1, otelmetric.WithAttributes(
			attribute.String("reader", "IngestProtoMetricsReader"),
			attribute.String("reason", "summary_not_implemented"),
		))
		return false, fmt.Errorf("summary data points not yet implemented")
	}

	return false, nil
}

// addNumberDatapointFields adds fields from a NumberDataPoint to the row.
// Returns (dropped, error) where dropped indicates if the datapoint was filtered out.
func (r *IngestProtoMetricsReader) addNumberDatapointFields(ret Row, dp pmetric.NumberDataPoint) (bool, error) {
	// Add datapoint attributes
	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[wkk.NewRowKey(prefixAttribute(name, "metric"))] = value
		return true
	})

	// Use Timestamp if available, fallback to StartTimestamp
	if dp.Timestamp() != 0 {
		ret[wkk.RowKeyCTimestamp] = dp.Timestamp().AsTime().UnixMilli()
	} else {
		ret[wkk.RowKeyCTimestamp] = dp.StartTimestamp().AsTime().UnixMilli()
	}
	ret[wkk.NewRowKey("start_timestamp")] = dp.StartTimestamp().AsTime().UnixMilli()

	// Get the actual value
	var value float64
	if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
		value = float64(dp.IntValue())
	} else {
		value = dp.DoubleValue()
	}

	// Check for NaN values and skip this datapoint if found
	if math.IsNaN(value) || math.IsInf(value, 0) {
		rowsDroppedCounter.Add(context.Background(), 1, otelmetric.WithAttributes(
			attribute.String("reader", "IngestProtoMetricsReader"),
			attribute.String("reason", "NaN"),
		))
		return true, nil // dropped=true, no error
	}

	// Use CardinalHQ single-value pattern for gauges/sums
	ret[wkk.RowKeySketch] = []byte{} // Empty sketch for single values
	ret[wkk.RowKeyRollupAvg] = value // For single value, all stats are the same
	ret[wkk.RowKeyRollupMax] = value
	ret[wkk.RowKeyRollupMin] = value
	ret[wkk.RowKeyRollupCount] = float64(1)
	ret[wkk.RowKeyRollupSum] = value // Single value: sum equals the value
	ret[wkk.RowKeyRollupP25] = value // All percentiles are the single value
	ret[wkk.RowKeyRollupP50] = value
	ret[wkk.RowKeyRollupP75] = value
	ret[wkk.RowKeyRollupP90] = value
	ret[wkk.RowKeyRollupP95] = value
	ret[wkk.RowKeyRollupP99] = value

	return false, nil
}

func (r *IngestProtoMetricsReader) addHistogramDatapointFields(ret Row, dp pmetric.HistogramDataPoint) error {
	// 0) Bail if there are no counts at all
	hasCounts := false
	for i := 0; i < dp.BucketCounts().Len(); i++ {
		if dp.BucketCounts().At(i) > 0 {
			hasCounts = true
			break
		}
	}
	if !hasCounts {
		rowsDroppedCounter.Add(context.Background(), 1, otelmetric.WithAttributes(
			attribute.String("reader", "IngestProtoMetricsReader"),
			attribute.String("reason", "histogram_no_counts"),
		))
		return ErrHistogramNoCounts
	}

	// 1) Attributes
	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		ret[wkk.NewRowKey(prefixAttribute(name, "metric"))] = v.AsString()
		return true
	})

	// 2) Timestamps
	if dp.Timestamp() != 0 {
		ret[wkk.RowKeyCTimestamp] = dp.Timestamp().AsTime().UnixMilli()
	} else {
		ret[wkk.RowKeyCTimestamp] = dp.StartTimestamp().AsTime().UnixMilli()
	}
	ret[wkk.NewRowKey("start_timestamp")] = dp.StartTimestamp().AsTime().UnixMilli()

	// 3) Build the sketch
	const alpha = 0.01
	sketch, err := ddsketch.NewDefaultDDSketch(alpha)
	if err != nil {
		return fmt.Errorf("failed to create sketch for histogram: %w", err)
	}

	// Gather OTel explicit bounds & counts
	m := dp.ExplicitBounds().Len()
	n := dp.BucketCounts().Len() // usually m+1
	if n == 0 {
		rowsDroppedCounter.Add(context.Background(), 1, otelmetric.WithAttributes(
			attribute.String("reader", "IngestProtoMetricsReader"),
			attribute.String("reason", "histogram_no_bucket_counts"),
		))
		return fmt.Errorf("dropping histogram datapoint with no bucket counts")
	}
	// Note: m == 0 is valid (single bucket from -inf to +inf)

	bounds := make([]float64, m)
	for i := range m {
		bounds[i] = dp.ExplicitBounds().At(i)
	}
	counts := make([]uint64, n)
	for i := range n {
		counts[i] = dp.BucketCounts().At(i)
	}

	// 3a) Import finite buckets via ImportStandardHistogram.
	// OTel buckets: (-inf,b0], (b0,b1], ... (b[m-2],b[m-1]], (b[m-1], +inf)
	// Handle special case: single bucket spanning -inf to +inf (no explicit bounds)
	if m == 0 && n == 1 {
		// Single bucket from -inf to +inf, add all counts at zero (or sum/count average if available)
		totalCount := counts[0]
		if totalCount > 0 {
			// Use a representative value - zero is reasonable for a single bucket
			// or better yet, use sum/count if we have a sum available
			rep := 0.0
			if dp.HasSum() && totalCount > 0 {
				rep = dp.Sum() / float64(totalCount)
			}
			if err := sketch.AddWithCount(rep, float64(totalCount)); err != nil {
				return fmt.Errorf("failed to add single bucket to sketch: %w", err)
			}
		}
	} else {
		// Multi-bucket case: process finite buckets and handle underflow/overflow
		// Our ImportStandardHistogram expects: (lo,cutoffs[0]], (cutoffs[0],cutoffs[1]], ...
		// So we feed it starting from b0 as the first "lo".
		if m >= 2 && n >= 2 {
			lower := bounds[0]
			// finite cutoffs are bounds[1:] ; finite counts are bucketCounts[1:m]
			// (they correspond to (b0,b1], ... , (b[m-2], b[m-1]])
			finiteCutoffs := make([]float64, 0, max(0, m-1))
			finiteCounts := make([]uint64, 0, max(0, m-1))
			for i := 1; i < m; i++ {
				finiteCutoffs = append(finiteCutoffs, bounds[i])
			}
			// counts indices 1..m-1 inclusive (length m-1) — guard n
			for i := 1; i < m && i < n; i++ {
				finiteCounts = append(finiteCounts, counts[i])
			}

			if len(finiteCutoffs) == len(finiteCounts) && len(finiteCounts) > 0 {
				if err := metricmath.ConvertHistogramToValues(
					sketch,
					lower,
					finiteCutoffs,
					finiteCounts,
					alpha,
					nil, // per-bin sums unknown for OTel explicit hist
				); err != nil {
					return fmt.Errorf("failed to import finite histogram buckets: %w", err)
				}
			}
		}

		// 3b) Handle underflow (-inf, b0] and overflow (b[m-1], +inf) by adding
		// single representatives near the edges. Use γ so this plays nicely with DDSketch mapping.
		if m > 0 {
			gamma := (1 + alpha) / (1 - alpha)
			underflow := counts[0]
			overflow := uint64(0)
			if n == m+1 {
				overflow = counts[n-1]
			} else if n > m+1 {
				// If an exporter supplied extra buckets, treat the last as overflow.
				overflow = counts[n-1]
			}

			if underflow > 0 {
				// Representative just "one DDSketch half-bucket" below b0 on a log scale if b0>0.
				// Otherwise, place it slightly below b0 linearly.
				rep := bounds[0]
				if rep > 0 {
					rep = rep / math.Sqrt(gamma)
				} else {
					// Fall back to a tiny linear step down if non-positive
					// (choose step as 1% of |b0| or 1.0 if b0==0).
					step := math.Max(1.0, math.Abs(rep)*0.01)
					rep = rep - step
				}
				if err := sketch.AddWithCount(rep, float64(underflow)); err != nil {
					return fmt.Errorf("failed to add underflow to sketch: %w", err)
				}
			}

			if overflow > 0 {
				last := bounds[m-1]
				rep := last
				if rep > 0 {
					rep = rep * math.Sqrt(gamma)
				} else {
					// If last bound is non-positive, step upward linearly.
					step := math.Max(1.0, math.Abs(rep)*0.01)
					rep = rep + step
				}
				if err := sketch.AddWithCount(rep, float64(overflow)); err != nil {
					return fmt.Errorf("failed to add overflow to sketch: %w", err)
				}
			}
		}
	}

	// 4) Rollup statistics from the (now non-empty) sketch
	maxvalue, err := sketch.GetMaxValue()
	if err != nil {
		return fmt.Errorf("failed to get max value from non-empty sketch: %w", err)
	}
	minvalue, err := sketch.GetMinValue()
	if err != nil {
		return fmt.Errorf("failed to get min value from non-empty sketch: %w", err)
	}
	quantiles, err := sketch.GetValuesAtQuantiles([]float64{0.25, 0.5, 0.75, 0.90, 0.95, 0.99})
	if err != nil {
		return fmt.Errorf("failed to get quantiles from non-empty sketch: %w", err)
	}
	if len(quantiles) < 6 {
		return fmt.Errorf("expected 6 quantiles, got %d", len(quantiles))
	}

	count := sketch.GetCount()
	sum := sketch.GetSum()
	avg := sum / count

	ret[wkk.RowKeyRollupAvg] = avg
	ret[wkk.RowKeyRollupMax] = maxvalue
	ret[wkk.RowKeyRollupMin] = minvalue
	ret[wkk.RowKeyRollupCount] = count
	ret[wkk.RowKeyRollupSum] = sum
	ret[wkk.RowKeyRollupP25] = quantiles[0]
	ret[wkk.RowKeyRollupP50] = quantiles[1]
	ret[wkk.RowKeyRollupP75] = quantiles[2]
	ret[wkk.RowKeyRollupP90] = quantiles[3]
	ret[wkk.RowKeyRollupP95] = quantiles[4]
	ret[wkk.RowKeyRollupP99] = quantiles[5]

	// 5) Encode the sketch
	ret[wkk.RowKeySketch] = helpers.EncodeSketch(sketch)
	return nil
}

// addExponentialHistogramDatapointFields adds fields from an ExponentialHistogramDataPoint to the row.
func (r *IngestProtoMetricsReader) addExponentialHistogramDatapointFields(ret Row, dp pmetric.ExponentialHistogramDataPoint) error {
	// Add datapoint attributes
	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[wkk.NewRowKey(prefixAttribute(name, "metric"))] = value
		return true
	})

	// Use Timestamp if available, fallback to StartTimestamp
	if dp.Timestamp() != 0 {
		ret[wkk.RowKeyCTimestamp] = dp.Timestamp().AsTime().UnixMilli()
	} else {
		ret[wkk.RowKeyCTimestamp] = dp.StartTimestamp().AsTime().UnixMilli()
	}
	ret[wkk.NewRowKey("start_timestamp")] = dp.StartTimestamp().AsTime().UnixMilli()

	// Extract exponential histogram data
	var positiveBuckets, negativeBuckets []uint64
	if dp.Positive().BucketCounts().Len() > 0 {
		positiveBuckets = make([]uint64, dp.Positive().BucketCounts().Len())
		for i := 0; i < dp.Positive().BucketCounts().Len(); i++ {
			positiveBuckets[i] = dp.Positive().BucketCounts().At(i)
		}
	}
	if dp.Negative().BucketCounts().Len() > 0 {
		negativeBuckets = make([]uint64, dp.Negative().BucketCounts().Len())
		for i := 0; i < dp.Negative().BucketCounts().Len(); i++ {
			negativeBuckets[i] = dp.Negative().BucketCounts().At(i)
		}
	}

	// Convert exponential histogram to value/count pairs
	expHist := metricmath.ExpHist{
		Scale:     dp.Scale(),
		ZeroCount: dp.ZeroCount(),
		PosOffset: dp.Positive().Offset(),
		PosCounts: positiveBuckets,
		NegOffset: dp.Negative().Offset(),
		NegCounts: negativeBuckets,
	}
	counts, values := metricmath.ConvertExponentialHistogramToValues(expHist)

	// Check if histogram has any data - if not, drop this datapoint
	hasData := false
	for _, count := range counts {
		if count > 0 {
			hasData = true
			break
		}
	}

	if !hasData {
		// Drop exponential histogram datapoints with no data
		rowsDroppedCounter.Add(context.Background(), 1, otelmetric.WithAttributes(
			attribute.String("reader", "IngestProtoMetricsReader"),
			attribute.String("reason", "exponential_histogram_no_data"),
		))
		return fmt.Errorf("dropping exponential histogram datapoint with no data")
	}

	// Create sketch for exponential histogram (only for histograms with data)
	sketch, err := ddsketch.NewDefaultDDSketch(0.01)
	if err != nil {
		return fmt.Errorf("failed to create sketch for exponential histogram: %w", err)
	}

	// Add histogram data to sketch
	for i, count := range counts {
		if count > 0 {
			if err := sketch.AddWithCount(values[i], count); err != nil {
				// Log error but continue processing other buckets
				continue
			}
		}
	}

	// Generate rollup statistics from sketch (sketch always has data at this point)
	maxvalue, err := sketch.GetMaxValue()
	if err != nil {
		return fmt.Errorf("failed to get max value from non-empty sketch: %w", err)
	}
	minvalue, err := sketch.GetMinValue()
	if err != nil {
		return fmt.Errorf("failed to get min value from non-empty sketch: %w", err)
	}
	quantiles, err := sketch.GetValuesAtQuantiles([]float64{0.25, 0.5, 0.75, 0.90, 0.95, 0.99})
	if err != nil {
		return fmt.Errorf("failed to get quantiles from non-empty sketch: %w", err)
	}
	if len(quantiles) < 6 {
		return fmt.Errorf("expected 6 quantiles, got %d", len(quantiles))
	}

	count := sketch.GetCount()
	sum := sketch.GetSum()
	avg := sum / count

	ret[wkk.RowKeyRollupAvg] = avg
	ret[wkk.RowKeyRollupMax] = maxvalue
	ret[wkk.RowKeyRollupMin] = minvalue
	ret[wkk.RowKeyRollupCount] = count
	ret[wkk.RowKeyRollupSum] = sum
	ret[wkk.RowKeyRollupP25] = quantiles[0]
	ret[wkk.RowKeyRollupP50] = quantiles[1]
	ret[wkk.RowKeyRollupP75] = quantiles[2]
	ret[wkk.RowKeyRollupP90] = quantiles[3]
	ret[wkk.RowKeyRollupP95] = quantiles[4]
	ret[wkk.RowKeyRollupP99] = quantiles[5]

	// Encode the sketch (always has data at this point)
	ret[wkk.RowKeySketch] = helpers.EncodeSketch(sketch)
	return nil
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

// TotalRowsReturned returns the total number of rows that have been successfully returned via Next().
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

// GetOTELMetrics implements the OTELMetricsProvider interface.
// Returns the underlying pmetric.Metrics structure for exemplar processing.
func (r *IngestProtoMetricsReader) GetOTELMetrics() (any, error) {
	if r.otelMetrics == nil {
		return nil, fmt.Errorf("no OTEL metrics available")
	}
	return r.otelMetrics, nil
}
