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
	"math"
	"sort"
	"strings"
	"time"

	"github.com/DataDog/sketches-go/ddsketch"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/metricmath"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// IngestProtoMetricsReader reads rows from OpenTelemetry protobuf metrics format for ingestion.
// This reader is specifically designed for metric ingestion and should not be used for
// compaction or rollup operations.
//
// Implements OTELMetricsProvider interface.
type IngestProtoMetricsReader struct {
	closed      bool
	rowCount    int64
	rowsSkipped int64
	batchSize   int

	// Store the original OTEL metrics for exemplar processing
	orgId       string
	otelMetrics *pmetric.Metrics

	// Streaming iterator state for metrics
	resourceIndex  int
	scopeIndex     int
	metricIndex    int
	datapointIndex int

	// Schema extracted from all metrics
	schema *ReaderSchema

	// Cached RowKeys for attribute names (entire file scope)
	resourceAttrCache *PrefixedRowKeyCache // "service.name" → wkk("resource_service_name")
	scopeAttrCache    *PrefixedRowKeyCache // "name" → wkk("scope_name")
	attrCache         *PrefixedRowKeyCache // "user.id" → wkk("attr_user_id")
}

var _ Reader = (*IngestProtoMetricsReader)(nil)

// NewIngestProtoMetricsReader creates a new IngestProtoMetricsReader for the given io.Reader.
func NewIngestProtoMetricsReader(reader io.Reader, opts ReaderOptions) (*IngestProtoMetricsReader, error) {
	metrics, err := parseProtoToOtelMetrics(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto to OTEL metrics: %w", err)
	}

	return NewIngestProtoMetricsReaderFromMetrics(metrics, opts)
}

// NewIngestProtoMetricsReaderFromMetrics creates a new IngestProtoMetricsReader from pre-parsed OTEL metrics.
// This is useful when you need to access the raw OTEL structure for processing (e.g., exemplars)
// while also reading rows from the same data.
func NewIngestProtoMetricsReaderFromMetrics(metrics *pmetric.Metrics, opts ReaderOptions) (*IngestProtoMetricsReader, error) {
	if metrics == nil {
		return nil, fmt.Errorf("metrics cannot be nil")
	}
	batchSize := opts.BatchSize
	if opts.BatchSize <= 0 {
		batchSize = 1000 // Default batch size
	}

	// Extract schema from all metrics (two-pass approach)
	schema := extractSchemaFromOTELMetrics(metrics)

	return &IngestProtoMetricsReader{
		otelMetrics:       metrics,
		orgId:             opts.OrgID,
		batchSize:         batchSize,
		schema:            schema,
		resourceAttrCache: NewPrefixedRowKeyCache("resource"),
		scopeAttrCache:    NewPrefixedRowKeyCache("scope"),
		attrCache:         NewPrefixedRowKeyCache("attr"),
	}, nil
}

// Next returns the next batch of rows from the OTEL metrics.
func (r *IngestProtoMetricsReader) Next(ctx context.Context) (*Batch, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	batch := pipeline.GetBatch()

	for batch.Len() < r.batchSize {
		// Get a row from the pool to populate
		row := pipeline.GetPooledRow()

		if err := r.getMetricRow(ctx, row); err != nil {
			// Return unused row to pool
			pipeline.ReturnPooledRow(row)
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

		// Add the populated row to the batch (batch takes ownership)
		batch.AppendRow(row)
	}

	if batch.Len() > 0 {
		r.rowCount += int64(batch.Len())
		rowsOutCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
			attribute.String("reader", "IngestProtoMetricsReader"),
		))
		return batch, nil
	}

	pipeline.ReturnBatch(batch)
	return nil, io.EOF
}

// getMetricRow handles reading the next datapoint row using streaming iteration.
// The provided row must be initialized as an empty map before calling this method.
func (r *IngestProtoMetricsReader) getMetricRow(ctx context.Context, row pipeline.Row) error {
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
					rowsInCounter.Add(ctx, 1, otelmetric.WithAttributes(
						attribute.String("reader", "IngestProtoMetricsReader"),
					))
					dropped, err := r.buildDatapointRow(ctx, row, rm, sm, metric, r.datapointIndex)
					r.datapointIndex++

					if err != nil {
						slog.Error("Failed to build datapoint row", "error", err)
						continue
					}
					if dropped {
						r.rowsSkipped++
						rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
							attribute.String("reader", "IngestProtoMetricsReader"),
							attribute.String("reason", "empty_metric_name"),
						))
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
func (r *IngestProtoMetricsReader) buildDatapointRow(ctx context.Context, row pipeline.Row, rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, metric pmetric.Metric, datapointIndex int) (bool, error) {
	rm.Resource().Attributes().Range(func(name string, v pcommon.Value) bool {
		key := r.resourceAttrCache.Get(name)
		value := v.AsString()
		row[key] = value
		return true
	})

	sm.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
		key := r.scopeAttrCache.Get(name)
		value := v.AsString()
		row[key] = value
		return true
	})

	row[wkk.NewRowKey("chq_scope_url")] = sm.Scope().Version()
	row[wkk.NewRowKey("chq_scope_name")] = sm.Scope().Name()

	metricName := strings.ReplaceAll(metric.Name(), ".", "_")

	// Skip metrics with empty names - these are malformed OTEL data
	// Check BEFORE setting the field to avoid mutating the row
	if metricName == "" {
		return true, nil // dropped=true, no error
	}

	row[wkk.RowKeyCName] = metricName

	row[wkk.NewRowKey("chq_description")] = metric.Description()
	row[wkk.NewRowKey("chq_unit")] = metric.Unit()

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

	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dp := metric.Gauge().DataPoints().At(datapointIndex)
		return r.addNumberDatapointFields(ctx, row, dp, "gauge")
	case pmetric.MetricTypeSum:
		dp := metric.Sum().DataPoints().At(datapointIndex)
		return r.addNumberDatapointFields(ctx, row, dp, "sum")
	case pmetric.MetricTypeHistogram:
		dp := metric.Histogram().DataPoints().At(datapointIndex)
		return r.addHistogramDatapointFields(ctx, row, dp)
	case pmetric.MetricTypeExponentialHistogram:
		dp := metric.ExponentialHistogram().DataPoints().At(datapointIndex)
		return r.addExponentialHistogramDatapointFields(ctx, row, dp)
	case pmetric.MetricTypeSummary:
		dp := metric.Summary().DataPoints().At(datapointIndex)
		return r.addSummaryDatapointFields(ctx, row, dp)
	}

	return false, nil
}

// SummaryToDDSketch approximates a DDSketch from an OTel Summary data point.
// Returns nil and no error if the summary has no quantile values.
func summaryToDDSketch(dp pmetric.SummaryDataPoint) (*ddsketch.DDSketch, error) {
	const maxSamples = 2048

	s, err := ddsketch.NewDefaultDDSketch(0.01)
	if err != nil {
		return nil, fmt.Errorf("failed to create sketch for summary: %w", err)
	}

	// Pull quantiles
	nq := dp.QuantileValues().Len()
	if nq == 0 {
		return s, nil
	}
	type qv struct{ q, v float64 }
	qs := make([]qv, 0, nq)
	for i := 0; i < nq; i++ {
		it := dp.QuantileValues().At(i)
		q, v := it.Quantile(), it.Value()
		if math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		// Clamp q to [0,1]
		if q < 0 {
			q = 0
		} else if q > 1 {
			q = 1
		}
		qs = append(qs, qv{q, v})
	}
	if len(qs) == 0 {
		return s, nil
	}
	sort.Slice(qs, func(i, j int) bool { return qs[i].q < qs[j].q })

	// Ensure endpoints for coverage of [0,1]
	if qs[0].q > 0 {
		qs = append([]qv{{0, qs[0].v}}, qs...)
	}
	if qs[len(qs)-1].q < 1 {
		last := qs[len(qs)-1]
		qs = append(qs, qv{1, last.v})
	}

	// Deduplicate non-monotone or repeated quantiles (keep first)
	dst := qs[:1]
	for i := 1; i < len(qs); i++ {
		if qs[i].q > dst[len(dst)-1].q {
			dst = append(dst, qs[i])
		}
	}
	qs = dst
	if len(qs) == 1 {
		// Single point: just add one representative
		_ = s.Add(qs[0].v)
		return s, nil
	}

	// Build segments
	type seg struct{ qL, qR, vL, vR float64 }
	segs := make([]seg, 0, len(qs)-1)
	totalMass := 0.0
	for i := 0; i+1 < len(qs); i++ {
		qL, qR := qs[i].q, qs[i+1].q
		if qR <= qL {
			continue
		}
		segs = append(segs, seg{qL, qR, qs[i].v, qs[i+1].v})
		totalMass += (qR - qL)
	}
	if totalMass == 0 || len(segs) == 0 {
		// Degenerate—dump endpoints
		for _, p := range qs {
			_ = s.Add(p.v)
		}
		return s, nil
	}

	// Sampling budget: distribute proportionally by mass; gently cap by dp.Count
	count := float64(dp.Count())
	budget := float64(maxSamples)
	if count > 0 && count < budget {
		// Don’t invent more samples than the actual count (soft cap).
		budget = count
	}
	if budget < float64(len(segs)) {
		budget = float64(len(segs))
	}

	lin := func(vL, vR, t float64) float64 { return vL + t*(vR-vL) }
	loglin := func(vL, vR, t float64) float64 {
		// If either endpoint non-positive, fall back to linear.
		if vL <= 0 || vR <= 0 {
			return lin(vL, vR, t)
		}
		return math.Exp(math.Log(vL) + t*(math.Log(vR)-math.Log(vL)))
	}

	remaining := int(budget)
	for i, g := range segs {
		w := (g.qR - g.qL) / totalMass
		n := int(math.Round(w * budget))
		if n == 0 {
			n = 1
		}
		if n > remaining {
			n = remaining
		}
		remaining -= n

		for k := 1; k <= n; k++ {
			// Evenly spaced internal points per segment
			t := (float64(k) - 0.5) / float64(n)
			val := loglin(g.vL, g.vR, t)
			if !math.IsNaN(val) && !math.IsInf(val, 0) {
				_ = s.Add(val)
			}
		}
		if remaining <= 0 && i < len(segs)-1 {
			break
		}
	}

	// Optional sanity anchors when mean is wildly off.
	// If dp.Sum() and dp.Count() are set and the reconstructed mean differs a lot,
	// you can nudge the sketch by adding a few values near the global mean.
	if dp.Count() > 0 {
		targetMean := dp.Sum() / float64(dp.Count())
		if sCount, sSum := s.GetCount(), s.GetSum(); sCount > 0 {
			gotMean := sSum / sCount
			relErr := math.Abs(gotMean-targetMean) / math.Max(1e-12, targetMean)
			if relErr > 0.25 { // tolerate 25% drift; tune to taste
				anchor := targetMean
				// Add a couple of anchors; this won't distort quantiles much on large n.
				_ = s.Add(anchor)
				_ = s.Add(anchor)
			}
		}
	}

	return s, nil
}

// addNumberDatapointFields adds fields from a NumberDataPoint to the row.
// Returns (dropped, error) where dropped indicates if the datapoint was filtered out.
func (r *IngestProtoMetricsReader) addNumberDatapointFields(ctx context.Context, ret pipeline.Row, dp pmetric.NumberDataPoint, metricType string) (bool, error) {
	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		key := r.attrCache.Get(name)
		value := v.AsString()
		ret[key] = value
		return true
	})

	if dp.Timestamp() != 0 {
		ret[wkk.RowKeyCTimestamp] = dp.Timestamp().AsTime().UnixMilli()
		ret[wkk.RowKeyCTsns] = int64(dp.Timestamp())
	} else if dp.StartTimestamp() != 0 {
		ret[wkk.RowKeyCTimestamp] = dp.StartTimestamp().AsTime().UnixMilli()
		ret[wkk.RowKeyCTsns] = int64(dp.StartTimestamp())
		timestampFallbackCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("signal_type", "metrics"),
			attribute.String("reason", "start_timestamp"),
		))
	} else {
		// Fallback to current time when both timestamps are zero
		currentTime := time.Now()
		ret[wkk.RowKeyCTimestamp] = currentTime.UnixMilli()
		ret[wkk.RowKeyCTsns] = currentTime.UnixNano()
		timestampFallbackCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("signal_type", "metrics"),
			attribute.String("reason", "current_fallback"),
		))
	}

	var value float64
	if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
		value = float64(dp.IntValue())
	} else {
		value = dp.DoubleValue()
	}

	if math.IsNaN(value) || math.IsInf(value, 0) {
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "IngestProtoMetricsReader"),
			attribute.String("metric_type", metricType),
			attribute.String("reason", "nan_or_inf"),
		))
		return true, nil
	}

	const alpha = 0.01
	sketch, err := ddsketch.NewDefaultDDSketch(alpha)
	if err != nil {
		return false, fmt.Errorf("failed to create sketch for %s: %w", metricType, err)
	}
	if err := sketch.Add(value); err != nil {
		return false, fmt.Errorf("failed to add value to sketch: %w", err)
	}

	// Encode the sketch
	sketchBytes := helpers.EncodeSketch(sketch)

	ret[wkk.RowKeySketch] = sketchBytes
	ret[wkk.RowKeyRollupAvg] = value
	ret[wkk.RowKeyRollupMax] = value
	ret[wkk.RowKeyRollupMin] = value
	ret[wkk.RowKeyRollupCount] = float64(1)
	ret[wkk.RowKeyRollupSum] = value
	ret[wkk.RowKeyRollupP25] = value
	ret[wkk.RowKeyRollupP50] = value
	ret[wkk.RowKeyRollupP75] = value
	ret[wkk.RowKeyRollupP90] = value
	ret[wkk.RowKeyRollupP95] = value
	ret[wkk.RowKeyRollupP99] = value

	return false, nil
}

func (r *IngestProtoMetricsReader) addHistogramDatapointFields(ctx context.Context, ret pipeline.Row, dp pmetric.HistogramDataPoint) (bool, error) {
	hasCounts := false
	for i := 0; i < dp.BucketCounts().Len(); i++ {
		if dp.BucketCounts().At(i) > 0 {
			hasCounts = true
			break
		}
	}
	if !hasCounts {
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "IngestProtoMetricsReader"),
			attribute.String("metric_type", "histogram"),
			attribute.String("reason", "no_counts"),
		))
		return true, nil
	}

	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		ret[prefixAttributeRowKey(name, "attr")] = v.AsString()
		return true
	})

	if dp.Timestamp() != 0 {
		ret[wkk.RowKeyCTimestamp] = dp.Timestamp().AsTime().UnixMilli()
		ret[wkk.RowKeyCTsns] = int64(dp.Timestamp())
	} else if dp.StartTimestamp() != 0 {
		ret[wkk.RowKeyCTimestamp] = dp.StartTimestamp().AsTime().UnixMilli()
		ret[wkk.RowKeyCTsns] = int64(dp.StartTimestamp())
		timestampFallbackCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("signal_type", "metrics"),
			attribute.String("reason", "start_timestamp"),
		))
	} else {
		// Fallback to current time when both timestamps are zero
		currentTime := time.Now()
		ret[wkk.RowKeyCTimestamp] = currentTime.UnixMilli()
		ret[wkk.RowKeyCTsns] = currentTime.UnixNano()
		timestampFallbackCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("signal_type", "metrics"),
			attribute.String("reason", "current_fallback"),
		))
	}

	const alpha = 0.01
	sketch, err := ddsketch.NewDefaultDDSketch(alpha)
	if err != nil {
		return false, fmt.Errorf("failed to create sketch for histogram: %w", err)
	}

	m := dp.ExplicitBounds().Len()
	n := dp.BucketCounts().Len()
	if n == 0 {
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "IngestProtoMetricsReader"),
			attribute.String("metric_type", "histogram"),
			attribute.String("reason", "no_bucket_counts"),
		))
		return true, nil
	}

	bounds := make([]float64, m)
	for i := range m {
		bounds[i] = dp.ExplicitBounds().At(i)
	}
	counts := make([]uint64, n)
	for i := range n {
		counts[i] = dp.BucketCounts().At(i)
	}

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
				return false, fmt.Errorf("failed to add single bucket to sketch: %w", err)
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
					return false, fmt.Errorf("failed to import finite histogram buckets: %w", err)
				}
			}
		}

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
					return false, fmt.Errorf("failed to add underflow to sketch: %w", err)
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
					return false, fmt.Errorf("failed to add overflow to sketch: %w", err)
				}
			}
		}
	}

	maxvalue, err := sketch.GetMaxValue()
	if err != nil {
		return false, fmt.Errorf("failed to get max value from non-empty sketch: %w", err)
	}
	minvalue, err := sketch.GetMinValue()
	if err != nil {
		return false, fmt.Errorf("failed to get min value from non-empty sketch: %w", err)
	}
	quantiles, err := sketch.GetValuesAtQuantiles([]float64{0.25, 0.5, 0.75, 0.90, 0.95, 0.99})
	if err != nil {
		return false, fmt.Errorf("failed to get quantiles from non-empty sketch: %w", err)
	}
	if len(quantiles) < 6 {
		return false, fmt.Errorf("expected 6 quantiles, got %d", len(quantiles))
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

	ret[wkk.RowKeySketch] = helpers.EncodeSketch(sketch)
	return false, nil
}

// addExponentialHistogramDatapointFields adds fields from an ExponentialHistogramDataPoint to the row.
func (r *IngestProtoMetricsReader) addExponentialHistogramDatapointFields(ctx context.Context, ret pipeline.Row, dp pmetric.ExponentialHistogramDataPoint) (bool, error) {
	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		key := r.attrCache.Get(name)
		value := v.AsString()
		ret[key] = value
		return true
	})

	if dp.Timestamp() != 0 {
		ret[wkk.RowKeyCTimestamp] = dp.Timestamp().AsTime().UnixMilli()
		ret[wkk.RowKeyCTsns] = int64(dp.Timestamp())
	} else if dp.StartTimestamp() != 0 {
		ret[wkk.RowKeyCTimestamp] = dp.StartTimestamp().AsTime().UnixMilli()
		ret[wkk.RowKeyCTsns] = int64(dp.StartTimestamp())
		timestampFallbackCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("signal_type", "metrics"),
			attribute.String("reason", "start_timestamp"),
		))
	} else {
		// Fallback to current time when both timestamps are zero
		currentTime := time.Now()
		ret[wkk.RowKeyCTimestamp] = currentTime.UnixMilli()
		ret[wkk.RowKeyCTsns] = currentTime.UnixNano()
		timestampFallbackCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("signal_type", "metrics"),
			attribute.String("reason", "current_fallback"),
		))
	}

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

	expHist := metricmath.ExpHist{
		Scale:     dp.Scale(),
		ZeroCount: dp.ZeroCount(),
		PosOffset: dp.Positive().Offset(),
		PosCounts: positiveBuckets,
		NegOffset: dp.Negative().Offset(),
		NegCounts: negativeBuckets,
	}
	counts, values := metricmath.ConvertExponentialHistogramToValues(expHist)

	hasData := false
	for _, count := range counts {
		if count > 0 {
			hasData = true
			break
		}
	}

	if !hasData {
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "IngestProtoMetricsReader"),
			attribute.String("metric_type", "exponential_histogram"),
			attribute.String("reason", "no_data"),
		))
		return true, nil
	}

	sketch, err := ddsketch.NewDefaultDDSketch(0.01)
	if err != nil {
		return false, fmt.Errorf("failed to create sketch for exponential histogram: %w", err)
	}

	for i, count := range counts {
		if count > 0 {
			if err := sketch.AddWithCount(values[i], count); err != nil {
				continue
			}
		}
	}

	maxvalue, err := sketch.GetMaxValue()
	if err != nil {
		return false, fmt.Errorf("failed to get max value from non-empty sketch: %w", err)
	}
	minvalue, err := sketch.GetMinValue()
	if err != nil {
		return false, fmt.Errorf("failed to get min value from non-empty sketch: %w", err)
	}
	quantiles, err := sketch.GetValuesAtQuantiles([]float64{0.25, 0.5, 0.75, 0.90, 0.95, 0.99})
	if err != nil {
		return false, fmt.Errorf("failed to get quantiles from non-empty sketch: %w", err)
	}
	if len(quantiles) < 6 {
		return false, fmt.Errorf("expected 6 quantiles, got %d", len(quantiles))
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

	ret[wkk.RowKeySketch] = helpers.EncodeSketch(sketch)
	return false, nil
}

// addSummaryDatapointFields adds fields from a SummaryDataPoint to the row.
// Returns (dropped, error) where dropped indicates if the datapoint was filtered out.
func (r *IngestProtoMetricsReader) addSummaryDatapointFields(ctx context.Context, ret pipeline.Row, dp pmetric.SummaryDataPoint) (bool, error) {
	// Add attributes
	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		key := r.attrCache.Get(name)
		value := v.AsString()
		ret[key] = value
		return true
	})

	// Set timestamp
	if dp.Timestamp() != 0 {
		ret[wkk.RowKeyCTimestamp] = dp.Timestamp().AsTime().UnixMilli()
		ret[wkk.RowKeyCTsns] = int64(dp.Timestamp())
	} else if dp.StartTimestamp() != 0 {
		ret[wkk.RowKeyCTimestamp] = dp.StartTimestamp().AsTime().UnixMilli()
		ret[wkk.RowKeyCTsns] = int64(dp.StartTimestamp())
		timestampFallbackCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("signal_type", "metrics"),
			attribute.String("reason", "start_timestamp"),
		))
	} else {
		// Fallback to current time when both timestamps are zero
		currentTime := time.Now()
		ret[wkk.RowKeyCTimestamp] = currentTime.UnixMilli()
		ret[wkk.RowKeyCTsns] = currentTime.UnixNano()
		timestampFallbackCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("signal_type", "metrics"),
			attribute.String("reason", "current_fallback"),
		))
	}

	// Check if we got any data in the sketch (no quantiles)
	if dp.Count() == 0 || dp.QuantileValues().Len() == 0 {
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "IngestProtoMetricsReader"),
			attribute.String("metric_type", "summary"),
			attribute.String("reason", "summary_no_quantiles"),
		))
		return true, nil // drop the row
	}

	// Convert summary to DDSketch
	sketch, err := summaryToDDSketch(dp)
	if err != nil {
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "IngestProtoMetricsReader"),
			attribute.String("metric_type", "summary"),
			attribute.String("reason", "sketch_conversion_failed"),
		))
		return true, nil // drop the row
	}

	// Update rollup fields from the sketch
	count := sketch.GetCount()
	sum := sketch.GetSum()

	ret[wkk.RowKeyRollupCount] = count
	ret[wkk.RowKeyRollupSum] = sum
	ret[wkk.RowKeyRollupAvg] = sum / count

	// Get min and max
	minVal, err := sketch.GetMinValue()
	if err == nil {
		ret[wkk.RowKeyRollupMin] = minVal
	} else {
		ret[wkk.RowKeyRollupMin] = 0.0
	}

	maxVal, err := sketch.GetMaxValue()
	if err == nil {
		ret[wkk.RowKeyRollupMax] = maxVal
	} else {
		ret[wkk.RowKeyRollupMax] = 0.0
	}

	// Get percentiles
	quantiles := []float64{0.25, 0.50, 0.75, 0.90, 0.95, 0.99}
	for i, q := range quantiles {
		val, err := sketch.GetValueAtQuantile(q)
		if err != nil {
			val = 0.0
		}
		switch i {
		case 0:
			ret[wkk.RowKeyRollupP25] = val
		case 1:
			ret[wkk.RowKeyRollupP50] = val
		case 2:
			ret[wkk.RowKeyRollupP75] = val
		case 3:
			ret[wkk.RowKeyRollupP90] = val
		case 4:
			ret[wkk.RowKeyRollupP95] = val
		case 5:
			ret[wkk.RowKeyRollupP99] = val
		}
	}

	// Encode and store the sketch
	ret[wkk.RowKeySketch] = helpers.EncodeSketch(sketch)
	return false, nil
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

// GetSchema returns the schema extracted from the OTEL metrics.
func (r *IngestProtoMetricsReader) GetSchema() *ReaderSchema {
	return r.schema
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
