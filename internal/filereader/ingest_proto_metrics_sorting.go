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
	"slices"
	"strings"
	"time"

	"github.com/DataDog/sketches-go/ddsketch"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/ddcache"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/metricmath"
	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// SortingIngestProtoMetricsReader reads OTEL protobuf metrics, applies translation,
// and returns rows pre-sorted by [name, tid, timestamp].
//
// This combines the functionality of:
// - IngestProtoMetricsReader (proto parsing)
// - TranslatingReader (TID computation, timestamp truncation)
// - DiskSortingReader (sorting)
//
// Memory optimization: Instead of materializing all rows upfront, we store only
// lightweight indices into the OTEL structure. Rows are built lazily during iteration.
type SortingIngestProtoMetricsReader struct {
	closed      bool
	rowCount    int64
	rowsSkipped int64
	batchSize   int

	// Translation context
	orgID    string
	bucket   string
	objectID string

	// Store the original OTEL metrics
	otelMetrics *pmetric.Metrics

	// Schema extracted from all metrics
	schema *ReaderSchema

	// Cached RowKeys for attribute names
	resourceAttrCache *PrefixedRowKeyCache
	scopeAttrCache    *PrefixedRowKeyCache
	attrCache         *PrefixedRowKeyCache

	// Sorted datapoints - lightweight indices instead of full rows
	sortedDatapoints []sortableDatapoint
	currentIndex     int
	sorted           bool

	// Fallback timestamp for datapoints missing timestamps.
	// Captured once at reader creation to ensure consistency between
	// sort key computation and row building.
	fallbackTimestamp time.Time
}

// sortableDatapoint holds the sort key and indices into the OTEL structure.
// This is much smaller than a full Row (~56 bytes vs 500+ bytes).
type sortableDatapoint struct {
	// Sort key fields
	name      string
	tid       int64
	timestamp int64

	// Indices into OTEL structure for lazy row building
	resourceIdx int
	scopeIdx    int
	metricIdx   int
	dpIdx       int
}

var _ Reader = (*SortingIngestProtoMetricsReader)(nil)

// SortingReaderOptions contains options for creating a SortingIngestProtoMetricsReader
type SortingReaderOptions struct {
	OrgID     string
	Bucket    string
	ObjectID  string
	BatchSize int
}

// NewSortingIngestProtoMetricsReader creates a new sorting proto metrics reader.
func NewSortingIngestProtoMetricsReader(reader io.Reader, opts SortingReaderOptions) (*SortingIngestProtoMetricsReader, error) {
	metrics, err := parseProtoToOtelMetrics(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto to OTEL metrics: %w", err)
	}

	return NewSortingIngestProtoMetricsReaderFromMetrics(metrics, opts)
}

// NewSortingIngestProtoMetricsReaderFromMetrics creates a reader from pre-parsed metrics.
func NewSortingIngestProtoMetricsReaderFromMetrics(metrics *pmetric.Metrics, opts SortingReaderOptions) (*SortingIngestProtoMetricsReader, error) {
	if metrics == nil {
		return nil, fmt.Errorf("metrics cannot be nil")
	}

	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	schema := extractSchemaFromOTELMetrics(metrics)

	return &SortingIngestProtoMetricsReader{
		otelMetrics:       metrics,
		orgID:             opts.OrgID,
		bucket:            opts.Bucket,
		objectID:          opts.ObjectID,
		batchSize:         batchSize,
		schema:            schema,
		resourceAttrCache: NewPrefixedRowKeyCache("resource"),
		scopeAttrCache:    NewPrefixedRowKeyCache("scope"),
		attrCache:         NewPrefixedRowKeyCache("attr"),
		fallbackTimestamp: time.Now(),
	}, nil
}

// Next returns the next batch of sorted rows.
func (r *SortingIngestProtoMetricsReader) Next(ctx context.Context) (*Batch, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	// On first call, collect sort keys and indices, then sort
	if !r.sorted {
		if err := r.collectAndSort(ctx); err != nil {
			return nil, err
		}
	}

	// Check if we've exhausted all datapoints
	if r.currentIndex >= len(r.sortedDatapoints) {
		return nil, io.EOF
	}

	batch := pipeline.GetBatch()

	// Build rows lazily from sorted indices
	for batch.Len() < r.batchSize && r.currentIndex < len(r.sortedDatapoints) {
		dp := &r.sortedDatapoints[r.currentIndex]
		row, dropped, err := r.buildRowFromIndices(ctx, dp)
		if err != nil {
			slog.Error("Failed to build row from indices", "error", err)
			r.currentIndex++
			continue
		}
		if dropped {
			r.rowsSkipped++
			r.currentIndex++
			continue
		}

		batch.AppendRow(row)
		r.currentIndex++
	}

	if batch.Len() > 0 {
		r.rowCount += int64(batch.Len())
		rowsOutCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
			attribute.String("reader", "SortingIngestProtoMetricsReader"),
		))
		return batch, nil
	}

	pipeline.ReturnBatch(batch)
	return nil, io.EOF
}

// collectAndSort collects sort keys and indices for all datapoints, then sorts.
// This is memory-efficient because we only store ~56 bytes per datapoint instead of full rows.
func (r *SortingIngestProtoMetricsReader) collectAndSort(ctx context.Context) error {
	// Pre-allocate with exact count from OTEL metrics
	r.sortedDatapoints = make([]sortableDatapoint, 0, r.otelMetrics.DataPointCount())

	// Iterate through all datapoints, collecting only sort keys and indices
	for ri := range r.otelMetrics.ResourceMetrics().Len() {
		rm := r.otelMetrics.ResourceMetrics().At(ri)

		for si := range rm.ScopeMetrics().Len() {
			sm := rm.ScopeMetrics().At(si)

			for mi := range sm.Metrics().Len() {
				metric := sm.Metrics().At(mi)
				r.collectMetricDatapoints(ctx, ri, si, mi, rm, metric)
			}
		}
	}

	// Sort by [name, tid, timestamp]
	slices.SortFunc(r.sortedDatapoints, func(a, b sortableDatapoint) int {
		// Compare name first
		if cmp := strings.Compare(a.name, b.name); cmp != 0 {
			return cmp
		}
		// Then TID
		if a.tid != b.tid {
			if a.tid < b.tid {
				return -1
			}
			return 1
		}
		// Then timestamp
		if a.timestamp < b.timestamp {
			return -1
		}
		if a.timestamp > b.timestamp {
			return 1
		}
		return 0
	})

	r.sorted = true
	return nil
}

// collectMetricDatapoints collects sort keys and indices for all datapoints of a metric.
func (r *SortingIngestProtoMetricsReader) collectMetricDatapoints(
	ctx context.Context,
	resourceIdx, scopeIdx, metricIdx int,
	rm pmetric.ResourceMetrics,
	metric pmetric.Metric,
) {
	metricName := wkk.NormalizeName(metric.Name())
	if metricName == "" {
		return // Skip metrics with empty names
	}

	datapointCount := getDatapointCount(metric)
	resourceAttrs := rm.Resource().Attributes()

	for di := range datapointCount {
		rowsInCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "SortingIngestProtoMetricsReader"),
		))

		// Get datapoint attributes and timestamp
		dpAttrs := fingerprinter.GetDatapointAttributes(metric, di)
		timestamp := r.getDatapointTimestamp(metric, di)

		// Truncate timestamp to 10-second intervals (matching translation logic)
		const tenSecondsMs = int64(10000)
		truncatedTs := (timestamp / tenSecondsMs) * tenSecondsMs

		// Compute TID directly from OTEL structures
		tid := fingerprinter.ComputeTIDFromOTEL(resourceAttrs, dpAttrs, metricName, metric.Type())

		r.sortedDatapoints = append(r.sortedDatapoints, sortableDatapoint{
			name:        metricName,
			tid:         tid,
			timestamp:   truncatedTs,
			resourceIdx: resourceIdx,
			scopeIdx:    scopeIdx,
			metricIdx:   metricIdx,
			dpIdx:       di,
		})
	}
}

// getDatapointTimestamp extracts the timestamp from any datapoint type.
func (r *SortingIngestProtoMetricsReader) getDatapointTimestamp(metric pmetric.Metric, dpIdx int) int64 {
	var ts, startTs pcommon.Timestamp

	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dp := metric.Gauge().DataPoints().At(dpIdx)
		ts, startTs = dp.Timestamp(), dp.StartTimestamp()
	case pmetric.MetricTypeSum:
		dp := metric.Sum().DataPoints().At(dpIdx)
		ts, startTs = dp.Timestamp(), dp.StartTimestamp()
	case pmetric.MetricTypeHistogram:
		dp := metric.Histogram().DataPoints().At(dpIdx)
		ts, startTs = dp.Timestamp(), dp.StartTimestamp()
	case pmetric.MetricTypeExponentialHistogram:
		dp := metric.ExponentialHistogram().DataPoints().At(dpIdx)
		ts, startTs = dp.Timestamp(), dp.StartTimestamp()
	case pmetric.MetricTypeSummary:
		dp := metric.Summary().DataPoints().At(dpIdx)
		ts, startTs = dp.Timestamp(), dp.StartTimestamp()
	}

	if ts != 0 {
		return ts.AsTime().UnixMilli()
	}
	if startTs != 0 {
		return startTs.AsTime().UnixMilli()
	}
	return r.fallbackTimestamp.UnixMilli()
}

// buildRowFromIndices builds a full row from the OTEL structure using stored indices.
func (r *SortingIngestProtoMetricsReader) buildRowFromIndices(
	ctx context.Context,
	sdp *sortableDatapoint,
) (pipeline.Row, bool, error) {
	rm := r.otelMetrics.ResourceMetrics().At(sdp.resourceIdx)
	sm := rm.ScopeMetrics().At(sdp.scopeIdx)
	metric := sm.Metrics().At(sdp.metricIdx)

	row := pipeline.GetPooledRow()

	dropped, err := r.buildAndTranslateRow(ctx, row, rm, sm, metric, sdp.dpIdx, sdp.tid)
	if err != nil || dropped {
		pipeline.ReturnPooledRow(row)
		return nil, dropped, err
	}

	return row, false, nil
}

// buildAndTranslateRow builds a row from a datapoint and applies translation.
func (r *SortingIngestProtoMetricsReader) buildAndTranslateRow(
	ctx context.Context,
	row pipeline.Row,
	rm pmetric.ResourceMetrics,
	sm pmetric.ScopeMetrics,
	metric pmetric.Metric,
	datapointIndex int,
	sortingTID int64,
) (bool, error) {
	// Add resource attributes
	rm.Resource().Attributes().Range(func(name string, v pcommon.Value) bool {
		key := r.resourceAttrCache.Get(name)
		row[key] = v.AsString()
		return true
	})

	// Add scope attributes
	sm.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
		key := r.scopeAttrCache.Get(name)
		row[key] = v.AsString()
		return true
	})

	row[wkk.NewRowKey("chq_scope_url")] = sm.Scope().Version()
	row[wkk.NewRowKey("chq_scope_name")] = sm.Scope().Name()

	metricName := wkk.NormalizeName(metric.Name())
	if metricName == "" {
		return true, nil // dropped
	}
	row[wkk.RowKeyCName] = metricName

	row[wkk.NewRowKey("chq_description")] = metric.Description()
	row[wkk.NewRowKey("chq_unit")] = metric.Unit()

	// Set metric type
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		row[wkk.RowKeyCMetricType] = "gauge"
	case pmetric.MetricTypeSum:
		row[wkk.RowKeyCMetricType] = "count"
	case pmetric.MetricTypeHistogram, pmetric.MetricTypeExponentialHistogram, pmetric.MetricTypeSummary:
		row[wkk.RowKeyCMetricType] = "histogram"
	default:
		row[wkk.RowKeyCMetricType] = "gauge"
	}

	// Add datapoint-specific fields
	var dropped bool
	var err error

	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		dp := metric.Gauge().DataPoints().At(datapointIndex)
		dropped, err = r.addNumberDatapointFields(ctx, row, dp, "gauge")
	case pmetric.MetricTypeSum:
		dp := metric.Sum().DataPoints().At(datapointIndex)
		dropped, err = r.addNumberDatapointFields(ctx, row, dp, "sum")
	case pmetric.MetricTypeHistogram:
		dp := metric.Histogram().DataPoints().At(datapointIndex)
		dropped, err = r.addHistogramDatapointFields(ctx, row, dp)
	case pmetric.MetricTypeExponentialHistogram:
		dp := metric.ExponentialHistogram().DataPoints().At(datapointIndex)
		dropped, err = r.addExponentialHistogramDatapointFields(ctx, row, dp)
	case pmetric.MetricTypeSummary:
		dp := metric.Summary().DataPoints().At(datapointIndex)
		dropped, err = r.addSummaryDatapointFields(ctx, row, dp)
	}

	if err != nil || dropped {
		return dropped, err
	}

	// Apply translation (from MetricTranslator)
	// Pass the sorting TID - we use it directly instead of recomputing
	r.applyTranslation(row, sortingTID)

	return false, nil
}

// applyTranslation applies the MetricTranslator logic to the row.
// The sortingTID is the TID computed during sorting (from ComputeTIDFromOTEL).
// We use it directly instead of recomputing - the test validates that
// ComputeTIDFromOTEL produces the same value as ComputeTID(row) would.
func (r *SortingIngestProtoMetricsReader) applyTranslation(row pipeline.Row, sortingTID int64) {
	// Add required fields
	row[wkk.RowKeyCCustomerID] = r.orgID
	row[wkk.RowKeyCTelemetryType] = "metrics"

	// Truncate timestamp to 10-second intervals
	if ts, ok := row[wkk.RowKeyCTimestamp].(int64); ok {
		const tenSecondsMs = int64(10000)
		row[wkk.RowKeyCTimestamp] = (ts / tenSecondsMs) * tenSecondsMs
	}

	// Filter resource keys (keep only specified keys)
	r.filterResourceKeys(row)

	// Use the sorting TID directly
	row[wkk.RowKeyCTID] = sortingTID
}

func (r *SortingIngestProtoMetricsReader) filterResourceKeys(row pipeline.Row) {
	for k := range row {
		name := wkk.RowKeyValue(k)
		if !strings.HasPrefix(name, "resource_") {
			continue
		}
		// Strip "resource_" prefix to check against KeepResourceKeys
		unprefixed := strings.TrimPrefix(name, "resource_")
		if fingerprinter.KeepResourceKeys[unprefixed] {
			continue
		}
		delete(row, k)
	}
}

// getDatapointCount returns the number of datapoints for a metric.
func getDatapointCount(metric pmetric.Metric) int {
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

// addNumberDatapointFields adds fields from a NumberDataPoint.
func (r *SortingIngestProtoMetricsReader) addNumberDatapointFields(ctx context.Context, row pipeline.Row, dp pmetric.NumberDataPoint, metricType string) (bool, error) {
	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		key := r.attrCache.Get(name)
		row[key] = v.AsString()
		return true
	})

	r.setTimestamp(ctx, row, dp.Timestamp(), dp.StartTimestamp())

	var value float64
	if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
		value = float64(dp.IntValue())
	} else {
		value = dp.DoubleValue()
	}

	if math.IsNaN(value) || math.IsInf(value, 0) {
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "SortingIngestProtoMetricsReader"),
			attribute.String("metric_type", metricType),
			attribute.String("reason", "nan_or_inf"),
		))
		return true, nil
	}

	sketchBytes, err := ddcache.Get().GetBytesForValue(value)
	if err != nil {
		return false, fmt.Errorf("failed to encode sketch for %s: %w", metricType, err)
	}

	row[wkk.RowKeySketch] = sketchBytes
	row[wkk.RowKeyRollupAvg] = value
	row[wkk.RowKeyRollupMax] = value
	row[wkk.RowKeyRollupMin] = value
	row[wkk.RowKeyRollupCount] = float64(1)
	row[wkk.RowKeyRollupSum] = value
	row[wkk.RowKeyRollupP25] = value
	row[wkk.RowKeyRollupP50] = value
	row[wkk.RowKeyRollupP75] = value
	row[wkk.RowKeyRollupP90] = value
	row[wkk.RowKeyRollupP95] = value
	row[wkk.RowKeyRollupP99] = value

	return false, nil
}

// setTimestamp sets the timestamp fields on a row.
func (r *SortingIngestProtoMetricsReader) setTimestamp(ctx context.Context, row pipeline.Row, ts, startTs pcommon.Timestamp) {
	if ts != 0 {
		row[wkk.RowKeyCTimestamp] = ts.AsTime().UnixMilli()
		row[wkk.RowKeyCTsns] = int64(ts)
	} else if startTs != 0 {
		row[wkk.RowKeyCTimestamp] = startTs.AsTime().UnixMilli()
		row[wkk.RowKeyCTsns] = int64(startTs)
		timestampFallbackCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("signal_type", "metrics"),
			attribute.String("reason", "start_timestamp"),
		))
	} else {
		// Use the fallback timestamp captured at reader creation to ensure
		// consistency with the sort key computed during collectAndSort.
		row[wkk.RowKeyCTimestamp] = r.fallbackTimestamp.UnixMilli()
		row[wkk.RowKeyCTsns] = r.fallbackTimestamp.UnixNano()
		timestampFallbackCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("signal_type", "metrics"),
			attribute.String("reason", "current_fallback"),
		))
	}
}

// addHistogramDatapointFields adds fields from a HistogramDataPoint.
func (r *SortingIngestProtoMetricsReader) addHistogramDatapointFields(ctx context.Context, row pipeline.Row, dp pmetric.HistogramDataPoint) (bool, error) {
	hasCounts := false
	for i := 0; i < dp.BucketCounts().Len(); i++ {
		if dp.BucketCounts().At(i) > 0 {
			hasCounts = true
			break
		}
	}
	if !hasCounts {
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "SortingIngestProtoMetricsReader"),
			attribute.String("metric_type", "histogram"),
			attribute.String("reason", "no_counts"),
		))
		return true, nil
	}

	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		row[prefixAttributeRowKey(name, "attr")] = v.AsString()
		return true
	})

	r.setTimestamp(ctx, row, dp.Timestamp(), dp.StartTimestamp())

	const alpha = 0.01
	sketch, err := helpers.GetSketch()
	if err != nil {
		return false, fmt.Errorf("failed to get sketch for histogram: %w", err)
	}

	m := dp.ExplicitBounds().Len()
	n := dp.BucketCounts().Len()
	if n == 0 {
		helpers.PutSketch(sketch)
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "SortingIngestProtoMetricsReader"),
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

	if m == 0 && n == 1 {
		totalCount := counts[0]
		if totalCount > 0 {
			rep := 0.0
			if dp.HasSum() && totalCount > 0 {
				rep = dp.Sum() / float64(totalCount)
			}
			if err := sketch.AddWithCount(rep, float64(totalCount)); err != nil {
				helpers.PutSketch(sketch)
				return false, fmt.Errorf("failed to add single bucket to sketch: %w", err)
			}
		}
	} else {
		if m >= 2 && n >= 2 {
			lower := bounds[0]
			finiteCutoffs := make([]float64, 0, max(0, m-1))
			finiteCounts := make([]uint64, 0, max(0, m-1))
			for i := 1; i < m; i++ {
				finiteCutoffs = append(finiteCutoffs, bounds[i])
			}
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
					nil,
				); err != nil {
					helpers.PutSketch(sketch)
					return false, fmt.Errorf("failed to import finite histogram buckets: %w", err)
				}
			}
		}

		if m > 0 {
			gamma := (1 + alpha) / (1 - alpha)
			underflow := counts[0]
			overflow := uint64(0)
			if n == m+1 {
				overflow = counts[n-1]
			} else if n > m+1 {
				overflow = counts[n-1]
			}

			if underflow > 0 {
				rep := bounds[0]
				if rep > 0 {
					rep = rep / math.Sqrt(gamma)
				} else {
					step := math.Max(1.0, math.Abs(rep)*0.01)
					rep = rep - step
				}
				if err := sketch.AddWithCount(rep, float64(underflow)); err != nil {
					helpers.PutSketch(sketch)
					return false, fmt.Errorf("failed to add underflow to sketch: %w", err)
				}
			}

			if overflow > 0 {
				last := bounds[m-1]
				rep := last
				if rep > 0 {
					rep = rep * math.Sqrt(gamma)
				} else {
					step := math.Max(1.0, math.Abs(rep)*0.01)
					rep = rep + step
				}
				if err := sketch.AddWithCount(rep, float64(overflow)); err != nil {
					helpers.PutSketch(sketch)
					return false, fmt.Errorf("failed to add overflow to sketch: %w", err)
				}
			}
		}
	}

	return r.finalizeSketchFields(row, sketch)
}

// addExponentialHistogramDatapointFields adds fields from an ExponentialHistogramDataPoint.
func (r *SortingIngestProtoMetricsReader) addExponentialHistogramDatapointFields(ctx context.Context, row pipeline.Row, dp pmetric.ExponentialHistogramDataPoint) (bool, error) {
	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		key := r.attrCache.Get(name)
		row[key] = v.AsString()
		return true
	})

	r.setTimestamp(ctx, row, dp.Timestamp(), dp.StartTimestamp())

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
			attribute.String("reader", "SortingIngestProtoMetricsReader"),
			attribute.String("metric_type", "exponential_histogram"),
			attribute.String("reason", "no_data"),
		))
		return true, nil
	}

	sketch, err := helpers.GetSketch()
	if err != nil {
		return false, fmt.Errorf("failed to get sketch for exponential histogram: %w", err)
	}

	for i, count := range counts {
		if count > 0 {
			_ = sketch.AddWithCount(values[i], count)
		}
	}

	return r.finalizeSketchFields(row, sketch)
}

// addSummaryDatapointFields adds fields from a SummaryDataPoint.
func (r *SortingIngestProtoMetricsReader) addSummaryDatapointFields(ctx context.Context, row pipeline.Row, dp pmetric.SummaryDataPoint) (bool, error) {
	dp.Attributes().Range(func(name string, v pcommon.Value) bool {
		key := r.attrCache.Get(name)
		row[key] = v.AsString()
		return true
	})

	r.setTimestamp(ctx, row, dp.Timestamp(), dp.StartTimestamp())

	if dp.Count() == 0 || dp.QuantileValues().Len() == 0 {
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "SortingIngestProtoMetricsReader"),
			attribute.String("metric_type", "summary"),
			attribute.String("reason", "summary_no_quantiles"),
		))
		return true, nil
	}

	sketch, err := summaryToDDSketch(dp)
	if err != nil {
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "SortingIngestProtoMetricsReader"),
			attribute.String("metric_type", "summary"),
			attribute.String("reason", "sketch_conversion_failed"),
		))
		return true, nil
	}

	return r.finalizeSketchFields(row, sketch)
}

// finalizeSketchFields extracts statistics from sketch and adds to row.
func (r *SortingIngestProtoMetricsReader) finalizeSketchFields(row pipeline.Row, sketch *ddsketch.DDSketch) (bool, error) {
	maxvalue, err := sketch.GetMaxValue()
	if err != nil {
		helpers.PutSketch(sketch)
		return false, fmt.Errorf("failed to get max value: %w", err)
	}
	minvalue, err := sketch.GetMinValue()
	if err != nil {
		helpers.PutSketch(sketch)
		return false, fmt.Errorf("failed to get min value: %w", err)
	}
	quantiles, err := sketch.GetValuesAtQuantiles([]float64{0.25, 0.5, 0.75, 0.90, 0.95, 0.99})
	if err != nil {
		helpers.PutSketch(sketch)
		return false, fmt.Errorf("failed to get quantiles: %w", err)
	}
	if len(quantiles) < 6 {
		helpers.PutSketch(sketch)
		return false, fmt.Errorf("expected 6 quantiles, got %d", len(quantiles))
	}

	count := sketch.GetCount()
	sum := sketch.GetSum()
	avg := sum / count

	row[wkk.RowKeyRollupAvg] = avg
	row[wkk.RowKeyRollupMax] = maxvalue
	row[wkk.RowKeyRollupMin] = minvalue
	row[wkk.RowKeyRollupCount] = count
	row[wkk.RowKeyRollupSum] = sum
	row[wkk.RowKeyRollupP25] = quantiles[0]
	row[wkk.RowKeyRollupP50] = quantiles[1]
	row[wkk.RowKeyRollupP75] = quantiles[2]
	row[wkk.RowKeyRollupP90] = quantiles[3]
	row[wkk.RowKeyRollupP95] = quantiles[4]
	row[wkk.RowKeyRollupP99] = quantiles[5]

	row[wkk.RowKeySketch] = helpers.EncodeAndReturnSketch(sketch)
	return false, nil
}

// Close closes the reader.
func (r *SortingIngestProtoMetricsReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Release sorted datapoints (no pooled resources to return)
	r.sortedDatapoints = nil
	r.otelMetrics = nil

	return nil
}

// TotalRowsReturned returns the number of rows returned.
func (r *SortingIngestProtoMetricsReader) TotalRowsReturned() int64 {
	return r.rowCount
}

// GetSchema returns the schema.
func (r *SortingIngestProtoMetricsReader) GetSchema() *ReaderSchema {
	return r.schema
}

// GetOTELMetrics returns the underlying OTEL metrics for exemplar processing.
func (r *SortingIngestProtoMetricsReader) GetOTELMetrics() (any, error) {
	if r.otelMetrics == nil {
		return nil, fmt.Errorf("OTEL metrics not available")
	}
	return r.otelMetrics, nil
}
