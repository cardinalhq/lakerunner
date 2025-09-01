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

	"github.com/DataDog/sketches-go/ddsketch"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// AggregatingMetricsReader wraps a sorted Reader to perform streaming aggregation of metrics.
// It aggregates rows with the same [metric_name, tid, truncated_timestamp] key.
// The underlying reader must return rows in sorted order by this key.
type AggregatingMetricsReader struct {
	reader            Reader
	aggregationPeriod int64 // milliseconds (e.g., 10000 for 10s)
	closed            bool
	rowCount          int64
	batchSize         int

	// Current aggregation state
	// currentKey removed - using direct value storage to avoid pooling corruption
	groupedRows  map[string][]Row // metric_type -> rows for that type
	pendingBatch *Batch           // Unprocessed rows from underlying reader
	pendingIndex int              // Index of next row to process in pendingBatch
	readerEOF    bool

	// Sort key provider for grouping
	keyProvider SortKeyProvider

	// Stored key values (not pooled to avoid corruption)
	currentKeyName string
	currentKeyTid  int64
	currentKeyTs   int64
	hasCurrentKey  bool
}

// NewAggregatingMetricsReader creates a new AggregatingMetricsReader that aggregates metrics
// with the same [metric_name, tid, truncated_timestamp] key.
//
// aggregationPeriodMs: period in milliseconds for timestamp truncation (e.g., 10000 for 10s)
// reader: underlying reader that returns rows in sorted order by [metric_name, tid, timestamp]
func NewAggregatingMetricsReader(reader Reader, aggregationPeriodMs int64, batchSize int) (*AggregatingMetricsReader, error) {
	if reader == nil {
		return nil, fmt.Errorf("reader cannot be nil")
	}
	if aggregationPeriodMs <= 0 {
		return nil, fmt.Errorf("aggregation period must be positive, got %d", aggregationPeriodMs)
	}

	if batchSize <= 0 {
		batchSize = 1000
	}

	return &AggregatingMetricsReader{
		reader:            reader,
		aggregationPeriod: aggregationPeriodMs,
		batchSize:         batchSize,
		groupedRows:       make(map[string][]Row),
		keyProvider:       &MetricSortKeyProvider{},
	}, nil
}

// makeAggregationKey creates a key for aggregation from [metric_name, tid, timestamp].
// Assumes timestamp has already been truncated to aggregation period.
func (ar *AggregatingMetricsReader) makeAggregationKey(row Row) (SortKey, error) {
	// Validate required fields
	_, nameOk := row[wkk.RowKeyCName].(string)
	if !nameOk {
		return nil, fmt.Errorf("missing or invalid _cardinalhq.name field")
	}

	_, tidOk := row[wkk.RowKeyCTID].(int64)
	if !tidOk {
		return nil, fmt.Errorf("missing or invalid _cardinalhq.tid field")
	}

	_, tsOk := row[wkk.RowKeyCTimestamp].(int64)
	if !tsOk {
		return nil, fmt.Errorf("missing or invalid _cardinalhq.timestamp field")
	}

	// Use the row directly - timestamp should already be truncated
	return ar.keyProvider.MakeKey(row), nil
}

// isSketchEmpty checks if a row has an empty sketch (indicating singleton value).
func isSketchEmpty(row Row) bool {
	sketch := row[wkk.RowKeySketch]
	if sketch == nil {
		return true
	}

	// Handle byte slice format (from proto ingestion)
	if sketchBytes, ok := sketch.([]byte); ok {
		return len(sketchBytes) == 0
	}

	// Handle string format (from parquet reading)
	if sketchStr, ok := sketch.(string); ok {
		return len(sketchStr) == 0
	}

	// Unknown format - this would be a bug
	slog.Error("Unexpected sketch data type - expected []byte or string",
		"type", fmt.Sprintf("%T", sketch),
		"metric", row[wkk.RowKeyCName])
	return true
}

// getSingletonValue extracts the singleton value from rollup_sum field.
func getSingletonValue(row Row) (float64, bool) {
	value, ok := row[wkk.RowKeyRollupSum].(float64)
	return value, ok
}

// getSketchBytes converts sketch data from various parquet formats back to []byte.
// Handles: []byte (direct), string (encoded bytes)
func getSketchBytes(sketchData interface{}) ([]byte, error) {
	if sketchData == nil {
		return nil, fmt.Errorf("sketch data is nil")
	}

	// Case 1: Already []byte (from proto ingestion or correct parquet reading)
	if bytes, ok := sketchData.([]byte); ok {
		return bytes, nil
	}

	// Case 2: String format (parquet sometimes returns []byte as string)
	if str, ok := sketchData.(string); ok {
		return []byte(str), nil
	}

	return nil, fmt.Errorf("unsupported sketch data type: %T", sketchData)
}

// isHistogramType checks if a row represents a histogram metric type.
func isHistogramType(row Row) bool {
	metricType, ok := row[wkk.RowKeyCMetricType].(string)
	return ok && metricType == "histogram"
}

// updateRowFromSketch updates all rollup fields in a row based on the sketch.
// This should only be called for valid sketches with data.
func updateRowFromSketch(row Row, sketch *ddsketch.DDSketch) error {
	count := sketch.GetCount()
	if count == 0 {
		return fmt.Errorf("cannot update row from empty sketch")
	}

	sum := sketch.GetSum()

	row[wkk.RowKeyRollupCount] = count
	row[wkk.RowKeyRollupSum] = sum
	row[wkk.RowKeyRollupAvg] = sum / count

	maxValue, err := sketch.GetMaxValue()
	if err != nil {
		return fmt.Errorf("getting max value: %w", err)
	}
	row[wkk.RowKeyRollupMax] = maxValue

	minValue, err := sketch.GetMinValue()
	if err != nil {
		return fmt.Errorf("getting min value: %w", err)
	}
	row[wkk.RowKeyRollupMin] = minValue

	quantiles, err := sketch.GetValuesAtQuantiles([]float64{0.25, 0.50, 0.75, 0.90, 0.95, 0.99})
	if err != nil {
		return fmt.Errorf("getting quantiles: %w", err)
	}

	row[wkk.RowKeyRollupP25] = quantiles[0]
	row[wkk.RowKeyRollupP50] = quantiles[1]
	row[wkk.RowKeyRollupP75] = quantiles[2]
	row[wkk.RowKeyRollupP90] = quantiles[3]
	row[wkk.RowKeyRollupP95] = quantiles[4]
	row[wkk.RowKeyRollupP99] = quantiles[5]

	row[wkk.RowKeySketch] = helpers.EncodeSketch(sketch)

	return nil
}

// aggregateGroup processes all rows for a single aggregation group and returns the aggregated results.
// Returns multiple rows - one per metric type found in the group.
func (ar *AggregatingMetricsReader) aggregateGroup(ctx context.Context) ([]Row, error) {
	if len(ar.groupedRows) == 0 {
		// No rows in this group - return empty slice
		ar.resetAggregation()
		return nil, nil
	}

	var results []Row

	// Process each metric type group separately
	for metricType, rows := range ar.groupedRows {
		if len(rows) == 0 {
			continue
		}

		// Aggregate this specific metric type
		result, err := ar.aggregateRowGroup(ctx, metricType, rows)
		if err != nil {
			return nil, fmt.Errorf("aggregating metric type %q: %w", metricType, err)
		}

		// Only add to results if aggregation produced a valid result
		if result != nil {
			results = append(results, result)
		}
	}

	ar.resetAggregation()
	return results, nil
}

// aggregateRowGroup aggregates all rows within an aggregation group.
func (ar *AggregatingMetricsReader) aggregateRowGroup(ctx context.Context, metricType string, rows []Row) (Row, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	// Use the first row as the base for the aggregated result
	aggregatedRow := make(Row)
	maps.Copy(aggregatedRow, rows[0])

	// Separate processing based on metric type
	if metricType == "histogram" {
		return ar.aggregateHistogramGroup(ctx, aggregatedRow, rows)
	}

	return ar.aggregateCounterGaugeGroup(ctx, aggregatedRow, rows)
}

// aggregateHistogramGroup handles aggregation for histogram metrics.
// Histograms must always have sketches and only merge sketches.
func (ar *AggregatingMetricsReader) aggregateHistogramGroup(ctx context.Context, baseRow Row, rows []Row) (Row, error) {
	var currentSketch *ddsketch.DDSketch
	var singletonValues []float64

	for _, row := range rows {
		// Handle sketch or singleton
		if isSketchEmpty(row) {
			// This is a singleton - collect its value
			if value, ok := getSingletonValue(row); ok {
				singletonValues = append(singletonValues, value)
			} else {
				rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
					attribute.String("reader", "AggregatingMetricsReader"),
					attribute.String("reason", "empty_sketch_no_rollup_sum"),
				))
			}
		} else {
			// This row has a sketch - handle sketch merging
			sketchBytes, err := getSketchBytes(row[wkk.RowKeySketch])
			if err != nil {
				return nil, fmt.Errorf("invalid sketch data: %w", err)
			}

			sketch, err := helpers.DecodeSketch(sketchBytes)
			if err != nil {
				return nil, fmt.Errorf("failed to decode sketch: %w", err)
			}

			if currentSketch == nil {
				// First sketch for this group
				currentSketch = sketch
			} else {
				// Merge with existing sketch
				if err := currentSketch.MergeWith(sketch); err != nil {
					return nil, fmt.Errorf("failed to merge sketch: %w", err)
				}
			}
		}
	}

	// VALIDATION: Histograms must always have a sketch
	if currentSketch == nil {
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "AggregatingMetricsReader"),
			attribute.String("reason", "histogram_no_sketch"),
		))
		return nil, fmt.Errorf("histogram missing sketch")
	}

	// For histograms, we should not have singleton values mixed with sketches
	if len(singletonValues) > 0 {
		slog.Warn("Histogram has both sketch and singleton values - ignoring singletons",
			"name", baseRow[wkk.RowKeyCName],
			"tid", baseRow[wkk.RowKeyCTID],
			"singletons", singletonValues)
	}

	// Update rollup fields from the sketch
	if err := updateRowFromSketch(baseRow, currentSketch); err != nil {
		return nil, fmt.Errorf("updating histogram row from sketch: %w", err)
	}

	return baseRow, nil
}

// aggregateCounterGaugeGroup handles aggregation for counter and gauge metrics.
// Can handle mixed sketches and singletons.
func (ar *AggregatingMetricsReader) aggregateCounterGaugeGroup(ctx context.Context, baseRow Row, rows []Row) (Row, error) {
	var currentSketch *ddsketch.DDSketch
	var singletonValues []float64

	for _, row := range rows {
		// Handle sketch or singleton
		if isSketchEmpty(row) {
			// This is a singleton - collect its value
			if value, ok := getSingletonValue(row); ok {
				singletonValues = append(singletonValues, value)
			} else {
				rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
					attribute.String("reader", "AggregatingMetricsReader"),
					attribute.String("reason", "empty_sketch_no_rollup_sum"),
				))
			}
		} else {
			// This row has a sketch - handle sketch merging
			sketchBytes, err := getSketchBytes(row[wkk.RowKeySketch])
			if err != nil {
				return nil, fmt.Errorf("invalid sketch data: %w", err)
			}

			sketch, err := helpers.DecodeSketch(sketchBytes)
			if err != nil {
				return nil, fmt.Errorf("failed to decode sketch: %w", err)
			}

			if currentSketch == nil {
				// First sketch for this group
				currentSketch = sketch
			} else {
				// Merge with existing sketch
				if err := currentSketch.MergeWith(sketch); err != nil {
					return nil, fmt.Errorf("failed to merge sketch: %w", err)
				}
			}
		}
	}

	if currentSketch != nil {
		// We have a sketch - add all singleton values to it
		for _, value := range singletonValues {
			if err := currentSketch.Add(value); err != nil {
				rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
					attribute.String("reader", "AggregatingMetricsReader"),
					attribute.String("reason", "failed_add_singleton"),
				))
				continue
			}
		}

		// Update rollup fields from the final sketch
		if err := updateRowFromSketch(baseRow, currentSketch); err != nil {
			return nil, fmt.Errorf("updating counter/gauge row from sketch: %w", err)
		}
	} else if len(singletonValues) > 1 {
		// Multiple singletons without sketch - create sketch and add all values
		sketch, err := ddsketch.NewDefaultDDSketch(0.01)
		if err != nil {
			return nil, fmt.Errorf("creating sketch for singletons: %w", err)
		}

		for _, value := range singletonValues {
			if err := sketch.Add(value); err != nil {
				rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
					attribute.String("reader", "AggregatingMetricsReader"),
					attribute.String("reason", "failed_add_singleton_new_sketch"),
				))
				continue
			}
		}

		// Update rollup fields from the sketch
		if err := updateRowFromSketch(baseRow, sketch); err != nil {
			return nil, fmt.Errorf("updating counter/gauge row from new sketch: %w", err)
		}
	}
	// Single singleton case: keep existing rollup values as-is

	return baseRow, nil
}

// resetAggregation clears the current aggregation state.
func (ar *AggregatingMetricsReader) resetAggregation() {
	// Reset stored key values
	ar.hasCurrentKey = false
	ar.currentKeyName = ""
	ar.currentKeyTid = 0
	ar.currentKeyTs = 0
	// Clear grouped rows map but keep the map allocated
	for k := range ar.groupedRows {
		delete(ar.groupedRows, k)
	}
}

// addRowToAggregation adds a row to the current aggregation group, organizing by metric_type.
func (ar *AggregatingMetricsReader) addRowToAggregation(ctx context.Context, row Row) error {
	// VALIDATION: Histograms must always have sketches
	if isHistogramType(row) && isSketchEmpty(row) {
		if name, ok := row[wkk.RowKeyCName].(string); ok {
			slog.Error("Dropping histogram row without sketch - this should not happen",
				"name", name,
				"tid", row[wkk.RowKeyCTID],
				"timestamp", row[wkk.RowKeyCTimestamp])
		}
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "AggregatingMetricsReader"),
			attribute.String("reason", "histogram_without_sketch"),
			attribute.String("metric_type", "histogram"),
		))
		return nil // Skip this row, don't add to aggregation
	}

	// Get metric type, default to empty string if missing
	metricType, _ := row[wkk.RowKeyCMetricType].(string)

	// Deep copy the row to avoid modifying the original
	rowCopy := make(Row)
	for k, v := range row {
		rowCopy[k] = v
	}

	// Add to the appropriate metric type group
	ar.groupedRows[metricType] = append(ar.groupedRows[metricType], rowCopy)

	return nil
}

// readNextBatchFromUnderlying reads the next batch from the underlying reader.
func (ar *AggregatingMetricsReader) readNextBatchFromUnderlying(ctx context.Context) (*Batch, error) {
	if ar.readerEOF {
		return nil, io.EOF
	}

	batch, err := ar.reader.Next(ctx)
	if err != nil {
		if err == io.EOF {
			ar.readerEOF = true
		}
		return batch, err
	}

	// Track rows read from underlying reader
	rowsInCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
		attribute.String("reader", "AggregatingMetricsReader"),
	))

	// Truncate timestamps in all rows to aggregation period
	for i := range batch.Len() {
		row := batch.Get(i)
		if timestamp, ok := row[wkk.RowKeyCTimestamp].(int64); ok {
			truncatedTimestamp := (timestamp / ar.aggregationPeriod) * ar.aggregationPeriod
			row[wkk.RowKeyCTimestamp] = truncatedTimestamp
		}
	}

	return batch, nil
}

// processRow processes a single row and adds aggregated results to the batch.
// Returns an error if processing fails.
func (ar *AggregatingMetricsReader) processRow(ctx context.Context, row Row, batch *Batch) error {

	// Create aggregation key for this row
	key, err := ar.makeAggregationKey(row)
	if err != nil {
		slog.Error("Failed to make aggregation key", "error", err, "row", row)
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "AggregatingMetricsReader"),
			attribute.String("reason", "invalid_aggregation_key"),
		))
		return nil // Skip this row, continue processing
	}

	// Check if this row belongs to current group using stored values
	newMetric := key.(*MetricSortKey)
	belongsToCurrentGroup := !ar.hasCurrentKey ||
		(ar.currentKeyName == newMetric.Name &&
			ar.currentKeyTid == newMetric.Tid &&
			ar.currentKeyTs == newMetric.Timestamp)

	if belongsToCurrentGroup {

		if !ar.hasCurrentKey {
			// Store the key values directly
			ar.currentKeyName = newMetric.Name
			ar.currentKeyTid = newMetric.Tid
			ar.currentKeyTs = newMetric.Timestamp
			ar.hasCurrentKey = true
		}

		// Always release the key since we store values separately
		key.Release()
		if err := ar.addRowToAggregation(ctx, row); err != nil {
			slog.Error("Failed to add row to aggregation", "error", err)
		}
		return nil
	}

	// Key changed - emit current aggregation and start new one
	if ar.hasCurrentKey {
		results, err := ar.aggregateGroup(ctx)
		if err != nil {
			return fmt.Errorf("failed to aggregate group: %w", err)
		}

		// Emit each aggregated result (one per metric type)
		for _, result := range results {
			batchRow := batch.AddRow()
			maps.Copy(batchRow, result)
			ar.rowCount++
		}
	}

	// Start new aggregation with the current row
	ar.currentKeyName = newMetric.Name
	ar.currentKeyTid = newMetric.Tid
	ar.currentKeyTs = newMetric.Timestamp
	ar.hasCurrentKey = true
	// Release the key since we store values separately
	key.Release()
	if err := ar.addRowToAggregation(ctx, row); err != nil {
		slog.Error("Failed to add row to new aggregation", "error", err)
	}

	return nil
}

// Next returns the next batch of aggregated rows.
func (ar *AggregatingMetricsReader) Next(ctx context.Context) (*Batch, error) {
	if ar.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	batch := pipeline.GetBatch()

	for {
		// Process pending rows from previous underlying batch if any
		if ar.pendingBatch != nil && ar.pendingIndex < ar.pendingBatch.Len() {
			row := ar.pendingBatch.Get(ar.pendingIndex)
			ar.pendingIndex++

			// Process this row
			if err := ar.processRow(ctx, row, batch); err != nil {
				pipeline.ReturnBatch(ar.pendingBatch)
				pipeline.ReturnBatch(batch)
				ar.pendingBatch = nil
				ar.pendingIndex = 0
				return nil, err
			}

			// Return batch if we have enough rows
			if batch.Len() >= ar.batchSize {
				// Track rows output to downstream
				rowsOutCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
					attribute.String("reader", "AggregatingMetricsReader"),
				))
				return batch, nil
			}

			continue
		}

		// Clear pending batch if we've processed all rows
		if ar.pendingBatch != nil {
			pipeline.ReturnBatch(ar.pendingBatch)
			ar.pendingBatch = nil
			ar.pendingIndex = 0
		}

		// Read next batch from underlying reader
		if !ar.readerEOF {
			underlyingBatch, err := ar.readNextBatchFromUnderlying(ctx)
			if err != nil {
				if underlyingBatch != nil {
					pipeline.ReturnBatch(underlyingBatch)
				}
				if err == io.EOF {
					// Check if we need to emit final aggregation
					if ar.hasCurrentKey {
						results, aggErr := ar.aggregateGroup(ctx)
						if aggErr != nil {
							pipeline.ReturnBatch(batch)
							return nil, fmt.Errorf("failed to aggregate final group: %w", aggErr)
						}
						// Emit each final aggregated result (one per metric type)
						for _, result := range results {
							row := batch.AddRow()
							maps.Copy(row, result)
							ar.rowCount++
						}
					}
					if batch.Len() == 0 {
						pipeline.ReturnBatch(batch)
						return nil, io.EOF
					}
					// Track rows output to downstream
					rowsOutCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
						attribute.String("reader", "AggregatingMetricsReader"),
					))
					return batch, nil
				}
				pipeline.ReturnBatch(batch)
				return nil, err
			}

			// Store the underlying batch for processing
			ar.pendingBatch = underlyingBatch
			ar.pendingIndex = 0
			continue
		}

		// If we have any aggregated rows, return them
		if batch.Len() > 0 {
			// Track rows output to downstream
			rowsOutCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
				attribute.String("reader", "AggregatingMetricsReader"),
			))
			return batch, nil
		}

		// If we have no rows and are at EOF, we're done
		if ar.readerEOF && !ar.hasCurrentKey {
			pipeline.ReturnBatch(batch)
			return nil, io.EOF
		}
	}
}

// Close closes the reader and the underlying reader.
func (ar *AggregatingMetricsReader) Close() error {
	if ar.closed {
		return nil
	}
	ar.closed = true

	ar.resetAggregation()
	return ar.reader.Close()
}

// TotalRowsReturned returns the total number of aggregated rows returned via Next().
func (ar *AggregatingMetricsReader) TotalRowsReturned() int64 {
	return ar.rowCount
}

// GetOTELMetrics implements the OTELMetricsProvider interface if the underlying reader supports it.
func (ar *AggregatingMetricsReader) GetOTELMetrics() (any, error) {
	if provider, ok := ar.reader.(interface{ GetOTELMetrics() (any, error) }); ok {
		return provider.GetOTELMetrics()
	}
	return nil, fmt.Errorf("underlying reader does not support OTEL metrics")
}
