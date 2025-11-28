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
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// aggregationState holds the minimal data needed for aggregation
type aggregationState struct {
	baseRow     pipeline.Row       // Template row with all metadata fields
	ownsBaseRow bool               // Whether we own baseRow (taken from batch)
	sketch      *ddsketch.DDSketch // Accumulated sketch (required for all metrics)
	rowCount    int                // Number of rows aggregated
}

// AggregatingMetricsReader wraps a sorted Reader to perform streaming aggregation of metrics.
// It aggregates rows with the same [metric_name, tid, truncated_timestamp] key.
// The underlying reader must return rows in sorted order by this key.
type AggregatingMetricsReader struct {
	reader            Reader
	aggregationPeriod int64 // milliseconds (e.g., 10000 for 10s)
	closed            bool
	rowCount          int64
	batchSize         int

	// Current aggregation state - lightweight, no full row copies
	aggState     *aggregationState // Single aggregation state for current key
	pendingBatch *Batch            // Unprocessed rows from underlying reader
	pendingIndex int               // Index of next row to process in pendingBatch
	readerEOF    bool

	// Sort key provider for grouping
	keyProvider SortKeyProvider

	// Stored key values (not pooled to avoid corruption)
	currentKeyName string
	currentKeyTid  int64
	currentKeyTs   int64
	hasCurrentKey  bool

	// Track for zero-copy optimization
	firstRowBatch *Batch // Batch containing first row of current aggregation
	firstRowIndex int    // Index in firstRowBatch (-1 if already taken)
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
		keyProvider:       &MetricSortKeyProvider{},
	}, nil
}

// makeAggregationKey creates a key for aggregation from [metric_name, tid, timestamp].
// Assumes timestamp has already been truncated to aggregation period.
func (ar *AggregatingMetricsReader) makeAggregationKey(row pipeline.Row) (SortKey, error) {
	// Validate required fields
	_, nameOk := row[wkk.RowKeyCName].(string)
	if !nameOk {
		return nil, fmt.Errorf("missing or invalid metric_name field")
	}

	_, tidOk := row[wkk.RowKeyCTID].(int64)
	if !tidOk {
		return nil, fmt.Errorf("missing or invalid chq_tid field")
	}

	_, tsOk := row[wkk.RowKeyCTimestamp].(int64)
	if !tsOk {
		return nil, fmt.Errorf("missing or invalid chq_timestamp field")
	}

	// Use the row directly - timestamp should already be truncated
	return ar.keyProvider.MakeKey(row), nil
}

// isSketchEmpty checks if a row has an empty sketch (indicating singleton value).
func isSketchEmpty(row pipeline.Row) bool {
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

// updateRowFromSketch updates all rollup fields in a row based on the sketch.
// This should only be called for valid sketches with data.
func updateRowFromSketch(row pipeline.Row, sketch *ddsketch.DDSketch) error {
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

// aggregateGroup finalizes the current aggregation and returns the result.
func (ar *AggregatingMetricsReader) aggregateGroup(ctx context.Context) (pipeline.Row, error) {
	if ar.aggState == nil {
		// No aggregation state - nothing to return
		ar.resetAggregation()
		return nil, nil
	}

	// Determine metric type from base row for proper aggregation
	metricType, _ := ar.aggState.baseRow[wkk.RowKeyCMetricType].(string)

	// Finalize the aggregation based on metric type
	var result pipeline.Row
	var err error
	if metricType == "histogram" {
		result, err = ar.finalizeHistogramAggregation(ctx, ar.aggState)
	} else {
		result, err = ar.finalizeCounterGaugeAggregation(ctx, ar.aggState)
	}

	if err != nil {
		return nil, fmt.Errorf("finalizing %s aggregation: %w", metricType, err)
	}

	// Transfer ownership of the row - don't return to pool since caller will use it
	if result != nil && ar.aggState.ownsBaseRow {
		ar.aggState.ownsBaseRow = false // Ownership transferred to caller
	}

	// Clear state but don't return the row (ownership transferred)
	ar.aggState = nil
	ar.firstRowBatch = nil
	ar.firstRowIndex = -1
	ar.hasCurrentKey = false
	ar.currentKeyName = ""
	ar.currentKeyTid = 0
	ar.currentKeyTs = 0

	return result, nil
}

// finalizeHistogramAggregation completes aggregation for a histogram metric.
func (ar *AggregatingMetricsReader) finalizeHistogramAggregation(ctx context.Context, state *aggregationState) (pipeline.Row, error) {
	// VALIDATION: Histograms must always have a sketch - no singleton values allowed
	if state.sketch == nil {
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "AggregatingMetricsReader"),
			attribute.String("reason", "histogram_no_sketch"),
		))
		// Drop this invalid histogram - return nil to skip it
		return nil, nil
	}

	// Use the base row directly - we own it and will transfer ownership to the caller
	result := state.baseRow

	// Update rollup fields from the sketch
	if err := updateRowFromSketch(result, state.sketch); err != nil {
		return nil, fmt.Errorf("updating histogram row from sketch: %w", err)
	}

	return result, nil
}

// finalizeCounterGaugeAggregation completes aggregation for counter/gauge metrics.
func (ar *AggregatingMetricsReader) finalizeCounterGaugeAggregation(_ context.Context, state *aggregationState) (pipeline.Row, error) {
	// Use the base row directly - we own it and will transfer ownership to the caller
	result := state.baseRow

	if state.sketch == nil {
		return nil, fmt.Errorf("aggregation state missing required sketch")
	}

	// Update rollup fields from the final sketch
	if err := updateRowFromSketch(result, state.sketch); err != nil {
		return nil, fmt.Errorf("updating counter/gauge row from sketch: %w", err)
	}

	return result, nil
}

// resetAggregation clears the current aggregation state.
func (ar *AggregatingMetricsReader) resetAggregation() {
	// Reset stored key values
	ar.hasCurrentKey = false
	ar.currentKeyName = ""
	ar.currentKeyTid = 0
	ar.currentKeyTs = 0

	// Return owned row to pool if we have one
	if ar.aggState != nil && ar.aggState.ownsBaseRow && ar.aggState.baseRow != nil {
		// Only return to pool if we own it and haven't transferred ownership
		pipeline.ReturnPooledRow(ar.aggState.baseRow)
	}

	// Clear aggregation state
	ar.aggState = nil
	ar.firstRowBatch = nil
	ar.firstRowIndex = -1
}

// addRowToAggregation adds a row to the current aggregation state.
// All metrics must have sketches for proper aggregation.
// rowIndex is the index in pendingBatch (-1 if not from current batch).
func (ar *AggregatingMetricsReader) addRowToAggregation(ctx context.Context, row pipeline.Row, rowIndex int) error {
	// VALIDATION: All metrics must have sketches
	if isSketchEmpty(row) {
		metricType := "unknown"
		if mt, ok := row[wkk.RowKeyCMetricType].(string); ok {
			metricType = mt
		}
		if name, ok := row[wkk.RowKeyCName].(string); ok {
			slog.Error("Dropping row without sketch - this should not happen",
				"name", name,
				"metric_type", metricType,
				"tid", row[wkk.RowKeyCTID],
				"timestamp", row[wkk.RowKeyCTimestamp])
		}
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "AggregatingMetricsReader"),
			attribute.String("reason", "missing_required_sketch"),
			attribute.String("metric_type", metricType),
		))
		return nil // Skip this row, don't add to aggregation
	}

	// Create aggregation state if this is the first row for this key
	if ar.aggState == nil {
		// First row for this aggregation key - create new state
		var baseRow pipeline.Row
		var ownsRow bool

		// Try zero-copy: take ownership of the row from the batch
		if rowIndex >= 0 && ar.pendingBatch != nil {
			// We can take ownership of this row from the batch
			baseRow = ar.pendingBatch.TakeRow(rowIndex)
			if baseRow != nil {
				ownsRow = true
				ar.firstRowBatch = ar.pendingBatch
				ar.firstRowIndex = rowIndex
			}
		}

		// Fallback: copy if we couldn't take ownership
		if baseRow == nil {
			baseRow = pipeline.GetPooledRow()
			maps.Copy(baseRow, row)
			ownsRow = true
			ar.firstRowBatch = nil
			ar.firstRowIndex = -1
		}

		ar.aggState = &aggregationState{
			baseRow:     baseRow,
			ownsBaseRow: ownsRow,
			rowCount:    0,
		}
	}

	state := ar.aggState

	ar.aggState.rowCount++

	// All rows must have sketches now
	if isSketchEmpty(row) {
		rowsDroppedCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "AggregatingMetricsReader"),
			attribute.String("reason", "missing_required_sketch"),
		))
		return fmt.Errorf("row missing required sketch")
	}

	// This row has a sketch
	sketchBytes, err := getSketchBytes(row[wkk.RowKeySketch])
	if err != nil {
		return fmt.Errorf("invalid sketch data: %w", err)
	}

	sketch, err := helpers.DecodeSketch(sketchBytes)
	if err != nil {
		return fmt.Errorf("failed to decode sketch: %w", err)
	}

	if state.sketch != nil {
		// Merge with existing sketch
		if err := state.sketch.MergeWith(sketch); err != nil {
			return fmt.Errorf("failed to merge sketch: %w", err)
		}
	} else {
		// First sketch for this group
		state.sketch = sketch
	}

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
		// Also truncate nanosecond timestamp to same period (in nanoseconds)
		if tsns, ok := row[wkk.RowKeyCTsns].(int64); ok {
			aggregationPeriodNs := ar.aggregationPeriod * 1_000_000
			truncatedTsns := (tsns / aggregationPeriodNs) * aggregationPeriodNs
			row[wkk.RowKeyCTsns] = truncatedTsns
		}
	}

	return batch, nil
}

// processRow processes a single row and adds aggregated results to the batch.
// Returns an error if processing fails.
// rowIndex is the index of this row in pendingBatch (for zero-copy optimization).
func (ar *AggregatingMetricsReader) processRow(ctx context.Context, row pipeline.Row, rowIndex int, batch *Batch) error {

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
		if err := ar.addRowToAggregation(ctx, row, rowIndex); err != nil {
			slog.Error("Failed to add row to aggregation", "error", err)
		}
		return nil
	}

	// Key changed - emit current aggregation and start new one
	if ar.hasCurrentKey {
		result, err := ar.aggregateGroup(ctx)
		if err != nil {
			return fmt.Errorf("failed to aggregate group: %w", err)
		}

		// Emit the aggregated result if we have one
		if result != nil {
			// Add a slot and replace it with our result row (zero-copy transfer)
			batch.AddRow()
			batch.ReplaceRow(batch.Len()-1, result)
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
	if err := ar.addRowToAggregation(ctx, row, rowIndex); err != nil {
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
			if err := ar.processRow(ctx, row, ar.pendingIndex-1, batch); err != nil {
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
						result, aggErr := ar.aggregateGroup(ctx)
						if aggErr != nil {
							pipeline.ReturnBatch(batch)
							return nil, fmt.Errorf("failed to aggregate final group: %w", aggErr)
						}
						// Emit the final aggregated result if we have one
						if result != nil {
							// Add a slot and replace it with our result row (zero-copy transfer)
							batch.AddRow()
							batch.ReplaceRow(batch.Len()-1, result)
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

// GetSchema returns the schema from the wrapped reader.
// This reader performs aggregation but does not change the schema structure.
func (ar *AggregatingMetricsReader) GetSchema() *ReaderSchema {
	if ar.reader != nil {
		return ar.reader.GetSchema()
	}
	return nil
}

// GetOTELMetrics implements the OTELMetricsProvider interface if the underlying reader supports it.
func (ar *AggregatingMetricsReader) GetOTELMetrics() (any, error) {
	if provider, ok := ar.reader.(interface{ GetOTELMetrics() (any, error) }); ok {
		return provider.GetOTELMetrics()
	}
	return nil, fmt.Errorf("underlying reader does not support OTEL metrics")
}
