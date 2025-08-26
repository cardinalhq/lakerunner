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
	"log/slog"

	"github.com/DataDog/sketches-go/ddsketch"

	"github.com/cardinalhq/lakerunner/internal/helpers"
)

// AggregatingMetricsReader wraps a sorted Reader to perform streaming aggregation of metrics.
// It aggregates rows with the same [metric_name, tid, truncated_timestamp] key.
// The underlying reader must return rows in sorted order by this key.
type AggregatingMetricsReader struct {
	reader            Reader
	aggregationPeriod int64 // milliseconds (e.g., 10000 for 10s)
	closed            bool
	rowCount          int64

	// Current aggregation state
	currentKey      string
	currentSketch   *ddsketch.DDSketch
	singletonValues []float64
	aggregatedRow   map[string]any
	pendingRow      *map[string]any // Next row read from underlying reader
	readerEOF       bool
}

// NewAggregatingMetricsReader creates a new AggregatingMetricsReader that aggregates metrics
// with the same [metric_name, tid, truncated_timestamp] key.
//
// aggregationPeriodMs: period in milliseconds for timestamp truncation (e.g., 10000 for 10s)
// reader: underlying reader that returns rows in sorted order by [metric_name, tid, timestamp]
func NewAggregatingMetricsReader(reader Reader, aggregationPeriodMs int64) (*AggregatingMetricsReader, error) {
	if reader == nil {
		return nil, fmt.Errorf("reader cannot be nil")
	}
	if aggregationPeriodMs <= 0 {
		return nil, fmt.Errorf("aggregation period must be positive, got %d", aggregationPeriodMs)
	}

	return &AggregatingMetricsReader{
		reader:            reader,
		aggregationPeriod: aggregationPeriodMs,
		singletonValues:   make([]float64, 0, 16),
	}, nil
}

// makeAggregationKey creates a key for aggregation from [metric_name, tid, truncated_timestamp].
func (ar *AggregatingMetricsReader) makeAggregationKey(row map[string]any) (string, error) {
	name, nameOk := row["_cardinalhq.name"].(string)
	if !nameOk {
		return "", fmt.Errorf("missing or invalid _cardinalhq.name field")
	}

	tid, tidOk := row["_cardinalhq.tid"].(int64)
	if !tidOk {
		return "", fmt.Errorf("missing or invalid _cardinalhq.tid field")
	}

	timestamp, tsOk := row["_cardinalhq.timestamp"].(int64)
	if !tsOk {
		return "", fmt.Errorf("missing or invalid _cardinalhq.timestamp field")
	}

	// Truncate timestamp to aggregation period
	truncatedTimestamp := (timestamp / ar.aggregationPeriod) * ar.aggregationPeriod

	return fmt.Sprintf("%s:%d:%d", name, tid, truncatedTimestamp), nil
}

// isSketchEmpty checks if a row has an empty sketch (indicating singleton value).
func isSketchEmpty(row map[string]any) bool {
	sketch, ok := row["sketch"].([]byte)
	return !ok || len(sketch) == 0
}

// getSingletonValue extracts the singleton value from rollup_sum field.
func getSingletonValue(row map[string]any) (float64, bool) {
	value, ok := row["rollup_sum"].(float64)
	return value, ok
}

// isHistogramType checks if a row represents a histogram metric type.
func isHistogramType(row map[string]any) bool {
	metricType, ok := row["type"].(string)
	return ok && metricType == "Histogram"
}

// updateRowFromSketch updates all rollup fields in a row based on the sketch.
func updateRowFromSketch(row map[string]any, sketch *ddsketch.DDSketch) error {
	count := sketch.GetCount()
	sum := sketch.GetSum()

	row["rollup_count"] = count
	row["rollup_sum"] = sum

	if count > 0 {
		row["rollup_avg"] = sum / count
	} else {
		row["rollup_avg"] = 0.0
	}

	maxValue, err := sketch.GetMaxValue()
	if err != nil {
		return fmt.Errorf("getting max value: %w", err)
	}
	row["rollup_max"] = maxValue

	minValue, err := sketch.GetMinValue()
	if err != nil {
		return fmt.Errorf("getting min value: %w", err)
	}
	row["rollup_min"] = minValue

	quantiles, err := sketch.GetValuesAtQuantiles([]float64{0.25, 0.50, 0.75, 0.90, 0.95, 0.99})
	if err != nil {
		return fmt.Errorf("getting quantiles: %w", err)
	}

	row["rollup_p25"] = quantiles[0]
	row["rollup_p50"] = quantiles[1]
	row["rollup_p75"] = quantiles[2]
	row["rollup_p90"] = quantiles[3]
	row["rollup_p95"] = quantiles[4]
	row["rollup_p99"] = quantiles[5]

	row["sketch"] = helpers.EncodeSketch(sketch)

	return nil
}

// aggregateGroup processes all rows for a single aggregation group and returns the aggregated result.
// Returns nil if all rows in the group were dropped (e.g., invalid histograms).
func (ar *AggregatingMetricsReader) aggregateGroup() (map[string]any, error) {
	if ar.aggregatedRow == nil {
		// All rows in this group were dropped - return nil to indicate no output
		ar.resetAggregation()
		return nil, nil
	}

	// Handle the aggregation based on what we collected
	if ar.currentSketch != nil {
		// We have a sketch - add all singleton values to it
		for _, value := range ar.singletonValues {
			if err := ar.currentSketch.Add(value); err != nil {
				slog.Error("Failed to add singleton value to sketch", "error", err, "value", value)
				continue
			}
		}

		// Update rollup fields from the final sketch
		if err := updateRowFromSketch(ar.aggregatedRow, ar.currentSketch); err != nil {
			return nil, fmt.Errorf("updating row from sketch: %w", err)
		}
	} else if len(ar.singletonValues) > 1 {
		// Multiple singletons without sketch - create sketch and add all values
		sketch, err := ddsketch.NewDefaultDDSketch(0.01)
		if err != nil {
			return nil, fmt.Errorf("creating sketch for singletons: %w", err)
		}

		for _, value := range ar.singletonValues {
			if err := sketch.Add(value); err != nil {
				slog.Error("Failed to add singleton value to new sketch", "error", err, "value", value)
				continue
			}
		}

		// Update rollup fields from the sketch
		if err := updateRowFromSketch(ar.aggregatedRow, sketch); err != nil {
			return nil, fmt.Errorf("updating row from new sketch: %w", err)
		}
	}
	// Single singleton case: keep existing rollup values as-is

	// Update timestamp to truncated value
	if timestamp, ok := ar.aggregatedRow["_cardinalhq.timestamp"].(int64); ok {
		truncatedTimestamp := (timestamp / ar.aggregationPeriod) * ar.aggregationPeriod
		ar.aggregatedRow["_cardinalhq.timestamp"] = truncatedTimestamp
	}

	result := ar.aggregatedRow
	ar.resetAggregation()
	return result, nil
}

// resetAggregation clears the current aggregation state.
func (ar *AggregatingMetricsReader) resetAggregation() {
	ar.currentKey = ""
	ar.currentSketch = nil
	ar.singletonValues = ar.singletonValues[:0] // Reuse slice capacity
	ar.aggregatedRow = nil
}

// addRowToAggregation adds a row to the current aggregation group.
func (ar *AggregatingMetricsReader) addRowToAggregation(row map[string]any) error {
	// Validate histogram rows have sketches - drop invalid histograms
	if isHistogramType(row) && isSketchEmpty(row) {
		// TODO: Add metric counter here for tracking dropped histogram rows without sketches
		slog.Warn("Dropping histogram row without sketch", "row", row)
		return nil // Skip this row, don't add to aggregation
	}

	// If this is the first row in the group, use it as the base
	if ar.aggregatedRow == nil {
		// Deep copy the row to avoid modifying the original
		ar.aggregatedRow = make(map[string]any)
		for k, v := range row {
			ar.aggregatedRow[k] = v
		}
	}

	// Handle sketch or singleton
	if isSketchEmpty(row) {
		// This is a singleton - collect its value
		if value, ok := getSingletonValue(row); ok {
			ar.singletonValues = append(ar.singletonValues, value)
		} else {
			slog.Error("Empty sketch without valid rollup_sum", "row", row)
		}
	} else {
		// This row has a sketch - handle sketch merging
		sketchBytes, ok := row["sketch"].([]byte)
		if !ok {
			return fmt.Errorf("invalid sketch bytes")
		}

		sketch, err := helpers.DecodeSketch(sketchBytes)
		if err != nil {
			return fmt.Errorf("failed to decode sketch: %w", err)
		}

		if ar.currentSketch == nil {
			// First sketch for this group
			ar.currentSketch = sketch
		} else {
			// Merge with existing sketch
			if err := ar.currentSketch.MergeWith(sketch); err != nil {
				return fmt.Errorf("failed to merge sketch: %w", err)
			}
		}
	}

	return nil
}

// readNextRowFromUnderlying reads the next row from the underlying reader.
func (ar *AggregatingMetricsReader) readNextRowFromUnderlying() error {
	if ar.readerEOF {
		return nil
	}

	rows := make([]Row, 1)
	resetRow(&rows[0])
	n, err := ar.reader.Read(rows)

	if n > 0 {
		// Convert Row to map[string]any
		rowMap := make(map[string]any)
		for k, v := range rows[0] {
			rowMap[k] = v
		}
		ar.pendingRow = &rowMap
	}

	if err != nil {
		if err == io.EOF {
			ar.readerEOF = true
			return nil
		}
		return err
	}

	return nil
}

// Read populates the provided slice with aggregated rows.
func (ar *AggregatingMetricsReader) Read(rows []Row) (int, error) {
	if ar.closed {
		return 0, fmt.Errorf("reader is closed")
	}

	if len(rows) == 0 {
		return 0, nil
	}

	n := 0
	for n < len(rows) {
		// Ensure we have a pending row to process
		if ar.pendingRow == nil && !ar.readerEOF {
			if err := ar.readNextRowFromUnderlying(); err != nil {
				return n, err
			}
		}

		// If no more rows and no current aggregation, we're done
		if ar.pendingRow == nil && ar.currentKey == "" {
			if n == 0 {
				return 0, io.EOF
			}
			break
		}

		// If we have a pending row, process it
		if ar.pendingRow != nil {
			key, err := ar.makeAggregationKey(*ar.pendingRow)
			if err != nil {
				slog.Error("Failed to make aggregation key", "error", err, "row", *ar.pendingRow)
				ar.pendingRow = nil
				continue
			}

			// If this row belongs to current group, add it
			if ar.currentKey == "" || ar.currentKey == key {
				ar.currentKey = key
				if err := ar.addRowToAggregation(*ar.pendingRow); err != nil {
					slog.Error("Failed to add row to aggregation", "error", err)
				}
				ar.pendingRow = nil
				continue
			}

			// Key changed - emit current aggregation and start new one
			if ar.currentKey != "" {
				result, err := ar.aggregateGroup()
				if err != nil {
					return n, fmt.Errorf("failed to aggregate group: %w", err)
				}

				// Only emit if we have a result (not all rows were dropped)
				if result != nil {
					resetRow(&rows[n])
					for k, v := range result {
						rows[n][k] = v
					}
					n++
					ar.rowCount++
				}

				// Start new aggregation with the pending row
				ar.currentKey = key
				if err := ar.addRowToAggregation(*ar.pendingRow); err != nil {
					slog.Error("Failed to add row to new aggregation", "error", err)
				}
				ar.pendingRow = nil
				continue
			}
		}

		// No pending row - check if we need to emit final aggregation
		if ar.currentKey != "" && ar.readerEOF {
			result, err := ar.aggregateGroup()
			if err != nil {
				return n, fmt.Errorf("failed to aggregate final group: %w", err)
			}

			// Only emit if we have a result (not all rows were dropped)
			if result != nil {
				resetRow(&rows[n])
				for k, v := range result {
					rows[n][k] = v
				}
				n++
				ar.rowCount++
			}
			continue
		}

		// Nothing more to process
		break
	}

	return n, nil
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

// TotalRowsReturned returns the total number of aggregated rows returned via Read().
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
