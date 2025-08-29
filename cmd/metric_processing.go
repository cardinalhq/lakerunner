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

package cmd

import (
	"fmt"
	"log/slog"

	"github.com/DataDog/sketches-go/ddsketch"

	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/internal/helpers"
)

// TagSketch holds a metric sketch with metadata
type TagSketch struct {
	MetricName     string
	MetricType     string
	Tags           map[string]any
	Sketch         *ddsketch.DDSketch
	DataPointCount int
	SingleValue    float64 // For single gauge/counter data points, store the actual value
}

// MetricRecord represents a parsed metric record ready for processing
type MetricRecord struct {
	Timestamp  int64
	MetricType string
	MetricName string
	Tags       map[string]any
	TID        int64
	Value      float64           // For gauge/counter
	BucketData *HistogramBuckets // For histogram
}

// HistogramBuckets holds histogram bucket data
type HistogramBuckets struct {
	Counts []float64
	Bounds []float64
}

// parseMetricRecord extracts and validates a metric record from raw parquet data
func parseMetricRecord(rec map[string]any, ll *slog.Logger) (*MetricRecord, error) {
	// Extract and validate timestamp
	ts, ok := helpers.GetInt64Value(rec, "_cardinalhq.timestamp")
	if !ok {
		return nil, fmt.Errorf("record missing timestamp")
	}
	delete(rec, "_cardinalhq.timestamp")

	// Extract and validate metric type
	metricType, ok := helpers.GetStringValue(rec, "_cardinalhq.metric_type")
	if !ok {
		return nil, fmt.Errorf("record missing metric type")
	}

	// Clean up record based on metric type
	if metricType == "count" || metricType == "gauge" {
		delete(rec, "_cardinalhq.bucket_bounds")
		delete(rec, "_cardinalhq.counts")
		delete(rec, "_cardinalhq.negative_counts")
		delete(rec, "_cardinalhq.positive_counts")
	} else {
		delete(rec, "_cardinalhq.value")
	}

	// Extract and validate metric name
	metricName, ok := helpers.GetStringValue(rec, "_cardinalhq.name")
	if !ok {
		return nil, fmt.Errorf("record missing metric name")
	}

	// Process tags
	tags := helpers.MakeTags(rec)
	tid := helpers.ComputeTID(metricName, tags)
	tags["_cardinalhq.tid"] = tid

	result := &MetricRecord{
		Timestamp:  ts,
		MetricType: metricType,
		MetricName: metricName,
		Tags:       tags,
		TID:        tid,
	}

	// Extract type-specific data
	switch metricType {
	case "count", "gauge":
		value, ok := helpers.GetFloat64Value(rec, "_cardinalhq.value")
		if !ok {
			return nil, fmt.Errorf("gauge/counter record missing value")
		}
		result.Value = value

	case "histogram":
		bucketCounts, ok := helpers.GetFloat64SliceJSON(rec, "_cardinalhq.counts")
		if !ok {
			return nil, fmt.Errorf("histogram record missing counts")
		}
		delete(rec, "_cardinalhq.counts")

		bucketBounds, ok := helpers.GetFloat64SliceJSON(rec, "_cardinalhq.bucket_bounds")
		if !ok {
			return nil, fmt.Errorf("histogram record missing bucket bounds")
		}
		delete(rec, "_cardinalhq.bucket_bounds")

		result.BucketData = &HistogramBuckets{
			Counts: bucketCounts,
			Bounds: bucketBounds,
		}

	default:
		return nil, fmt.Errorf("unsupported metric type: %s", metricType)
	}

	return result, nil
}

// getOrCreateSketch gets an existing sketch or creates a new one for the given TID
func getOrCreateSketch(sketches *map[int64]TagSketch, metricRecord *MetricRecord) (TagSketch, error) {
	sketch, exists := (*sketches)[metricRecord.TID]
	if !exists {
		sketch = TagSketch{
			MetricName:     metricRecord.MetricName,
			MetricType:     metricRecord.MetricType,
			Tags:           metricRecord.Tags,
			Sketch:         nil,
			DataPointCount: 0,
		}
		(*sketches)[metricRecord.TID] = sketch
	} else {
		// Validate consistency - these should NEVER happen if TID computation is correct
		if sketch.MetricName != metricRecord.MetricName {
			return TagSketch{}, fmt.Errorf("TID collision or computation error: TID %d has conflicting metric names: existing=%s, new=%s",
				metricRecord.TID, sketch.MetricName, metricRecord.MetricName)
		}
		if sketch.MetricType != metricRecord.MetricType {
			return TagSketch{}, fmt.Errorf("TID collision or computation error: TID %d has conflicting metric types: existing=%s, new=%s",
				metricRecord.TID, sketch.MetricType, metricRecord.MetricType)
		}
		// Note: We're not checking tag consistency here for performance
		// The TID should ensure consistency, but these error checks catch the most critical issues
	}

	return sketch, nil
}

// updateSketchWithValue processes a single metric value and updates the sketch accordingly
func updateSketchWithValue(sketch TagSketch, value float64) (TagSketch, error) {
	sketch.DataPointCount++

	// Create sketch if we have multiple data points OR if it's a histogram (always need sketch for histograms)
	if (sketch.DataPointCount > 1 || sketch.MetricType == "histogram") && sketch.Sketch == nil {
		s, err := ddsketch.NewDefaultDDSketch(0.01)
		if err != nil {
			return sketch, fmt.Errorf("creating sketch: %w", err)
		}
		sketch.Sketch = s
	}

	switch sketch.DataPointCount {
	case 1:
		// First data point - store the single value
		sketch.SingleValue = value
	case 2:
		// Second data point - add both to the newly created sketch
		if err := sketch.Sketch.Add(sketch.SingleValue); err != nil {
			return sketch, fmt.Errorf("adding previous single value to sketch: %w", err)
		}
		if err := sketch.Sketch.Add(value); err != nil {
			return sketch, fmt.Errorf("adding current value to sketch: %w", err)
		}
	default:
		// Multiple data points - add to existing sketch
		if err := sketch.Sketch.Add(value); err != nil {
			return sketch, fmt.Errorf("adding value to sketch: %w", err)
		}
	}

	return sketch, nil
}

// updateSketchWithHistogram processes histogram data and updates the sketch
func updateSketchWithHistogram(sketch TagSketch, bucketData *HistogramBuckets) (TagSketch, error) {
	sketch.DataPointCount++

	// For histograms, we always create sketches
	if sketch.Sketch == nil {
		s, err := ddsketch.NewDefaultDDSketch(0.01)
		if err != nil {
			return sketch, fmt.Errorf("creating sketch: %w", err)
		}
		sketch.Sketch = s
	}

	counts, values := legacyHandleHistogram(bucketData.Counts, bucketData.Bounds)
	if len(counts) == 0 {
		return sketch, nil // No data to add
	}

	// Add histogram data to sketch
	for i, count := range counts {
		if err := sketch.Sketch.AddWithCount(values[i], count); err != nil {
			return sketch, fmt.Errorf("adding histogram value to sketch: %w", err)
		}
	}

	return sketch, nil
}

// processMetricRecord processes a single metric record and updates the appropriate sketch
func processMetricRecord(metricRecord *MetricRecord, sketches *map[int64]TagSketch, nodeBuilder *buffet.NodeMapBuilder) error {
	// Add tags to node builder for schema
	if err := nodeBuilder.Add(metricRecord.Tags); err != nil {
		return fmt.Errorf("adding tags to node builder: %w", err)
	}

	// Get or create sketch for this TID
	sketch, err := getOrCreateSketch(sketches, metricRecord)
	if err != nil {
		return fmt.Errorf("getting or creating sketch: %w", err)
	}

	// Process based on metric type
	switch metricRecord.MetricType {
	case "count", "gauge":
		sketch, err = updateSketchWithValue(sketch, metricRecord.Value)
		if err != nil {
			return fmt.Errorf("updating sketch with value: %w", err)
		}

	case "histogram":
		sketch, err = updateSketchWithHistogram(sketch, metricRecord.BucketData)
		if err != nil {
			return fmt.Errorf("updating sketch with histogram: %w", err)
		}

	default:
		return fmt.Errorf("unsupported metric type: %s", metricRecord.MetricType)
	}

	// Update the sketch in the map
	(*sketches)[metricRecord.TID] = sketch

	return nil
}

// legacyHandleHistogram processes histogram bucket data and returns counts and values for sketches
func legacyHandleHistogram(bucketCounts []float64, bucketBounds []float64) (counts, values []float64) {
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
		if value <= maxTrackableValue {
			counts = append(counts, count)
			values = append(values, value)
		}
	}

	return counts, values
}
