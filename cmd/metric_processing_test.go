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
	"log/slog"
	"math"
	"testing"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/buffet"
)

func TestParseMetricRecord(t *testing.T) {
	ll := slog.Default()

	tests := []struct {
		name        string
		rec         map[string]any
		expectError bool
		expected    *MetricRecord
	}{
		{
			name: "ValidGaugeRecord",
			rec: map[string]any{
				"_cardinalhq.timestamp":   int64(1234567890),
				"_cardinalhq.metric_type": "gauge",
				"_cardinalhq.name":        "cpu_usage",
				"_cardinalhq.value":       42.5,
				"host":                    "server1",
				"region":                  "us-east-1",
			},
			expectError: false,
			expected: &MetricRecord{
				Timestamp:  1234567890,
				MetricType: "gauge",
				MetricName: "cpu_usage",
				Value:      42.5,
				Tags:       map[string]any{"host": "server1", "region": "us-east-1", "_cardinalhq.tid": "123"}, // TID will be computed
			},
		},
		{
			name: "ValidCounterRecord",
			rec: map[string]any{
				"_cardinalhq.timestamp":   int64(9876543210),
				"_cardinalhq.metric_type": "count",
				"_cardinalhq.name":        "requests_total",
				"_cardinalhq.value":       100.0,
				"service":                 "api",
			},
			expectError: false,
			expected: &MetricRecord{
				Timestamp:  9876543210,
				MetricType: "count",
				MetricName: "requests_total",
				Value:      100.0,
				Tags:       map[string]any{"service": "api", "_cardinalhq.tid": "456"}, // TID will be computed
			},
		},
		{
			name: "ValidHistogramRecord",
			rec: map[string]any{
				"_cardinalhq.timestamp":     int64(1111111111),
				"_cardinalhq.metric_type":   "histogram",
				"_cardinalhq.name":          "response_time",
				"_cardinalhq.counts":        `[1, 2, 3]`,
				"_cardinalhq.bucket_bounds": `[0.1, 0.5, 1.0]`,
				"endpoint":                  "/api/v1",
			},
			expectError: false,
			expected: &MetricRecord{
				Timestamp:  1111111111,
				MetricType: "histogram",
				MetricName: "response_time",
				BucketData: &HistogramBuckets{
					Counts: []float64{1, 2, 3},
					Bounds: []float64{0.1, 0.5, 1.0},
				},
				Tags: map[string]any{"endpoint": "/api/v1", "_cardinalhq.tid": "789"},
			},
		},
		{
			name: "MissingTimestamp",
			rec: map[string]any{
				"_cardinalhq.metric_type": "gauge",
				"_cardinalhq.name":        "cpu_usage",
				"_cardinalhq.value":       42.5,
			},
			expectError: true,
		},
		{
			name: "MissingMetricType",
			rec: map[string]any{
				"_cardinalhq.timestamp": int64(1234567890),
				"_cardinalhq.name":      "cpu_usage",
				"_cardinalhq.value":     42.5,
			},
			expectError: true,
		},
		{
			name: "MissingMetricName",
			rec: map[string]any{
				"_cardinalhq.timestamp":   int64(1234567890),
				"_cardinalhq.metric_type": "gauge",
				"_cardinalhq.value":       42.5,
			},
			expectError: true,
		},
		{
			name: "GaugeMissingValue",
			rec: map[string]any{
				"_cardinalhq.timestamp":   int64(1234567890),
				"_cardinalhq.metric_type": "gauge",
				"_cardinalhq.name":        "cpu_usage",
			},
			expectError: true,
		},
		{
			name: "HistogramMissingCounts",
			rec: map[string]any{
				"_cardinalhq.timestamp":     int64(1111111111),
				"_cardinalhq.metric_type":   "histogram",
				"_cardinalhq.name":          "response_time",
				"_cardinalhq.bucket_bounds": `[0.1, 0.5, 1.0]`,
			},
			expectError: true,
		},
		{
			name: "HistogramMissingBounds",
			rec: map[string]any{
				"_cardinalhq.timestamp":   int64(1111111111),
				"_cardinalhq.metric_type": "histogram",
				"_cardinalhq.name":        "response_time",
				"_cardinalhq.counts":      `[1, 2, 3]`,
			},
			expectError: true,
		},
		{
			name: "UnsupportedMetricType",
			rec: map[string]any{
				"_cardinalhq.timestamp":   int64(1234567890),
				"_cardinalhq.metric_type": "summary",
				"_cardinalhq.name":        "some_metric",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseMetricRecord(tt.rec, ll)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)

				assert.Equal(t, tt.expected.Timestamp, result.Timestamp)
				assert.Equal(t, tt.expected.MetricType, result.MetricType)
				assert.Equal(t, tt.expected.MetricName, result.MetricName)
				assert.NotZero(t, result.TID) // TID should be computed

				if tt.expected.MetricType == "histogram" {
					require.NotNil(t, result.BucketData)
					assert.Equal(t, tt.expected.BucketData.Counts, result.BucketData.Counts)
					assert.Equal(t, tt.expected.BucketData.Bounds, result.BucketData.Bounds)
				} else {
					assert.Equal(t, tt.expected.Value, result.Value)
				}
			}
		})
	}
}

func TestGetOrCreateSketch(t *testing.T) {
	tests := []struct {
		name         string
		existingTID  int64
		newTID       int64
		expectCreate bool
	}{
		{
			name:         "CreateNewSketch",
			existingTID:  0, // No existing sketch
			newTID:       123,
			expectCreate: true,
		},
		{
			name:         "ReuseExistingSketch",
			existingTID:  123,
			newTID:       123,
			expectCreate: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sketches := make(map[int64]TagSketch)

			// Setup existing sketch if needed
			if tt.existingTID != 0 {
				sketches[tt.existingTID] = TagSketch{
					MetricName:     "test_metric", // Use same name to avoid TID collision
					MetricType:     "count",       // Use same type to avoid TID collision
					Tags:           map[string]any{"test": "tag"},
					DataPointCount: 1,
				}
			}

			metricRecord := &MetricRecord{
				TID:        tt.newTID,
				MetricName: "test_metric",
				MetricType: "count",
				Tags:       map[string]any{"test": "tag"},
			}

			result, err := getOrCreateSketch(&sketches, metricRecord)
			require.NoError(t, err)

			if tt.expectCreate {
				// Should create new sketch
				assert.Equal(t, "test_metric", result.MetricName)
				assert.Equal(t, "count", result.MetricType)
				assert.Equal(t, 0, result.DataPointCount)
				assert.Nil(t, result.Sketch)

				// Should be in the map
				stored, exists := sketches[tt.newTID]
				assert.True(t, exists)
				assert.Equal(t, result, stored)
			} else {
				// Should return existing sketch
				assert.Equal(t, "test_metric", result.MetricName)
				assert.Equal(t, "count", result.MetricType)
				assert.Equal(t, 1, result.DataPointCount)
			}
		})
	}
}

func TestGetOrCreateSketch_TIDCollision(t *testing.T) {
	sketches := make(map[int64]TagSketch)

	// Setup existing sketch with different metric name
	sketches[123] = TagSketch{
		MetricName:     "existing_metric",
		MetricType:     "gauge",
		Tags:           map[string]any{"existing": "tag"},
		DataPointCount: 1,
	}

	metricRecord := &MetricRecord{
		TID:        123,
		MetricName: "different_metric", // Different name should trigger collision error
		MetricType: "gauge",
		Tags:       map[string]any{"test": "tag"},
	}

	_, err := getOrCreateSketch(&sketches, metricRecord)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "TID collision or computation error")
	assert.Contains(t, err.Error(), "conflicting metric names")
}

func TestUpdateSketchWithValue(t *testing.T) {
	tests := []struct {
		name              string
		initialCount      int
		initialValue      float64
		newValue          float64
		expectSketch      bool
		expectCount       int
		expectSingleValue float64
		expectSketchSum   float64 // Expected sum in sketch for verification
	}{
		{
			name:              "FirstValue_StoredAsSingle",
			initialCount:      0,
			newValue:          42.5,
			expectSketch:      false,
			expectCount:       1,
			expectSingleValue: 42.5,
		},
		{
			name:            "SecondValue_CreatesSketch",
			initialCount:    1,
			initialValue:    10.0,
			newValue:        20.0,
			expectSketch:    true,
			expectCount:     2,
			expectSketchSum: 30.0, // 10.0 + 20.0
		},
		{
			name:            "ThirdValue_AddsToExistingSketch",
			initialCount:    2,
			newValue:        30.0,
			expectSketch:    true,
			expectCount:     3,
			expectSketchSum: 30.0, // Only new value since we start with empty sketch
		},
		{
			name:              "NegativeValue_SingleValue",
			initialCount:      0,
			newValue:          -15.5,
			expectSketch:      false,
			expectCount:       1,
			expectSingleValue: -15.5,
		},
		{
			name:            "ZeroValue_CreatesSketch",
			initialCount:    1,
			initialValue:    5.0,
			newValue:        0.0,
			expectSketch:    true,
			expectCount:     2,
			expectSketchSum: 5.0, // 5.0 + 0.0
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sketch := TagSketch{
				MetricName:     "test_metric",
				MetricType:     "gauge",
				DataPointCount: tt.initialCount,
				SingleValue:    tt.initialValue,
			}

			// If we're testing third value scenario, create a sketch with some existing data
			if tt.initialCount == 2 {
				s, err := ddsketch.NewDefaultDDSketch(0.01)
				require.NoError(t, err)
				// Add some existing data to make the test more realistic
				err = s.Add(5.0)
				require.NoError(t, err)
				err = s.Add(10.0)
				require.NoError(t, err)
				sketch.Sketch = s
			}

			result, err := updateSketchWithValue(sketch, tt.newValue)
			require.NoError(t, err)

			// Verify basic properties
			assert.Equal(t, tt.expectCount, result.DataPointCount)

			if tt.expectSketch {
				assert.NotNil(t, result.Sketch, "Should have sketch for multiple values")
				assert.Greater(t, result.Sketch.GetCount(), float64(0))

				// For second value test, verify both original and new values are in sketch
				if tt.name == "SecondValue_CreatesSketch" {
					assert.Equal(t, float64(2), result.Sketch.GetCount())
					assert.InDelta(t, tt.expectSketchSum, result.Sketch.GetSum(), tt.expectSketchSum*0.01) // 1% tolerance for sketch approximation
				}
				// For zero value test, verify zero is properly handled
				if tt.name == "ZeroValue_CreatesSketch" {
					assert.Equal(t, float64(2), result.Sketch.GetCount())
					assert.InDelta(t, tt.expectSketchSum, result.Sketch.GetSum(), 0.1) // Larger tolerance when involving zero
				}
			} else {
				assert.Nil(t, result.Sketch, "Should not have sketch for single value")
				assert.Equal(t, tt.expectSingleValue, result.SingleValue)
			}
		})
	}
}

func TestUpdateSketchWithHistogram(t *testing.T) {
	tests := []struct {
		name         string
		initialCount int
		bucketData   *HistogramBuckets
		expectCount  int
		expectSum    float64 // Expected total count from histogram buckets
	}{
		{
			name:         "FirstHistogram",
			initialCount: 0,
			bucketData: &HistogramBuckets{
				Counts: []float64{1, 2, 3},
				Bounds: []float64{0.1, 0.5, 1.0},
			},
			expectCount: 1,
			expectSum:   6.0, // 1 + 2 + 3 total count
		},
		{
			name:         "SecondHistogram",
			initialCount: 1,
			bucketData: &HistogramBuckets{
				Counts: []float64{2, 1},
				Bounds: []float64{0.2, 0.8},
			},
			expectCount: 2,
			expectSum:   3.0, // 2 + 1 total count from this histogram
		},
		{
			name:         "EmptyHistogram",
			initialCount: 0,
			bucketData: &HistogramBuckets{
				Counts: []float64{0, 0, 0},
				Bounds: []float64{0.1, 0.5, 1.0},
			},
			expectCount: 1,
			expectSum:   0.0, // No actual data points
		},
		{
			name:         "SingleBucketHistogram",
			initialCount: 0,
			bucketData: &HistogramBuckets{
				Counts: []float64{5},
				Bounds: []float64{1.0},
			},
			expectCount: 1,
			expectSum:   5.0, // Single bucket with 5 observations
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sketch := TagSketch{
				MetricName:     "test_histogram",
				MetricType:     "histogram",
				DataPointCount: tt.initialCount,
			}

			// If testing second histogram, create initial sketch with some data
			if tt.initialCount > 0 {
				s, err := ddsketch.NewDefaultDDSketch(0.01)
				require.NoError(t, err)
				// Add some initial histogram data
				err = s.AddWithCount(0.3, 2)
				require.NoError(t, err)
				err = s.AddWithCount(0.7, 1)
				require.NoError(t, err)
				sketch.Sketch = s
			}

			result, err := updateSketchWithHistogram(sketch, tt.bucketData)
			require.NoError(t, err)

			// Verify basic properties
			assert.Equal(t, tt.expectCount, result.DataPointCount)
			assert.NotNil(t, result.Sketch, "Histograms should always have sketches")

			// For non-empty histograms, verify the sketch contains data
			if tt.expectSum > 0 {
				assert.Greater(t, result.Sketch.GetCount(), float64(0))

				// For first histogram test, verify exact count matches
				if tt.name == "FirstHistogram" {
					assert.Equal(t, tt.expectSum, result.Sketch.GetCount())
				}
			} else {
				// Empty histogram should still create sketch but with no data
				assert.Equal(t, float64(0), result.Sketch.GetCount())
			}
		})
	}
}

func TestProcessMetricRecord(t *testing.T) {
	tests := []struct {
		name         string
		metricRecord *MetricRecord
		expectError  bool
	}{
		{
			name: "ValidGaugeRecord",
			metricRecord: &MetricRecord{
				TID:        123,
				MetricName: "cpu_usage",
				MetricType: "gauge",
				Value:      42.5,
				Tags:       map[string]any{"host": "server1"},
			},
			expectError: false,
		},
		{
			name: "ValidHistogramRecord",
			metricRecord: &MetricRecord{
				TID:        456,
				MetricName: "response_time",
				MetricType: "histogram",
				BucketData: &HistogramBuckets{
					Counts: []float64{1, 2, 3},
					Bounds: []float64{0.1, 0.5, 1.0},
				},
				Tags: map[string]any{"endpoint": "/api/v1"},
			},
			expectError: false,
		},
		{
			name: "UnsupportedMetricType",
			metricRecord: &MetricRecord{
				TID:        789,
				MetricName: "some_metric",
				MetricType: "summary", // Not supported
				Tags:       map[string]any{},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sketches := make(map[int64]TagSketch)
			nodeBuilder := buffet.NewNodeMapBuilder()

			err := processMetricRecord(tt.metricRecord, &sketches, nodeBuilder)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)

				// Check that sketch was created/updated
				sketch, exists := sketches[tt.metricRecord.TID]
				assert.True(t, exists)
				assert.Equal(t, tt.metricRecord.MetricName, sketch.MetricName)
				assert.Equal(t, tt.metricRecord.MetricType, sketch.MetricType)
				assert.Equal(t, 1, sketch.DataPointCount)
			}
		})
	}
}

func TestProcessMetricRecord_MultipleValues(t *testing.T) {
	sketches := make(map[int64]TagSketch)
	nodeBuilder := buffet.NewNodeMapBuilder()

	// First record - should store as single value (no sketch yet)
	record1 := &MetricRecord{
		TID:        123,
		MetricName: "cpu_usage",
		MetricType: "gauge",
		Value:      10.0,
		Tags:       map[string]any{"host": "server1"},
	}

	err := processMetricRecord(record1, &sketches, nodeBuilder)
	require.NoError(t, err)

	sketch := sketches[123]
	assert.Equal(t, 1, sketch.DataPointCount)
	assert.Nil(t, sketch.Sketch, "First value should not create sketch")
	assert.Equal(t, 10.0, sketch.SingleValue, "First value should be stored as single value")

	// Second record - should create DDSketch and add both values
	record2 := &MetricRecord{
		TID:        123,
		MetricName: "cpu_usage",
		MetricType: "gauge",
		Value:      20.0,
		Tags:       map[string]any{"host": "server1"},
	}

	err = processMetricRecord(record2, &sketches, nodeBuilder)
	require.NoError(t, err)

	sketch = sketches[123]
	assert.Equal(t, 2, sketch.DataPointCount, "Should have 2 data points after second record")
	assert.NotNil(t, sketch.Sketch, "Second value should create sketch")
	assert.Equal(t, float64(2), sketch.Sketch.GetCount(), "Sketch should contain both values")
	assert.InDelta(t, 30.0, sketch.Sketch.GetSum(), 0.5, "Sketch sum should be 10.0 + 20.0 = 30.0")

	// Third record - should add to existing sketch
	record3 := &MetricRecord{
		TID:        123,
		MetricName: "cpu_usage",
		MetricType: "gauge",
		Value:      30.0,
		Tags:       map[string]any{"host": "server1"},
	}

	err = processMetricRecord(record3, &sketches, nodeBuilder)
	require.NoError(t, err)

	sketch = sketches[123]
	assert.Equal(t, 3, sketch.DataPointCount, "Should have 3 data points after third record")
	assert.NotNil(t, sketch.Sketch, "Should still have sketch")
	assert.Equal(t, float64(3), sketch.Sketch.GetCount(), "Sketch should contain all three values")
	assert.InDelta(t, 60.0, sketch.Sketch.GetSum(), 1.0, "Sketch sum should be 10.0 + 20.0 + 30.0 = 60.0")
}

// Test edge cases and value validation
func TestProcessMetricRecord_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		records     []*MetricRecord
		expectError bool
		expectSum   float64
		expectCount int
	}{
		{
			name: "NegativeValues",
			records: []*MetricRecord{
				{
					TID: 100, MetricName: "test", MetricType: "gauge", Value: -5.5,
					Tags: map[string]any{"test": "negative"},
				},
				{
					TID: 100, MetricName: "test", MetricType: "gauge", Value: -10.0,
					Tags: map[string]any{"test": "negative"},
				},
			},
			expectError: false,
			expectSum:   -15.5,
			expectCount: 2,
		},
		{
			name: "ZeroValues",
			records: []*MetricRecord{
				{
					TID: 200, MetricName: "test", MetricType: "count", Value: 0.0,
					Tags: map[string]any{"test": "zero"},
				},
				{
					TID: 200, MetricName: "test", MetricType: "count", Value: 5.0,
					Tags: map[string]any{"test": "zero"},
				},
			},
			expectError: false,
			expectSum:   5.0,
			expectCount: 2,
		},
		{
			name: "LargeValues",
			records: []*MetricRecord{
				{
					TID: 300, MetricName: "test", MetricType: "gauge", Value: 1e6,
					Tags: map[string]any{"test": "large"},
				},
				{
					TID: 300, MetricName: "test", MetricType: "gauge", Value: 2e6,
					Tags: map[string]any{"test": "large"},
				},
			},
			expectError: false,
			expectSum:   3e6,
			expectCount: 2,
		},
		{
			name: "HistogramAlwaysCreatesSketches",
			records: []*MetricRecord{
				{
					TID: 400, MetricName: "histogram_test", MetricType: "histogram",
					BucketData: &HistogramBuckets{Counts: []float64{1}, Bounds: []float64{1.0}},
					Tags:       map[string]any{"test": "hist"},
				},
			},
			expectError: false,
			expectSum:   -1, // Special case: histogram sum is not predictable due to midpoint calculation
			expectCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sketches := make(map[int64]TagSketch)
			nodeBuilder := buffet.NewNodeMapBuilder()

			for _, record := range tt.records {
				err := processMetricRecord(record, &sketches, nodeBuilder)
				if tt.expectError {
					assert.Error(t, err)
					return
				} else {
					require.NoError(t, err)
				}
			}

			// Verify final state
			require.Len(t, sketches, 1, "Should have exactly one sketch")

			var sketch TagSketch
			for _, s := range sketches {
				sketch = s
				break
			}

			assert.Equal(t, tt.expectCount, sketch.DataPointCount)

			if tt.expectCount == 1 && sketch.MetricType != "histogram" {
				// Single value case (non-histogram)
				assert.Nil(t, sketch.Sketch, "Single non-histogram value should not create sketch")
				assert.Equal(t, tt.records[0].Value, sketch.SingleValue)
			} else {
				// Multiple values or histogram case
				assert.NotNil(t, sketch.Sketch, "Multiple values or histogram should create sketch")

				if tt.expectSum >= 0 {
					// Use proportional tolerance for sketch approximation
					tolerance := math.Max(math.Abs(tt.expectSum)*0.02, 0.1) // 2% or minimum 0.1
					assert.InDelta(t, tt.expectSum, sketch.Sketch.GetSum(), tolerance, "Sketch sum should match expected")
				} else {
					// Special case: histogram sums are not predictable, just verify sketch has data
					assert.Greater(t, sketch.Sketch.GetCount(), float64(0), "Histogram should have data in sketch")
				}
			}
		})
	}
}
