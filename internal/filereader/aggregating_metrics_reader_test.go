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
	"testing"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockAggregatingMetricsReader implements Reader interface for testing aggregation
type mockAggregatingMetricsReader struct {
	rows     []Row
	index    int
	rowCount int64
	closed   bool
}

func newMockAggregatingMetricsReader(rows []Row) *mockAggregatingMetricsReader {
	return &mockAggregatingMetricsReader{rows: rows}
}

func (r *mockAggregatingMetricsReader) Next() (*Batch, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	if r.index >= len(r.rows) {
		return nil, io.EOF
	}

	batch := &Batch{
		Rows: make([]Row, 0, 100),
	}

	for len(batch.Rows) < 100 && r.index < len(r.rows) {
		row := make(Row)
		for k, v := range r.rows[r.index] {
			row[k] = v
		}
		batch.Rows = append(batch.Rows, row)
		r.index++
	}

	r.rowCount += int64(len(batch.Rows))
	return batch, nil
}

func (r *mockAggregatingMetricsReader) Close() error {
	r.closed = true
	return nil
}

func (r *mockAggregatingMetricsReader) TotalRowsReturned() int64 {
	return r.rowCount
}

func TestAggregatingMetricsReader_SingleSingleton(t *testing.T) {
	// Single singleton row - should pass through unchanged
	inputRows := []Row{
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10500), // Will be truncated to 10000
			"sketch":                []byte{},     // Empty sketch = singleton
			"rollup_sum":            75.0,
			"rollup_count":          1.0,
			"rollup_avg":            75.0,
			"rollup_min":            75.0,
			"rollup_max":            75.0,
		},
	}

	mockReader := newMockAggregatingMetricsReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000) // 10s aggregation
	require.NoError(t, err)
	defer aggregatingReader.Close()

	// Read the aggregated result
	batch, err := aggregatingReader.Next()
	require.NoError(t, err)
	require.Len(t, batch.Rows, 1)

	row := batch.Rows[0]
	// Verify timestamp was truncated
	assert.Equal(t, int64(10000), row["_cardinalhq.timestamp"])

	// Verify other fields unchanged
	assert.Equal(t, "cpu.usage", row["_cardinalhq.name"])
	assert.Equal(t, int64(12345), row["_cardinalhq.tid"])
	assert.Equal(t, 75.0, row["rollup_sum"])
	assert.Equal(t, 1.0, row["rollup_count"])

	// Should be EOF on next read
	_, err = aggregatingReader.Next()
	assert.Equal(t, io.EOF, err)
}

func TestAggregatingMetricsReader_MultipleSingletons(t *testing.T) {
	// Multiple singleton rows with same key - should be aggregated into sketch
	inputRows := []Row{
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10500), // Will be truncated to 10000
			"sketch":                []byte{},     // Empty sketch = singleton
			"rollup_sum":            75.0,
			"rollup_count":          1.0,
		},
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10800), // Will be truncated to 10000 (same group)
			"sketch":                []byte{},     // Empty sketch = singleton
			"rollup_sum":            85.0,
			"rollup_count":          1.0,
		},
	}

	mockReader := newMockAggregatingMetricsReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000) // 10s aggregation
	require.NoError(t, err)
	defer aggregatingReader.Close()

	// Read the aggregated result
	batch, err := aggregatingReader.Next()
	require.NoError(t, err)
	require.Len(t, batch.Rows, 1)

	row := batch.Rows[0]
	// Verify aggregated values
	assert.Equal(t, int64(10000), row["_cardinalhq.timestamp"])
	assert.Equal(t, "cpu.usage", row["_cardinalhq.name"])
	assert.Equal(t, int64(12345), row["_cardinalhq.tid"])

	// Should have aggregated to 2 count, sum should be approximately 160.0
	assert.Equal(t, 2.0, row["rollup_count"])
	assert.InDelta(t, 160.0, row["rollup_sum"], 1.0) // DDSketch is approximate
	assert.InDelta(t, 80.0, row["rollup_avg"], 1.0)  // 160/2
	assert.InDelta(t, 75.0, row["rollup_min"], 1.0)
	assert.InDelta(t, 85.0, row["rollup_max"], 1.0)

	// Should have a non-empty sketch now
	sketch, ok := row["sketch"].([]byte)
	assert.True(t, ok)
	assert.NotEmpty(t, sketch)
}

func TestAggregatingMetricsReader_SketchAndSingletons(t *testing.T) {
	// Create a sketch with one value for testing
	testSketch, err := ddsketch.NewDefaultDDSketch(0.01)
	require.NoError(t, err)
	err = testSketch.Add(100.0)
	require.NoError(t, err)

	var sketchBytes []byte
	testSketch.Encode(&sketchBytes, false)

	// Mix of sketch and singleton rows with same key - should merge properly
	inputRows := []Row{
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10500), // Will be truncated to 10000
			"sketch":                []byte{},     // Empty sketch = singleton
			"rollup_sum":            75.0,
			"rollup_count":          1.0,
		},
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10800), // Will be truncated to 10000 (same group)
			"sketch":                sketchBytes,  // Has sketch
			"rollup_sum":            100.0,        // Should be ignored
			"rollup_count":          1.0,          // Should be ignored
		},
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10900), // Will be truncated to 10000 (same group)
			"sketch":                []byte{},     // Empty sketch = singleton
			"rollup_sum":            85.0,
			"rollup_count":          1.0,
		},
	}

	mockReader := newMockAggregatingMetricsReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000) // 10s aggregation
	require.NoError(t, err)
	defer aggregatingReader.Close()

	// Read the aggregated result
	batch, err := aggregatingReader.Next()
	require.NoError(t, err)
	require.Len(t, batch.Rows, 1)

	row := batch.Rows[0]
	// Verify the result
	assert.Equal(t, int64(10000), row["_cardinalhq.timestamp"])
	assert.Equal(t, "cpu.usage", row["_cardinalhq.name"])
	assert.Equal(t, int64(12345), row["_cardinalhq.tid"])

	// Should have aggregated: sketch(100.0) + singleton(75.0) + singleton(85.0) = 3 values
	assert.Equal(t, 3.0, row["rollup_count"])
	assert.InDelta(t, 260.0, row["rollup_sum"], 5.0) // 100 + 75 + 85, DDSketch is approximate

	// Should have a non-empty sketch
	sketch, ok := row["sketch"].([]byte)
	assert.True(t, ok)
	assert.NotEmpty(t, sketch)
}

func TestAggregatingMetricsReader_DifferentKeys(t *testing.T) {
	// Rows with different keys - should not be aggregated
	inputRows := []Row{
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10000),
			"sketch":                []byte{},
			"rollup_sum":            75.0,
			"rollup_count":          1.0,
		},
		{
			"_cardinalhq.name":      "memory.usage", // Different metric name
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10000),
			"sketch":                []byte{},
			"rollup_sum":            85.0,
			"rollup_count":          1.0,
		},
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(54321), // Different TID
			"_cardinalhq.timestamp": int64(10000),
			"sketch":                []byte{},
			"rollup_sum":            65.0,
			"rollup_count":          1.0,
		},
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(20000), // Different timestamp bucket
			"sketch":                []byte{},
			"rollup_sum":            95.0,
			"rollup_count":          1.0,
		},
	}

	mockReader := newMockAggregatingMetricsReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000) // 10s aggregation
	require.NoError(t, err)
	defer aggregatingReader.Close()

	// Read all results
	var allRows []Row
	for {
		batch, err := aggregatingReader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		allRows = append(allRows, batch.Rows...)
	}

	// Should have 4 separate rows (no aggregation)
	require.Len(t, allRows, 4)

	// All should be singletons (count = 1.0)
	for i, row := range allRows {
		assert.Equal(t, 1.0, row["rollup_count"], "Row %d should be singleton", i)
	}
}

func TestAggregatingMetricsReader_InvalidRows(t *testing.T) {
	// Rows with missing required fields - should be skipped
	inputRows := []Row{
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10000),
			"sketch":                []byte{},
			"rollup_sum":            75.0,
			"rollup_count":          1.0,
		},
		{
			// Missing TID - should be skipped
			"_cardinalhq.name":      "memory.usage",
			"_cardinalhq.timestamp": int64(10000),
			"sketch":                []byte{},
			"rollup_sum":            85.0,
			"rollup_count":          1.0,
		},
		{
			"_cardinalhq.name":      "disk.usage",
			"_cardinalhq.tid":       int64(54321),
			"_cardinalhq.timestamp": int64(10000),
			"sketch":                []byte{},
			"rollup_sum":            65.0,
			"rollup_count":          1.0,
		},
	}

	mockReader := newMockAggregatingMetricsReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000)
	require.NoError(t, err)
	defer aggregatingReader.Close()

	// Read all results
	var allRows []Row
	for {
		batch, err := aggregatingReader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		allRows = append(allRows, batch.Rows...)
	}

	// Should have 2 valid rows (invalid row skipped)
	require.Len(t, allRows, 2)

	// Verify the valid rows
	assert.Equal(t, "cpu.usage", allRows[0]["_cardinalhq.name"])
	assert.Equal(t, "disk.usage", allRows[1]["_cardinalhq.name"])
}

func TestAggregatingMetricsReader_TimestampTruncation(t *testing.T) {
	// Test that timestamps are properly truncated to aggregation period
	inputRows := []Row{
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10123), // Should truncate to 10000
			"sketch":                []byte{},
			"rollup_sum":            75.0,
			"rollup_count":          1.0,
		},
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(19999), // Should truncate to 10000 (same group)
			"sketch":                []byte{},
			"rollup_sum":            85.0,
			"rollup_count":          1.0,
		},
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(20001), // Should truncate to 20000 (different group)
			"sketch":                []byte{},
			"rollup_sum":            95.0,
			"rollup_count":          1.0,
		},
	}

	mockReader := newMockAggregatingMetricsReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000) // 10s aggregation
	require.NoError(t, err)
	defer aggregatingReader.Close()

	// Read all results
	var allRows []Row
	for {
		batch, err := aggregatingReader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		allRows = append(allRows, batch.Rows...)
	}

	// Should have 2 rows: one for 10000 timestamp bucket, one for 20000
	require.Len(t, allRows, 2)

	// First row: aggregated from first two input rows
	assert.Equal(t, int64(10000), allRows[0]["_cardinalhq.timestamp"])
	assert.Equal(t, 2.0, allRows[0]["rollup_count"])
	assert.InDelta(t, 160.0, allRows[0]["rollup_sum"], 1.0) // 75 + 85, DDSketch is approximate

	// Second row: third input row
	assert.Equal(t, int64(20000), allRows[1]["_cardinalhq.timestamp"])
	assert.Equal(t, 1.0, allRows[1]["rollup_count"])
	assert.Equal(t, 95.0, allRows[1]["rollup_sum"])
}

func TestAggregatingMetricsReader_MultipleSketchesMerging(t *testing.T) {
	// Create two sketches with different values
	sketch1, err := ddsketch.NewDefaultDDSketch(0.01)
	require.NoError(t, err)
	err = sketch1.Add(100.0)
	require.NoError(t, err)
	err = sketch1.Add(200.0)
	require.NoError(t, err)

	var sketch1Bytes []byte
	sketch1.Encode(&sketch1Bytes, false)

	sketch2, err := ddsketch.NewDefaultDDSketch(0.01)
	require.NoError(t, err)
	err = sketch2.Add(300.0)
	require.NoError(t, err)

	var sketch2Bytes []byte
	sketch2.Encode(&sketch2Bytes, false)

	// Two rows with sketches - should be merged
	inputRows := []Row{
		{
			"_cardinalhq.name":      "response.time",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10000),
			"sketch":                sketch1Bytes, // Sketch with 2 values: 100, 200
			"rollup_count":          2.0,
			"rollup_sum":            300.0,
		},
		{
			"_cardinalhq.name":      "response.time",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10000), // Same key
			"sketch":                sketch2Bytes, // Sketch with 1 value: 300
			"rollup_count":          1.0,
			"rollup_sum":            300.0,
		},
	}

	mockReader := newMockAggregatingMetricsReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000)
	require.NoError(t, err)
	defer aggregatingReader.Close()

	// Read the aggregated result
	batch, err := aggregatingReader.Next()
	require.NoError(t, err)
	require.Len(t, batch.Rows, 1)

	row := batch.Rows[0]
	// Verify merged sketch has all 3 values (approximately)
	assert.Equal(t, 3.0, row["rollup_count"])
	assert.InDelta(t, 600.0, row["rollup_sum"], 10.0) // 100 + 200 + 300, DDSketch is approximate
	assert.InDelta(t, 200.0, row["rollup_avg"], 10.0) // 600/3
	assert.InDelta(t, 100.0, row["rollup_min"], 5.0)
	assert.InDelta(t, 300.0, row["rollup_max"], 5.0)

	// Should have a non-empty sketch
	sketch, ok := row["sketch"].([]byte)
	assert.True(t, ok)
	assert.NotEmpty(t, sketch)
}

// mockEOFReader implements Reader interface for testing EOF with data condition
type mockEOFReader struct {
	rows     []Row
	index    int
	rowCount int64
	closed   bool
}

func newMockEOFReader(rows []Row) *mockEOFReader {
	return &mockEOFReader{rows: rows}
}

func (r *mockEOFReader) Next() (*Batch, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	if r.index >= len(r.rows) {
		return nil, io.EOF
	}

	batch := &Batch{
		Rows: make([]Row, 0, 100),
	}

	for len(batch.Rows) < 100 && r.index < len(r.rows) {
		row := make(Row)
		for k, v := range r.rows[r.index] {
			row[k] = v
		}
		batch.Rows = append(batch.Rows, row)
		r.index++
	}

	r.rowCount += int64(len(batch.Rows))
	return batch, nil
}

func (r *mockEOFReader) Close() error {
	r.closed = true
	return nil
}

func (r *mockEOFReader) TotalRowsReturned() int64 {
	return r.rowCount
}

func TestAggregatingMetricsReader_EOFWithData(t *testing.T) {
	// Test the n>0 && io.EOF case: ensure last row is processed correctly
	inputRows := []Row{
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10000),
			"sketch":                []byte{},
			"rollup_sum":            75.0,
			"rollup_count":          1.0,
			"rollup_avg":            75.0,
			"rollup_min":            75.0,
			"rollup_max":            75.0,
		},
	}

	mockReader := newMockEOFReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000)
	require.NoError(t, err)
	defer aggregatingReader.Close()

	// Read should return the row even though EOF is returned with data
	batch, err := aggregatingReader.Next()
	require.NoError(t, err)
	require.Len(t, batch.Rows, 1)

	row := batch.Rows[0]
	// Verify the row was processed correctly
	assert.Equal(t, "cpu.usage", row["_cardinalhq.name"])
	assert.Equal(t, int64(12345), row["_cardinalhq.tid"])
	assert.Equal(t, int64(10000), row["_cardinalhq.timestamp"])
	assert.Equal(t, 75.0, row["rollup_sum"])

	// Second read should return EOF
	_, err = aggregatingReader.Next()
	assert.Equal(t, io.EOF, err)
}

func TestAggregatingMetricsReader_DropHistogramWithoutSketch(t *testing.T) {
	// Test that histogram rows without sketches are dropped
	inputRows := []Row{
		{
			"_cardinalhq.name":        "http.request_duration",
			"_cardinalhq.tid":         int64(12345),
			"_cardinalhq.timestamp":   int64(10500),
			"_cardinalhq.metric_type": "histogram", // This is a histogram
			"sketch":                  []byte{},    // But has empty sketch - should be dropped
			"rollup_sum":              75.0,
			"rollup_count":            1.0,
		},
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10500),
			"type":                  "Gauge",  // This is a gauge
			"sketch":                []byte{}, // Empty sketch is OK for gauges
			"rollup_sum":            50.0,
			"rollup_count":          1.0,
		},
	}

	mockReader := newMockAggregatingMetricsReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000) // 10s aggregation
	require.NoError(t, err)
	defer aggregatingReader.Close()

	// Read the aggregated result
	batch, err := aggregatingReader.Next()
	require.NoError(t, err)
	require.Len(t, batch.Rows, 1, "Should only return one row (gauge), histogram should be dropped")

	row := batch.Rows[0]
	// Verify only the gauge row was returned
	assert.Equal(t, "cpu.usage", row["_cardinalhq.name"])
	assert.Equal(t, "Gauge", row["type"])
	assert.Equal(t, 50.0, row["rollup_sum"])

	// Second read should return EOF
	_, err = aggregatingReader.Next()
	assert.Equal(t, io.EOF, err)
}

func TestAggregatingMetricsReader_PendingRowReset(t *testing.T) {
	inputRows := []Row{
		{
			"_cardinalhq.name":      "cpu.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10000),
			"sketch":                []byte{},
			"rollup_sum":            1.0,
			"rollup_count":          1.0,
			"unused":                "extra",
		},
		{
			"_cardinalhq.name":      "memory.usage",
			"_cardinalhq.tid":       int64(12345),
			"_cardinalhq.timestamp": int64(10000),
			"sketch":                []byte{},
			"rollup_sum":            2.0,
			"rollup_count":          1.0,
		},
	}

	mockReader := newMockAggregatingMetricsReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000)
	require.NoError(t, err)
	defer aggregatingReader.Close()

	// Read all results - they should be in separate batches since they have different keys
	var allRows []Row
	for {
		batch, err := aggregatingReader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		allRows = append(allRows, batch.Rows...)
	}

	require.Len(t, allRows, 2)
	_, ok := allRows[1]["unused"]
	assert.False(t, ok, "unused field should not leak into subsequent rows")
}
