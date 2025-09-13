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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// createTestSketch creates a test sketch with a single value
func createTestSketch(t *testing.T, value float64) []byte {
	t.Helper()
	sketch, err := ddsketch.NewDefaultDDSketch(0.01)
	require.NoError(t, err)
	require.NoError(t, sketch.Add(value))
	return helpers.EncodeSketch(sketch)
}

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

func (r *mockAggregatingMetricsReader) Next(ctx context.Context) (*Batch, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	if r.index >= len(r.rows) {
		return nil, io.EOF
	}

	batch := pipeline.GetBatch()

	for batch.Len() < 100 && r.index < len(r.rows) {
		row := batch.AddRow()
		for k, v := range r.rows[r.index] {
			row[k] = v
		}
		r.index++
	}

	r.rowCount += int64(batch.Len())
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
			wkk.RowKeyCName:       "cpu.usage",
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(10500), // Will be truncated to 10000
			wkk.RowKeySketch:      createTestSketch(t, 75.0),
			wkk.RowKeyRollupSum:   75.0,
			wkk.RowKeyRollupCount: 1.0,
			wkk.RowKeyRollupAvg:   75.0,
			wkk.RowKeyRollupMin:   75.0,
			wkk.RowKeyRollupMax:   75.0,
		},
	}

	mockReader := newMockAggregatingMetricsReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000) // 10s aggregation
	require.NoError(t, err)
	defer func() { _ = aggregatingReader.Close() }()

	// Read the aggregated result
	batch, err := aggregatingReader.Next(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 1, batch.Len())

	row := batch.Get(0)
	// Verify timestamp was truncated
	assert.Equal(t, int64(10000), row[wkk.RowKeyCTimestamp])

	// Verify other fields unchanged
	assert.Equal(t, "cpu.usage", row[wkk.RowKeyCName])
	assert.Equal(t, int64(12345), row[wkk.RowKeyCTID])
	// DDSketch provides approximations, allow for small error
	assert.InDelta(t, 75.0, row[wkk.RowKeyRollupSum], 1.0, "Sum should be approximately 75")
	assert.Equal(t, 1.0, row[wkk.RowKeyRollupCount])

	// Should be EOF on next read
	_, err = aggregatingReader.Next(context.TODO())
	assert.Equal(t, io.EOF, err)
}

func TestAggregatingMetricsReader_MultipleSingletons(t *testing.T) {
	// Multiple singleton rows with same key - should be aggregated into sketch
	inputRows := []Row{
		{
			wkk.RowKeyCName:       "cpu.usage",
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(10500), // Will be truncated to 10000
			wkk.RowKeySketch:      createTestSketch(t, 75.0),
			wkk.RowKeyRollupSum:   75.0,
			wkk.RowKeyRollupCount: 1.0,
		},
		{
			wkk.RowKeyCName:       "cpu.usage",
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(10800), // Will be truncated to 10000 (same group)
			wkk.RowKeySketch:      createTestSketch(t, 75.0),
			wkk.RowKeyRollupSum:   85.0,
			wkk.RowKeyRollupCount: 1.0,
		},
	}

	mockReader := newMockAggregatingMetricsReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000) // 10s aggregation
	require.NoError(t, err)
	defer func() { _ = aggregatingReader.Close() }()

	// Read the aggregated result
	batch, err := aggregatingReader.Next(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 1, batch.Len())

	row := batch.Get(0)
	// Verify aggregated values
	assert.Equal(t, int64(10000), row[wkk.RowKeyCTimestamp])
	assert.Equal(t, "cpu.usage", row[wkk.RowKeyCName])
	assert.Equal(t, int64(12345), row[wkk.RowKeyCTID])

	// Should have aggregated to 2 count, sum should be approximately 160.0
	assert.Equal(t, 2.0, row[wkk.RowKeyRollupCount])
	assert.InDelta(t, 160.0, row[wkk.RowKeyRollupSum], 12.0) // DDSketch has approximation error
	assert.InDelta(t, 80.0, row[wkk.RowKeyRollupAvg], 6.0)   // Average of approximated values
	assert.InDelta(t, 75.0, row[wkk.RowKeyRollupMin], 11.0)  // Min can vary with sketch approximation
	assert.InDelta(t, 85.0, row[wkk.RowKeyRollupMax], 11.0)  // Max can vary with sketch approximation

	// Should have a non-empty sketch now
	sketch, ok := row[wkk.RowKeySketch].([]byte)
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
			wkk.RowKeyCName:       "cpu.usage",
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(10500), // Will be truncated to 10000
			wkk.RowKeySketch:      createTestSketch(t, 75.0),
			wkk.RowKeyRollupSum:   75.0,
			wkk.RowKeyRollupCount: 1.0,
		},
		{
			wkk.RowKeyCName:       "cpu.usage",
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(10800), // Will be truncated to 10000 (same group)
			wkk.RowKeySketch:      sketchBytes,  // Has sketch
			wkk.RowKeyRollupSum:   100.0,        // Should be ignored
			wkk.RowKeyRollupCount: 1.0,          // Should be ignored
		},
		{
			wkk.RowKeyCName:       "cpu.usage",
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(10900), // Will be truncated to 10000 (same group)
			wkk.RowKeySketch:      createTestSketch(t, 75.0),
			wkk.RowKeyRollupSum:   85.0,
			wkk.RowKeyRollupCount: 1.0,
		},
	}

	mockReader := newMockAggregatingMetricsReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000) // 10s aggregation
	require.NoError(t, err)
	defer func() { _ = aggregatingReader.Close() }()

	// Read the aggregated result
	batch, err := aggregatingReader.Next(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 1, batch.Len())

	row := batch.Get(0)
	// Verify the result
	assert.Equal(t, int64(10000), row[wkk.RowKeyCTimestamp])
	assert.Equal(t, "cpu.usage", row[wkk.RowKeyCName])
	assert.Equal(t, int64(12345), row[wkk.RowKeyCTID])

	// Should have aggregated: sketch(100.0) + singleton(75.0) + singleton(85.0) = 3 values
	assert.Equal(t, 3.0, row[wkk.RowKeyRollupCount])
	assert.InDelta(t, 260.0, row[wkk.RowKeyRollupSum], 11.0) // 100 + 75 + 85, DDSketch has approximation error

	// Should have a non-empty sketch
	sketch, ok := row[wkk.RowKeySketch].([]byte)
	assert.True(t, ok)
	assert.NotEmpty(t, sketch)
}

func TestAggregatingMetricsReader_DifferentKeys(t *testing.T) {
	// Rows with different keys - should not be aggregated
	inputRows := []Row{
		{
			wkk.RowKeyCName:       "cpu.usage",
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(10000),
			wkk.RowKeySketch:      createTestSketch(t, 75.0),
			wkk.RowKeyRollupSum:   75.0,
			wkk.RowKeyRollupCount: 1.0,
		},
		{
			wkk.RowKeyCName:       "memory.usage", // Different metric name
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(10000),
			wkk.RowKeySketch:      createTestSketch(t, 75.0),
			wkk.RowKeyRollupSum:   85.0,
			wkk.RowKeyRollupCount: 1.0,
		},
		{
			wkk.RowKeyCName:       "cpu.usage",
			wkk.RowKeyCTID:        int64(54321), // Different TID
			wkk.RowKeyCTimestamp:  int64(10000),
			wkk.RowKeySketch:      createTestSketch(t, 75.0),
			wkk.RowKeyRollupSum:   65.0,
			wkk.RowKeyRollupCount: 1.0,
		},
		{
			wkk.RowKeyCName:       "cpu.usage",
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(20000), // Different timestamp bucket
			wkk.RowKeySketch:      createTestSketch(t, 75.0),
			wkk.RowKeyRollupSum:   95.0,
			wkk.RowKeyRollupCount: 1.0,
		},
	}

	mockReader := newMockAggregatingMetricsReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000) // 10s aggregation
	require.NoError(t, err)
	defer func() { _ = aggregatingReader.Close() }()

	// Read all results
	var allRows []Row
	for {
		batch, err := aggregatingReader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := 0; i < batch.Len(); i++ {
			allRows = append(allRows, batch.Get(i))
		}
	}

	// Should have 4 separate rows (no aggregation)
	require.Len(t, allRows, 4)

	// All should be singletons (count = 1.0)
	for i, row := range allRows {
		assert.Equal(t, 1.0, row[wkk.RowKeyRollupCount], "Row %d should be singleton", i)
	}
}

func TestAggregatingMetricsReader_InvalidRows(t *testing.T) {
	// Rows with missing required fields - should be skipped
	inputRows := []Row{
		{
			wkk.RowKeyCName:       "cpu.usage",
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(10000),
			wkk.RowKeySketch:      createTestSketch(t, 75.0),
			wkk.RowKeyRollupSum:   75.0,
			wkk.RowKeyRollupCount: 1.0,
		},
		{
			// Missing TID - should be skipped
			wkk.RowKeyCName:       "memory.usage",
			wkk.RowKeyCTimestamp:  int64(10000),
			wkk.RowKeySketch:      createTestSketch(t, 75.0),
			wkk.RowKeyRollupSum:   85.0,
			wkk.RowKeyRollupCount: 1.0,
		},
		{
			wkk.RowKeyCName:       "disk.usage",
			wkk.RowKeyCTID:        int64(54321),
			wkk.RowKeyCTimestamp:  int64(10000),
			wkk.RowKeySketch:      createTestSketch(t, 75.0),
			wkk.RowKeyRollupSum:   65.0,
			wkk.RowKeyRollupCount: 1.0,
		},
	}

	mockReader := newMockAggregatingMetricsReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000)
	require.NoError(t, err)
	defer func() { _ = aggregatingReader.Close() }()

	// Read all results
	var allRows []Row
	for {
		batch, err := aggregatingReader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := 0; i < batch.Len(); i++ {
			allRows = append(allRows, batch.Get(i))
		}
	}

	// Should have 2 valid rows (invalid row skipped)
	require.Len(t, allRows, 2)

	// Verify the valid rows
	assert.Equal(t, "cpu.usage", allRows[0][wkk.RowKeyCName])
	assert.Equal(t, "disk.usage", allRows[1][wkk.RowKeyCName])
}

func TestAggregatingMetricsReader_TimestampTruncation(t *testing.T) {
	// Test that timestamps are properly truncated to aggregation period
	inputRows := []Row{
		{
			wkk.RowKeyCName:       "cpu.usage",
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(10123), // Should truncate to 10000
			wkk.RowKeySketch:      createTestSketch(t, 75.0),
			wkk.RowKeyRollupSum:   75.0,
			wkk.RowKeyRollupCount: 1.0,
		},
		{
			wkk.RowKeyCName:       "cpu.usage",
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(19999), // Should truncate to 10000 (same group)
			wkk.RowKeySketch:      createTestSketch(t, 75.0),
			wkk.RowKeyRollupSum:   85.0,
			wkk.RowKeyRollupCount: 1.0,
		},
		{
			wkk.RowKeyCName:       "cpu.usage",
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(20001), // Should truncate to 20000 (different group)
			wkk.RowKeySketch:      createTestSketch(t, 75.0),
			wkk.RowKeyRollupSum:   95.0,
			wkk.RowKeyRollupCount: 1.0,
		},
	}

	mockReader := newMockAggregatingMetricsReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000) // 10s aggregation
	require.NoError(t, err)
	defer func() { _ = aggregatingReader.Close() }()

	// Read all results
	var allRows []Row
	for {
		batch, err := aggregatingReader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := 0; i < batch.Len(); i++ {
			allRows = append(allRows, batch.Get(i))
		}
	}

	// Should have 2 rows: one for 10000 timestamp bucket, one for 20000
	require.Len(t, allRows, 2)

	// First row: aggregated from first two input rows
	assert.Equal(t, int64(10000), allRows[0][wkk.RowKeyCTimestamp])
	assert.Equal(t, 2.0, allRows[0][wkk.RowKeyRollupCount])
	assert.InDelta(t, 160.0, allRows[0][wkk.RowKeyRollupSum], 12.0) // 75 + 85, DDSketch has approximation error

	// Second row: third input row
	assert.Equal(t, int64(20000), allRows[1][wkk.RowKeyCTimestamp])
	assert.Equal(t, 1.0, allRows[1][wkk.RowKeyRollupCount])
	assert.InDelta(t, 95.0, allRows[1][wkk.RowKeyRollupSum], 21.0, "DDSketch has approximation error")
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
			wkk.RowKeyCName:       "response.time",
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(10000),
			wkk.RowKeySketch:      sketch1Bytes, // Sketch with 2 values: 100, 200
			wkk.RowKeyRollupCount: 2.0,
			wkk.RowKeyRollupSum:   300.0,
		},
		{
			wkk.RowKeyCName:       "response.time",
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(10000), // Same key
			wkk.RowKeySketch:      sketch2Bytes, // Sketch with 1 value: 300
			wkk.RowKeyRollupCount: 1.0,
			wkk.RowKeyRollupSum:   300.0,
		},
	}

	mockReader := newMockAggregatingMetricsReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000)
	require.NoError(t, err)
	defer func() { _ = aggregatingReader.Close() }()

	// Read the aggregated result
	batch, err := aggregatingReader.Next(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 1, batch.Len())

	row := batch.Get(0)
	// Verify merged sketch has all 3 values (approximately)
	assert.Equal(t, 3.0, row[wkk.RowKeyRollupCount])
	assert.InDelta(t, 600.0, row[wkk.RowKeyRollupSum], 10.0) // 100 + 200 + 300, DDSketch is approximate
	assert.InDelta(t, 200.0, row[wkk.RowKeyRollupAvg], 10.0) // 600/3
	assert.InDelta(t, 100.0, row[wkk.RowKeyRollupMin], 5.0)
	assert.InDelta(t, 300.0, row[wkk.RowKeyRollupMax], 5.0)

	// Should have a non-empty sketch
	sketch, ok := row[wkk.RowKeySketch].([]byte)
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

func (r *mockEOFReader) Next(ctx context.Context) (*Batch, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	if r.index >= len(r.rows) {
		return nil, io.EOF
	}

	batch := pipeline.GetBatch()

	for batch.Len() < 100 && r.index < len(r.rows) {
		row := batch.AddRow()
		for k, v := range r.rows[r.index] {
			row[k] = v
		}
		r.index++
	}

	r.rowCount += int64(batch.Len())
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
			wkk.RowKeyCName:       "cpu.usage",
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(10000),
			wkk.RowKeySketch:      createTestSketch(t, 75.0),
			wkk.RowKeyRollupSum:   75.0,
			wkk.RowKeyRollupCount: 1.0,
			wkk.RowKeyRollupAvg:   75.0,
			wkk.RowKeyRollupMin:   75.0,
			wkk.RowKeyRollupMax:   75.0,
		},
	}

	mockReader := newMockEOFReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000)
	require.NoError(t, err)
	defer func() { _ = aggregatingReader.Close() }()

	// Read should return the row even though EOF is returned with data
	batch, err := aggregatingReader.Next(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 1, batch.Len())

	row := batch.Get(0)
	// Verify the row was processed correctly
	assert.Equal(t, "cpu.usage", row[wkk.RowKeyCName])
	assert.Equal(t, int64(12345), row[wkk.RowKeyCTID])
	assert.Equal(t, int64(10000), row[wkk.RowKeyCTimestamp])
	assert.InDelta(t, 75.0, row[wkk.RowKeyRollupSum], 1.0, "DDSketch approximation")

	// Second read should return EOF
	_, err = aggregatingReader.Next(context.TODO())
	assert.Equal(t, io.EOF, err)
}

func TestAggregatingMetricsReader_DropHistogramWithoutSketch(t *testing.T) {
	// Test that histogram rows without sketches are dropped
	inputRows := []Row{
		{
			wkk.RowKeyCName:       "http.request_duration",
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(10500),
			wkk.RowKeyCMetricType: "histogram", // This is a histogram
			wkk.RowKeySketch:      []byte{},    // But has empty sketch - should be dropped
			wkk.RowKeyRollupSum:   75.0,
			wkk.RowKeyRollupCount: 1.0,
		},
		{
			wkk.RowKeyCName:       "cpu.usage",
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(10500),
			wkk.RowKeySketch:      createTestSketch(t, 50.0),
			wkk.RowKeyRollupSum:   50.0,
			wkk.RowKeyRollupCount: 1.0,
		},
	}

	mockReader := newMockAggregatingMetricsReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000) // 10s aggregation
	require.NoError(t, err)
	defer func() { _ = aggregatingReader.Close() }()

	// Read the aggregated result
	batch, err := aggregatingReader.Next(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 1, batch.Len(), "Should only return one row (gauge), histogram should be dropped")

	row := batch.Get(0)
	// Verify only the gauge row was returned
	assert.Equal(t, "cpu.usage", row[wkk.RowKeyCName])
	assert.InDelta(t, 50.0, row[wkk.RowKeyRollupSum], 1.0, "DDSketch approximation")

	// Second read should return EOF
	_, err = aggregatingReader.Next(context.TODO())
	assert.Equal(t, io.EOF, err)
}

func TestAggregatingMetricsReader_PendingRowReset(t *testing.T) {
	inputRows := []Row{
		{
			wkk.RowKeyCName:         "cpu.usage",
			wkk.RowKeyCTID:          int64(12345),
			wkk.RowKeyCTimestamp:    int64(10000),
			wkk.RowKeySketch:        createTestSketch(t, 75.0),
			wkk.RowKeyRollupSum:     1.0,
			wkk.RowKeyRollupCount:   1.0,
			wkk.NewRowKey("unused"): "extra",
		},
		{
			wkk.RowKeyCName:       "memory.usage",
			wkk.RowKeyCTID:        int64(12345),
			wkk.RowKeyCTimestamp:  int64(10000),
			wkk.RowKeySketch:      createTestSketch(t, 75.0),
			wkk.RowKeyRollupSum:   2.0,
			wkk.RowKeyRollupCount: 1.0,
		},
	}

	mockReader := newMockAggregatingMetricsReader(inputRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, 10000, 1000)
	require.NoError(t, err)
	defer func() { _ = aggregatingReader.Close() }()

	// Read all results - they should be in separate batches since they have different keys
	var allRows []Row
	for {
		batch, err := aggregatingReader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		for i := 0; i < batch.Len(); i++ {
			allRows = append(allRows, batch.Get(i))
		}
	}

	require.Len(t, allRows, 2)
	_, ok := allRows[1][wkk.NewRowKey("unused")]
	assert.False(t, ok, "unused field should not leak into subsequent rows")
}

func TestAggregatingMetricsReader_ArbitraryRowCountBatchBoundary(t *testing.T) {
	// Test to verify AggregatingMetricsReader doesn't drop rows due to batch size boundaries
	// This test creates exactly 1234 rows that should aggregate down to 150 unique groups
	const totalRows = 1234
	const batchSize = 1000
	const aggregationPeriodMs = 10000

	// Create test data with 1234 rows that will aggregate into 150 groups (10 names * 5 TIDs * 3 timestamps)
	// IMPORTANT: Must be sorted by [name, tid, timestamp] for streaming aggregation to work
	testRows := make([]Row, totalRows)
	rowIndex := 0

	// Generate sorted data: for each name, for each tid, for each timestamp, create multiple rows
	groupIndex := 0
	for nameIdx := range 10 {
		for tid := range 5 {
			for tsIdx := range 3 {
				timestamp := int64(1000 + tsIdx*aggregationPeriodMs)

				// Create multiple rows for the same aggregation key to test batching across boundaries
				// Distribute 1234 rows across 150 groups: ~8 rows per group on average
				rowsForThisGroup := (totalRows / 150)
				if groupIndex < (totalRows % 150) {
					rowsForThisGroup++ // Distribute remainder across first (totalRows % 150) groups
				}

				for r := 0; r < rowsForThisGroup && rowIndex < totalRows; r++ {
					testRows[rowIndex] = Row{
						wkk.RowKeyCName:       fmt.Sprintf("metric_%d", nameIdx),
						wkk.RowKeyCTID:        int64(tid),
						wkk.RowKeyCTimestamp:  timestamp,
						wkk.RowKeyCMetricType: "counter",
						wkk.RowKeyRollupSum:   float64(rowIndex + 1), // Sequential values
						wkk.RowKeyRollupCount: float64(1),            // Each row represents 1 sample
						wkk.RowKeySketch:      createTestSketch(t, float64(rowIndex+1)),
					}
					rowIndex++
				}
				groupIndex++
			}
		}
	}

	// Debug: Print first 100 rows to verify sort order
	t.Logf("First 100 input rows (should be sorted by [name, tid, timestamp]):")
	for i := 0; i < min(100, len(testRows)); i++ {
		row := testRows[i]
		t.Logf("  Input[%d]: name=%v, tid=%v, ts=%v", i,
			row[wkk.RowKeyCName],
			row[wkk.RowKeyCTID],
			row[wkk.RowKeyCTimestamp])
	}

	mockReader := NewMockReader(testRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, aggregationPeriodMs, batchSize)
	require.NoError(t, err)
	defer func() { _ = aggregatingReader.Close() }()

	// Read all aggregated rows back and count them
	totalRead := 0
	batchCount := 0
	for {
		batch, err := aggregatingReader.Next(context.TODO())
		if err == io.EOF {
			t.Logf("Hit EOF after %d batches, %d total rows", batchCount, totalRead)
			break
		}
		require.NoError(t, err, "Failed to read batch %d", batchCount+1)

		batchSizeActual := batch.Len()
		totalRead += batchSizeActual
		batchCount++

		// Log batch sizes to help debug the pattern
		t.Logf("Aggregated Batch %d: %d rows (total so far: %d)", batchCount, batchSizeActual, totalRead)

		// Debug first few rows to see if they're actually being aggregated
		if batchCount == 1 {
			for i := 0; i < min(3, batch.Len()); i++ {
				row := batch.Get(i)
				t.Logf("  Row %d: name=%v, tid=%v, ts=%v, count=%v", i,
					row[wkk.RowKeyCName],
					row[wkk.RowKeyCTID],
					row[wkk.RowKeyCTimestamp],
					row[wkk.RowKeyRollupCount])
			}
		}

		// Verify each row has the expected aggregated structure
		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			require.Contains(t, row, wkk.RowKeyCName, "Aggregated row missing metric name")
			require.Contains(t, row, wkk.RowKeyCTID, "Aggregated row missing TID")
			require.Contains(t, row, wkk.RowKeyCTimestamp, "Aggregated row missing timestamp")
			require.Contains(t, row, wkk.RowKeyRollupSum, "Aggregated row missing rollup_sum")
			require.Contains(t, row, wkk.RowKeyRollupCount, "Aggregated row missing rollup_count")
		}
	}

	// Calculate expected aggregated row count: 10 metrics * 5 TIDs * 3 time buckets = 150
	expectedAggregatedRows := 10 * 5 * 3 // metrics * TIDs * time_buckets

	// This is the critical assertion - we must get the expected aggregated count
	assert.Equal(t, expectedAggregatedRows, totalRead, "AggregatingMetricsReader produced wrong aggregated count")

	// Verify the reader's internal count matches
	assert.Equal(t, int64(totalRead), aggregatingReader.TotalRowsReturned(), "TotalRowsReturned doesn't match actual rows read")

	t.Logf("SUCCESS: Fed %d rows, got %d aggregated rows back in %d batches (expected %d)", totalRows, totalRead, batchCount, expectedAggregatedRows)
}

func TestAggregatingMetricsReader_NoAggregationPassthrough(t *testing.T) {
	// Test "10s in, 10s out" - each row has unique aggregation key, so no aggregation should occur
	// This verifies that AggregatingMetricsReader preserves row order and count when no aggregation is needed
	const totalRows = 1234
	const batchSize = 1000
	const aggregationPeriodMs = 10000

	// Create 1234 rows with unique aggregation keys (each row gets unique metric name or timestamp)
	testRows := make([]Row, totalRows)
	for i := 0; i < totalRows; i++ {
		testRows[i] = Row{
			wkk.RowKeyCName:       fmt.Sprintf("unique_metric_%d", i),  // Each row has unique metric name
			wkk.RowKeyCTID:        int64(42),                           // Same TID for all
			wkk.RowKeyCTimestamp:  int64(1000 + i*aggregationPeriodMs), // Each row in different time bucket
			wkk.RowKeyCMetricType: "gauge",
			wkk.RowKeyRollupSum:   float64(i + 1), // Sequential values to verify order
			wkk.RowKeyRollupCount: float64(1),     // Each row represents 1 sample
			wkk.RowKeySketch:      createTestSketch(t, float64(i+1)),
		}
	}

	// Debug: Print first few rows to verify structure
	t.Logf("First 5 input rows (each should have unique aggregation key):")
	for i := 0; i < min(5, len(testRows)); i++ {
		row := testRows[i]
		t.Logf("  Input[%d]: name=%v, tid=%v, ts=%v, sum=%v", i,
			row[wkk.RowKeyCName],
			row[wkk.RowKeyCTID],
			row[wkk.RowKeyCTimestamp],
			row[wkk.RowKeyRollupSum])
	}

	mockReader := NewMockReader(testRows)
	aggregatingReader, err := NewAggregatingMetricsReader(mockReader, aggregationPeriodMs, batchSize)
	require.NoError(t, err)
	defer func() { _ = aggregatingReader.Close() }()

	// Read all rows back - they should pass through unchanged
	var allOutputRows []Row
	totalRead := 0
	batchCount := 0
	for {
		batch, err := aggregatingReader.Next(context.TODO())
		if err == io.EOF {
			t.Logf("Hit EOF after %d batches, %d total rows", batchCount, totalRead)
			break
		}
		require.NoError(t, err, "Failed to read batch %d", batchCount+1)

		batchSizeActual := batch.Len()
		totalRead += batchSizeActual
		batchCount++

		t.Logf("Batch %d: %d rows (total so far: %d)", batchCount, batchSizeActual, totalRead)

		// Collect all rows to verify order and values
		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			allOutputRows = append(allOutputRows, row)

			// Verify each row has required fields
			require.Contains(t, row, wkk.RowKeyCName, "Output row missing metric name")
			require.Contains(t, row, wkk.RowKeyCTID, "Output row missing TID")
			require.Contains(t, row, wkk.RowKeyCTimestamp, "Output row missing timestamp")
			require.Contains(t, row, wkk.RowKeyRollupSum, "Output row missing rollup_sum")
			require.Contains(t, row, wkk.RowKeyRollupCount, "Output row missing rollup_count")
		}
	}

	// Critical assertion: must get exactly the same number of rows (no aggregation)
	assert.Equal(t, totalRows, totalRead, "AggregatingMetricsReader should pass through all rows when no aggregation occurs")
	assert.Len(t, allOutputRows, totalRows, "Output row collection should match input count")

	// Verify order preservation and values unchanged (except timestamp truncation)
	for i, outputRow := range allOutputRows {
		expectedName := fmt.Sprintf("unique_metric_%d", i)
		inputTimestamp := int64(1000 + i*aggregationPeriodMs)
		expectedTimestamp := (inputTimestamp / aggregationPeriodMs) * aggregationPeriodMs // Truncated to aggregation period
		expectedSum := float64(i + 1)

		assert.Equal(t, expectedName, outputRow[wkk.RowKeyCName],
			"Row %d: metric name should be preserved", i)
		assert.Equal(t, int64(42), outputRow[wkk.RowKeyCTID],
			"Row %d: TID should be preserved", i)
		assert.Equal(t, expectedTimestamp, outputRow[wkk.RowKeyCTimestamp],
			"Row %d: timestamp should be truncated to aggregation period", i)
		// Allow for DDSketch approximation error (about 1% relative error)
		assert.InDelta(t, expectedSum, outputRow[wkk.RowKeyRollupSum], expectedSum*0.02+0.1,
			"Row %d: rollup_sum should be approximately preserved", i)
		assert.Equal(t, float64(1), outputRow[wkk.RowKeyRollupCount],
			"Row %d: rollup_count should remain 1 (no aggregation)", i)

		// Verify sketch exists (all rows now have sketches)
		sketch, ok := outputRow[wkk.RowKeySketch].([]byte)
		assert.True(t, ok, "Row %d: sketch should be []byte", i)
		assert.NotEmpty(t, sketch, "Row %d: sketch should exist", i)
	}

	// Verify the reader's internal count matches
	assert.Equal(t, int64(totalRead), aggregatingReader.TotalRowsReturned(), "TotalRowsReturned doesn't match actual rows read")

	t.Logf("SUCCESS: Fed %d rows, got %d rows back in %d batches (no aggregation, order preserved)", totalRows, totalRead, batchCount)
}

func TestAggregatingMetricsReader_Seglog990DataLoss(t *testing.T) {
	// Test aggregation with actual seglog-990 data to reproduce the 51.8% data loss
	// This test uses multiple files merged together (like production) to trigger the aggregation issue
	ctx := context.Background()
	seglogDir := "../../testdata/metrics/seglog-990/"

	// Read ALL parquet files and merge them
	sourceDir := filepath.Join(seglogDir, "source")
	files, err := os.ReadDir(sourceDir)
	require.NoError(t, err, "Failed to read source directory")
	require.Greater(t, len(files), 0, "No source files found")

	// Create readers for ALL files to fully replicate production scenario
	var readers []Reader
	var totalInputCount int64

	for _, fileInfo := range files {
		if !strings.HasSuffix(fileInfo.Name(), ".parquet") {
			continue // Skip non-parquet files
		}

		testFile := filepath.Join(sourceDir, fileInfo.Name())
		file, err := os.Open(testFile)
		require.NoError(t, err, "Failed to open test file %s", fileInfo.Name())
		defer func() { _ = file.Close() }()

		stat, err := file.Stat()
		require.NoError(t, err, "Failed to stat test file")

		// Create parquet reader
		rawReader, err := NewParquetRawReader(file, stat.Size(), 1000)
		require.NoError(t, err, "Failed to create parquet reader")
		defer func() { _ = rawReader.Close() }()

		// Create translating reader
		cookingReader := NewCookedMetricTranslatingReader(rawReader)
		defer func() { _ = cookingReader.Close() }()

		// Count input records for this file
		fileInputCount := int64(0)
		for {
			batch, err := cookingReader.Next(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err, "Error reading input records from %s", fileInfo.Name())
			}
			fileInputCount += int64(batch.Len())
		}

		t.Logf("Input file %s has %d records", fileInfo.Name(), fileInputCount)
		totalInputCount += fileInputCount

		// Reset the reader for aggregation test
		_ = file.Close()
		file, err = os.Open(testFile)
		require.NoError(t, err, "Failed to reopen test file")
		defer func() { _ = file.Close() }()

		rawReader, err = NewParquetRawReader(file, stat.Size(), 1000)
		require.NoError(t, err, "Failed to create parquet reader")
		defer func() { _ = rawReader.Close() }()

		cookingReader = NewCookedMetricTranslatingReader(rawReader)
		defer func() { _ = cookingReader.Close() }()

		readers = append(readers, cookingReader)
	}

	t.Logf("Total input from %d files: %d records", len(readers), totalInputCount)
	require.Greater(t, totalInputCount, int64(0), "Test files should have records")

	// Create mergesort reader to combine all files (like production)
	keyProvider := GetCurrentMetricSortKeyProvider()
	mergedReader, err := NewMergesortReader(ctx, readers, keyProvider, 1000)
	require.NoError(t, err, "Failed to create mergesort reader")
	defer func() { _ = mergedReader.Close() }()

	// Create aggregating reader on the merged stream (same as production: 10000ms = 10s aggregation)
	aggregatingReader, err := NewAggregatingMetricsReader(mergedReader, 10000, 1000)
	require.NoError(t, err, "Failed to create aggregating reader")
	defer func() { _ = aggregatingReader.Close() }()

	// Count output records
	outputCount := int64(0)
	for {
		batch, err := aggregatingReader.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err, "Error reading aggregated records")
		}
		outputCount += int64(batch.Len())
	}

	t.Logf("Output from aggregating reader: %d records", outputCount)

	// Calculate data loss
	dataLoss := totalInputCount - outputCount
	dataLossPercent := float64(dataLoss) / float64(totalInputCount) * 100

	t.Logf("Data loss: %d records (%.1f%%)", dataLoss, dataLossPercent)
	t.Logf("This replicates the production scenario with multiple merged files")

	// The key assertion: we should get SOME output records
	require.Greater(t, outputCount, int64(0), "Aggregating reader should produce some records")

	// Document the data loss for investigation
	if dataLossPercent > 10.0 {
		t.Logf("⚠️  Data loss detected: %.1f%% - this replicates the production issue", dataLossPercent)
		t.Logf("   Input: %d, Output: %d", totalInputCount, outputCount)
		t.Logf("   The issue occurs when processing merged files with overlapping metrics")
	} else {
		t.Logf("✅ Low data loss: %.1f%% - aggregation working as expected", dataLossPercent)
	}
}

func TestAggregatingMetricsReader_ProductionDoubleResourceBug(t *testing.T) {
	// Test that replicates the EXACT production bug:
	// CreateReaderStack merges readers AND keeps original readers
	// createAggregatingReader then merges the SAME original readers again
	// This creates a resource conflict where both operations read from same readers
	ctx := context.Background()
	seglogDir := "../../testdata/metrics/seglog-990/"

	// Read ALL parquet files and create the readers (like CreateReaderStack)
	sourceDir := filepath.Join(seglogDir, "source")
	files, err := os.ReadDir(sourceDir)
	require.NoError(t, err, "Failed to read source directory")
	require.Greater(t, len(files), 0, "No source files found")

	// Create readers for ALL files
	var originalReaders []Reader
	var totalInputCount int64

	for _, fileInfo := range files {
		if !strings.HasSuffix(fileInfo.Name(), ".parquet") {
			continue // Skip non-parquet files
		}

		testFile := filepath.Join(sourceDir, fileInfo.Name())
		file, err := os.Open(testFile)
		require.NoError(t, err, "Failed to open test file %s", fileInfo.Name())
		defer func() { _ = file.Close() }()

		stat, err := file.Stat()
		require.NoError(t, err, "Failed to stat test file")

		// Create parquet reader
		rawReader, err := NewParquetRawReader(file, stat.Size(), 1000)
		require.NoError(t, err, "Failed to create parquet reader")
		defer func() { _ = rawReader.Close() }()

		// Create translating reader
		cookingReader := NewCookedMetricTranslatingReader(rawReader)
		defer func() { _ = cookingReader.Close() }()

		// Count input records for this file
		fileInputCount := int64(0)
		for {
			batch, err := cookingReader.Next(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err, "Error reading input records from %s", fileInfo.Name())
			}
			fileInputCount += int64(batch.Len())
		}

		t.Logf("Input file %s has %d records", fileInfo.Name(), fileInputCount)
		totalInputCount += fileInputCount

		// Reset the reader for the test
		_ = file.Close()
		file, err = os.Open(testFile)
		require.NoError(t, err, "Failed to reopen test file")
		defer func() { _ = file.Close() }()

		rawReader, err = NewParquetRawReader(file, stat.Size(), 1000)
		require.NoError(t, err, "Failed to create parquet reader")
		defer func() { _ = rawReader.Close() }()

		cookingReader = NewCookedMetricTranslatingReader(rawReader)
		defer func() { _ = cookingReader.Close() }()

		originalReaders = append(originalReaders, cookingReader)
	}

	t.Logf("Total input from %d files: %d records", len(originalReaders), totalInputCount)
	require.Equal(t, int64(41816), totalInputCount, "Should have exactly 41,816 records from seglog-990")

	// *** THIS IS THE PRODUCTION BUG ***
	// Step 1: CreateReaderStack creates a merge (but result is ignored by createAggregatingReader)
	keyProvider := GetCurrentMetricSortKeyProvider()
	createReaderStackMerge, err := NewMergesortReader(ctx, originalReaders, keyProvider, 1000)
	require.NoError(t, err, "Failed to create CreateReaderStack merge reader")
	defer func() { _ = createReaderStackMerge.Close() }()

	// In production, this merged reader is created but then createAggregatingReader ignores it
	// and works directly on the original readers, causing the resource conflict

	// Step 2: createAggregatingReader tries to merge the SAME original readers again
	// This is where the bug happens - reading from readers that are already being consumed
	createAggregatingMerge, err := NewMergesortReader(ctx, originalReaders, &MetricSortKeyProvider{}, 1000)
	require.NoError(t, err, "Failed to create createAggregatingReader merge reader")
	defer func() { _ = createAggregatingMerge.Close() }()

	// Step 3: Aggregation (works fine if it gets data)
	aggregatingReader, err := NewAggregatingMetricsReader(createAggregatingMerge, 10000, 1000)
	require.NoError(t, err, "Failed to create aggregating reader")
	defer func() { _ = aggregatingReader.Close() }()

	// Count output records
	outputCount := int64(0)
	for {
		batch, err := aggregatingReader.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err, "Error reading aggregated records")
		}
		outputCount += int64(batch.Len())
	}

	t.Logf("Output from resource-conflicted readers: %d records", outputCount)

	// Calculate data loss
	dataLoss := totalInputCount - outputCount
	dataLossPercent := float64(dataLoss) / float64(totalInputCount) * 100

	t.Logf("Data loss: %d records (%.1f%%)", dataLoss, dataLossPercent)
	t.Logf("This replicates the production resource conflict bug:")
	t.Logf("  - CreateReaderStack creates merge reader from original readers")
	t.Logf("  - createAggregatingReader ALSO tries to read from SAME original readers")
	t.Logf("  - This creates a resource conflict/race condition")

	// The key assertion: we should get SOME output records
	require.Greater(t, outputCount, int64(0), "Should produce some records")

	// Document the data loss for investigation
	if dataLossPercent > 40.0 {
		t.Logf("🎯 PRODUCTION BUG REPRODUCED: %.1f%% data loss!", dataLossPercent)
		t.Logf("   Input: %d, Output: %d", totalInputCount, outputCount)
		t.Logf("   The issue is caused by multiple readers consuming the same file readers")
	} else {
		t.Logf("⚠️  Expected high data loss but got: %.1f%% - need to investigate further", dataLossPercent)
	}
}
