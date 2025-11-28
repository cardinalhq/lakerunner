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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// createMetricParquetInMemory creates a metric Parquet file in memory for testing
func createMetricParquetInMemory(t *testing.T, rowCount int, nanRowIndices []int) []byte {
	t.Helper()

	rows := make([]map[string]any, rowCount)
	for i := range rows {
		hasNaN := false
		for _, nanIdx := range nanRowIndices {
			if i == nanIdx {
				hasNaN = true
				break
			}
		}

		row := map[string]any{
			"chq_timestamp":    int64(1000000 + i),
			"chq_collector_id": fmt.Sprintf("collector-%d", i),
			"metric_name":      "test_metric",
			"chq_rollup_sum":   float64(i * 100),
		}

		// Add NaN value in rollup field if this is a NaN row
		if hasNaN {
			row["chq_rollup_sum"] = math.NaN()
		}

		rows[i] = row
	}

	// Build schema
	nodes := make(map[string]parquet.Node)
	for key, value := range rows[0] {
		var node parquet.Node
		switch value.(type) {
		case int64:
			node = parquet.Optional(parquet.Int(64))
		case string:
			node = parquet.Optional(parquet.String())
		case float64:
			node = parquet.Optional(parquet.Leaf(parquet.DoubleType))
		default:
			t.Fatalf("Unsupported type %T for key %s", value, key)
		}
		nodes[key] = node
	}

	schema := parquet.NewSchema("metrics", parquet.Group(nodes))

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[map[string]any](&buf, schema)

	for _, row := range rows {
		_, err := writer.Write([]map[string]any{row})
		require.NoError(t, err, "Failed to write row")
	}

	err := writer.Close()
	require.NoError(t, err, "Failed to close writer")

	return buf.Bytes()
}

func TestCookedMetricTranslatingReader_NaNFiltering(t *testing.T) {
	// Test cases with NaN values which should be filtered
	testCases := []struct {
		name          string
		rawCount      int64 // Count from raw reader
		filteredCount int64 // Count after CookedMetricTranslatingReader
		nanIndices    []int // Indices of rows with NaN values
	}{
		{"case1", 227, 226, []int{100}}, // 1 NaN row filtered
		{"case2", 227, 226, []int{150}}, // 1 NaN row filtered
		{"case3", 227, 226, []int{200}}, // 1 NaN row filtered
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Generate Parquet data with NaN values
			data := createMetricParquetInMemory(t, int(tc.rawCount), tc.nanIndices)

			reader := bytes.NewReader(data)
			rawReader, err := NewParquetRawReader(reader, int64(len(data)), 1000)
			require.NoError(t, err, "Failed to create raw reader")
			defer func() { _ = rawReader.Close() }()

			// Wrap with CookedMetricTranslatingReader
			translatingReader := NewCookedMetricTranslatingReader(rawReader)
			defer func() { _ = translatingReader.Close() }()

			var totalRows int64
			for {
				batch, err := translatingReader.Next(context.TODO())
				if batch != nil {
					totalRows += int64(batch.Len())
				}
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err, "Reader error in %s", tc.name)
			}

			assert.Equal(t, tc.filteredCount, totalRows,
				"Case %s should have %d records after NaN filtering (raw has %d)",
				tc.name, tc.filteredCount, tc.rawCount)
		})
	}
}

func TestCookedMetricTranslatingReader_TIDConversion(t *testing.T) {
	// Create a mock reader that returns rows with different TID types
	mockReader := &testMockReader{
		batches: []*Batch{
			createBatchWithRows([]map[wkk.RowKey]any{
				{
					wkk.RowKeyCTID:       "12345", // String TID to be converted
					wkk.RowKeyCTimestamp: int64(1000000),
				},
				{
					wkk.RowKeyCTID:       int64(67890), // Already int64
					wkk.RowKeyCTimestamp: int64(2000000),
				},
				{
					wkk.RowKeyCTID:       "invalid", // Invalid string - should be removed
					wkk.RowKeyCTimestamp: int64(3000000),
				},
			}),
		},
	}

	reader := NewCookedMetricTranslatingReader(mockReader)
	defer func() { _ = reader.Close() }()

	batch, err := reader.Next(context.TODO())
	require.NoError(t, err)
	require.NotNil(t, batch)
	assert.Equal(t, 3, batch.Len())

	// Check first row - string converted to int64
	row1 := batch.Get(0)
	tid1, exists := row1[wkk.RowKeyCTID]
	assert.True(t, exists)
	assert.Equal(t, int64(12345), tid1)

	// Check second row - already int64
	row2 := batch.Get(1)
	tid2, exists := row2[wkk.RowKeyCTID]
	assert.True(t, exists)
	assert.Equal(t, int64(67890), tid2)

	// Check third row - invalid TID removed
	row3 := batch.Get(2)
	_, exists = row3[wkk.RowKeyCTID]
	assert.False(t, exists, "Invalid TID should be removed")
}

func TestCookedMetricTranslatingReader_SketchConversion(t *testing.T) {
	// Create a mock reader that returns rows with sketch fields
	mockReader := &testMockReader{
		batches: []*Batch{
			createBatchWithRows([]map[wkk.RowKey]any{
				{
					wkk.RowKeySketch:     "sketch_as_string", // String to be converted to []byte
					wkk.RowKeyCTimestamp: int64(1000000),
				},
				{
					wkk.RowKeySketch:     []byte("already_bytes"), // Already []byte
					wkk.RowKeyCTimestamp: int64(2000000),
				},
			}),
		},
	}

	reader := NewCookedMetricTranslatingReader(mockReader)
	defer func() { _ = reader.Close() }()

	batch, err := reader.Next(context.TODO())
	require.NoError(t, err)
	require.NotNil(t, batch)
	assert.Equal(t, 2, batch.Len())

	// Check first row - string converted to []byte
	row1 := batch.Get(0)
	sketch1, exists := row1[wkk.RowKeySketch]
	assert.True(t, exists)
	assert.Equal(t, []byte("sketch_as_string"), sketch1)

	// Check second row - already []byte
	row2 := batch.Get(1)
	sketch2, exists := row2[wkk.RowKeySketch]
	assert.True(t, exists)
	assert.Equal(t, []byte("already_bytes"), sketch2)
}

// Helper to create a batch with specific rows
func createBatchWithRows(rows []map[wkk.RowKey]any) *Batch {
	batch := pipeline.GetBatch()
	for _, row := range rows {
		batchRow := batch.AddRow()
		for k, v := range row {
			batchRow[k] = v
		}
	}
	return batch
}

// testMockReader is a simple reader for testing
type testMockReader struct {
	batches []*Batch
	index   int
	closed  bool
}

func (m *testMockReader) Next(ctx context.Context) (*Batch, error) {
	if m.closed || m.index >= len(m.batches) {
		return nil, io.EOF
	}
	batch := m.batches[m.index]
	m.index++
	return batch, nil
}

func (m *testMockReader) Close() error {
	m.closed = true
	return nil
}

func (m *testMockReader) TotalRowsReturned() int64 {
	total := int64(0)
	for i := 0; i < m.index && i < len(m.batches); i++ {
		total += int64(m.batches[i].Len())
	}
	return total
}

func (m *testMockReader) GetSchema() *ReaderSchema {
	return nil
}

// TestCookedMetricTranslatingReader_ShouldDropRow tests if shouldDropRow is correctly identifying rows to drop
func TestCookedMetricTranslatingReader_ShouldDropRow(t *testing.T) {
	testCases := []struct {
		name            string
		expectedRecords int
	}{
		{"small_file", 480},
		{"medium_file", 456},
		{"large_file", 1414},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Generate metric Parquet data
			data := createMetricParquetInMemory(t, tc.expectedRecords, nil)

			reader := bytes.NewReader(data)
			rawReader, err := NewParquetRawReader(reader, int64(len(data)), 1000)
			require.NoError(t, err, "Should create NewParquetRawReader")
			defer func() { _ = rawReader.Close() }()

			// First, read one batch from the raw reader to get sample rows for shouldDropRow testing
			rawBatch, err := rawReader.Next(ctx)
			require.NoError(t, err, "Should read from raw reader")
			require.NotNil(t, rawBatch, "Raw batch should not be nil")
			require.Greater(t, rawBatch.Len(), 0, "Raw batch should have rows")

			t.Logf("Case %s: Raw reader returned %d records for shouldDropRow testing", tc.name, rawBatch.Len())

			// Create a temporary cooked reader just for testing shouldDropRow
			tempCookedReader := NewCookedMetricTranslatingReader(rawReader)
			defer func() { _ = tempCookedReader.Close() }()

			// Test shouldDropRow on the first few rows
			droppedCount := 0
			totalTested := 0

			// Test up to 10 rows or all rows if fewer
			maxToTest := min(10, rawBatch.Len())

			for i := 0; i < maxToTest; i++ {
				row := rawBatch.Get(i)
				totalTested++

				// Test shouldDropRow with the actual row data
				if tempCookedReader.shouldDropRow(ctx, row) {
					droppedCount++
					t.Logf("Case %s: pipeline.Row %d would be DROPPED by shouldDropRow", tc.name, i)

					// Log the row content for debugging
					t.Logf("Case %s: Dropped row data: %+v", tc.name, row)
				} else {
					t.Logf("Case %s: pipeline.Row %d would be KEPT by shouldDropRow", tc.name, i)
				}
			}

			dropRate := float64(droppedCount) / float64(totalTested) * 100
			t.Logf("Case %s: Drop rate in first %d rows: %.1f%% (%d/%d)",
				tc.name, totalTested, dropRate, droppedCount, totalTested)
		})
	}
}

// TestCookedMetricTranslatingReader_NextWithSmallFiles focuses specifically on the Next() method behavior
// of CookedMetricTranslatingReader with small files to verify it works correctly in isolation
func TestCookedMetricTranslatingReader_NextWithSmallFiles(t *testing.T) {
	testCases := []struct {
		name            string
		expectedRecords int
	}{
		{"small_file", 480},
		{"medium_file", 456},
		{"large_file", 1414},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Generate metric Parquet data
			data := createMetricParquetInMemory(t, tc.expectedRecords, nil)

			reader := bytes.NewReader(data)
			rawReader, err := NewParquetRawReader(reader, int64(len(data)), 1000)
			require.NoError(t, err, "Should create NewParquetRawReader")
			defer func() { _ = rawReader.Close() }()

			cookedReader := NewCookedMetricTranslatingReader(rawReader)
			defer func() { _ = cookedReader.Close() }()

			// Read from the cooked reader and see what exactly happens
			cookedRecordCount := 0
			batchNum := 0
			for {
				cookedBatch, err := cookedReader.Next(ctx)
				batchNum++

				if err != nil {
					t.Logf("Case %s: Batch %d - Got error: %v", tc.name, batchNum, err)
					break // EOF or error
				}
				if cookedBatch == nil {
					t.Logf("Case %s: Batch %d - Got nil batch", tc.name, batchNum)
					break
				}

				batchSize := cookedBatch.Len()
				cookedRecordCount += batchSize
				t.Logf("Case %s: Batch %d - Got %d records (total: %d)", tc.name, batchNum, batchSize, cookedRecordCount)

				if batchSize == 0 {
					t.Logf("Case %s: Batch %d - Empty batch, stopping", tc.name, batchNum)
					break
				}
			}

			t.Logf("Case %s: Cooked reader returned %d records (expected %d)",
				tc.name, cookedRecordCount, tc.expectedRecords)

			// These tests verify that CookedMetricTranslatingReader works correctly in isolation
			require.Equal(t, tc.expectedRecords, cookedRecordCount,
				"CookedMetricTranslatingReader should read all records correctly when used in isolation")
		})
	}
}

// TestCookedMetricTranslatingReader_GetSchema tests that schema is augmented with chq_tsns.
func TestCookedMetricTranslatingReader_GetSchema(t *testing.T) {
	// Create test parquet data
	rows := []map[string]any{
		{
			"chq_timestamp":  int64(1000),
			"chq_tid":        int64(123),
			"chq_name":       "test_metric",
			"chq_rollup_sum": float64(100.5),
		},
	}

	parquetData, _ := createTestParquetInMemory(t, rows)
	reader := bytes.NewReader(parquetData)

	// ParquetRawReader implements SchemafiedReader
	rawReader, err := NewParquetRawReader(reader, int64(len(parquetData)), 1000)
	require.NoError(t, err)
	defer func() { _ = rawReader.Close() }()

	// CookedMetricTranslatingReader wraps it and augments schema
	cookedReader := NewCookedMetricTranslatingReader(rawReader)
	defer func() { _ = cookedReader.Close() }()

	// Schema should be valid and include chq_tsns
	schema := cookedReader.GetSchema()
	assert.NotNil(t, schema, "Schema must not be nil")
	assert.True(t, schema.HasColumn(wkk.RowKeyValue(wkk.RowKeyCTsns)), "Schema should include chq_tsns column")
}

// TestCookedMetricTranslatingReader_GetSchema_NoSchema tests behavior with non-schema reader.
func TestCookedMetricTranslatingReader_GetSchema_NoSchema(t *testing.T) {
	// Create a mock reader without schema support
	mockReader := newMockReader("test", []pipeline.Row{
		{wkk.NewRowKey("test"): "value"},
	}, nil)

	cookedReader := NewCookedMetricTranslatingReader(mockReader)
	defer func() { _ = cookedReader.Close() }()

	// Should return valid schema with chq_tsns even when wrapped reader doesn't provide schema
	schema := cookedReader.GetSchema()
	assert.NotNil(t, schema, "Schema must not be nil even when wrapped reader has no schema")
	assert.True(t, schema.HasColumn(wkk.RowKeyValue(wkk.RowKeyCTsns)), "Schema should include chq_tsns column")
}
