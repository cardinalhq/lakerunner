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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func TestCookedMetricTranslatingReader_NaNFiltering(t *testing.T) {
	// Test files that contain NaN values which should be filtered
	testCases := []struct {
		filename      string
		rawCount      int64 // Count from raw reader
		filteredCount int64 // Count after CookedMetricTranslatingReader
	}{
		{"tbl_299476441865651503.parquet", 227, 226}, // 1 NaN row filtered
		{"tbl_299476464716219172.parquet", 227, 226}, // 1 NaN row filtered
		{"tbl_299476496878142244.parquet", 227, 226}, // 1 NaN row filtered
	}

	for _, tc := range testCases {
		t.Run(tc.filename, func(t *testing.T) {
			fullPath := fmt.Sprintf("../../testdata/metrics/compact-test-0001/%s", tc.filename)

			file, err := os.Open(fullPath)
			require.NoError(t, err, "Failed to open file: %s", fullPath)
			defer func() { _ = file.Close() }()

			stat, err := file.Stat()
			require.NoError(t, err)

			// Create raw reader
			rawReader, err := NewParquetRawReader(file, stat.Size(), 1000)
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
				require.NoError(t, err, "Reader error in file %s", tc.filename)
			}

			assert.Equal(t, tc.filteredCount, totalRows,
				"File %s should have %d records after NaN filtering (raw has %d)",
				tc.filename, tc.filteredCount, tc.rawCount)
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

// TestCookedMetricTranslatingReader_ShouldDropRow tests if shouldDropRow is correctly identifying rows to drop
// from actual parquet files that are showing data loss in production
func TestCookedMetricTranslatingReader_ShouldDropRow(t *testing.T) {
	testFiles := []struct {
		filename        string
		expectedRecords int
	}{
		{"tbl_301228791710090615.parquet", 480},
		{"tbl_301228792783832948.parquet", 456},
		{"tbl_301228792733501300.parquet", 1414}, // A larger file for comparison
	}

	ctx := context.Background()

	for _, testFile := range testFiles {
		t.Run(testFile.filename, func(t *testing.T) {
			filePath := filepath.Join("..", "..", "testdata", "metrics", "seglog-990", "source", testFile.filename)

			file, err := os.Open(filePath)
			require.NoError(t, err, "Should open parquet file")
			defer func() { _ = file.Close() }()

			stat, err := file.Stat()
			require.NoError(t, err, "Should stat parquet file")

			// Create raw reader for testing shouldDropRow
			rawReader, err := NewParquetRawReader(file, stat.Size(), 1000)
			require.NoError(t, err, "Should create NewParquetRawReader")
			defer func() { _ = rawReader.Close() }()

			// First, read one batch from the raw reader to get sample rows for shouldDropRow testing
			rawBatch, err := rawReader.Next(ctx)
			require.NoError(t, err, "Should read from raw reader")
			require.NotNil(t, rawBatch, "Raw batch should not be nil")
			require.Greater(t, rawBatch.Len(), 0, "Raw batch should have rows")

			t.Logf("File %s: Raw reader returned %d records for shouldDropRow testing", testFile.filename, rawBatch.Len())

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
					t.Logf("File %s: Row %d would be DROPPED by shouldDropRow", testFile.filename, i)

					// Log the row content for debugging
					t.Logf("File %s: Dropped row data: %+v", testFile.filename, row)
				} else {
					t.Logf("File %s: Row %d would be KEPT by shouldDropRow", testFile.filename, i)
				}
			}

			dropRate := float64(droppedCount) / float64(totalTested) * 100
			t.Logf("File %s: Drop rate in first %d rows: %.1f%% (%d/%d)",
				testFile.filename, totalTested, dropRate, droppedCount, totalTested)
		})
	}
}

// TestCookedMetricTranslatingReader_NextWithSmallFiles focuses specifically on the Next() method behavior
// of CookedMetricTranslatingReader with small files to verify it works correctly in isolation
func TestCookedMetricTranslatingReader_NextWithSmallFiles(t *testing.T) {
	testFiles := []struct {
		filename        string
		expectedRecords int
	}{
		{"tbl_301228791710090615.parquet", 480},
		{"tbl_301228792783832948.parquet", 456},
		{"tbl_301228792733501300.parquet", 1414}, // A larger file for comparison
	}

	ctx := context.Background()

	for _, testFile := range testFiles {
		t.Run(testFile.filename, func(t *testing.T) {
			filePath := filepath.Join("..", "..", "testdata", "metrics", "seglog-990", "source", testFile.filename)

			file, err := os.Open(filePath)
			require.NoError(t, err, "Should open parquet file")
			defer func() { _ = file.Close() }()

			stat, err := file.Stat()
			require.NoError(t, err, "Should stat parquet file")

			rawReader, err := NewParquetRawReader(file, stat.Size(), 1000)
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
					t.Logf("File %s: Batch %d - Got error: %v", testFile.filename, batchNum, err)
					break // EOF or error
				}
				if cookedBatch == nil {
					t.Logf("File %s: Batch %d - Got nil batch", testFile.filename, batchNum)
					break
				}

				batchSize := cookedBatch.Len()
				cookedRecordCount += batchSize
				t.Logf("File %s: Batch %d - Got %d records (total: %d)", testFile.filename, batchNum, batchSize, cookedRecordCount)

				if batchSize == 0 {
					t.Logf("File %s: Batch %d - Empty batch, stopping", testFile.filename, batchNum)
					break
				}
			}

			t.Logf("File %s: Cooked reader returned %d records (expected %d)",
				testFile.filename, cookedRecordCount, testFile.expectedRecords)

			// These tests verify that CookedMetricTranslatingReader works correctly in isolation
			require.Equal(t, testFile.expectedRecords, cookedRecordCount,
				"CookedMetricTranslatingReader should read all records correctly when used in isolation")
		})
	}
}
