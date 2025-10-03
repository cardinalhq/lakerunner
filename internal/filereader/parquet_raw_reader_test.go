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

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// createTestParquetInMemory generates a parquet file in memory with the specified rows.
// Returns the parquet data as bytes and the number of rows written.
func createTestParquetInMemory(t *testing.T, rows []map[string]any) ([]byte, int) {
	t.Helper()

	if len(rows) == 0 {
		t.Fatal("Cannot create parquet file with zero rows")
	}

	// Build schema from first row
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
		case bool:
			node = parquet.Optional(parquet.Leaf(parquet.BooleanType))
		default:
			t.Fatalf("Unsupported type %T for key %s", value, key)
		}
		nodes[key] = node
	}

	schema := parquet.NewSchema("test", parquet.Group(nodes))

	// Create parquet writer to buffer
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[map[string]any](&buf, schema)

	// Write all rows
	for _, row := range rows {
		_, err := writer.Write([]map[string]any{row})
		require.NoError(t, err, "Failed to write row")
	}

	// Close writer to finalize
	err := writer.Close()
	require.NoError(t, err, "Failed to close writer")

	return buf.Bytes(), len(rows)
}

// TestParquetRawReaderNext tests the actual Next() method behavior
func TestParquetRawReaderNext(t *testing.T) {
	// Generate test data with 32 rows
	testRows := make([]map[string]any, 32)
	for i := range testRows {
		testRows[i] = map[string]any{
			"chq_collector_id": fmt.Sprintf("collector-%d", i),
			"chq_timestamp":    int64(1000000 + i),
			"log_message":      fmt.Sprintf("Test message %d", i),
			"chq_severity":     "INFO",
		}
	}

	// Generate parquet file in memory
	parquetData, _ := createTestParquetInMemory(t, testRows)

	reader, err := NewParquetRawReader(bytes.NewReader(parquetData), int64(len(parquetData)), 10) // Small batch size
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	var totalRows int64
	batchCount := 0

	// Test actual Next() behavior
	for {
		batch, err := reader.Next(context.TODO())
		if batch != nil {
			batchCount++
			totalRows += int64(batch.Len())

			// Verify batch structure
			assert.LessOrEqual(t, batch.Len(), 10, "Batch should respect batch size limit")
			for i := 0; i < batch.Len(); i++ {
				row := batch.Get(i)
				assert.NotNil(t, row, "Row %d in batch %d should not be nil", i, batchCount)
				assert.Greater(t, len(row), 0, "Row %d in batch %d should have fields", i, batchCount)
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err, "Next() should not return other errors")
	}

	// Verify we read the expected number of rows in multiple batches
	assert.Equal(t, int64(32), totalRows, "Should read all 32 rows from the file")
	assert.GreaterOrEqual(t, batchCount, 4, "Should read in at least 4 batches with batch size 10")
	assert.Equal(t, totalRows, reader.TotalRowsReturned(), "TotalRowsReturned should match actual rows read")
}

// TestParquetRawReaderBatching tests batching behavior with different batch sizes
func TestParquetRawReaderBatching(t *testing.T) {
	// Generate test data with 32 rows
	testRows := make([]map[string]any, 32)
	for i := range testRows {
		testRows[i] = map[string]any{
			"chq_collector_id": fmt.Sprintf("collector-%d", i),
			"chq_timestamp":    int64(1000000 + i),
			"log_message":      fmt.Sprintf("Test message %d", i),
			"chq_severity":     "INFO",
		}
	}

	// Generate parquet file in memory
	parquetData, _ := createTestParquetInMemory(t, testRows)

	// Test different batch sizes
	testCases := []struct {
		batchSize          int
		expectedMinBatches int
		expectedMaxBatches int
	}{
		{batchSize: 5, expectedMinBatches: 7, expectedMaxBatches: 7},  // 32/5 = 6.4 -> 7 batches
		{batchSize: 10, expectedMinBatches: 4, expectedMaxBatches: 4}, // 32/10 = 3.2 -> 4 batches
		{batchSize: 50, expectedMinBatches: 1, expectedMaxBatches: 1}, // 32/50 = 0.64 -> 1 batch
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("BatchSize%d", tc.batchSize), func(t *testing.T) {
			reader, err := NewParquetRawReader(bytes.NewReader(parquetData), int64(len(parquetData)), tc.batchSize)
			require.NoError(t, err)
			defer func() { _ = reader.Close() }()

			var totalRows int64
			batchCount := 0

			for {
				batch, err := reader.Next(context.TODO())
				if batch != nil {
					batchCount++
					totalRows += int64(batch.Len())
					assert.LessOrEqual(t, batch.Len(), tc.batchSize, "Batch should not exceed batch size")
				}
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
			}

			assert.Equal(t, int64(32), totalRows, "Should read all 32 rows")
			assert.GreaterOrEqual(t, batchCount, tc.expectedMinBatches, "Should have at least %d batches", tc.expectedMinBatches)
			assert.LessOrEqual(t, batchCount, tc.expectedMaxBatches, "Should have at most %d batches", tc.expectedMaxBatches)
		})
	}
}

// TestParquetRawReaderWithRealFile tests ParquetRawReader with generated parquet files
func TestParquetRawReaderWithRealFile(t *testing.T) {
	// Create test data with proper field names
	testRows := make([]map[string]any, 32)
	for i := range testRows {
		testRows[i] = map[string]any{
			"chq_collector_id": fmt.Sprintf("collector-%d", i),
			"chq_timestamp":    int64(1000000 + i),
			"log_message":      fmt.Sprintf("Test message %d", i),
			"chq_severity":     "INFO",
		}
	}

	// Generate parquet file in memory
	parquetData, expectedRows := createTestParquetInMemory(t, testRows)

	// Create reader from in-memory parquet data
	reader, err := NewParquetRawReader(bytes.NewReader(parquetData), int64(len(parquetData)), 1000)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Read all rows and verify we get the expected count
	var rowCount int64

	for {
		batch, err := reader.Next(context.TODO())
		if batch != nil {
			rowCount += int64(batch.Len())

			// Verify each row that was read
			for i := 0; i < batch.Len(); i++ {
				row := batch.Get(i)
				require.NotNil(t, row, "Row %d should not be nil", i)
				require.Greater(t, len(row), 0, "Row %d should have columns", i)
				// Verify the expected collector_id field exists
				_, hasCollectorId := row[wkk.RowKeyCCollectorID]
				assert.True(t, hasCollectorId, "Row should have collector_id field")
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}

	// Verify we read all the expected rows
	assert.Equal(t, int64(expectedRows), rowCount, "Should read exactly %d rows from generated parquet file", expectedRows)
}

// TestParquetRawReaderMultipleFiles tests ParquetRawReader with different file sizes
func TestParquetRawReaderMultipleFiles(t *testing.T) {
	testCases := map[string]int{
		"small":  32,   // 32 rows
		"medium": 211,  // 211 rows
		"large":  1807, // 1807 rows
	}

	for name, expectedRows := range testCases {
		t.Run(name, func(t *testing.T) {
			// Generate test data
			testRows := make([]map[string]any, expectedRows)
			for i := range testRows {
				testRows[i] = map[string]any{
					"chq_collector_id": fmt.Sprintf("collector-%d", i),
					"chq_timestamp":    int64(1000000 + i),
					"log_message":      fmt.Sprintf("Test message %d", i),
					"chq_severity":     "INFO",
				}
			}

			// Generate parquet file in memory
			parquetData, rowCount := createTestParquetInMemory(t, testRows)

			// Create reader from in-memory parquet data
			reader, err := NewParquetRawReader(bytes.NewReader(parquetData), int64(len(parquetData)), 1000)
			require.NoError(t, err)
			defer func() { _ = reader.Close() }()

			// Read all rows and count them
			var totalRows int64

			for {
				batch, err := reader.Next(context.TODO())
				if batch != nil {
					totalRows += int64(batch.Len())

					// Verify each row that was read
					for i := 0; i < batch.Len(); i++ {
						row := batch.Get(i)
						require.NotNil(t, row)
						require.Greater(t, len(row), 0, "Row should have columns")
						// Verify the expected collector_id field exists
						_, hasCollectorId := row[wkk.RowKeyCCollectorID]
						assert.True(t, hasCollectorId, "Row should have collector_id field")
					}
				}
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
			}

			// Verify exact row count
			assert.Equal(t, int64(rowCount), totalRows, "Should read exactly %d rows from %s", rowCount, name)
		})
	}
}

// TestParquetRawReaderClose tests proper cleanup
func TestParquetRawReaderClose(t *testing.T) {
	// Generate test data
	testRows := make([]map[string]any, 32)
	for i := range testRows {
		testRows[i] = map[string]any{
			"chq_collector_id": fmt.Sprintf("collector-%d", i),
			"chq_timestamp":    int64(1000000 + i),
			"log_message":      fmt.Sprintf("Test message %d", i),
			"chq_severity":     "INFO",
		}
	}

	// Generate parquet file in memory
	parquetData, _ := createTestParquetInMemory(t, testRows)

	reader, err := NewParquetRawReader(bytes.NewReader(parquetData), int64(len(parquetData)), 1000)
	require.NoError(t, err)

	// Should be able to read before closing
	batch, err := reader.Next(context.TODO())
	require.NotNil(t, batch)
	require.Greater(t, batch.Len(), 0)
	// EOF is acceptable if we got data (it means we read all the data in one batch)
	require.True(t, err == nil || errors.Is(err, io.EOF), "Expected no error or EOF, got: %v", err)
	require.NotNil(t, batch.Get(0))

	// Close should work
	err = reader.Close()
	assert.NoError(t, err)

	// Reading after close should return error
	_, err = reader.Next(context.TODO())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "closed")
}

// testTranslator is a simple translator for testing
type testTranslator struct {
	addField string
	addValue string
}

func (t *testTranslator) TranslateRow(ctx context.Context, row *pipeline.Row) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}
	(*row)[wkk.NewRowKey(t.addField)] = t.addValue
	return nil
}

func TestParquetRawReader_WithTranslator(t *testing.T) {
	// Test ParquetRawReader with TranslatingReader using generated data
	// Generate test data
	testRows := make([]map[string]any, 32)
	for i := range testRows {
		testRows[i] = map[string]any{
			"chq_collector_id": fmt.Sprintf("collector-%d", i),
			"chq_timestamp":    int64(1000000 + i),
			"log_message":      fmt.Sprintf("Test message %d", i),
			"chq_severity":     "INFO",
		}
	}

	// Generate parquet file in memory
	parquetData, _ := createTestParquetInMemory(t, testRows)

	// Create base parquet reader
	baseReader, err := NewParquetRawReader(bytes.NewReader(parquetData), int64(len(parquetData)), 1000)
	require.NoError(t, err)
	defer func() { _ = baseReader.Close() }()

	// Create simple translator that just adds a test field
	translator := &testTranslator{addField: "test.translator", addValue: "parquet"}

	// Wrap with TranslatingReader
	reader, err := NewTranslatingReader(baseReader, translator, 1000)
	require.NoError(t, err)

	// Read rows
	batch, err := reader.Next(context.TODO())
	n := 0
	if batch != nil {
		n = batch.Len()
	}
	t.Logf("TranslatingReader with ParquetRawReader: n=%d, err=%v", n, err)

	if err != nil && err != io.EOF {
		t.Fatalf("Read failed: %v", err)
	}

	require.Greater(t, n, 0, "Expected to read some rows")

	// Verify translation worked
	for i := 0; i < n; i++ {
		row := batch.Get(i)
		assert.NotNil(t, row)
		assert.Equal(t, "parquet", row[wkk.NewRowKey("test.translator")])
		t.Logf("Row %d: %d fields, translator field present", i, len(row))
	}
}

// TestIngestProtoLogsReader_WithTranslator tests TranslatingReader with IngestProtoLogsReader for comparison
func TestIngestProtoLogsReader_WithTranslator(t *testing.T) {
	filename := createSyntheticLogsFile(t, true)

	// Create base proto reader
	options := ReaderOptions{SignalType: SignalTypeLogs}
	baseReader, err := createProtoBinaryGzReader(filename, options)
	require.NoError(t, err)
	defer func() { _ = baseReader.Close() }()

	// Create simple translator that just adds a test field
	translator := &testTranslator{addField: "test.translator", addValue: "proto"}

	// Wrap with TranslatingReader
	reader, err := NewTranslatingReader(baseReader, translator, 1000)
	require.NoError(t, err)

	// Read rows
	batch, err := reader.Next(context.TODO())
	n := 0
	if batch != nil {
		n = batch.Len()
	}
	t.Logf("TranslatingReader with IngestProtoLogsReader: n=%d, err=%v", n, err)

	if err != nil && err != io.EOF {
		t.Fatalf("Read failed: %v", err)
	}

	require.Greater(t, n, 0, "Expected to read some rows")

	// Verify translation worked
	for i := 0; i < n; i++ {
		row := batch.Get(i)
		assert.NotNil(t, row)
		assert.Equal(t, "proto", row[wkk.NewRowKey("test.translator")])
		t.Logf("Row %d: %d fields, translator field present", i, len(row))
	}
}

func TestParquetRawReader_CompactTestFiles(t *testing.T) {
	// Test ParquetRawReader with different file sizes that would be typical in compaction
	testCases := []struct {
		name         string
		expectedRows int64
	}{
		{"compact_227_rows", 227},
		{"compact_231_rows", 231},
		{"compact_250_rows", 250},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Generate test data with metric fields
			testRows := make([]map[string]any, tc.expectedRows)
			for i := range testRows {
				testRows[i] = map[string]any{
					"chq_collector_id": fmt.Sprintf("collector-%d", i),
					"chq_timestamp":    int64(1000000 + i),
					"chq_metric_name":  "test_metric",
					"chq_rollup_sum":   float64(i * 10),
					"chq_rollup_count": float64(i),
				}
			}

			// Generate parquet file in memory
			parquetData, _ := createTestParquetInMemory(t, testRows)

			reader, err := NewParquetRawReader(bytes.NewReader(parquetData), int64(len(parquetData)), 1000)
			require.NoError(t, err, "Failed to create reader")
			defer func() { _ = reader.Close() }()

			var totalRows int64
			batchCount := 0

			for {
				batch, err := reader.Next(context.TODO())
				if batch != nil {
					batchCount++
					totalRows += int64(batch.Len())

					// Verify batch structure for first batch only to avoid spam
					if batchCount == 1 {
						assert.Greater(t, batch.Len(), 0, "First batch should have rows")
						for i := 0; i < batch.Len() && i < 3; i++ {
							row := batch.Get(i)
							assert.NotNil(t, row, "Row %d should not be nil", i)
							assert.Greater(t, len(row), 0, "Row %d should have fields", i)
						}
					}
				}
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err, "Reader error in test %s", tc.name)
			}

			assert.Equal(t, tc.expectedRows, totalRows, "Test %s should have exactly %d records", tc.name, tc.expectedRows)
			assert.Equal(t, totalRows, reader.TotalRowsReturned(), "TotalRowsReturned should match for test %s", tc.name)

			t.Logf("Test %s: successfully read %d records in %d batches", tc.name, totalRows, batchCount)
		})
	}
}

func TestParquetRawReader_TIDConversion(t *testing.T) {
	// Test TID field type - should be int64
	testRows := []map[string]any{
		{
			"chq_tid":         int64(123456789),
			"chq_timestamp":   int64(1000000),
			"chq_metric_name": "test_metric",
		},
		{
			"chq_tid":         int64(987654321),
			"chq_timestamp":   int64(1000001),
			"chq_metric_name": "test_metric",
		},
	}

	// Generate parquet file in memory
	parquetData, _ := createTestParquetInMemory(t, testRows)

	reader, err := NewParquetRawReader(bytes.NewReader(parquetData), int64(len(parquetData)), 1000)
	require.NoError(t, err, "Failed to create reader")
	defer func() { _ = reader.Close() }()

	// Read first batch
	batch, err := reader.Next(context.TODO())
	if err != nil && !errors.Is(err, io.EOF) {
		require.NoError(t, err)
	}

	if batch == nil || batch.Len() == 0 {
		t.Fatal("No rows returned from ParquetReader")
	}

	// Examine first row TID field type
	row := batch.Get(0)
	require.NotNil(t, row)

	// Check what type the TID field actually is
	tidValue, hasTID := row[wkk.RowKeyCTID]
	require.True(t, hasTID, "Row should have TID field")
	t.Logf("TID value: %v (type: %T)", tidValue, tidValue)

	// Test if it's int64 as expected
	tidInt64, isInt64 := tidValue.(int64)
	require.True(t, isInt64, "TID should be int64, got %T", tidValue)
	assert.Equal(t, int64(123456789), tidInt64, "TID value should match")

	// Also check timestamp
	timestampValue, hasTimestamp := row[wkk.RowKeyCTimestamp]
	require.True(t, hasTimestamp, "Row should have timestamp field")
	t.Logf("Timestamp value: %v (type: %T)", timestampValue, timestampValue)

	tsInt64, isInt64 := timestampValue.(int64)
	require.True(t, isInt64, "Timestamp should be int64, got %T", timestampValue)
	assert.Equal(t, int64(1000000), tsInt64, "Timestamp value should match")
}

func TestDiskSortingReader_WithParquetCompactTestFiles(t *testing.T) {
	// Test DiskSortingReader(CookedMetricTranslatingReader(ParquetReader)) combination
	// CookedMetricTranslatingReader filters out rows with NaN values
	testCases := []struct {
		name          string
		rowCount      int
		nanRows       int // number of rows with NaN values that will be filtered
		expectedCount int64
	}{
		{"no_nan_rows", 227, 0, 227},
		{"with_one_nan", 227, 1, 226},
		{"with_multiple_nan", 231, 2, 229},
		{"all_valid", 227, 0, 227},
	}

	// Use the standard metric sorting provider
	keyProvider := &MetricSortKeyProvider{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create test data with metric fields
			testRows := make([]map[string]any, tc.rowCount)
			for i := range testRows {
				testRows[i] = map[string]any{
					"chq_collector_id": fmt.Sprintf("collector-%d", i),
					"chq_timestamp":    int64(1000000 + i),
					"chq_metric_name":  "test_metric",
					"chq_rollup_sum":   float64(i * 10),
					"chq_rollup_count": float64(i),
					"chq_rollup_avg":   float64(10),
				}

				// Add NaN values to some rows to test filtering
				if i < tc.nanRows {
					testRows[i]["chq_rollup_sum"] = math.NaN()
				}
			}

			// Generate parquet file in memory
			parquetData, _ := createTestParquetInMemory(t, testRows)

			// Create the ParquetReader
			parquetReader, err := NewParquetRawReader(bytes.NewReader(parquetData), int64(len(parquetData)), 1000)
			require.NoError(t, err, "Failed to create ParquetReader")
			defer func() { _ = parquetReader.Close() }()

			// Wrap with CookedMetricTranslatingReader to handle metric-specific transformations
			translatingReader := NewCookedMetricTranslatingReader(parquetReader)
			defer func() { _ = translatingReader.Close() }()

			// Wrap with DiskSortingReader
			diskSortingReader, err := NewDiskSortingReader(translatingReader, keyProvider, 1000)
			require.NoError(t, err, "Failed to create DiskSortingReader")
			defer func() { _ = diskSortingReader.Close() }()

			var totalRows int64
			batchCount := 0

			for {
				batch, err := diskSortingReader.Next(context.TODO())
				if batch != nil {
					batchCount++
					totalRows += int64(batch.Len())

					// Verify batch structure for first batch only
					if batchCount == 1 {
						assert.Greater(t, batch.Len(), 0, "First batch should have rows")
						for i := 0; i < batch.Len() && i < 3; i++ {
							row := batch.Get(i)
							assert.NotNil(t, row, "Row %d should not be nil", i)
							assert.Greater(t, len(row), 0, "Row %d should have fields", i)
						}
					}
				}
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err, "DiskSortingReader error in test %s", tc.name)
			}

			assert.Equal(t, tc.expectedCount, totalRows, "DiskSortingReader should preserve all %d records from %s", tc.expectedCount, tc.name)
			assert.Equal(t, totalRows, diskSortingReader.TotalRowsReturned(), "DiskSortingReader TotalRowsReturned should match for %s", tc.name)

			t.Logf("Test %s: DiskSortingReader successfully read %d records in %d batches (filtered %d NaN rows)", tc.name, totalRows, batchCount, tc.nanRows)
		})
	}
}

// TestParquetRawReaderSmallFiles tests the NewParquetRawReader with different file sizes
func TestParquetRawReaderSmallFiles(t *testing.T) {
	// Test files with different record counts
	testCases := []struct {
		name            string
		expectedRecords int
	}{
		{"small_480", 480},
		{"medium_456", 456},
		{"large_1414", 1414},
	}

	ctx := context.Background()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Generate test data
			testRows := make([]map[string]any, tc.expectedRecords)
			for i := range testRows {
				testRows[i] = map[string]any{
					"chq_collector_id": fmt.Sprintf("collector-%d", i),
					"chq_timestamp":    int64(1000000 + i),
					"chq_metric_name":  "test_metric",
					"chq_rollup_sum":   float64(i * 10),
				}
			}

			// Generate parquet file in memory
			parquetData, _ := createTestParquetInMemory(t, testRows)

			reader, err := NewParquetRawReader(bytes.NewReader(parquetData), int64(len(parquetData)), 1000)
			require.NoError(t, err, "Should create NewParquetRawReader")
			defer func() { _ = reader.Close() }()

			recordCount := 0
			for {
				batch, err := reader.Next(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						t.Logf("Test %s: Got EOF after reading %d records", tc.name, recordCount)
						break
					}
					require.NoError(t, err, "Next should not fail")
				}

				if batch == nil {
					t.Logf("Test %s: Got nil batch after reading %d records", tc.name, recordCount)
					break
				}

				batchSize := batch.Len()
				recordCount += batchSize
				t.Logf("Test %s: Read batch of %d records (total: %d)", tc.name, batchSize, recordCount)

				if batchSize == 0 {
					t.Logf("Test %s: Got empty batch, stopping", tc.name)
					break
				}
			}

			t.Logf("Test %s: Final count %d, expected %d", tc.name, recordCount, tc.expectedRecords)
			require.Equal(t, tc.expectedRecords, recordCount, "Should read correct number of records from %s", tc.name)
		})
	}
}
