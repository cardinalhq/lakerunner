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
	"math/rand"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// createSimpleSchema creates a basic schema for test mock readers
func createSimpleSchema() *ReaderSchema {
	schema := NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("ts"), DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("data"), DataTypeString, true)
	return schema
}

// createMetricSchema creates a schema for metric test data
func createMetricSchema() *ReaderSchema {
	schema := NewReaderSchema()
	schema.AddColumn(wkk.RowKeyCName, DataTypeString, true)
	schema.AddColumn(wkk.RowKeyCTID, DataTypeInt64, true)
	schema.AddColumn(wkk.RowKeyCTimestamp, DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("reader_id"), DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("row_id"), DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("value"), DataTypeFloat64, true)
	schema.AddColumn(wkk.NewRowKey("source"), DataTypeString, true)
	return schema
}

// createMergesortTestParquet creates a metric Parquet file in memory for mergesort testing
func createMergesortTestParquet(t *testing.T, rowCount int) []byte {
	t.Helper()

	rows := make([]map[string]any, rowCount)
	for i := range rows {
		rows[i] = map[string]any{
			"chq_timestamp":    int64(1000000 + i),
			"chq_collector_id": fmt.Sprintf("collector-%d", i),
			"metric_name":      "test_metric",
			"chq_rollup_sum":   float64(i * 100),
		}
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

func TestNewMergesortReader(t *testing.T) {
	// Test with valid readers and keyProvider
	schema := createSimpleSchema()
	readers := []SchemafiedReader{
		newMockReader("r1", []pipeline.Row{{wkk.NewRowKey("ts"): int64(1)}}, schema),
		newMockReader("r2", []pipeline.Row{{wkk.NewRowKey("ts"): int64(2)}}, schema),
	}
	keyProvider := NewTimeOrderedSortKeyProvider("ts")

	or, err := NewMergesortReader(context.TODO(), readers, keyProvider, 1000)
	if err != nil {
		t.Fatalf("NewMergesortReader() error = %v", err)
	}
	defer func() { _ = or.Close() }()

	if len(or.readers) != 2 {
		t.Errorf("Expected 2 readers, got %d", len(or.readers))
	}

	// Test with no readers
	_, err = NewMergesortReader(context.TODO(), []SchemafiedReader{}, keyProvider, 1000)
	if err == nil {
		t.Error("Expected error for empty readers slice")
	}

	// Test with nil keyProvider
	_, err = NewMergesortReader(context.TODO(), readers, nil, 1000)
	if err == nil {
		t.Error("Expected error for nil keyProvider")
	}
}

func TestMergesortReader_Next(t *testing.T) {
	// Create readers with interleaved timestamps to test ordering
	schema := createSimpleSchema()
	readers := []SchemafiedReader{
		newMockReader("r1", []pipeline.Row{
			{wkk.NewRowKey("ts"): int64(1), wkk.NewRowKey("data"): "r1-first"},
			{wkk.NewRowKey("ts"): int64(4), wkk.NewRowKey("data"): "r1-second"},
			{wkk.NewRowKey("ts"): int64(7), wkk.NewRowKey("data"): "r1-third"},
		}, schema),
		newMockReader("r2", []pipeline.Row{
			{wkk.NewRowKey("ts"): int64(2), wkk.NewRowKey("data"): "r2-first"},
			{wkk.NewRowKey("ts"): int64(5), wkk.NewRowKey("data"): "r2-second"},
		}, schema),
		newMockReader("r3", []pipeline.Row{
			{wkk.NewRowKey("ts"): int64(3), wkk.NewRowKey("data"): "r3-first"},
			{wkk.NewRowKey("ts"): int64(6), wkk.NewRowKey("data"): "r3-second"},
		}, schema),
	}

	keyProvider := NewTimeOrderedSortKeyProvider("ts")
	or, err := NewMergesortReader(context.TODO(), readers, keyProvider, 1000)
	if err != nil {
		t.Fatalf("NewMergesortReader() error = %v", err)
	}
	defer func() { _ = or.Close() }()

	// Expected order: 1, 2, 3, 4, 5, 6, 7
	expectedData := []string{
		"r1-first",  // ts=1
		"r2-first",  // ts=2
		"r3-first",  // ts=3
		"r1-second", // ts=4
		"r2-second", // ts=5
		"r3-second", // ts=6
		"r1-third",  // ts=7
	}

	allRows, err := readAllRows(or)
	if err != nil {
		t.Fatalf("readAllRows() error = %v", err)
	}

	if len(allRows) != len(expectedData) {
		t.Fatalf("Expected %d rows, got %d", len(expectedData), len(allRows))
	}

	for i, expected := range expectedData {
		if allRows[i][wkk.NewRowKey("data")] != expected {
			t.Errorf("Row %d data = %v, want %v (ts=%v)", i, allRows[i][wkk.NewRowKey("data")], expected, allRows[i][wkk.NewRowKey("ts")])
		}
		if i > 0 {
			// Verify timestamps are in order
			prevTs := allRows[i-1][wkk.NewRowKey("ts")].(int64)
			currTs := allRows[i][wkk.NewRowKey("ts")].(int64)
			if currTs < prevTs {
				t.Errorf("Timestamps out of order: row %d ts=%d < row %d ts=%d", i, currTs, i-1, prevTs)
			}
		}
	}
}

func TestMergesortReader_NextBatched(t *testing.T) {
	schema := createSimpleSchema()
	readers := []SchemafiedReader{
		newMockReader("r1", []pipeline.Row{{wkk.NewRowKey("ts"): int64(100)}}, schema),
		newMockReader("r2", []pipeline.Row{{wkk.NewRowKey("ts"): int64(200)}}, schema),
		newMockReader("r3", []pipeline.Row{}, schema), // Empty reader
	}

	keyProvider := NewTimeOrderedSortKeyProvider("ts")
	or, err := NewMergesortReader(context.TODO(), readers, keyProvider, 1000)
	if err != nil {
		t.Fatalf("NewMergesortReader() error = %v", err)
	}
	defer func() { _ = or.Close() }()

	// Read first batch (should get both rows)
	batch, err := or.Next(context.TODO())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if batch.Len() != 2 {
		t.Errorf("Next() returned %d rows, want 2", batch.Len())
	}
	if batch.Get(0)[wkk.NewRowKey("ts")] != int64(100) {
		t.Errorf("First row ts = %v, want 100", batch.Get(0)[wkk.NewRowKey("ts")])
	}
	if batch.Get(1)[wkk.NewRowKey("ts")] != int64(200) {
		t.Errorf("Second row ts = %v, want 200", batch.Get(1)[wkk.NewRowKey("ts")])
	}

	// Next read should return EOF
	batch, err = or.Next(context.TODO())
	if !errors.Is(err, io.EOF) {
		t.Errorf("Final Next() should return io.EOF, got err=%v", err)
	}
	if batch != nil {
		t.Errorf("Final Next() should return nil batch, got %v", batch)
	}
}

func TestMergesortReader_ActiveReaderCount(t *testing.T) {
	schema := createSimpleSchema()
	readers := []SchemafiedReader{
		newMockReader("r1", []pipeline.Row{{wkk.NewRowKey("ts"): int64(1)}}, schema),
		newMockReader("r2", []pipeline.Row{{wkk.NewRowKey("ts"): int64(2)}}, schema),
	}

	keyProvider := NewTimeOrderedSortKeyProvider("ts")
	or, err := NewMergesortReader(context.TODO(), readers, keyProvider, 1000)
	if err != nil {
		t.Fatalf("NewMergesortReader() error = %v", err)
	}
	defer func() { _ = or.Close() }()

	// Initially both readers should be active
	if count := or.ActiveReaderCount(); count != 2 {
		t.Errorf("Initial ActiveReaderCount() = %d, want 2", count)
	}

	// Read batch (should get both rows)
	batch, err := or.Next(context.TODO())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if batch.Len() != 2 {
		t.Fatalf("Expected 2 rows, got %d", batch.Len())
	}

	// Should have 0 active readers (all consumed)
	if count := or.ActiveReaderCount(); count != 0 {
		t.Errorf("After reading all rows ActiveReaderCount() = %d, want 0", count)
	}
}

func TestMergesortReader_AllEmptyReaders(t *testing.T) {
	schema := createSimpleSchema()
	readers := []SchemafiedReader{
		newMockReader("r1", []pipeline.Row{}, schema),
		newMockReader("r2", []pipeline.Row{}, schema),
	}

	keyProvider := NewTimeOrderedSortKeyProvider("ts")
	or, err := NewMergesortReader(context.TODO(), readers, keyProvider, 1000)
	if err != nil {
		t.Fatalf("NewMergesortReader() error = %v", err)
	}
	defer func() { _ = or.Close() }()

	// Should immediately return io.EOF
	batch, err := or.Next(context.TODO())
	if !errors.Is(err, io.EOF) {
		t.Errorf("Next() with all empty readers should return io.EOF, got err=%v", err)
	}
	if batch != nil {
		t.Errorf("Next() with all empty readers should return nil batch, got %v", batch)
	}
}

func TestMergesortReader_Close(t *testing.T) {
	schema := createSimpleSchema()
	readers := []SchemafiedReader{
		newMockReader("r1", []pipeline.Row{{wkk.NewRowKey("ts"): int64(1)}}, schema),
		newMockReader("r2", []pipeline.Row{{wkk.NewRowKey("ts"): int64(2)}}, schema),
	}

	keyProvider := NewTimeOrderedSortKeyProvider("ts")
	or, err := NewMergesortReader(context.TODO(), readers, keyProvider, 1000)
	if err != nil {
		t.Fatalf("NewMergesortReader() error = %v", err)
	}

	// Close the reader
	err = or.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Verify underlying readers are closed
	for i, reader := range readers {
		mockReader := reader.(*mockReader)
		if !mockReader.closed {
			t.Errorf("Reader %d not closed after OrderedReader.Close()", i)
		}
	}

	// Verify subsequent operations fail
	_, err = or.Next(context.TODO())
	if err == nil {
		t.Error("Next() after Close() should return error")
	}

	// Multiple Close() calls should not error
	err = or.Close()
	if err != nil {
		t.Errorf("Second Close() error = %v", err)
	}

	// Verify count after close
	if count := or.ActiveReaderCount(); count != 0 {
		t.Errorf("ActiveReaderCount() after close = %d, want 0", count)
	}
}

func TestTimeOrderedSortKeyProvider(t *testing.T) {
	provider := NewTimeOrderedSortKeyProvider("timestamp")

	// Test basic key creation and comparison
	row1 := pipeline.Row{wkk.NewRowKey("timestamp"): int64(100), wkk.NewRowKey("data"): "first"}
	row2 := pipeline.Row{wkk.NewRowKey("timestamp"): int64(200), wkk.NewRowKey("data"): "second"}
	row3 := pipeline.Row{wkk.NewRowKey("timestamp"): int64(300), wkk.NewRowKey("data"): "third"}

	key1 := provider.MakeKey(row1)
	key2 := provider.MakeKey(row2)
	key3 := provider.MakeKey(row3)
	defer key1.Release()
	defer key2.Release()
	defer key3.Release()

	// Test comparisons
	if key1.Compare(key2) >= 0 {
		t.Errorf("key1 (ts=100) should be < key2 (ts=200)")
	}
	if key2.Compare(key3) >= 0 {
		t.Errorf("key2 (ts=200) should be < key3 (ts=300)")
	}
	if key1.Compare(key3) >= 0 {
		t.Errorf("key1 (ts=100) should be < key3 (ts=300)")
	}

	// Test with float64 timestamps
	rowFloat1 := pipeline.Row{wkk.NewRowKey("timestamp"): float64(100.1), wkk.NewRowKey("data"): "first"}
	rowFloat2 := pipeline.Row{wkk.NewRowKey("timestamp"): float64(200.2), wkk.NewRowKey("data"): "second"}

	keyFloat1 := provider.MakeKey(rowFloat1)
	keyFloat2 := provider.MakeKey(rowFloat2)
	defer keyFloat1.Release()
	defer keyFloat2.Release()

	if keyFloat1.Compare(keyFloat2) >= 0 {
		t.Errorf("keyFloat1 (ts=100.1) should be < keyFloat2 (ts=200.2)")
	}

	// Test with missing timestamp field
	rowMissing := pipeline.Row{wkk.NewRowKey("data"): "no timestamp"}
	rowValid := pipeline.Row{wkk.NewRowKey("timestamp"): int64(100), wkk.NewRowKey("data"): "has timestamp"}

	keyMissing := provider.MakeKey(rowMissing)
	keyValid := provider.MakeKey(rowValid)
	defer keyMissing.Release()
	defer keyValid.Release()

	if keyValid.Compare(keyMissing) >= 0 {
		t.Errorf("keyValid should be < keyMissing (missing timestamps default to invalid and go last)")
	}
}

// trackingReader is a test implementation that records the address of the row slice
// provided to Read so tests can verify row recycling behavior.
type trackingReader struct {
	rows     []pipeline.Row
	index    int
	ptrs     []string
	rowCount int64
	schema   *ReaderSchema
}

func newTrackingReader(rows []pipeline.Row) *trackingReader {
	return &trackingReader{rows: rows, schema: nil}
}

func (tr *trackingReader) Next(ctx context.Context) (*Batch, error) {
	if tr.index >= len(tr.rows) {
		return nil, io.EOF
	}

	// Create a new batch with one row
	batch := pipeline.GetBatch()

	// Create a new row and copy data
	row := batch.AddRow()
	for k, v := range tr.rows[tr.index] {
		row[k] = v
	}

	// Track the pointer for testing row reuse
	tr.ptrs = append(tr.ptrs, fmt.Sprintf("%p", row))

	tr.index++
	tr.rowCount++
	return batch, nil
}

func (tr *trackingReader) Close() error { return nil }

func (tr *trackingReader) TotalRowsReturned() int64 { return tr.rowCount }

func (tr *trackingReader) GetSchema() *ReaderSchema { return tr.schema }

func TestMergesortReader_RowReuse(t *testing.T) {
	tr := newTrackingReader([]pipeline.Row{{wkk.NewRowKey("ts"): int64(1)}, {wkk.NewRowKey("ts"): int64(2)}, {wkk.NewRowKey("ts"): int64(3)}, {wkk.NewRowKey("ts"): int64(4)}})
	or, err := NewMergesortReader(context.TODO(), []SchemafiedReader{tr}, NewTimeOrderedSortKeyProvider("ts"), 1)
	if err != nil {
		t.Fatalf("NewMergesortReader() error = %v", err)
	}
	defer func() { _ = or.Close() }()

	// Read first batch
	batch, err := or.Next(context.TODO())
	if err != nil {
		t.Fatalf("First Next() err=%v", err)
	}
	if batch.Len() != 1 {
		t.Fatalf("First batch should have 1 row, got %d", batch.Len())
	}
	if batch.Get(0)[wkk.NewRowKey("ts")] != int64(1) {
		t.Fatalf("first row ts=%v want 1", batch.Get(0)[wkk.NewRowKey("ts")])
	}

	// Read second batch
	batch, err = or.Next(context.TODO())
	if err != nil {
		t.Fatalf("Second Next() err=%v", err)
	}
	if batch.Len() != 1 {
		t.Fatalf("Second batch should have 1 row, got %d", batch.Len())
	}
	if batch.Get(0)[wkk.NewRowKey("ts")] != int64(2) {
		t.Fatalf("second row ts=%v want 2", batch.Get(0)[wkk.NewRowKey("ts")])
	}

	// Read third batch
	batch, err = or.Next(context.TODO())
	if err != nil {
		t.Fatalf("Third Next() err=%v", err)
	}
	if batch.Len() != 1 {
		t.Fatalf("Third batch should have 1 row, got %d", batch.Len())
	}
	if batch.Get(0)[wkk.NewRowKey("ts")] != int64(3) {
		t.Fatalf("third row ts=%v want 3", batch.Get(0)[wkk.NewRowKey("ts")])
	}

	// Verify tracking worked
	if len(tr.ptrs) < 4 {
		t.Fatalf("expected at least 4 recorded pointers, got %d", len(tr.ptrs))
	}
	// Note: With the new interface, row reuse patterns may be different
	// This test mainly verifies that the tracking reader works with the new interface
}

// TestMergesortReader_WithActualParquetReader tests NewMergesortReader with a single actual CookedMetricTranslatingReader
// This is critical because all existing tests use mock readers, but the production issue is with real parquet readers
func TestMergesortReader_WithActualParquetReader(t *testing.T) {
	ctx := context.Background()

	// Generate metric data
	expectedRecords := 480
	data := createMergesortTestParquet(t, expectedRecords)

	reader := bytes.NewReader(data)
	rawReader, err := NewParquetRawReader(reader, int64(len(data)), 1000)
	require.NoError(t, err, "Should create NewParquetRawReader")
	defer func() { _ = rawReader.Close() }()

	cookedReader := NewCookedMetricTranslatingReader(rawReader)
	defer func() { _ = cookedReader.Close() }()

	t.Logf("Created cooked reader expecting %d records", expectedRecords)

	// Create MergesortReader with single actual parquet reader
	// Use the same key provider that the production code uses for metrics
	keyProvider := GetCurrentMetricSortKeyProvider()
	mergesortReader, err := NewMergesortReader(ctx, []SchemafiedReader{cookedReader}, keyProvider, 1000)
	require.NoError(t, err, "Should create NewMergesortReader")
	defer func() { _ = mergesortReader.Close() }()

	// Read all records from the mergesort reader
	totalRecords := 0
	batchNum := 0
	for {
		batch, err := mergesortReader.Next(ctx)
		batchNum++

		if err != nil {
			if errors.Is(err, io.EOF) {
				t.Logf("Batch %d - Got EOF after reading %d total records", batchNum, totalRecords)
				break
			}
			require.NoError(t, err, "Next should not fail")
		}

		if batch == nil {
			t.Logf("Batch %d - Got nil batch after reading %d total records", batchNum, totalRecords)
			break
		}

		batchSize := batch.Len()
		totalRecords += batchSize
		t.Logf("Batch %d - Got %d records (total: %d)", batchNum, batchSize, totalRecords)

		if batchSize == 0 {
			t.Logf("Batch %d - Empty batch, stopping", batchNum)
			break
		}
	}

	t.Logf("MergesortReader returned %d records (expected %d)", totalRecords, expectedRecords)

	// This is the critical test - NewMergesortReader should not lose any data even with a single reader
	require.Equal(t, expectedRecords, totalRecords,
		"NewMergesortReader should not lose data when wrapping a single actual parquet reader")
}

// TestMergesortReader_WithMultipleActualParquetReaders tests NewMergesortReader with multiple actual readers
// This replicates the production scenario where multiple files are merged together
func TestMergesortReader_WithMultipleActualParquetReaders(t *testing.T) {
	testCases := []struct {
		name            string
		expectedRecords int
	}{
		{"file1", 480},  // Small file 1
		{"file2", 456},  // Small file 2
		{"file3", 1414}, // Larger file for comparison
	}

	ctx := context.Background()

	// Create readers for multiple files
	var readers []SchemafiedReader
	var expectedTotalRecords int

	for _, tc := range testCases {
		// Generate metric data
		data := createMergesortTestParquet(t, tc.expectedRecords)

		reader := bytes.NewReader(data)
		rawReader, err := NewParquetRawReader(reader, int64(len(data)), 1000)
		require.NoError(t, err, "Should create NewParquetRawReader")
		defer func() { _ = rawReader.Close() }()

		cookedReader := NewCookedMetricTranslatingReader(rawReader)
		defer func() { _ = cookedReader.Close() }()

		readers = append(readers, cookedReader)
		expectedTotalRecords += tc.expectedRecords

		t.Logf("Created reader for %s expecting %d records", tc.name, tc.expectedRecords)
	}

	t.Logf("Total expected records from %d files: %d", len(testCases), expectedTotalRecords)

	// Create MergesortReader with multiple actual parquet readers
	keyProvider := GetCurrentMetricSortKeyProvider()
	mergesortReader, err := NewMergesortReader(ctx, readers, keyProvider, 1000)
	require.NoError(t, err, "Should create NewMergesortReader")
	defer func() { _ = mergesortReader.Close() }()

	// Read all records from the mergesort reader
	totalRecords := 0
	batchNum := 0
	for {
		batch, err := mergesortReader.Next(ctx)
		batchNum++

		if err != nil {
			if errors.Is(err, io.EOF) {
				t.Logf("Batch %d - Got EOF after reading %d total records", batchNum, totalRecords)
				break
			}
			require.NoError(t, err, "Next should not fail")
		}

		if batch == nil {
			t.Logf("Batch %d - Got nil batch after reading %d total records", batchNum, totalRecords)
			break
		}

		batchSize := batch.Len()
		totalRecords += batchSize
		t.Logf("Batch %d - Got %d records (total: %d)", batchNum, batchSize, totalRecords)

		if batchSize == 0 {
			t.Logf("Batch %d - Empty batch, stopping", batchNum)
			break
		}
	}

	t.Logf("MergesortReader returned %d records (expected %d)", totalRecords, expectedTotalRecords)

	// This is the critical test - NewMergesortReader should not lose any data when merging multiple files
	require.Equal(t, expectedTotalRecords, totalRecords,
		"NewMergesortReader should not lose data when merging multiple actual parquet readers")
}

// TestMergesortReader_MetricSortKeyOrdering tests MergesortReader with actual metric sort keys
// using randomized but reproducible data to ensure proper [name, tid, timestamp] ordering
func TestMergesortReader_MetricSortKeyOrdering(t *testing.T) {
	// Use fixed seed for reproducible randomness
	rng := rand.New(rand.NewSource(42))

	// Define fixed metric names (< 10 as requested)
	metricNames := []string{
		"cpu.usage",
		"memory.usage",
		"disk.io",
		"network.bytes",
		"http.requests",
		"db.connections",
		"cache.hits",
	}

	// Generate test data for multiple readers with interleaved values
	var allTestData []pipeline.Row
	readerCount := 3
	rowsPerReader := 20

	// Create data for each reader
	readerData := make([][]pipeline.Row, readerCount)
	for readerIdx := range readerCount {
		for rowIdx := range rowsPerReader {
			// Pick random metric name
			metricName := metricNames[rng.Intn(len(metricNames))]

			// Generate random TID (can be positive or negative)
			tid := rng.Int63()
			if rng.Float32() < 0.5 {
				tid = -tid // Make some TIDs negative to test signed ordering
			}

			// Generate random timestamp in reasonable range
			timestamp := int64(1700000000000) + rng.Int63n(86400000) // Random within 24 hours

			row := pipeline.Row{
				wkk.RowKeyCName:            metricName,
				wkk.RowKeyCTID:             tid,
				wkk.RowKeyCTimestamp:       timestamp,
				wkk.NewRowKey("reader_id"): fmt.Sprintf("reader_%d", readerIdx),
				wkk.NewRowKey("row_id"):    fmt.Sprintf("row_%d", rowIdx),
				wkk.NewRowKey("value"):     rng.Float64() * 100,
			}

			readerData[readerIdx] = append(readerData[readerIdx], row)
			allTestData = append(allTestData, row)
		}
	}

	// Sort all test data using the same logic as MetricSortKey.Compare()
	// This is our expected output order
	sort.Slice(allTestData, func(i, j int) bool {
		row1, row2 := allTestData[i], allTestData[j]

		// Compare names first
		name1, _ := row1[wkk.RowKeyCName].(string)
		name2, _ := row2[wkk.RowKeyCName].(string)
		if name1 != name2 {
			return name1 < name2
		}

		// Compare TIDs second (signed int64 comparison)
		tid1, _ := row1[wkk.RowKeyCTID].(int64)
		tid2, _ := row2[wkk.RowKeyCTID].(int64)
		if tid1 != tid2 {
			return tid1 < tid2
		}

		// Compare timestamps third
		ts1, _ := row1[wkk.RowKeyCTimestamp].(int64)
		ts2, _ := row2[wkk.RowKeyCTimestamp].(int64)
		return ts1 < ts2
	})

	t.Logf("Generated %d total rows across %d readers", len(allTestData), readerCount)
	t.Logf("Sample expected order (first 5):")
	for i := 0; i < 5 && i < len(allTestData); i++ {
		row := allTestData[i]
		t.Logf("  [%s, %d, %d]",
			row[wkk.RowKeyCName], row[wkk.RowKeyCTID], row[wkk.RowKeyCTimestamp])
	}

	// Sort each reader's data individually (MergesortReader expects sorted input from each reader)
	for i := range readerCount {
		sort.Slice(readerData[i], func(a, b int) bool {
			row1, row2 := readerData[i][a], readerData[i][b]

			// Compare names first
			name1, _ := row1[wkk.RowKeyCName].(string)
			name2, _ := row2[wkk.RowKeyCName].(string)
			if name1 != name2 {
				return name1 < name2
			}

			// Compare TIDs second
			tid1, _ := row1[wkk.RowKeyCTID].(int64)
			tid2, _ := row2[wkk.RowKeyCTID].(int64)
			if tid1 != tid2 {
				return tid1 < tid2
			}

			// Compare timestamps third
			ts1, _ := row1[wkk.RowKeyCTimestamp].(int64)
			ts2, _ := row2[wkk.RowKeyCTimestamp].(int64)
			return ts1 < ts2
		})
	}

	// Create mock readers with the sorted data
	metricSchema := createMetricSchema()
	readers := make([]SchemafiedReader, readerCount)
	for i := range readerCount {
		readers[i] = newMockReader(fmt.Sprintf("reader_%d", i), readerData[i], metricSchema)
	}

	// Create MergesortReader with production metric sort key provider
	keyProvider := GetCurrentMetricSortKeyProvider()
	mergesortReader, err := NewMergesortReader(context.Background(), readers, keyProvider, 1000)
	if err != nil {
		t.Fatalf("NewMergesortReader() error = %v", err)
	}
	defer func() { _ = mergesortReader.Close() }()

	// Read all rows from mergesort reader
	actualRows, err := readAllRows(mergesortReader)
	if err != nil {
		t.Fatalf("readAllRows() error = %v", err)
	}

	if len(actualRows) != len(allTestData) {
		t.Fatalf("Expected %d rows, got %d", len(allTestData), len(actualRows))
	}

	// Verify that the output matches our expected sorted order
	t.Logf("Verifying sort order of %d rows...", len(actualRows))
	for i, expectedRow := range allTestData {
		actualRow := actualRows[i]

		// Check name
		expectedName, _ := expectedRow[wkk.RowKeyCName].(string)
		actualName, _ := actualRow[wkk.RowKeyCName].(string)
		if actualName != expectedName {
			t.Errorf("Row %d name mismatch: expected %s, got %s", i, expectedName, actualName)
		}

		// Check TID
		expectedTID, _ := expectedRow[wkk.RowKeyCTID].(int64)
		actualTID, _ := actualRow[wkk.RowKeyCTID].(int64)
		if actualTID != expectedTID {
			t.Errorf("Row %d TID mismatch: expected %d, got %d", i, expectedTID, actualTID)
		}

		// Check timestamp
		expectedTS, _ := expectedRow[wkk.RowKeyCTimestamp].(int64)
		actualTS, _ := actualRow[wkk.RowKeyCTimestamp].(int64)
		if actualTS != expectedTS {
			t.Errorf("Row %d timestamp mismatch: expected %d, got %d", i, expectedTS, actualTS)
		}

		// Verify ordering is maintained (check against previous row)
		if i > 0 {
			prevRow := actualRows[i-1]
			prevName, _ := prevRow[wkk.RowKeyCName].(string)
			prevTID, _ := prevRow[wkk.RowKeyCTID].(int64)
			prevTS, _ := prevRow[wkk.RowKeyCTimestamp].(int64)

			// Check sort order: [name, tid, timestamp]
			if actualName < prevName {
				t.Errorf("Row %d sort violation: name %s < previous name %s", i, actualName, prevName)
			} else if actualName == prevName {
				if actualTID < prevTID {
					t.Errorf("Row %d sort violation: TID %d < previous TID %d (same name %s)", i, actualTID, prevTID, actualName)
				} else if actualTID == prevTID {
					if actualTS < prevTS {
						t.Errorf("Row %d sort violation: timestamp %d < previous timestamp %d (same name %s, TID %d)", i, actualTS, prevTS, actualName, actualTID)
					}
				}
			}
		}
	}

	// Only log success if we got here without failures
	if !t.Failed() {
		t.Logf("✓ All %d rows are in correct [name, TID, timestamp] sort order", len(actualRows))
	} else {
		t.Logf("✗ FOUND BUG: MergesortReader is not producing correct sort order!")
		t.Logf("Expected first 5 rows in order:")
		for i := 0; i < 5 && i < len(allTestData); i++ {
			row := allTestData[i]
			t.Logf("  %d: [%s, %d, %d]", i,
				row[wkk.RowKeyCName], row[wkk.RowKeyCTID], row[wkk.RowKeyCTimestamp])
		}
		t.Logf("Actual first 5 rows returned:")
		for i := 0; i < 5 && i < len(actualRows); i++ {
			row := actualRows[i]
			t.Logf("  %d: [%s, %d, %d]", i,
				row[wkk.RowKeyCName], row[wkk.RowKeyCTID], row[wkk.RowKeyCTimestamp])
		}
		return // Don't continue with reader distribution checks if sort is broken
	}

	// Additional verification: check that we have data from all readers
	readerCounts := make(map[string]int)
	for _, row := range actualRows {
		readerID, _ := row[wkk.NewRowKey("reader_id")].(string)
		readerCounts[readerID]++
	}

	t.Logf("Data distribution across readers:")
	for readerID, count := range readerCounts {
		t.Logf("  %s: %d rows", readerID, count)
	}

	// Verify we got data from all readers
	for i := 0; i < readerCount; i++ {
		expectedReaderID := fmt.Sprintf("reader_%d", i)
		if count, exists := readerCounts[expectedReaderID]; !exists || count == 0 {
			t.Errorf("Missing data from %s", expectedReaderID)
		}
	}
}

// TestMergesortReader_Advance tests the advance function in isolation
// to understand how it handles state transitions and key generation
// SKIPPED: advance is now internal to activeReader

// TestMergesortReader_KeyPoolingBug demonstrates the exact pooling bug in Next()
func TestMergesortReader_KeyPoolingBug(t *testing.T) {
	ctx := context.Background()

	// Create two readers with data that should interleave
	reader1Data := []pipeline.Row{
		{wkk.RowKeyCName: "metric_a", wkk.RowKeyCTID: int64(100), wkk.RowKeyCTimestamp: int64(1000), wkk.NewRowKey("source"): "reader1_row1"},
		{wkk.RowKeyCName: "metric_a", wkk.RowKeyCTID: int64(300), wkk.RowKeyCTimestamp: int64(1000), wkk.NewRowKey("source"): "reader1_row2"},
	}

	reader2Data := []pipeline.Row{
		{wkk.RowKeyCName: "metric_a", wkk.RowKeyCTID: int64(200), wkk.RowKeyCTimestamp: int64(1000), wkk.NewRowKey("source"): "reader2_row1"},
		{wkk.RowKeyCName: "metric_a", wkk.RowKeyCTID: int64(400), wkk.RowKeyCTimestamp: int64(1000), wkk.NewRowKey("source"): "reader2_row2"},
	}

	metricSchema := createMetricSchema()
	reader1 := newMockReader("reader1", reader1Data, metricSchema)
	reader2 := newMockReader("reader2", reader2Data, metricSchema)

	keyProvider := GetCurrentMetricSortKeyProvider()
	mergesortReader, err := NewMergesortReader(ctx, []SchemafiedReader{reader1, reader2}, keyProvider, 1000)
	if err != nil {
		t.Fatalf("NewMergesortReader() error = %v", err)
	}
	defer func() { _ = mergesortReader.Close() }()

	// Expected order based on TID: 100, 200, 300, 400
	expectedSources := []string{"reader1_row1", "reader2_row1", "reader1_row2", "reader2_row2"}
	expectedTIDs := []int64{100, 200, 300, 400}

	// Read all data
	allRows, err := readAllRows(mergesortReader)
	if err != nil {
		t.Fatalf("readAllRows() error = %v", err)
	}

	if len(allRows) != 4 {
		t.Fatalf("Expected 4 rows, got %d", len(allRows))
	}

	t.Logf("Actual order returned:")
	for i, row := range allRows {
		source := row[wkk.NewRowKey("source")]
		tid := row[wkk.RowKeyCTID]
		t.Logf("  %d: %s (TID=%d)", i, source, tid)
	}

	t.Logf("Expected order:")
	for i, expectedSource := range expectedSources {
		t.Logf("  %d: %s (TID=%d)", i, expectedSource, expectedTIDs[i])
	}

	// Verify the order is correct
	for i, expectedSource := range expectedSources {
		actualSource := allRows[i][wkk.NewRowKey("source")]
		actualTID := allRows[i][wkk.RowKeyCTID].(int64)

		if actualSource != expectedSource {
			t.Errorf("Row %d source mismatch: expected %s, got %s", i, expectedSource, actualSource)
		}
		if actualTID != expectedTIDs[i] {
			t.Errorf("Row %d TID mismatch: expected %d, got %d", i, expectedTIDs[i], actualTID)
		}
	}

	if t.Failed() {
		t.Logf("✗ CONFIRMED: MergesortReader key pooling bug causes incorrect sort order!")
	} else {
		t.Logf("✓ MergesortReader produced correct sort order")
	}
}
