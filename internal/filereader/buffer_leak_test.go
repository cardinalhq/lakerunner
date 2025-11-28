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
	"io"
	"strings"
	"testing"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// mockLeakTestReader is a simple reader for testing buffer leaks
type mockLeakTestReader struct {
	rows  []pipeline.Row
	index int
}

func newMockLeakTestReader(numRows int) *mockLeakTestReader {
	rows := make([]pipeline.Row, numRows)
	for i := range rows {
		row := make(pipeline.Row)
		row[wkk.RowKeyCName] = "test_metric"
		row[wkk.RowKeyCTID] = int64(i)
		row[wkk.RowKeyCTimestamp] = int64(i * 1000)
		rows[i] = row
	}
	return &mockLeakTestReader{rows: rows}
}

func (m *mockLeakTestReader) Next(ctx context.Context) (*Batch, error) {
	if m.index >= len(m.rows) {
		return nil, io.EOF
	}

	batch := pipeline.GetBatch()
	row := batch.AddRow()
	for k, v := range m.rows[m.index] {
		row[k] = v
	}
	m.index++
	return batch, nil
}

func (m *mockLeakTestReader) Close() error {
	return nil
}

func (m *mockLeakTestReader) TotalRowsReturned() int64 {
	return int64(m.index)
}

func (m *mockLeakTestReader) GetSchema() *ReaderSchema {
	return NewReaderSchema()
}

// assertNoBufferLeaks checks that the number of GetBatch calls equals ReturnBatch calls
func assertNoBufferLeaks(t *testing.T, initialStats, finalStats pipeline.BatchPoolStats) {
	t.Helper()

	initialLeaks := initialStats.LeakedBatches()
	finalLeaks := finalStats.LeakedBatches()

	if finalLeaks > initialLeaks {
		t.Errorf("Buffer leak detected! Leaked batches increased from %d to %d (leaked %d batches)",
			initialLeaks, finalLeaks, finalLeaks-initialLeaks)
	}
}

// readAllBatches reads all batches from a reader and returns them (for testing purposes)
func readAllBatches(t *testing.T, reader Reader) []*Batch {
	t.Helper()
	var batches []*Batch

	for {
		batch, err := reader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Error reading batch: %v", err)
		}
		batches = append(batches, batch)
	}

	return batches
}

func TestMergesortReader_BufferLeak(t *testing.T) {
	// Record initial buffer pool stats
	initialStats := pipeline.GlobalBatchPoolStats()

	// Create simple JSON readers that don't leak
	jsonData1 := `{"metric_name": "test_metric", "chq_tid": 1, "chq_timestamp": 1000}
{"metric_name": "test_metric", "chq_tid": 2, "chq_timestamp": 3000}
{"metric_name": "test_metric", "chq_tid": 3, "chq_timestamp": 5000}`

	jsonData2 := `{"metric_name": "test_metric", "chq_tid": 4, "chq_timestamp": 2000}
{"metric_name": "test_metric", "chq_tid": 5, "chq_timestamp": 4000}`

	reader1, err := NewJSONLinesReader(io.NopCloser(strings.NewReader(jsonData1)), 100)
	if err != nil {
		t.Fatalf("Failed to create JSONLinesReader 1: %v", err)
	}
	defer func() { _ = reader1.Close() }()

	reader2, err := NewJSONLinesReader(io.NopCloser(strings.NewReader(jsonData2)), 100)
	if err != nil {
		t.Fatalf("Failed to create JSONLinesReader 2: %v", err)
	}
	defer func() { _ = reader2.Close() }()

	readers := []Reader{reader1, reader2}

	// Create MergesortReader
	msReader, err := NewMergesortReader(context.TODO(), readers, NewTimeOrderedSortKeyProvider("chq_timestamp"), 100)
	if err != nil {
		t.Fatalf("Failed to create MergesortReader: %v", err)
	}
	defer func() { _ = msReader.Close() }()

	// Read all batches
	batches := readAllBatches(t, msReader)

	// Return all batches to pool (simulating proper consumer behavior)
	for _, batch := range batches {
		pipeline.ReturnBatch(batch)
	}

	// Check for buffer leaks
	finalStats := pipeline.GlobalBatchPoolStats()
	assertNoBufferLeaks(t, initialStats, finalStats)
}

func TestSequentialReader_BufferLeak(t *testing.T) {
	// Record initial buffer pool stats
	initialStats := pipeline.GlobalBatchPoolStats()

	// Create test readers
	reader1 := newMockLeakTestReader(5)
	reader2 := newMockLeakTestReader(3)
	readers := []Reader{reader1, reader2}

	// Create SequentialReader
	seqReader, err := NewSequentialReader(readers, 100)
	if err != nil {
		t.Fatalf("Failed to create SequentialReader: %v", err)
	}
	defer func() { _ = seqReader.Close() }()

	// Read all batches
	batches := readAllBatches(t, seqReader)

	// Return all batches to pool (simulating proper consumer behavior)
	for _, batch := range batches {
		pipeline.ReturnBatch(batch)
	}

	// Check for buffer leaks
	finalStats := pipeline.GlobalBatchPoolStats()
	assertNoBufferLeaks(t, initialStats, finalStats)
}

func TestJSONLinesReader_BufferLeak(t *testing.T) {
	// Record initial buffer pool stats
	initialStats := pipeline.GlobalBatchPoolStats()

	// Create test JSON data
	jsonData := `{"metric_name": "test_metric", "chq_tid": 1, "chq_timestamp": 1000}
{"metric_name": "test_metric", "chq_tid": 2, "chq_timestamp": 2000}
{"metric_name": "test_metric", "chq_tid": 3, "chq_timestamp": 3000}`

	reader := io.NopCloser(strings.NewReader(jsonData))

	// Create JSONLinesReader
	jsonReader, err := NewJSONLinesReader(reader, 100)
	if err != nil {
		t.Fatalf("Failed to create JSONLinesReader: %v", err)
	}
	defer func() { _ = jsonReader.Close() }()

	// Read all batches
	batches := readAllBatches(t, jsonReader)

	// Return all batches to pool (simulating proper consumer behavior)
	for _, batch := range batches {
		pipeline.ReturnBatch(batch)
	}

	// Check for buffer leaks
	finalStats := pipeline.GlobalBatchPoolStats()
	assertNoBufferLeaks(t, initialStats, finalStats)
}

func TestMemorySortingReader_BufferLeak(t *testing.T) {
	// Record initial buffer pool stats
	initialStats := pipeline.GlobalBatchPoolStats()

	// Create test reader
	baseReader := newMockLeakTestReader(10)

	// Create MemorySortingReader
	sortingReader, err := NewMemorySortingReader(
		baseReader,
		&TimestampSortKeyProvider{},
		100,
	)
	if err != nil {
		t.Fatalf("Failed to create MemorySortingReader: %v", err)
	}
	defer func() { _ = sortingReader.Close() }()

	// Read all batches
	batches := readAllBatches(t, sortingReader)

	// Return all batches to pool (simulating proper consumer behavior)
	for _, batch := range batches {
		pipeline.ReturnBatch(batch)
	}

	// Check for buffer leaks
	finalStats := pipeline.GlobalBatchPoolStats()
	assertNoBufferLeaks(t, initialStats, finalStats)
}

func TestDiskSortingReader_BufferLeak(t *testing.T) {
	// Record initial buffer pool stats
	initialStats := pipeline.GlobalBatchPoolStats()

	// Create test reader
	baseReader := newMockLeakTestReader(10)

	// Create DiskSortingReader
	sortingReader, err := NewDiskSortingReader(
		baseReader,
		&TimestampSortKeyProvider{},
		100,
	)
	if err != nil {
		t.Fatalf("Failed to create DiskSortingReader: %v", err)
	}
	defer func() { _ = sortingReader.Close() }()

	// Read all batches
	batches := readAllBatches(t, sortingReader)

	// Return all batches to pool (simulating proper consumer behavior)
	for _, batch := range batches {
		pipeline.ReturnBatch(batch)
	}

	// Check for buffer leaks
	finalStats := pipeline.GlobalBatchPoolStats()
	assertNoBufferLeaks(t, initialStats, finalStats)
}

func TestAggregatingMetricsReader_BufferLeak(t *testing.T) {
	// Record initial buffer pool stats
	initialStats := pipeline.GlobalBatchPoolStats()

	// Create test reader with sorted data
	baseReader := newMockLeakTestReader(10)

	// Create AggregatingMetricsReader
	aggReader, err := NewAggregatingMetricsReader(baseReader, 10000, 100)
	if err != nil {
		t.Fatalf("Failed to create AggregatingMetricsReader: %v", err)
	}
	defer func() { _ = aggReader.Close() }()

	// Read all batches
	batches := readAllBatches(t, aggReader)

	// Return all batches to pool (simulating proper consumer behavior)
	for _, batch := range batches {
		pipeline.ReturnBatch(batch)
	}

	// Check for buffer leaks
	finalStats := pipeline.GlobalBatchPoolStats()
	assertNoBufferLeaks(t, initialStats, finalStats)
}

func TestTranslatingReader_BufferLeak(t *testing.T) {
	// Record initial buffer pool stats
	initialStats := pipeline.GlobalBatchPoolStats()

	// Create test reader
	baseReader := newMockLeakTestReader(5)

	// Create simple translator
	translator := &mockLeakTestTranslator{}

	// Create TranslatingReader
	transReader, err := NewTranslatingReader(baseReader, translator, 100)
	if err != nil {
		t.Fatalf("Failed to create TranslatingReader: %v", err)
	}
	defer func() { _ = transReader.Close() }()

	// Read all batches
	batches := readAllBatches(t, transReader)

	// Return all batches to pool (simulating proper consumer behavior)
	for _, batch := range batches {
		pipeline.ReturnBatch(batch)
	}

	// Check for buffer leaks
	finalStats := pipeline.GlobalBatchPoolStats()
	assertNoBufferLeaks(t, initialStats, finalStats)
}

// mockLeakTestTranslator for testing (avoid conflict with existing mockTranslator)
type mockLeakTestTranslator struct{}

func (m *mockLeakTestTranslator) TranslateRow(ctx context.Context, row *pipeline.Row) error {
	// Simple translation: add a field
	(*row)[wkk.NewRowKey("translated")] = true
	return nil
}

// TestStackedReaders_BufferLeak tests stacked readers to simulate real-world usage
func TestStackedReaders_BufferLeak(t *testing.T) {
	// Record initial buffer pool stats
	initialStats := pipeline.GlobalBatchPoolStats()

	// Create base readers
	reader1 := newMockLeakTestReader(20)
	reader2 := newMockLeakTestReader(15)

	// Stack readers: Sequential -> Sorting -> Aggregating
	seqReader, err := NewSequentialReader([]Reader{reader1, reader2}, 50)
	if err != nil {
		t.Fatalf("Failed to create SequentialReader: %v", err)
	}
	defer func() { _ = seqReader.Close() }()

	// Add sorting
	sortingReader, err := NewMemorySortingReader(
		seqReader,
		&TimestampSortKeyProvider{},
		50,
	)
	if err != nil {
		t.Fatalf("Failed to create MemorySortingReader: %v", err)
	}
	defer func() { _ = sortingReader.Close() }()

	// Add aggregation
	aggReader, err := NewAggregatingMetricsReader(sortingReader, 10000, 50)
	if err != nil {
		t.Fatalf("Failed to create AggregatingMetricsReader: %v", err)
	}
	defer func() { _ = aggReader.Close() }()

	// Read all batches
	batches := readAllBatches(t, aggReader)

	// Return all batches to pool (simulating proper consumer behavior)
	for _, batch := range batches {
		pipeline.ReturnBatch(batch)
	}

	// Check for buffer leaks
	finalStats := pipeline.GlobalBatchPoolStats()
	assertNoBufferLeaks(t, initialStats, finalStats)
}
