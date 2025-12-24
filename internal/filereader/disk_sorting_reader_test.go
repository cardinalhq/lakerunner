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
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func TestDiskSortingReader_BasicSorting(t *testing.T) {
	// Create test data with unsorted metric names, TIDs, and timestamps
	testRows := []pipeline.Row{
		{
			wkk.RowKeyCName:        "metric_z",
			wkk.RowKeyCTID:         int64(200),
			wkk.RowKeyCTimestamp:   int64(3000),
			wkk.NewRowKey("value"): float64(1.0),
		},
		{
			wkk.RowKeyCName:        "metric_a",
			wkk.RowKeyCTID:         int64(100),
			wkk.RowKeyCTimestamp:   int64(1000),
			wkk.NewRowKey("value"): float64(2.0),
		},
		{
			wkk.RowKeyCName:        "metric_a",
			wkk.RowKeyCTID:         int64(100),
			wkk.RowKeyCTimestamp:   int64(2000),
			wkk.NewRowKey("value"): float64(3.0),
		},
	}

	mockReader := NewMockReader(testRows)
	sortingReader, err := NewDiskSortingReader(mockReader, &LogSortKeyProvider{}, 1000)
	require.NoError(t, err)
	defer func() { _ = sortingReader.Close() }()

	// Read all rows
	var allRows []pipeline.Row
	for {
		batch, err := sortingReader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := 0; i < batch.Len(); i++ {
			allRows = append(allRows, batch.Get(i))
		}
	}

	// Verify sorting: metric_a (both rows), then metric_z
	require.Len(t, allRows, 3)

	// First two rows should be metric_a, sorted by timestamp
	assert.Equal(t, "metric_a", allRows[0][wkk.RowKeyCName])
	assert.Equal(t, int64(1000), allRows[0][wkk.RowKeyCTimestamp])

	assert.Equal(t, "metric_a", allRows[1][wkk.RowKeyCName])
	assert.Equal(t, int64(2000), allRows[1][wkk.RowKeyCTimestamp])

	// Last row should be metric_z
	assert.Equal(t, "metric_z", allRows[2][wkk.RowKeyCName])
	assert.Equal(t, int64(3000), allRows[2][wkk.RowKeyCTimestamp])
}

func TestDiskSortingReader_TypePreservation(t *testing.T) {
	// Test various types that need to be preserved through SpillCodec
	testRow := pipeline.Row{
		wkk.RowKeyCName:                "test_metric",
		wkk.RowKeyCTID:                 int64(12345),
		wkk.RowKeyCTimestamp:           int64(1640995200000),
		wkk.NewRowKey("string_field"):  "test_string",
		wkk.NewRowKey("int64_field"):   int64(9223372036854775807), // Max int64
		wkk.NewRowKey("float64_field"): float64(3.14159),
		wkk.NewRowKey("byte_slice"):    []byte{0x01, 0x02, 0x03},
		wkk.NewRowKey("bool_field"):    true,
		wkk.NewRowKey("nil_field"):     nil,
	}

	mockReader := NewMockReader([]pipeline.Row{testRow})
	sortingReader, err := NewDiskSortingReader(mockReader, &nameTidTimestampSortKeyProvider{}, 1000)
	require.NoError(t, err)
	defer func() { _ = sortingReader.Close() }()

	// Read the row back
	batch, err := sortingReader.Next(context.TODO())
	require.NoError(t, err)
	require.Equal(t, 1, batch.Len())

	decoded := batch.Get(0)

	// Verify all types are preserved
	assert.Equal(t, "test_metric", decoded[wkk.RowKeyCName])
	assert.Equal(t, int64(12345), decoded[wkk.RowKeyCTID])
	assert.Equal(t, int64(1640995200000), decoded[wkk.RowKeyCTimestamp])
	assert.Equal(t, "test_string", decoded[wkk.NewRowKey("string_field")])
	assert.Equal(t, int64(9223372036854775807), decoded[wkk.NewRowKey("int64_field")])
	assert.Equal(t, float64(3.14159), decoded[wkk.NewRowKey("float64_field")])
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, decoded[wkk.NewRowKey("byte_slice")])
	assert.Equal(t, true, decoded[wkk.NewRowKey("bool_field")])
	assert.Nil(t, decoded[wkk.NewRowKey("nil_field")])
}

func TestDiskSortingReader_EmptyInput(t *testing.T) {
	mockReader := NewMockReader([]pipeline.Row{})
	sortingReader, err := NewDiskSortingReader(mockReader, &nameTidTimestampSortKeyProvider{}, 1000)
	require.NoError(t, err)
	defer func() { _ = sortingReader.Close() }()

	// Should get EOF immediately
	_, err = sortingReader.Next(context.TODO())
	assert.Equal(t, io.EOF, err)
}

func TestDiskSortingReader_MissingFields(t *testing.T) {
	// Test row missing some sort fields - should be handled gracefully by sort function
	testRows := []pipeline.Row{
		{
			wkk.RowKeyCName:        "metric_b",
			wkk.RowKeyCTID:         int64(100),
			wkk.RowKeyCTimestamp:   int64(2000),
			wkk.NewRowKey("value"): float64(2.0),
		},
		{
			wkk.RowKeyCName: "metric_a",
			// Missing TID and timestamp - sort function should handle this gracefully
			wkk.NewRowKey("value"): float64(1.0),
		},
	}

	mockReader := NewMockReader(testRows)
	sortingReader, err := NewDiskSortingReader(mockReader, &nameTidTimestampSortKeyProvider{}, 1000)
	require.NoError(t, err)
	defer func() { _ = sortingReader.Close() }()

	// Should succeed - missing fields are handled by the sort function
	batch, err := sortingReader.Next(context.TODO())

	// Should read successfully (missing fields don't cause failures anymore)
	assert.NoError(t, err)
	assert.Equal(t, 2, batch.Len())

	// pipeline.Row with missing fields should be sorted to the end by MetricNameTidTimestampSort
	assert.Equal(t, "metric_a", batch.Get(0)[wkk.RowKeyCName]) // Missing fields sort first
	assert.Equal(t, "metric_b", batch.Get(1)[wkk.RowKeyCName]) // Complete row sorts later
}

func TestDiskSortingReader_CleanupOnError(t *testing.T) {
	// Create reader that will fail during reading
	mockReader := &MockReader{
		rows:      []pipeline.Row{{wkk.NewRowKey("test"): "value"}},
		readError: fmt.Errorf("simulated read error"),
	}

	sortingReader, err := NewDiskSortingReader(mockReader, &nameTidTimestampSortKeyProvider{}, 1000)
	require.NoError(t, err)

	tempFileName := sortingReader.tempFile.Name()

	// Try to read - should fail
	_, err = sortingReader.Next(context.TODO())
	assert.Error(t, err)

	// Close should clean up temp file
	err = sortingReader.Close()
	assert.NoError(t, err)

	// Verify temp file was removed
	_, err = os.Stat(tempFileName)
	assert.True(t, os.IsNotExist(err), "Temp file should be removed")
}

// MockReader for testing
type MockReader struct {
	rows       []pipeline.Row
	currentIdx int
	readError  error
	closed     bool
}

func NewMockReader(rows []pipeline.Row) *MockReader {
	return &MockReader{rows: rows}
}

func (m *MockReader) Next(ctx context.Context) (*Batch, error) {
	if m.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	if m.readError != nil {
		return nil, m.readError
	}

	if m.currentIdx >= len(m.rows) {
		return nil, io.EOF
	}

	batch := pipeline.GetBatch()

	for batch.Len() < 100 && m.currentIdx < len(m.rows) {
		row := batch.AddRow()
		for k, v := range m.rows[m.currentIdx] {
			row[k] = v
		}
		m.currentIdx++
	}

	return batch, nil
}

func (m *MockReader) Close() error {
	m.closed = true
	return nil
}

func (m *MockReader) TotalRowsReturned() int64 {
	return int64(m.currentIdx)
}

func (m *MockReader) GetSchema() *ReaderSchema {
	return NewReaderSchema()
}

// nameTidTimestampSortKeyProvider is a test sort key provider that sorts by name, TID, and timestamp.
// This replaces the production MetricSortKeyProvider for testing purposes.
type nameTidTimestampSortKeyProvider struct{}

func (p *nameTidTimestampSortKeyProvider) MakeKey(row pipeline.Row) SortKey {
	key := &nameTidTimestampSortKey{}
	key.name, key.nameOk = row[wkk.RowKeyCName].(string)
	key.tid, key.tidOk = row[wkk.RowKeyCTID].(int64)
	key.timestamp, key.tsOk = row[wkk.RowKeyCTimestamp].(int64)
	return key
}

type nameTidTimestampSortKey struct {
	name      string
	tid       int64
	timestamp int64
	nameOk    bool
	tidOk     bool
	tsOk      bool
}

func (k *nameTidTimestampSortKey) Compare(other SortKey) int {
	o := other.(*nameTidTimestampSortKey)

	// Compare name
	if cmp := compareOptional(k.name, k.nameOk, o.name, o.nameOk); cmp != 0 {
		return cmp
	}

	// Compare TID
	if cmp := compareOptional(k.tid, k.tidOk, o.tid, o.tidOk); cmp != 0 {
		return cmp
	}

	// Compare timestamp
	return compareOptional(k.timestamp, k.tsOk, o.timestamp, o.tsOk)
}

func (k *nameTidTimestampSortKey) Release() {
	// No pooling for test-only key
}

func TestDiskSortingReader_ArbitraryRowCount(t *testing.T) {
	// Test to verify DiskSortingReader doesn't drop rows due to batch size boundaries
	// This test creates exactly 1234 rows and verifies we get exactly 1234 back
	const totalRows = 1234
	const batchSize = 1000

	// Create test data with 1234 rows
	testRows := make([]pipeline.Row, totalRows)
	for i := 0; i < totalRows; i++ {
		testRows[i] = pipeline.Row{
			wkk.RowKeyCName:        fmt.Sprintf("metric_%04d", i%10), // 10 different metric names
			wkk.RowKeyCTID:         int64(i % 100),                   // 100 different TIDs
			wkk.RowKeyCTimestamp:   int64(1000 + i),                  // Sequential timestamps
			wkk.NewRowKey("value"): float64(i),                       // Sequential values
		}
	}

	mockReader := NewMockReader(testRows)
	sortingReader, err := NewDiskSortingReader(mockReader, &nameTidTimestampSortKeyProvider{}, batchSize)
	require.NoError(t, err)
	defer func() { _ = sortingReader.Close() }()

	// Read all rows back and count them
	totalRead := 0
	batchCount := 0
	for {
		batch, err := sortingReader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "Failed to read batch %d", batchCount+1)

		batchSize := batch.Len()
		totalRead += batchSize
		batchCount++

		// Log batch sizes to help debug the pattern
		t.Logf("Batch %d: %d rows (total so far: %d)", batchCount, batchSize, totalRead)

		// Verify each row has the expected structure
		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			require.Contains(t, row, wkk.RowKeyCName, "Row missing metric name")
			require.Contains(t, row, wkk.RowKeyCTID, "Row missing TID")
			require.Contains(t, row, wkk.RowKeyCTimestamp, "Row missing timestamp")
		}
	}

	// This is the critical assertion - we must get exactly the same number of rows out
	assert.Equal(t, totalRows, totalRead, "DiskSortingReader dropped or duplicated rows")

	// Verify the reader's internal count matches
	assert.Equal(t, int64(totalRead), sortingReader.TotalRowsReturned(), "TotalRowsReturned doesn't match actual rows read")

	t.Logf("SUCCESS: Fed %d rows, got %d rows back in %d batches", totalRows, totalRead, batchCount)
}

// manyBatchReader yields a fixed number of single-row batches.
// It exercises writeAndIndexAllRows across many batches to ensure
// batches are properly returned to the pool.
type manyBatchReader struct {
	remaining int
}

func (m *manyBatchReader) Next(ctx context.Context) (*Batch, error) {
	if m.remaining == 0 {
		return nil, io.EOF
	}

	b := pipeline.GetBatch()
	row := b.AddRow()
	row[wkk.RowKeyCName] = "metric"
	row[wkk.RowKeyCTID] = int64(m.remaining)
	row[wkk.RowKeyCTimestamp] = int64(m.remaining)
	m.remaining--
	return b, nil
}

func (m *manyBatchReader) Close() error { return nil }

func (m *manyBatchReader) TotalRowsReturned() int64 { return 0 }

func (m *manyBatchReader) GetSchema() *ReaderSchema { return nil }

// TestWriteAndIndexAllRowsDoesNotLeakBatches verifies that batches are always
// returned to the pool, preventing unbounded heap allocations when processing
// many batches.
func TestWriteAndIndexAllRowsDoesNotLeakBatches(t *testing.T) {
	const batchCount = 200

	// Get initial pool state before processing
	initialStats := pipeline.GlobalBatchPoolStats()

	reader := &manyBatchReader{remaining: batchCount}
	dsr, err := NewDiskSortingReader(reader, &nameTidTimestampSortKeyProvider{}, 10)
	require.NoError(t, err)
	defer func() { _ = dsr.Close() }()

	require.NoError(t, dsr.writeAndIndexAllRows(context.TODO()))

	// Get final pool state after processing
	finalStats := pipeline.GlobalBatchPoolStats()

	// Calculate gets and puts during the test
	getsUsed := finalStats.Gets - initialStats.Gets
	putsUsed := finalStats.Puts - initialStats.Puts

	// Every GetBatch() call should have a matching ReturnBatch() call
	assert.Equal(t, getsUsed, putsUsed,
		"every GetBatch() should have matching ReturnBatch() - gets: %d, puts: %d",
		getsUsed, putsUsed)

	// Sanity check: we should have used some batches during processing
	assert.Greater(t, getsUsed, uint64(0), "test should have used some batches")
}
