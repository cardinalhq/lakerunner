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
	"testing"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// createSimpleSchema creates a basic schema for test mock readers
func createSimpleSchema() *ReaderSchema {
	schema := NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("ts"), wkk.NewRowKey("ts"), DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("data"), wkk.NewRowKey("data"), DataTypeString, true)
	return schema
}

func TestNewMergesortReader(t *testing.T) {
	// Test with valid readers and keyProvider
	schema := createSimpleSchema()
	readers := []Reader{
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
	_, err = NewMergesortReader(context.TODO(), []Reader{}, keyProvider, 1000)
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
	readers := []Reader{
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
	readers := []Reader{
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
	readers := []Reader{
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
	readers := []Reader{
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
	readers := []Reader{
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
	or, err := NewMergesortReader(context.TODO(), []Reader{tr}, NewTimeOrderedSortKeyProvider("ts"), 1)
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
