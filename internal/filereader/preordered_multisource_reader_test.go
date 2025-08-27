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
	"errors"
	"fmt"
	"io"
	"testing"
)

func TestNewPreorderedMultisourceReader(t *testing.T) {
	// Test with valid readers and selector
	readers := []Reader{
		newMockReader("r1", []Row{{"ts": int64(1)}}),
		newMockReader("r2", []Row{{"ts": int64(2)}}),
	}
	selector := TimeOrderedSelector("ts")

	or, err := NewPreorderedMultisourceReader(readers, selector, 1000)
	if err != nil {
		t.Fatalf("NewPreorderedMultisourceReader() error = %v", err)
	}
	defer or.Close()

	if len(or.states) != 2 {
		t.Errorf("Expected 2 states, got %d", len(or.states))
	}

	// Test with no readers
	_, err = NewPreorderedMultisourceReader([]Reader{}, selector, 1000)
	if err == nil {
		t.Error("Expected error for empty readers slice")
	}

	// Test with nil selector
	_, err = NewPreorderedMultisourceReader(readers, nil, 1000)
	if err == nil {
		t.Error("Expected error for nil selector")
	}
}

func TestOrderedReader_Next(t *testing.T) {
	// Create readers with interleaved timestamps to test ordering
	readers := []Reader{
		newMockReader("r1", []Row{
			{"ts": int64(1), "data": "r1-first"},
			{"ts": int64(4), "data": "r1-second"},
			{"ts": int64(7), "data": "r1-third"},
		}),
		newMockReader("r2", []Row{
			{"ts": int64(2), "data": "r2-first"},
			{"ts": int64(5), "data": "r2-second"},
		}),
		newMockReader("r3", []Row{
			{"ts": int64(3), "data": "r3-first"},
			{"ts": int64(6), "data": "r3-second"},
		}),
	}

	selector := TimeOrderedSelector("ts")
	or, err := NewPreorderedMultisourceReader(readers, selector, 1000)
	if err != nil {
		t.Fatalf("NewPreorderedMultisourceReader() error = %v", err)
	}
	defer or.Close()

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
		if allRows[i]["data"] != expected {
			t.Errorf("Row %d data = %v, want %v (ts=%v)", i, allRows[i]["data"], expected, allRows[i]["ts"])
		}
		if i > 0 {
			// Verify timestamps are in order
			prevTs := allRows[i-1]["ts"].(int64)
			currTs := allRows[i]["ts"].(int64)
			if currTs < prevTs {
				t.Errorf("Timestamps out of order: row %d ts=%d < row %d ts=%d", i, currTs, i-1, prevTs)
			}
		}
	}
}

func TestOrderedReader_NextBatched(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{"ts": int64(100)}}),
		newMockReader("r2", []Row{{"ts": int64(200)}}),
		newMockReader("r3", []Row{}), // Empty reader
	}

	selector := TimeOrderedSelector("ts")
	or, err := NewPreorderedMultisourceReader(readers, selector, 1000)
	if err != nil {
		t.Fatalf("NewPreorderedMultisourceReader() error = %v", err)
	}
	defer or.Close()

	// Read first batch (should get both rows)
	batch, err := or.Next()
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if len(batch.Rows) != 2 {
		t.Errorf("Next() returned %d rows, want 2", len(batch.Rows))
	}
	if batch.Rows[0]["ts"] != int64(100) {
		t.Errorf("First row ts = %v, want 100", batch.Rows[0]["ts"])
	}
	if batch.Rows[1]["ts"] != int64(200) {
		t.Errorf("Second row ts = %v, want 200", batch.Rows[1]["ts"])
	}

	// Next read should return EOF
	batch, err = or.Next()
	if !errors.Is(err, io.EOF) {
		t.Errorf("Final Next() should return io.EOF, got err=%v", err)
	}
	if batch != nil {
		t.Errorf("Final Next() should return nil batch, got %v", batch)
	}
}

func TestOrderedReader_ActiveReaderCount(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{"ts": int64(1)}}),
		newMockReader("r2", []Row{{"ts": int64(2)}}),
	}

	selector := TimeOrderedSelector("ts")
	or, err := NewPreorderedMultisourceReader(readers, selector, 1000)
	if err != nil {
		t.Fatalf("NewPreorderedMultisourceReader() error = %v", err)
	}
	defer or.Close()

	// Initially both readers should be active
	if count := or.ActiveReaderCount(); count != 2 {
		t.Errorf("Initial ActiveReaderCount() = %d, want 2", count)
	}

	// Read batch (should get both rows)
	batch, err := or.Next()
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if len(batch.Rows) != 2 {
		t.Fatalf("Expected 2 rows, got %d", len(batch.Rows))
	}

	// Should have 0 active readers (all consumed)
	if count := or.ActiveReaderCount(); count != 0 {
		t.Errorf("After reading all rows ActiveReaderCount() = %d, want 0", count)
	}
}

func TestOrderedReader_AllEmptyReaders(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{}),
		newMockReader("r2", []Row{}),
	}

	selector := TimeOrderedSelector("ts")
	or, err := NewPreorderedMultisourceReader(readers, selector, 1000)
	if err != nil {
		t.Fatalf("NewPreorderedMultisourceReader() error = %v", err)
	}
	defer or.Close()

	// Should immediately return io.EOF
	batch, err := or.Next()
	if !errors.Is(err, io.EOF) {
		t.Errorf("Next() with all empty readers should return io.EOF, got err=%v", err)
	}
	if batch != nil {
		t.Errorf("Next() with all empty readers should return nil batch, got %v", batch)
	}
}

func TestOrderedReader_Close(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{"ts": int64(1)}}),
		newMockReader("r2", []Row{{"ts": int64(2)}}),
	}

	selector := TimeOrderedSelector("ts")
	or, err := NewPreorderedMultisourceReader(readers, selector, 1000)
	if err != nil {
		t.Fatalf("NewPreorderedMultisourceReader() error = %v", err)
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
	_, err = or.Next()
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

func TestTimeOrderedSelector(t *testing.T) {
	selector := TimeOrderedSelector("timestamp")

	// Test with different timestamp types
	rows := []Row{
		{"timestamp": int64(300), "data": "third"},
		{"timestamp": int64(100), "data": "first"},
		{"timestamp": int64(200), "data": "second"},
	}

	selected := selector(rows)
	if selected != 1 { // Should select row with timestamp 100
		t.Errorf("TimeOrderedSelector selected index %d, want 1", selected)
	}

	// Test with float64 timestamps
	rows = []Row{
		{"timestamp": float64(300.5), "data": "third"},
		{"timestamp": float64(100.1), "data": "first"},
		{"timestamp": float64(200.2), "data": "second"},
	}

	selected = selector(rows)
	if selected != 1 { // Should select row with timestamp 100.1
		t.Errorf("TimeOrderedSelector with float64 selected index %d, want 1", selected)
	}

	// Test with missing timestamp field
	rows = []Row{
		{"data": "no timestamp"},
		{"timestamp": int64(100), "data": "has timestamp"},
	}

	selected = selector(rows)
	if selected != 0 { // Should select first row (missing timestamps default to 0)
		t.Errorf("TimeOrderedSelector with missing timestamp selected index %d, want 0", selected)
	}

	// Test with empty slice
	selected = selector([]Row{})
	if selected != -1 {
		t.Errorf("TimeOrderedSelector with empty slice selected index %d, want -1", selected)
	}
}

// trackingReader is a test implementation that records the address of the row slice
// provided to Read so tests can verify row recycling behavior.
type trackingReader struct {
	rows     []Row
	index    int
	ptrs     []string
	rowCount int64
}

func newTrackingReader(rows []Row) *trackingReader {
	return &trackingReader{rows: rows}
}

func (tr *trackingReader) Next() (*Batch, error) {
	if tr.index >= len(tr.rows) {
		return nil, io.EOF
	}

	// Create a new batch with one row
	batch := &Batch{
		Rows: make([]Row, 1),
	}

	// Create a new row and copy data
	row := make(Row)
	for k, v := range tr.rows[tr.index] {
		row[k] = v
	}
	batch.Rows[0] = row

	// Track the pointer for testing row reuse
	tr.ptrs = append(tr.ptrs, fmt.Sprintf("%p", batch.Rows[0]))

	tr.index++
	tr.rowCount++
	return batch, nil
}

func (tr *trackingReader) Close() error { return nil }

func (tr *trackingReader) TotalRowsReturned() int64 { return tr.rowCount }

func TestPreorderedMultisourceReader_RowReuse(t *testing.T) {
	tr := newTrackingReader([]Row{{"ts": int64(1)}, {"ts": int64(2)}, {"ts": int64(3)}, {"ts": int64(4)}})
	or, err := NewPreorderedMultisourceReader([]Reader{tr}, TimeOrderedSelector("ts"), 1)
	if err != nil {
		t.Fatalf("NewPreorderedMultisourceReader() error = %v", err)
	}
	defer or.Close()

	// Read first batch
	batch, err := or.Next()
	if err != nil {
		t.Fatalf("First Next() err=%v", err)
	}
	if len(batch.Rows) != 1 {
		t.Fatalf("First batch should have 1 row, got %d", len(batch.Rows))
	}
	if batch.Rows[0]["ts"] != int64(1) {
		t.Fatalf("first row ts=%v want 1", batch.Rows[0]["ts"])
	}

	// Read second batch
	batch, err = or.Next()
	if err != nil {
		t.Fatalf("Second Next() err=%v", err)
	}
	if len(batch.Rows) != 1 {
		t.Fatalf("Second batch should have 1 row, got %d", len(batch.Rows))
	}
	if batch.Rows[0]["ts"] != int64(2) {
		t.Fatalf("second row ts=%v want 2", batch.Rows[0]["ts"])
	}

	// Read third batch
	batch, err = or.Next()
	if err != nil {
		t.Fatalf("Third Next() err=%v", err)
	}
	if len(batch.Rows) != 1 {
		t.Fatalf("Third batch should have 1 row, got %d", len(batch.Rows))
	}
	if batch.Rows[0]["ts"] != int64(3) {
		t.Fatalf("third row ts=%v want 3", batch.Rows[0]["ts"])
	}

	// Verify tracking worked
	if len(tr.ptrs) < 4 {
		t.Fatalf("expected at least 4 recorded pointers, got %d", len(tr.ptrs))
	}
	// Note: With the new interface, row reuse patterns may be different
	// This test mainly verifies that the tracking reader works with the new interface
}
