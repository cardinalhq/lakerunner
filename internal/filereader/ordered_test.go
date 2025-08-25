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
	"io"
	"testing"
)

func TestNewOrderedReader(t *testing.T) {
	// Test with valid readers and selector
	readers := []Reader{
		newMockReader("r1", []Row{{"ts": int64(1)}}),
		newMockReader("r2", []Row{{"ts": int64(2)}}),
	}
	selector := TimeOrderedSelector("ts")

	or, err := NewOrderedReader(readers, selector)
	if err != nil {
		t.Fatalf("NewOrderedReader() error = %v", err)
	}
	defer or.Close()

	if len(or.states) != 2 {
		t.Errorf("Expected 2 states, got %d", len(or.states))
	}

	// Test with no readers
	_, err = NewOrderedReader([]Reader{}, selector)
	if err == nil {
		t.Error("Expected error for empty readers slice")
	}

	// Test with nil selector
	_, err = NewOrderedReader(readers, nil)
	if err == nil {
		t.Error("Expected error for nil selector")
	}
}

func TestOrderedReader_Read(t *testing.T) {
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
	or, err := NewOrderedReader(readers, selector)
	if err != nil {
		t.Fatalf("NewOrderedReader() error = %v", err)
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

func TestOrderedReader_Read_Batched(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{"ts": int64(100)}}),
		newMockReader("r2", []Row{{"ts": int64(200)}}),
		newMockReader("r3", []Row{}), // Empty reader
	}

	selector := TimeOrderedSelector("ts")
	or, err := NewOrderedReader(readers, selector)
	if err != nil {
		t.Fatalf("NewOrderedReader() error = %v", err)
	}
	defer or.Close()

	// Read in batch of 2 (should get both rows)
	rows := make([]Row, 2)
	for i := range rows {
		rows[i] = make(Row)
	}

	n, err := or.Read(rows)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if n != 2 {
		t.Errorf("Read() returned %d rows, want 2", n)
	}
	if rows[0]["ts"] != int64(100) {
		t.Errorf("First row ts = %v, want 100", rows[0]["ts"])
	}
	if rows[1]["ts"] != int64(200) {
		t.Errorf("Second row ts = %v, want 200", rows[1]["ts"])
	}

	// Next read should return EOF
	for i := range rows {
		rows[i] = make(Row)
	}
	n, err = or.Read(rows)
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Errorf("Final Read() should return 0 rows and io.EOF, got n=%d, err=%v", n, err)
	}
}

func TestOrderedReader_ActiveReaderCount(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{"ts": int64(1)}}),
		newMockReader("r2", []Row{{"ts": int64(2)}}),
	}

	selector := TimeOrderedSelector("ts")
	or, err := NewOrderedReader(readers, selector)
	if err != nil {
		t.Fatalf("NewOrderedReader() error = %v", err)
	}
	defer or.Close()

	// Initially both readers should be active
	if count := or.ActiveReaderCount(); count != 2 {
		t.Errorf("Initial ActiveReaderCount() = %d, want 2", count)
	}

	// Read one row (from r1)
	rows := make([]Row, 1)
	rows[0] = make(Row)
	_, err = or.Read(rows)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	// Should still have 1 active reader
	if count := or.ActiveReaderCount(); count != 1 {
		t.Errorf("After one read ActiveReaderCount() = %d, want 1", count)
	}

	// Read final row
	rows[0] = make(Row)
	_, err = or.Read(rows)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	// Should have 0 active readers
	if count := or.ActiveReaderCount(); count != 0 {
		t.Errorf("After all reads ActiveReaderCount() = %d, want 0", count)
	}
}

func TestOrderedReader_AllEmptyReaders(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{}),
		newMockReader("r2", []Row{}),
	}

	selector := TimeOrderedSelector("ts")
	or, err := NewOrderedReader(readers, selector)
	if err != nil {
		t.Fatalf("NewOrderedReader() error = %v", err)
	}
	defer or.Close()

	// Should immediately return io.EOF
	rows := make([]Row, 1)
	rows[0] = make(Row)
	n, err := or.Read(rows)
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Errorf("Read() with all empty readers should return 0 rows and io.EOF, got n=%d, err=%v", n, err)
	}
}

func TestOrderedReader_Close(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{"ts": int64(1)}}),
		newMockReader("r2", []Row{{"ts": int64(2)}}),
	}

	selector := TimeOrderedSelector("ts")
	or, err := NewOrderedReader(readers, selector)
	if err != nil {
		t.Fatalf("NewOrderedReader() error = %v", err)
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
	rows := make([]Row, 1)
	rows[0] = make(Row)
	_, err = or.Read(rows)
	if err == nil {
		t.Error("Read() after Close() should return error")
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
