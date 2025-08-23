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

// mockReader is a test implementation of Reader
type mockReader struct {
	rows   []Row
	index  int
	closed bool
	name   string
}

func newMockReader(name string, rows []Row) *mockReader {
	return &mockReader{
		rows: rows,
		name: name,
	}
}

func (m *mockReader) GetRow() (Row, error) {
	if m.closed {
		return nil, fmt.Errorf("reader %s is closed", m.name)
	}
	if m.index >= len(m.rows) {
		return nil, io.EOF
	}
	row := m.rows[m.index]
	m.index++
	return row, nil
}

func (m *mockReader) Close() error {
	m.closed = true
	return nil
}

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

func TestOrderedReader_GetRow(t *testing.T) {
	// Create readers with timestamp-ordered data
	readers := []Reader{
		newMockReader("r1", []Row{
			{"ts": int64(100), "data": "r1-first"},
			{"ts": int64(300), "data": "r1-second"},
			{"ts": int64(500), "data": "r1-third"},
		}),
		newMockReader("r2", []Row{
			{"ts": int64(200), "data": "r2-first"},
			{"ts": int64(400), "data": "r2-second"},
		}),
		newMockReader("r3", []Row{
			{"ts": int64(150), "data": "r3-first"},
		}),
	}

	selector := TimeOrderedSelector("ts")
	or, err := NewOrderedReader(readers, selector)
	if err != nil {
		t.Fatalf("NewOrderedReader() error = %v", err)
	}
	defer or.Close()

	// Expected order: 100, 150, 200, 300, 400, 500
	expectedData := []string{
		"r1-first",  // ts=100
		"r3-first",  // ts=150
		"r2-first",  // ts=200
		"r1-second", // ts=300
		"r2-second", // ts=400
		"r1-third",  // ts=500
	}

	for i, expected := range expectedData {
		row, err := or.GetRow()
		if err != nil {
			t.Fatalf("GetRow() at index %d error = %v", i, err)
		}
		if row["data"] != expected {
			t.Errorf("GetRow() at index %d data = %v, want %v", i, row["data"], expected)
		}
	}

	// Should return EOF now
	_, err = or.GetRow()
	if !errors.Is(err, io.EOF) {
		t.Errorf("Final GetRow() should return io.EOF, got %v", err)
	}
}

func TestOrderedReader_ActiveReaderCount(t *testing.T) {
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

	// Initially, should have 2 active readers (r3 is empty)
	if count := or.ActiveReaderCount(); count != 2 {
		t.Errorf("Initial ActiveReaderCount() = %d, want 2", count)
	}

	// Read one row
	_, err = or.GetRow()
	if err != nil {
		t.Fatalf("GetRow() error = %v", err)
	}

	// Should still have 1 active reader
	if count := or.ActiveReaderCount(); count != 1 {
		t.Errorf("After one read ActiveReaderCount() = %d, want 1", count)
	}

	// Read final row
	_, err = or.GetRow()
	if err != nil {
		t.Fatalf("GetRow() error = %v", err)
	}

	// Should have 0 active readers
	if count := or.ActiveReaderCount(); count != 0 {
		t.Errorf("After all reads ActiveReaderCount() = %d, want 0", count)
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
	_, err = or.GetRow()
	if err == nil {
		t.Error("GetRow() after Close() should return error")
	}

	// Multiple Close() calls should not error
	err = or.Close()
	if err != nil {
		t.Errorf("Second Close() error = %v", err)
	}
}

func TestOrderedReader_EmptyReaders(t *testing.T) {
	// All readers are empty
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
	_, err = or.GetRow()
	if !errors.Is(err, io.EOF) {
		t.Errorf("GetRow() with empty readers should return io.EOF, got %v", err)
	}
}

// errorReader always returns an error
type errorReader struct {
	closed bool
}

func (e *errorReader) GetRow() (Row, error) {
	return nil, fmt.Errorf("test error")
}

func (e *errorReader) Close() error {
	e.closed = true
	return nil
}

func TestOrderedReader_WithErrors(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{"ts": int64(1)}}),
		&errorReader{},
	}

	selector := TimeOrderedSelector("ts")

	// Should fail during creation when priming readers
	or, err := NewOrderedReader(readers, selector)
	if err == nil {
		if or != nil {
			or.Close()
		}
		t.Error("Expected error when creating OrderedReader with error reader")
	}
}

func TestOrderedReader_SelectorError(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{"ts": int64(1)}}),
		newMockReader("r2", []Row{{"ts": int64(2)}}),
	}

	// Selector that returns invalid index
	selector := func(rows []Row) int {
		return 99 // Invalid index
	}

	or, err := NewOrderedReader(readers, selector)
	if err != nil {
		t.Fatalf("NewOrderedReader() error = %v", err)
	}
	defer or.Close()

	_, err = or.GetRow()
	if err == nil {
		t.Error("Expected error when selector returns invalid index")
	}
}
