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

func TestNewMultiReader(t *testing.T) {
	// Test with valid readers
	readers := []Reader{
		newMockReader("r1", []Row{{"data": "r1"}}),
		newMockReader("r2", []Row{{"data": "r2"}}),
	}

	mr, err := NewMultiReader(readers)
	if err != nil {
		t.Fatalf("NewMultiReader() error = %v", err)
	}
	defer mr.Close()

	if len(mr.readers) != 2 {
		t.Errorf("Expected 2 readers, got %d", len(mr.readers))
	}

	// Test with no readers
	_, err = NewMultiReader([]Reader{})
	if err == nil {
		t.Error("Expected error for empty readers slice")
	}

	// Test with nil reader
	readersWithNil := []Reader{
		newMockReader("r1", []Row{}),
		nil,
	}
	_, err = NewMultiReader(readersWithNil)
	if err == nil {
		t.Error("Expected error for nil reader")
	}
}

func TestMultiReader_GetRow(t *testing.T) {
	// Create readers with different data
	readers := []Reader{
		newMockReader("r1", []Row{
			{"data": "r1-first"},
			{"data": "r1-second"},
		}),
		newMockReader("r2", []Row{
			{"data": "r2-first"},
		}),
		newMockReader("r3", []Row{}), // Empty reader
		newMockReader("r4", []Row{
			{"data": "r4-first"},
		}),
	}

	mr, err := NewMultiReader(readers)
	if err != nil {
		t.Fatalf("NewMultiReader() error = %v", err)
	}
	defer mr.Close()

	// Expected order: all from r1, then all from r2, skip r3 (empty), then all from r4
	expectedData := []string{
		"r1-first",
		"r1-second",
		"r2-first",
		"r4-first",
	}

	for i, expected := range expectedData {
		row, err := mr.GetRow()
		if err != nil {
			t.Fatalf("GetRow() at index %d error = %v", i, err)
		}
		if row["data"] != expected {
			t.Errorf("GetRow() at index %d data = %v, want %v", i, row["data"], expected)
		}
	}

	// Should return EOF now
	_, err = mr.GetRow()
	if !errors.Is(err, io.EOF) {
		t.Errorf("Final GetRow() should return io.EOF, got %v", err)
	}
}

func TestMultiReader_CurrentReaderIndex(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{"data": "r1"}}),
		newMockReader("r2", []Row{{"data": "r2"}}),
		newMockReader("r3", []Row{}), // Empty reader
	}

	mr, err := NewMultiReader(readers)
	if err != nil {
		t.Fatalf("NewMultiReader() error = %v", err)
	}
	defer mr.Close()

	// Initially reading from reader 0
	if index := mr.CurrentReaderIndex(); index != 0 {
		t.Errorf("Initial CurrentReaderIndex() = %d, want 0", index)
	}

	// Read from first reader
	_, err = mr.GetRow()
	if err != nil {
		t.Fatalf("GetRow() error = %v", err)
	}

	// Still on reader 0 because it hasn't hit EOF yet
	if index := mr.CurrentReaderIndex(); index != 0 {
		t.Errorf("After first read CurrentReaderIndex() = %d, want 0", index)
	}

	// Read from second reader (this will exhaust r1, advance to r2, skip empty r3)
	_, err = mr.GetRow()
	if err != nil {
		t.Fatalf("GetRow() error = %v", err)
	}

	// Should be on reader 1 now
	if index := mr.CurrentReaderIndex(); index != 1 {
		t.Errorf("After second read CurrentReaderIndex() = %d, want 1", index)
	}

	// Try to read again - should return io.EOF and be exhausted
	_, err = mr.GetRow()
	if !errors.Is(err, io.EOF) {
		t.Errorf("Final GetRow() should return io.EOF, got %v", err)
	}

	// Should be exhausted now
	if index := mr.CurrentReaderIndex(); index != -1 {
		t.Errorf("After all reads CurrentReaderIndex() = %d, want -1", index)
	}
}

func TestMultiReader_TotalReaderCount(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{}),
		newMockReader("r2", []Row{}),
		newMockReader("r3", []Row{}),
	}

	mr, err := NewMultiReader(readers)
	if err != nil {
		t.Fatalf("NewMultiReader() error = %v", err)
	}
	defer mr.Close()

	if count := mr.TotalReaderCount(); count != 3 {
		t.Errorf("TotalReaderCount() = %d, want 3", count)
	}
}

func TestMultiReader_RemainingReaderCount(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{"data": "r1"}}),
		newMockReader("r2", []Row{{"data": "r2"}}),
		newMockReader("r3", []Row{{"data": "r3"}}),
	}

	mr, err := NewMultiReader(readers)
	if err != nil {
		t.Fatalf("NewMultiReader() error = %v", err)
	}
	defer mr.Close()

	// Initially all 3 readers remaining
	if count := mr.RemainingReaderCount(); count != 3 {
		t.Errorf("Initial RemainingReaderCount() = %d, want 3", count)
	}

	// Read one row (from r1)
	_, err = mr.GetRow()
	if err != nil {
		t.Fatalf("GetRow() error = %v", err)
	}

	// Should still have 3 readers remaining since r1 hasn't been exhausted yet
	if count := mr.RemainingReaderCount(); count != 3 {
		t.Errorf("After one read RemainingReaderCount() = %d, want 3", count)
	}

	// Read remaining rows
	for {
		_, err := mr.GetRow()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("GetRow() error = %v", err)
		}
	}

	// Should have 0 readers remaining
	if count := mr.RemainingReaderCount(); count != 0 {
		t.Errorf("After all reads RemainingReaderCount() = %d, want 0", count)
	}
}

func TestMultiReader_Close(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{"data": "r1"}}),
		newMockReader("r2", []Row{{"data": "r2"}}),
	}

	mr, err := NewMultiReader(readers)
	if err != nil {
		t.Fatalf("NewMultiReader() error = %v", err)
	}

	// Close the reader
	err = mr.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Verify underlying readers are closed
	for i, reader := range readers {
		mockReader := reader.(*mockReader)
		if !mockReader.closed {
			t.Errorf("Reader %d not closed after MultiReader.Close()", i)
		}
	}

	// Verify subsequent operations fail
	_, err = mr.GetRow()
	if err == nil {
		t.Error("GetRow() after Close() should return error")
	}

	// Multiple Close() calls should not error
	err = mr.Close()
	if err != nil {
		t.Errorf("Second Close() error = %v", err)
	}

	// Verify counts after close
	if count := mr.CurrentReaderIndex(); count != -1 {
		t.Errorf("CurrentReaderIndex() after close = %d, want -1", count)
	}
	if count := mr.RemainingReaderCount(); count != 0 {
		t.Errorf("RemainingReaderCount() after close = %d, want 0", count)
	}
}

func TestMultiReader_AllEmptyReaders(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{}),
		newMockReader("r2", []Row{}),
		newMockReader("r3", []Row{}),
	}

	mr, err := NewMultiReader(readers)
	if err != nil {
		t.Fatalf("NewMultiReader() error = %v", err)
	}
	defer mr.Close()

	// Should immediately return io.EOF
	_, err = mr.GetRow()
	if !errors.Is(err, io.EOF) {
		t.Errorf("GetRow() with all empty readers should return io.EOF, got %v", err)
	}
}

func TestMultiReader_WithErrors(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{"data": "r1"}}),
		&errorReader{}, // This reader always returns errors
	}

	mr, err := NewMultiReader(readers)
	if err != nil {
		t.Fatalf("NewMultiReader() error = %v", err)
	}
	defer mr.Close()

	// First read should succeed (from r1)
	_, err = mr.GetRow()
	if err != nil {
		t.Fatalf("First GetRow() error = %v", err)
	}

	// Second read should fail (from error reader)
	_, err = mr.GetRow()
	if err == nil {
		t.Error("Expected error when reading from error reader")
	}
}

// Test that MultiReader properly handles a reader that returns an error after some successful reads
func TestMultiReader_ReaderWithDelayedError(t *testing.T) {
	// Create a reader that succeeds once then errors
	delayedErrorReader := &struct {
		calls  int
		closed bool
	}{}

	readers := []Reader{
		newMockReader("r1", []Row{{"data": "r1"}}),
		// Anonymous struct implementing Reader with delayed error
		ReaderFunc(func() (Row, error) {
			delayedErrorReader.calls++
			if delayedErrorReader.calls == 1 {
				return Row{"data": "delayed"}, nil
			}
			return nil, fmt.Errorf("delayed error on call %d", delayedErrorReader.calls)
		}),
	}

	mr, err := NewMultiReader(readers)
	if err != nil {
		t.Fatalf("NewMultiReader() error = %v", err)
	}
	defer mr.Close()

	// First read from r1
	row, err := mr.GetRow()
	if err != nil {
		t.Fatalf("First GetRow() error = %v", err)
	}
	if row["data"] != "r1" {
		t.Fatal("First GetRow() should succeed with r1 data")
	}

	// Second read from delayed error reader (first call succeeds)
	row, err = mr.GetRow()
	if err != nil {
		t.Fatalf("Second GetRow() error = %v", err)
	}
	if row["data"] != "delayed" {
		t.Fatal("Second GetRow() should succeed with delayed data")
	}

	// Third read from delayed error reader (second call fails)
	_, err = mr.GetRow()
	if err == nil {
		t.Error("Third GetRow() should fail with delayed error")
	}
}

// ReaderFunc is a helper type to create Reader from a function
type ReaderFunc func() (Row, error)

func (f ReaderFunc) GetRow() (Row, error) {
	return f()
}

func (f ReaderFunc) Close() error {
	return nil
}
