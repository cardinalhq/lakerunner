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

func TestNewSequentialReader(t *testing.T) {
	// Test with valid readers
	readers := []Reader{
		newMockReader("r1", []Row{{"data": "r1"}}),
		newMockReader("r2", []Row{{"data": "r2"}}),
	}

	sr, err := NewSequentialReader(readers)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}
	defer sr.Close()

	if len(sr.readers) != 2 {
		t.Errorf("Expected 2 readers, got %d", len(sr.readers))
	}

	// Test with no readers
	_, err = NewSequentialReader([]Reader{})
	if err == nil {
		t.Error("Expected error for empty readers slice")
	}

	// Test with nil reader
	readersWithNil := []Reader{
		newMockReader("r1", []Row{}),
		nil,
	}
	_, err = NewSequentialReader(readersWithNil)
	if err == nil {
		t.Error("Expected error for nil reader")
	}
}

func TestSequentialReader_Read(t *testing.T) {
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

	sr, err := NewSequentialReader(readers)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}
	defer sr.Close()

	// Expected order: all from r1, then all from r2, skip r3 (empty), then all from r4
	expectedData := []string{
		"r1-first",
		"r1-second",
		"r2-first",
		"r4-first",
	}

	allRows, err := readAllRows(sr)
	if err != nil {
		t.Fatalf("readAllRows() error = %v", err)
	}

	if len(allRows) != len(expectedData) {
		t.Fatalf("Expected %d rows, got %d", len(expectedData), len(allRows))
	}

	for i, expected := range expectedData {
		if allRows[i]["data"] != expected {
			t.Errorf("Row %d data = %v, want %v", i, allRows[i]["data"], expected)
		}
	}
}

func TestSequentialReader_Read_Batched(t *testing.T) {
	// Create readers with different data
	readers := []Reader{
		newMockReader("r1", []Row{
			{"data": "r1-first"},
			{"data": "r1-second"},
		}),
		newMockReader("r2", []Row{
			{"data": "r2-first"},
		}),
	}

	sr, err := NewSequentialReader(readers)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}
	defer sr.Close()

	// Read first batch
	rows := make([]Row, 2)
	for i := range rows {
		rows[i] = make(Row)
	}

	n, err := sr.Read(rows)
	if err != nil {
		t.Fatalf("First Read() error = %v", err)
	}
	if n != 2 {
		t.Errorf("First Read() returned %d rows, want 2", n)
	}
	if rows[0]["data"] != "r1-first" {
		t.Errorf("First row data = %v, want r1-first", rows[0]["data"])
	}
	if rows[1]["data"] != "r1-second" {
		t.Errorf("Second row data = %v, want r1-second", rows[1]["data"])
	}

	// Read second batch
	for i := range rows {
		rows[i] = make(Row)
	}
	n, err = sr.Read(rows)
	if err != nil {
		t.Fatalf("Second Read() error = %v", err)
	}
	if n != 1 {
		t.Errorf("Second Read() returned %d rows, want 1", n)
	}
	if rows[0]["data"] != "r2-first" {
		t.Errorf("Third row data = %v, want r2-first", rows[0]["data"])
	}

	// Should return EOF now
	for i := range rows {
		rows[i] = make(Row)
	}
	n, err = sr.Read(rows)
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Errorf("Final Read() should return 0 rows and io.EOF, got n=%d, err=%v", n, err)
	}
}

func TestSequentialReader_CurrentReaderIndex(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{"data": "r1"}}),
		newMockReader("r2", []Row{{"data": "r2"}}),
		newMockReader("r3", []Row{}), // Empty reader
	}

	sr, err := NewSequentialReader(readers)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}
	defer sr.Close()

	// Initially reading from reader 0
	if index := sr.CurrentReaderIndex(); index != 0 {
		t.Errorf("Initial CurrentReaderIndex() = %d, want 0", index)
	}

	// Read from first reader
	rows := make([]Row, 1)
	rows[0] = make(Row)
	_, err = sr.Read(rows)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	// Still on reader 0 because it hasn't hit EOF yet
	if index := sr.CurrentReaderIndex(); index != 0 {
		t.Errorf("After first read CurrentReaderIndex() = %d, want 0", index)
	}

	// Read from second reader (this will exhaust r1, advance to r2, skip empty r3)
	rows[0] = make(Row)
	_, err = sr.Read(rows)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	// Should be on reader 1 now
	if index := sr.CurrentReaderIndex(); index != 1 {
		t.Errorf("After second read CurrentReaderIndex() = %d, want 1", index)
	}

	// Try to read again - should return io.EOF and be exhausted
	rows[0] = make(Row)
	_, err = sr.Read(rows)
	if !errors.Is(err, io.EOF) {
		t.Errorf("Final Read() should return io.EOF, got %v", err)
	}

	// Should be exhausted now
	if index := sr.CurrentReaderIndex(); index != -1 {
		t.Errorf("After all reads CurrentReaderIndex() = %d, want -1", index)
	}
}

func TestSequentialReader_TotalReaderCount(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{}),
		newMockReader("r2", []Row{}),
		newMockReader("r3", []Row{}),
	}

	sr, err := NewSequentialReader(readers)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}
	defer sr.Close()

	if count := sr.TotalReaderCount(); count != 3 {
		t.Errorf("TotalReaderCount() = %d, want 3", count)
	}
}

func TestSequentialReader_RemainingReaderCount(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{"data": "r1"}}),
		newMockReader("r2", []Row{{"data": "r2"}}),
		newMockReader("r3", []Row{{"data": "r3"}}),
	}

	sr, err := NewSequentialReader(readers)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}
	defer sr.Close()

	// Initially all 3 readers remaining
	if count := sr.RemainingReaderCount(); count != 3 {
		t.Errorf("Initial RemainingReaderCount() = %d, want 3", count)
	}

	// Read one row (from r1)
	rows := make([]Row, 1)
	rows[0] = make(Row)
	_, err = sr.Read(rows)
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}

	// Should still have 3 readers remaining since r1 hasn't been exhausted yet
	if count := sr.RemainingReaderCount(); count != 3 {
		t.Errorf("After one read RemainingReaderCount() = %d, want 3", count)
	}

	// Read remaining rows
	for {
		rows[0] = make(Row)
		_, err := sr.Read(rows)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Read() error = %v", err)
		}
	}

	// Should have 0 readers remaining
	if count := sr.RemainingReaderCount(); count != 0 {
		t.Errorf("After all reads RemainingReaderCount() = %d, want 0", count)
	}
}

func TestSequentialReader_Close(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{"data": "r1"}}),
		newMockReader("r2", []Row{{"data": "r2"}}),
	}

	sr, err := NewSequentialReader(readers)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}

	// Close the reader
	err = sr.Close()
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}

	// Verify underlying readers are closed
	for i, reader := range readers {
		mockReader := reader.(*mockReader)
		if !mockReader.closed {
			t.Errorf("Reader %d not closed after SequentialReader.Close()", i)
		}
	}

	// Verify subsequent operations fail
	rows := make([]Row, 1)
	rows[0] = make(Row)
	_, err = sr.Read(rows)
	if err == nil {
		t.Error("Read() after Close() should return error")
	}

	// Multiple Close() calls should not error
	err = sr.Close()
	if err != nil {
		t.Errorf("Second Close() error = %v", err)
	}

	// Verify counts after close
	if count := sr.CurrentReaderIndex(); count != -1 {
		t.Errorf("CurrentReaderIndex() after close = %d, want -1", count)
	}
	if count := sr.RemainingReaderCount(); count != 0 {
		t.Errorf("RemainingReaderCount() after close = %d, want 0", count)
	}
}

func TestSequentialReader_AllEmptyReaders(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{}),
		newMockReader("r2", []Row{}),
		newMockReader("r3", []Row{}),
	}

	sr, err := NewSequentialReader(readers)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}
	defer sr.Close()

	// Should immediately return io.EOF
	rows := make([]Row, 1)
	rows[0] = make(Row)
	n, err := sr.Read(rows)
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Errorf("Read() with all empty readers should return 0 rows and io.EOF, got n=%d, err=%v", n, err)
	}
}

func TestSequentialReader_WithErrors(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []Row{{"data": "r1"}}),
		&errorReader{}, // This reader always returns errors
	}

	sr, err := NewSequentialReader(readers)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}
	defer sr.Close()

	// First read should succeed (from r1)
	rows := make([]Row, 1)
	rows[0] = make(Row)
	_, err = sr.Read(rows)
	if err != nil {
		t.Fatalf("First Read() error = %v", err)
	}

	// Second read should fail (from error reader)
	rows[0] = make(Row)
	_, err = sr.Read(rows)
	if err == nil {
		t.Error("Expected error when reading from error reader")
	}
}

// Test that SequentialReader properly handles a reader that returns an error after some successful reads
func TestSequentialReader_ReaderWithDelayedError(t *testing.T) {
	// Create a reader that succeeds once then errors
	delayedErrorReader := &delayedErrorReaderImpl{
		data: []Row{{"data": "delayed"}},
	}

	readers := []Reader{
		newMockReader("r1", []Row{{"data": "r1"}}),
		delayedErrorReader,
	}

	sr, err := NewSequentialReader(readers)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}
	defer sr.Close()

	// First read from r1
	rows := make([]Row, 1)
	rows[0] = make(Row)
	n, err := sr.Read(rows)
	if err != nil {
		t.Fatalf("First Read() error = %v", err)
	}
	if n != 1 || rows[0]["data"] != "r1" {
		t.Fatalf("First Read() should return 1 row with r1 data, got n=%d, data=%v", n, rows[0]["data"])
	}

	// Second read from delayed error reader (first call succeeds)
	rows[0] = make(Row)
	n, err = sr.Read(rows)
	if err != nil {
		t.Fatalf("Second Read() error = %v", err)
	}
	if n != 1 || rows[0]["data"] != "delayed" {
		t.Fatalf("Second Read() should return 1 row with delayed data, got n=%d, data=%v", n, rows[0]["data"])
	}

	// Third read from delayed error reader (second call fails)
	rows[0] = make(Row)
	_, err = sr.Read(rows)
	if err == nil {
		t.Error("Third Read() should fail with delayed error")
	}
}

// delayedErrorReaderImpl is a helper type that succeeds once then errors
type delayedErrorReaderImpl struct {
	data     []Row
	position int
	closed   bool
	rowCount int64
}

func (d *delayedErrorReaderImpl) Read(rows []Row) (int, error) {
	if d.closed {
		return 0, errors.New("reader closed")
	}

	if d.position == 0 && len(d.data) > 0 {
		// First call - return data
		if len(rows) > 0 {
			for k, v := range d.data[0] {
				rows[0][k] = v
			}
			d.position++
			d.rowCount++
			return 1, nil
		}
		return 0, nil
	}

	// Subsequent calls - return error
	return 0, fmt.Errorf("delayed error on call %d", d.position+1)
}

func (d *delayedErrorReaderImpl) Close() error {
	d.closed = true
	return nil
}

func (d *delayedErrorReaderImpl) TotalRowsReturned() int64 {
	return d.rowCount
}
