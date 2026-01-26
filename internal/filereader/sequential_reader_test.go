// Copyright (C) 2025-2026 CardinalHQ, Inc
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

func TestNewSequentialReader(t *testing.T) {
	// Test with valid readers
	readers := []Reader{
		newMockReader("r1", []pipeline.Row{{wkk.NewRowKey("data"): "r1"}}, nil),
		newMockReader("r2", []pipeline.Row{{wkk.NewRowKey("data"): "r2"}}, nil),
	}

	sr, err := NewSequentialReader(readers, 1000)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}
	defer func() { _ = sr.Close() }()

	if len(sr.readers) != 2 {
		t.Errorf("Expected 2 readers, got %d", len(sr.readers))
	}

	// Test with no readers
	_, err = NewSequentialReader([]Reader{}, 1000)
	if err == nil {
		t.Error("Expected error for empty readers slice")
	}

	// Test with nil reader
	readersWithNil := []Reader{
		newMockReader("r1", []pipeline.Row{}, nil),
		nil,
	}
	_, err = NewSequentialReader(readersWithNil, 1000)
	if err == nil {
		t.Error("Expected error for nil reader")
	}
}

func TestSequentialReader_Next(t *testing.T) {
	// Create readers with different data
	readers := []Reader{
		newMockReader("r1", []pipeline.Row{
			{wkk.NewRowKey("data"): "r1-first"},
			{wkk.NewRowKey("data"): "r1-second"},
		}, nil),

		newMockReader("r2", []pipeline.Row{
			{wkk.NewRowKey("data"): "r2-first"},
		}, nil),

		newMockReader("r3", []pipeline.Row{}, nil), // Empty reader
		newMockReader("r4", []pipeline.Row{
			{wkk.NewRowKey("data"): "r4-first"},
		}, nil),
	}

	sr, err := NewSequentialReader(readers, 1000)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}
	defer func() { _ = sr.Close() }()

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
		if allRows[i][wkk.NewRowKey("data")] != expected {
			t.Errorf("Row %d data = %v, want %v", i, allRows[i][wkk.NewRowKey("data")], expected)
		}
	}
}

func TestSequentialReader_NextBatched(t *testing.T) {
	// Create readers with different data
	readers := []Reader{
		newMockReader("r1", []pipeline.Row{
			{wkk.NewRowKey("data"): "r1-first"},
			{wkk.NewRowKey("data"): "r1-second"},
		}, nil),

		newMockReader("r2", []pipeline.Row{
			{wkk.NewRowKey("data"): "r2-first"},
		}, nil),
	}

	sr, err := NewSequentialReader(readers, 1000)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}
	defer func() { _ = sr.Close() }()

	// Read first batch
	batch, err := sr.Next(context.TODO())
	if err != nil {
		t.Fatalf("First Next() error = %v", err)
	}
	if batch == nil || batch.Len() != 2 {
		t.Errorf("First Next() returned %d rows, want 2", batch.Len())
	}
	if batch.Get(0)[wkk.NewRowKey("data")] != "r1-first" {
		t.Errorf("First row data = %v, want r1-first", batch.Get(0)[wkk.NewRowKey("data")])
	}
	if batch.Get(1)[wkk.NewRowKey("data")] != "r1-second" {
		t.Errorf("Second row data = %v, want r1-second", batch.Get(1)[wkk.NewRowKey("data")])
	}

	// Read second batch
	batch, err = sr.Next(context.TODO())
	if err != nil {
		t.Fatalf("Second Next() error = %v", err)
	}
	if batch == nil || batch.Len() != 1 {
		t.Errorf("Second Next() returned %d rows, want 1", batch.Len())
	}
	if batch.Get(0)[wkk.NewRowKey("data")] != "r2-first" {
		t.Errorf("Third row data = %v, want r2-first", batch.Get(0)[wkk.NewRowKey("data")])
	}

	// Should return EOF now
	batch, err = sr.Next(context.TODO())
	if batch != nil || !errors.Is(err, io.EOF) {
		t.Errorf("Final Next() should return nil batch and io.EOF, got batch=%v, err=%v", batch, err)
	}
}

func TestSequentialReader_CurrentReaderIndex(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []pipeline.Row{{wkk.NewRowKey("data"): "r1"}}, nil),
		newMockReader("r2", []pipeline.Row{{wkk.NewRowKey("data"): "r2"}}, nil),
		newMockReader("r3", []pipeline.Row{}, nil), // Empty reader
	}

	sr, err := NewSequentialReader(readers, 1000)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}
	defer func() { _ = sr.Close() }()

	// Initially reading from reader 0
	if index := sr.CurrentReaderIndex(); index != 0 {
		t.Errorf("Initial CurrentReaderIndex() = %d, want 0", index)
	}

	// Read from first reader
	_, err = sr.Next(context.TODO())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}

	// Still on reader 0 because it hasn't hit EOF yet
	if index := sr.CurrentReaderIndex(); index != 0 {
		t.Errorf("After first read CurrentReaderIndex() = %d, want 0", index)
	}

	// Read from second reader (this will exhaust r1, advance to r2, skip empty r3)
	_, err = sr.Next(context.TODO())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}

	// Should be on reader 1 now
	if index := sr.CurrentReaderIndex(); index != 1 {
		t.Errorf("After second read CurrentReaderIndex() = %d, want 1", index)
	}

	// Try to read again - should return io.EOF and be exhausted
	_, err = sr.Next(context.TODO())
	if !errors.Is(err, io.EOF) {
		t.Errorf("Final Next() should return io.EOF, got %v", err)
	}

	// Should be exhausted now
	if index := sr.CurrentReaderIndex(); index != -1 {
		t.Errorf("After all reads CurrentReaderIndex() = %d, want -1", index)
	}
}

func TestSequentialReader_TotalReaderCount(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []pipeline.Row{}, nil),
		newMockReader("r2", []pipeline.Row{}, nil),
		newMockReader("r3", []pipeline.Row{}, nil),
	}

	sr, err := NewSequentialReader(readers, 1000)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}
	defer func() { _ = sr.Close() }()

	if count := sr.TotalReaderCount(); count != 3 {
		t.Errorf("TotalReaderCount() = %d, want 3", count)
	}
}

func TestSequentialReader_RemainingReaderCount(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []pipeline.Row{{wkk.NewRowKey("data"): "r1"}}, nil),
		newMockReader("r2", []pipeline.Row{{wkk.NewRowKey("data"): "r2"}}, nil),
		newMockReader("r3", []pipeline.Row{{wkk.NewRowKey("data"): "r3"}}, nil),
	}

	sr, err := NewSequentialReader(readers, 1000)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}
	defer func() { _ = sr.Close() }()

	// Initially all 3 readers remaining
	if count := sr.RemainingReaderCount(); count != 3 {
		t.Errorf("Initial RemainingReaderCount() = %d, want 3", count)
	}

	// Read one row (from r1)
	_, err = sr.Next(context.TODO())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}

	// Should still have 3 readers remaining since r1 hasn't been exhausted yet
	if count := sr.RemainingReaderCount(); count != 3 {
		t.Errorf("After one read RemainingReaderCount() = %d, want 3", count)
	}

	// Read remaining rows
	for {
		_, err := sr.Next(context.TODO())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
	}

	// Should have 0 readers remaining
	if count := sr.RemainingReaderCount(); count != 0 {
		t.Errorf("After all reads RemainingReaderCount() = %d, want 0", count)
	}
}

func TestSequentialReader_Close(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []pipeline.Row{{wkk.NewRowKey("data"): "r1"}}, nil),
		newMockReader("r2", []pipeline.Row{{wkk.NewRowKey("data"): "r2"}}, nil),
	}

	sr, err := NewSequentialReader(readers, 1000)
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
	_, err = sr.Next(context.TODO())
	if err == nil {
		t.Error("Next() after Close() should return error")
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
		newMockReader("r1", []pipeline.Row{}, nil),
		newMockReader("r2", []pipeline.Row{}, nil),
		newMockReader("r3", []pipeline.Row{}, nil),
	}

	sr, err := NewSequentialReader(readers, 1000)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}
	defer func() { _ = sr.Close() }()

	// Should immediately return io.EOF
	batch, err := sr.Next(context.TODO())
	if batch != nil || !errors.Is(err, io.EOF) {
		t.Errorf("Next() with all empty readers should return nil batch and io.EOF, got batch=%v, err=%v", batch, err)
	}
}

func TestSequentialReader_WithErrors(t *testing.T) {
	readers := []Reader{
		newMockReader("r1", []pipeline.Row{{wkk.NewRowKey("data"): "r1"}}, nil),
		&errorReader{}, // This reader always returns errors
	}

	sr, err := NewSequentialReader(readers, 1000)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}
	defer func() { _ = sr.Close() }()

	// First read should succeed (from r1)
	_, err = sr.Next(context.TODO())
	if err != nil {
		t.Fatalf("First Next() error = %v", err)
	}

	// Second read should fail (from error reader)
	_, err = sr.Next(context.TODO())
	if err == nil {
		t.Error("Expected error when reading from error reader")
	}
}

// Test that SequentialReader properly handles a reader that returns an error after some successful reads
func TestSequentialReader_ReaderWithDelayedError(t *testing.T) {
	// Create a reader that succeeds once then errors
	delayedErrorReader := &delayedErrorReaderImpl{
		data: []pipeline.Row{{wkk.NewRowKey("data"): "delayed"}},
	}

	readers := []Reader{
		newMockReader("r1", []pipeline.Row{{wkk.NewRowKey("data"): "r1"}}, nil),
		delayedErrorReader,
	}

	sr, err := NewSequentialReader(readers, 1000)
	if err != nil {
		t.Fatalf("NewSequentialReader() error = %v", err)
	}
	defer func() { _ = sr.Close() }()

	// First read from r1
	batch, err := sr.Next(context.TODO())
	if err != nil {
		t.Fatalf("First Next() error = %v", err)
	}
	if batch == nil || batch.Len() != 1 || batch.Get(0)[wkk.NewRowKey("data")] != "r1" {
		t.Fatalf("First Next() should return 1 row with r1 data, got len=%d, data=%v", batch.Len(), batch.Get(0)[wkk.NewRowKey("data")])
	}

	// Second read from delayed error reader (first call succeeds)
	batch, err = sr.Next(context.TODO())
	if err != nil {
		t.Fatalf("Second Next() error = %v", err)
	}
	if batch == nil || batch.Len() != 1 || batch.Get(0)[wkk.NewRowKey("data")] != "delayed" {
		t.Fatalf("Second Next() should return 1 row with delayed data, got len=%d, data=%v", batch.Len(), batch.Get(0)[wkk.NewRowKey("data")])
	}

	// Third read from delayed error reader (second call fails)
	_, err = sr.Next(context.TODO())
	if err == nil {
		t.Error("Third Next() should fail with delayed error")
	}
}

// delayedErrorReaderImpl is a helper type that succeeds once then errors
type delayedErrorReaderImpl struct {
	data     []pipeline.Row
	position int
	closed   bool
	rowCount int64
}

func (d *delayedErrorReaderImpl) Next(ctx context.Context) (*Batch, error) {
	if d.closed {
		return nil, errors.New("reader closed")
	}

	if d.position == 0 && len(d.data) > 0 {
		// First call - return data
		batch := pipeline.GetBatch()
		row := batch.AddRow()
		for k, v := range d.data[0] {
			row[k] = v
		}
		d.position++
		d.rowCount++
		return batch, nil
	}

	// Subsequent calls - return error
	return nil, fmt.Errorf("delayed error on call %d", d.position+1)
}

func (d *delayedErrorReaderImpl) Close() error {
	d.closed = true
	return nil
}

func (d *delayedErrorReaderImpl) TotalRowsReturned() int64 {
	return d.rowCount
}

func (d *delayedErrorReaderImpl) GetSchema() *ReaderSchema {
	return NewReaderSchema()
}
