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

package parquetwriter

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

func TestUnifiedWriter_Basic(t *testing.T) {
	tmpdir := t.TempDir()

	config := NewTestConfig("test", tmpdir)
	writer, err := NewUnifiedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer writer.Abort()

	// Write some test data
	testRows := []map[string]any{
		{"id": int64(1), "timestamp": int64(1000), "message": "first"},
		{"id": int64(2), "timestamp": int64(2000), "message": "second"},
		{"id": int64(3), "timestamp": int64(3000), "message": "third"},
	}

	for _, row := range testRows {
		if err := writer.Write(row); err != nil {
			t.Fatalf("Failed to write row: %v", err)
		}
	}

	// Close and get results
	ctx := context.Background()
	results, err := writer.Close(ctx)
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Verify results
	if len(results) != 1 {
		t.Errorf("Expected 1 result file, got %d", len(results))
	}

	result := results[0]
	if result.RecordCount != 3 {
		t.Errorf("Expected 3 records, got %d", result.RecordCount)
	}
	if result.FileSize <= 0 {
		t.Errorf("Expected positive file size, got %d", result.FileSize)
	}
	if result.FileName == "" {
		t.Error("Expected non-empty filename")
	}

	// Verify file exists and can be read
	if _, err := os.Stat(result.FileName); os.IsNotExist(err) {
		t.Error("Result file does not exist")
	}

	// Clean up
	os.Remove(result.FileName)
}

func TestUnifiedWriter_FileSplitting(t *testing.T) {
	tmpdir := t.TempDir()

	config := WriterConfig{
		BaseName:       "split-test",
		TmpDir:         tmpdir,
		TargetFileSize: 100, // Very small to force splitting
		OrderBy:        OrderNone,
		BytesPerRecord: 50.0, // Each row is ~50 bytes
	}

	writer, err := NewUnifiedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer writer.Abort()

	// Write enough data to force multiple files
	for i := range 10 {
		row := map[string]any{
			"id":      int64(i),
			"message": "This is a test message that should take some space",
		}
		if err := writer.Write(row); err != nil {
			t.Fatalf("Failed to write row %d: %v", i, err)
		}
	}

	ctx := context.Background()
	results, err := writer.Close(ctx)
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Should have multiple files due to small target size
	if len(results) < 2 {
		t.Errorf("Expected multiple result files due to splitting, got %d", len(results))
	}

	// Verify total record count
	totalRecords := int64(0)
	for _, result := range results {
		totalRecords += result.RecordCount
		if result.RecordCount <= 0 {
			t.Errorf("File has no records: %s", result.FileName)
		}
		os.Remove(result.FileName)
	}

	if totalRecords != 10 {
		t.Errorf("Expected 10 total records, got %d", totalRecords)
	}
}

func TestUnifiedWriter_NoSplitGroups(t *testing.T) {
	tmpdir := t.TempDir()

	config := WriterConfig{
		BaseName:       "group-test",
		TmpDir:         tmpdir,
		TargetFileSize: 100, // Small to encourage splitting
		OrderBy:        OrderNone,
		GroupKeyFunc: func(row map[string]any) any {
			return row["group_id"].(int64)
		},
		NoSplitGroups:  true,
		BytesPerRecord: 30.0,
	}

	writer, err := NewUnifiedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer writer.Abort()

	// Write data with groups that should not be split
	testData := []map[string]any{
		{"group_id": int64(1), "value": int64(100)},
		{"group_id": int64(1), "value": int64(200)},
		{"group_id": int64(1), "value": int64(300)},
		{"group_id": int64(1), "value": int64(400)}, // This should exceed target size but stay in same file
		{"group_id": int64(2), "value": int64(500)}, // This should start a new file
		{"group_id": int64(2), "value": int64(600)},
	}

	for _, row := range testData {
		if err := writer.Write(row); err != nil {
			t.Fatalf("Failed to write row: %v", err)
		}
	}

	ctx := context.Background()
	results, err := writer.Close(ctx)
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Should have exactly 2 files (one per group)
	if len(results) != 2 {
		t.Errorf("Expected 2 result files (one per group), got %d", len(results))
	}

	// Clean up
	for _, result := range results {
		os.Remove(result.FileName)
	}
}

func TestUnifiedWriter_OrderingInMemory(t *testing.T) {
	tmpdir := t.TempDir()

	config := WriterConfig{
		BaseName:       "order-test",
		TmpDir:         tmpdir,
		TargetFileSize: 10000,
		OrderBy:        OrderInMemory,
		OrderKeyFunc: func(row map[string]any) any {
			return row["timestamp"].(int64)
		},
		BytesPerRecord: 100.0,
	}

	writer, err := NewUnifiedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer writer.Abort()

	// Write data out of order
	testData := []map[string]any{
		{"id": int64(3), "timestamp": int64(3000), "value": "third"},
		{"id": int64(1), "timestamp": int64(1000), "value": "first"},
		{"id": int64(4), "timestamp": int64(4000), "value": "fourth"},
		{"id": int64(2), "timestamp": int64(2000), "value": "second"},
	}

	for _, row := range testData {
		if err := writer.Write(row); err != nil {
			t.Fatalf("Failed to write row: %v", err)
		}
	}

	ctx := context.Background()
	results, err := writer.Close(ctx)
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result file, got %d", len(results))
	}

	// Read back and verify order
	file, err := os.Open(results[0].FileName)
	if err != nil {
		t.Fatalf("Failed to open result file: %v", err)
	}
	defer file.Close()
	defer os.Remove(results[0].FileName)

	// For verification, create schema matching the written data
	nodes := map[string]parquet.Node{
		"id":        parquet.Int(64),
		"timestamp": parquet.Int(64),
		"value":     parquet.String(),
	}
	schema := parquet.NewSchema("order-test", parquet.Group(nodes))
	reader := parquet.NewGenericReader[map[string]any](file, schema)
	defer reader.Close()

	var records []map[string]any
	for {
		rows := make([]map[string]any, 1)
		rows[0] = make(map[string]any)
		n, err := reader.Read(rows)
		if n == 0 {
			break
		}
		if err != nil && err.Error() != "EOF" {
			t.Fatalf("Failed to read from parquet: %v", err)
		}
		records = append(records, rows[0])
	}

	// Verify records are in timestamp order
	if len(records) != 4 {
		t.Fatalf("Expected 4 records, got %d", len(records))
	}

	expectedOrder := []int64{1000, 2000, 3000, 4000}
	for i, record := range records {
		ts := record["timestamp"].(int64)
		if ts != expectedOrder[i] {
			t.Errorf("Record %d has timestamp %d, expected %d", i, ts, expectedOrder[i])
		}
	}
}

func TestUnifiedWriter_ErrorHandling(t *testing.T) {
	tmpdir := t.TempDir()

	t.Run("nil row", func(t *testing.T) {
		config := NewTestConfig("test", tmpdir)
		writer, err := NewUnifiedWriter(config)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}
		defer writer.Abort()

		err = writer.Write(nil)
		if err == nil {
			t.Error("Expected error for nil row")
		}
	})

	t.Run("write after close", func(t *testing.T) {
		config := NewTestConfig("test", tmpdir)
		writer, err := NewUnifiedWriter(config)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}

		ctx := context.Background()
		_, err = writer.Close(ctx)
		if err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		err = writer.Write(map[string]any{"id": int64(1)})
		if err != ErrWriterClosed {
			t.Errorf("Expected ErrWriterClosed, got %v", err)
		}
	})

	t.Run("unsupported data type", func(t *testing.T) {
		config := NewTestConfig("test", tmpdir)
		writer, err := NewUnifiedWriter(config)
		if err != nil {
			t.Fatalf("Failed to create writer: %v", err)
		}
		defer writer.Abort()

		// Try to write an unsupported type - this should fail
		unsupportedData := map[string]any{
			"complex_data": map[string]string{"nested": "data"}, // Unsupported nested map
		}
		err = writer.Write(unsupportedData)
		if err == nil {
			// If no immediate error, should get error during close
			ctx := context.Background()
			_, err = writer.Close(ctx)
			if err == nil {
				t.Error("Expected error for unsupported data type")
			}
		}
	})
}

func TestUnifiedWriter_WriteBatch(t *testing.T) {
	tmpdir := t.TempDir()

	config := NewTestConfig("batch-test", tmpdir)
	writer, err := NewUnifiedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer writer.Abort()

	// Prepare batch data
	batch := make([]map[string]any, 100)
	for i := range batch {
		batch[i] = map[string]any{
			"id":    int64(i),
			"value": "test message",
		}
	}

	// Write batch
	if err := writer.WriteBatch(batch); err != nil {
		t.Fatalf("Failed to write batch: %v", err)
	}

	ctx := context.Background()
	results, err := writer.Close(ctx)
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Verify total records
	totalRecords := int64(0)
	for _, result := range results {
		totalRecords += result.RecordCount
		os.Remove(result.FileName)
	}

	if totalRecords != 100 {
		t.Errorf("Expected 100 total records, got %d", totalRecords)
	}
}

func TestUnifiedWriter_ContextCancellation(t *testing.T) {
	tmpdir := t.TempDir()

	config := NewTestConfig("cancel-test", tmpdir)
	writer, err := NewUnifiedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer writer.Abort()

	// Write some data
	for i := range 10 {
		if err := writer.Write(map[string]any{"id": int64(i)}); err != nil {
			t.Fatalf("Failed to write row: %v", err)
		}
	}

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Close should handle cancellation gracefully
	_, err = writer.Close(ctx)
	if err == nil {
		t.Error("Expected context cancellation error")
	}
	// Check if the error is related to context cancellation
	if err != context.Canceled && !errors.Is(err, context.Canceled) &&
		!strings.Contains(err.Error(), "context canceled") {
		t.Errorf("Expected context cancellation error, got %v", err)
	}
}

func TestUnifiedWriter_Stats(t *testing.T) {
	tmpdir := t.TempDir()

	config := NewTestConfig("stats-test", tmpdir)
	writer, err := NewUnifiedWriter(config)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	defer writer.Abort()

	// Check initial stats
	stats := writer.GetCurrentStats()
	if stats.Closed {
		t.Error("Writer should not be closed initially")
	}

	// Write some data
	if err := writer.Write(map[string]any{"id": int64(1)}); err != nil {
		t.Fatalf("Failed to write row: %v", err)
	}

	// Close writer
	ctx := context.Background()
	results, err := writer.Close(ctx)
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Check final stats
	stats = writer.GetCurrentStats()
	if !stats.Closed {
		t.Error("Writer should be closed after Close()")
	}

	// Clean up
	for _, result := range results {
		os.Remove(result.FileName)
	}
}

func BenchmarkUnifiedWriter(b *testing.B) {
	tmpdir := b.TempDir()

	config := WriterConfig{
		BaseName:       "bench",
		TmpDir:         tmpdir,
		TargetFileSize: 1000000,
		OrderBy:        OrderNone,
		BytesPerRecord: 100.0,
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		writer, err := NewUnifiedWriter(config)
		if err != nil {
			b.Fatalf("Failed to create writer: %v", err)
		}

		// Write 1000 rows
		for j := range 1000 {
			row := map[string]any{
				"id":        int64(j),
				"timestamp": int64(time.Now().UnixNano()),
				"message":   "benchmark test message",
			}
			if err := writer.Write(row); err != nil {
				b.Fatalf("Failed to write row: %v", err)
			}
		}

		ctx := context.Background()
		results, err := writer.Close(ctx)
		if err != nil {
			b.Fatalf("Failed to close writer: %v", err)
		}

		// Clean up
		for _, result := range results {
			os.Remove(result.FileName)
		}
	}
}
