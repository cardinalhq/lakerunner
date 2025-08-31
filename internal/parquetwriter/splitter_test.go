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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// mockStatsProvider implements StatsProvider for testing
type mockStatsProvider struct {
	accumulatorFunc func() StatsAccumulator
}

func (p *mockStatsProvider) NewAccumulator() StatsAccumulator {
	if p.accumulatorFunc != nil {
		return p.accumulatorFunc()
	}
	return &mockStatsAccumulator{}
}

// mockStatsAccumulator implements StatsAccumulator for testing
type mockStatsAccumulator struct {
	rows []map[string]any
}

func (a *mockStatsAccumulator) Add(row map[string]any) {
	a.rows = append(a.rows, row)
}

func (a *mockStatsAccumulator) Finalize() any {
	return map[string]any{
		"row_count": len(a.rows),
	}
}

func TestNewFileSplitter(t *testing.T) {
	config := WriterConfig{
		TmpDir:         "/tmp",
		RecordsPerFile: 100,
	}

	splitter := NewFileSplitter(config)

	if splitter == nil {
		t.Fatal("NewFileSplitter returned nil")
	}
	if splitter.closed {
		t.Error("Expected splitter to not be closed initially")
	}
	if len(splitter.results) != 0 {
		t.Error("Expected results to be empty initially")
	}
}

func TestFileSplitterWriteBatchRows_NilBatch(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		RecordsPerFile: 100,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	err := splitter.WriteBatchRows(ctx, nil)
	if err == nil {
		t.Error("Expected error for nil batch")
	}
	if err.Error() != "batch cannot be nil" {
		t.Errorf("Expected 'batch cannot be nil' error, got %q", err.Error())
	}
}

func TestFileSplitterWriteBatchRows_ClosedWriter(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		RecordsPerFile: 100,
	}

	splitter := NewFileSplitter(config)
	splitter.closed = true

	ctx := context.Background()
	batch := createTestBatch(t, 1)

	err := splitter.WriteBatchRows(ctx, batch)
	if err != ErrWriterClosed {
		t.Errorf("Expected ErrWriterClosed, got %v", err)
	}
}

func TestFileSplitterWriteBatchRows_EmptyBatch(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		RecordsPerFile: 100,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	batch := createEmptyBatch(t)
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Errorf("Expected no error for empty batch, got %v", err)
	}

	// Should not create any files
	if splitter.bufferFile != nil {
		t.Error("Expected no current buffer file for empty batch")
	}
}

func TestFileSplitterWriteBatchRows_SingleBatch(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		RecordsPerFile: 100,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	batch := createTestBatch(t, 3)
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Fatalf("WriteBatchRows failed: %v", err)
	}

	// Should have created a buffer file
	if splitter.bufferFile == nil {
		t.Error("Expected current buffer file to be created")
	}
	if splitter.currentRows != 3 {
		t.Errorf("Expected 3 current rows, got %d", splitter.currentRows)
	}

	// Close and check results
	results, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
	if results[0].RecordCount != 3 {
		t.Errorf("Expected 3 records in result, got %d", results[0].RecordCount)
	}

	// Verify file exists and has content
	if _, err := os.Stat(results[0].FileName); err != nil {
		t.Errorf("Result file does not exist: %v", err)
	}
	if results[0].FileSize <= 0 {
		t.Error("Expected file size to be positive")
	}

	// Clean up
	os.Remove(results[0].FileName)
}

func TestFileSplitterWriteBatchRows_WithStats(t *testing.T) {
	tmpDir := t.TempDir()
	statsProvider := &mockStatsProvider{
		accumulatorFunc: func() StatsAccumulator {
			return &mockStatsAccumulator{}
		},
	}

	config := WriterConfig{
		TmpDir:         tmpDir,
		RecordsPerFile: 100,
		StatsProvider:  statsProvider,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	batch := createTestBatch(t, 2)
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Fatalf("WriteBatchRows failed: %v", err)
	}

	results, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	metadata, ok := results[0].Metadata.(map[string]any)
	if !ok {
		t.Fatal("Expected metadata to be map[string]any")
	}
	if metadata["row_count"] != 2 {
		t.Errorf("Expected row_count to be 2, got %v", metadata["row_count"])
	}

	// Clean up
	os.Remove(results[0].FileName)
}

func TestFileSplitterWriteBatchRows_FileSplittingByRecordCount(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		RecordsPerFile: 2, // Split after 2 records
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	// First batch - should fit in first file
	batch1 := createTestBatch(t, 2)
	err := splitter.WriteBatchRows(ctx, batch1)
	if err != nil {
		t.Fatalf("WriteBatchRows batch1 failed: %v", err)
	}

	// Second batch - should trigger new file since we'd exceed RecordsPerFile
	batch2 := createTestBatch(t, 1)
	err = splitter.WriteBatchRows(ctx, batch2)
	if err != nil {
		t.Fatalf("WriteBatchRows batch2 failed: %v", err)
	}

	results, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Should have 2 files
	if len(results) != 2 {
		t.Errorf("Expected 2 result files, got %d", len(results))
	}
	if results[0].RecordCount != 2 {
		t.Errorf("Expected first file to have 2 records, got %d", results[0].RecordCount)
	}
	if results[1].RecordCount != 1 {
		t.Errorf("Expected second file to have 1 record, got %d", results[1].RecordCount)
	}

	// Clean up
	for _, result := range results {
		os.Remove(result.FileName)
	}
}

func TestFileSplitterWriteBatchRows_WithGroupKeyFunc(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		RecordsPerFile: 100,
		GroupKeyFunc: func(row map[string]any) any {
			return row["group"]
		},
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	// Create batch with different group values
	batch := createTestBatchWithGroups(t, []string{"A", "A", "B"})
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Fatalf("WriteBatchRows failed: %v", err)
	}

	// Check that currentGroup is set to the last row's group
	if splitter.currentGroup != "B" {
		t.Errorf("Expected currentGroup to be 'B', got %v", splitter.currentGroup)
	}

	results, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Clean up
	for _, result := range results {
		os.Remove(result.FileName)
	}
}

func TestFileSplitterClose_MultipleTimes(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		RecordsPerFile: 100,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	batch := createTestBatch(t, 1)
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Fatalf("WriteBatchRows failed: %v", err)
	}

	// First close
	results1, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("First Close failed: %v", err)
	}

	// Second close should return same results without error
	results2, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("Second Close failed: %v", err)
	}

	if len(results1) != len(results2) {
		t.Error("Multiple Close calls should return same results")
	}

	// Clean up
	for _, result := range results1 {
		os.Remove(result.FileName)
	}
}

func TestFileSplitterAbort(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		RecordsPerFile: 100,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	batch := createTestBatch(t, 1)
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Fatalf("WriteBatchRows failed: %v", err)
	}

	// Get buffer file name before abort
	var fileName string
	if splitter.bufferFile != nil {
		fileName = splitter.bufferFile.Name()
	}

	// Abort should clean up
	splitter.Abort()

	// Check that buffer file is cleaned up
	if splitter.bufferFile != nil {
		t.Error("Expected bufferFile to be nil after abort")
	}
	if splitter.encoder != nil {
		t.Error("Expected encoder to be nil after abort")
	}
	if !splitter.closed {
		t.Error("Expected splitter to be closed after abort")
	}

	// File should be removed
	if _, err := os.Stat(fileName); !os.IsNotExist(err) {
		t.Error("Expected temp file to be removed after abort")
	}
}

func TestFileSplitterAbort_MultipleTimes(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		RecordsPerFile: 100,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	batch := createTestBatch(t, 1)
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Fatalf("WriteBatchRows failed: %v", err)
	}

	// Multiple aborts should be safe
	splitter.Abort()
	splitter.Abort()
	splitter.Abort()

	// Should still be closed
	if !splitter.closed {
		t.Error("Expected splitter to remain closed after multiple aborts")
	}
}

func TestFileSplitterEmptyFileHandling(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		RecordsPerFile: 1,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	// Write one batch
	batch := createTestBatch(t, 1)
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Fatalf("WriteBatchRows failed: %v", err)
	}

	// Write empty batch (should not create new file)
	emptyBatch := createEmptyBatch(t)
	err = splitter.WriteBatchRows(ctx, emptyBatch)
	if err != nil {
		t.Fatalf("WriteBatchRows with empty batch failed: %v", err)
	}

	results, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Should only have one file (empty batch shouldn't create a file)
	if len(results) != 1 {
		t.Errorf("Expected 1 result file, got %d", len(results))
	}

	// Clean up
	for _, result := range results {
		os.Remove(result.FileName)
	}
}

func TestFileSplitterTempFileCreation(t *testing.T) {
	tmpDir := t.TempDir()
	config := WriterConfig{
		TmpDir:         tmpDir,
		RecordsPerFile: 100,
	}

	splitter := NewFileSplitter(config)
	ctx := context.Background()

	batch := createTestBatch(t, 1)
	err := splitter.WriteBatchRows(ctx, batch)
	if err != nil {
		t.Fatalf("WriteBatchRows failed: %v", err)
	}

	results, err := splitter.Close(ctx)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Check that result file is in the correct directory and has correct prefix
	if len(results) != 1 {
		t.Fatalf("Expected 1 result file, got %d", len(results))
	}

	fileName := results[0].FileName
	if filepath.Dir(fileName) != tmpDir {
		t.Errorf("Expected file to be in %s, got %s", tmpDir, filepath.Dir(fileName))
	}
	baseName := filepath.Base(fileName)
	if !strings.HasSuffix(baseName, ".parquet") {
		t.Errorf("Expected filename to end with '.parquet', got %s", baseName)
	}

	// Clean up
	for _, result := range results {
		os.Remove(result.FileName)
	}
}

// Helper functions

func createTestBatch(t *testing.T, numRows int) *pipeline.Batch {
	t.Helper()
	batch := pipeline.GetBatch()

	for i := range numRows {
		row := batch.AddRow()
		row[wkk.NewRowKey("field1")] = "value" + string(rune('0'+i%10))
		row[wkk.NewRowKey("field2")] = int64(i * 10)
		row[wkk.NewRowKey("timestamp")] = int64(1000000000 + i)
	}

	return batch
}

func createTestBatchWithGroups(t *testing.T, groups []string) *pipeline.Batch {
	t.Helper()
	batch := pipeline.GetBatch()

	for i, group := range groups {
		row := batch.AddRow()
		row[wkk.NewRowKey("field1")] = "value" + string(rune('0'+i%10))
		row[wkk.NewRowKey("field2")] = int64(i * 10)
		row[wkk.NewRowKey("group")] = group
	}

	return batch
}

func createEmptyBatch(t *testing.T) *pipeline.Batch {
	t.Helper()
	return pipeline.GetBatch()
}
