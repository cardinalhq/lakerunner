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
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestNewMetricsWriter(t *testing.T) {
	tmpdir := t.TempDir()

	nodes := map[string]parquet.Node{
		"_cardinalhq.tid":       parquet.Int(64),
		"_cardinalhq.timestamp": parquet.Int(64),
		"value":                 parquet.Leaf(parquet.DoubleType),
	}

	writer, err := NewMetricsWriter("metrics-test", tmpdir, nodes, 200) // Very small to force splitting
	if err != nil {
		t.Fatalf("Failed to create metrics writer: %v", err)
	}
	defer writer.Abort()

	// Test TID grouping - these should not be split across files
	testData := []map[string]any{
		{"_cardinalhq.tid": int64(100), "_cardinalhq.timestamp": int64(1000), "value": float64(1.0)},
		{"_cardinalhq.tid": int64(100), "_cardinalhq.timestamp": int64(2000), "value": float64(2.0)},
		{"_cardinalhq.tid": int64(100), "_cardinalhq.timestamp": int64(3000), "value": float64(3.0)},
		{"_cardinalhq.tid": int64(100), "_cardinalhq.timestamp": int64(4000), "value": float64(4.0)}, // This should exceed target size but stay with TID 100
		{"_cardinalhq.tid": int64(200), "_cardinalhq.timestamp": int64(5000), "value": float64(5.0)}, // New TID, new file
	}

	for _, row := range testData {
		if err := ValidateMetricsRow(row); err != nil {
			t.Fatalf("Row validation failed: %v", err)
		}
		if err := writer.Write(row); err != nil {
			t.Fatalf("Failed to write row: %v", err)
		}
	}

	ctx := context.Background()
	results, err := writer.Close(ctx)
	if err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// Should have exactly 2 files (one per TID)
	if len(results) != 2 {
		t.Errorf("Expected 2 files (one per TID), got %d", len(results))
	}

	// Check stats
	for _, result := range results {
		if stats, ok := result.Metadata.(MetricsFileStats); ok {
			if stats.TIDCount != 1 {
				t.Errorf("Expected 1 TID per file, got %d", stats.TIDCount)
			}
		} else {
			t.Error("Expected MetricsFileStats metadata")
		}
		os.Remove(result.FileName)
	}
}

func TestNewLogsWriter(t *testing.T) {
	tmpdir := t.TempDir()

	nodes := map[string]parquet.Node{
		"_cardinalhq.timestamp":   parquet.Int(64),
		"_cardinalhq.fingerprint": parquet.Int(64),
		"message":                 parquet.String(),
	}

	writer, err := NewLogsWriter("logs-test", tmpdir, nodes, 10000)
	if err != nil {
		t.Fatalf("Failed to create logs writer: %v", err)
	}
	defer writer.Abort()

	// Verify it's configured for spillable ordering
	if writer.config.OrderBy != OrderSpillable {
		t.Errorf("Expected OrderBy = OrderSpillable, got %v", writer.config.OrderBy)
	}

	// Test timestamp ordering - write out of order
	testData := []map[string]any{
		{"_cardinalhq.timestamp": int64(3000), "_cardinalhq.fingerprint": int64(300), "message": "third"},
		{"_cardinalhq.timestamp": int64(1000), "_cardinalhq.fingerprint": int64(100), "message": "first"},
		{"_cardinalhq.timestamp": int64(4000), "_cardinalhq.fingerprint": int64(400), "message": "fourth"},
		{"_cardinalhq.timestamp": int64(2000), "_cardinalhq.fingerprint": int64(200), "message": "second"},
	}

	for _, row := range testData {
		if err := ValidateLogsRow(row); err != nil {
			t.Fatalf("Row validation failed: %v", err)
		}
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
		t.Fatalf("Expected 1 file, got %d", len(results))
	}

	// Verify the file is timestamp-ordered
	file, err := os.Open(results[0].FileName)
	if err != nil {
		t.Fatalf("Failed to open result file: %v", err)
	}
	defer file.Close()
	defer os.Remove(results[0].FileName)

	// Need to create a schema for the reader
	schema := parquet.NewSchema("logs-test", parquet.Group(nodes))
	reader := parquet.NewGenericReader[map[string]any](file, schema)
	defer reader.Close()

	var lastTimestamp int64
	rowCount := 0
	for {
		rows := make([]map[string]any, 1)
		// Initialize the map
		rows[0] = make(map[string]any)

		n, err := reader.Read(rows)
		if n == 0 {
			break
		}
		if err != nil && err.Error() != "EOF" {
			t.Fatalf("Failed to read from parquet: %v", err)
		}

		ts := rows[0]["_cardinalhq.timestamp"].(int64)
		if rowCount > 0 && ts < lastTimestamp {
			t.Errorf("Timestamps not in order: %d followed by %d", lastTimestamp, ts)
		}
		lastTimestamp = ts
		rowCount++
	}

	if rowCount != 4 {
		t.Errorf("Expected 4 rows, got %d", rowCount)
	}

	// Check stats
	if stats, ok := results[0].Metadata.(LogsFileStats); ok {
		if stats.FingerprintCount != 4 {
			t.Errorf("Expected 4 fingerprints, got %d", stats.FingerprintCount)
		}
		if stats.FirstTS != 1000 {
			t.Errorf("Expected first timestamp 1000, got %d", stats.FirstTS)
		}
		if stats.LastTS != 4000 {
			t.Errorf("Expected last timestamp 4000, got %d", stats.LastTS)
		}
	} else {
		t.Error("Expected LogsFileStats metadata")
	}
}

func TestNewTracesWriter(t *testing.T) {
	tmpdir := t.TempDir()

	nodes := map[string]parquet.Node{
		"_cardinalhq.trace_id":           parquet.String(),
		"_cardinalhq.span_id":            parquet.String(),
		"_cardinalhq.start_time_unix_ns": parquet.Int(64),
		"operation_name":                 parquet.String(),
	}

	slotID := int32(42)
	writer, err := NewTracesWriter("traces-test", tmpdir, nodes, 10000, slotID)
	if err != nil {
		t.Fatalf("Failed to create traces writer: %v", err)
	}
	defer writer.Abort()

	// Verify it's configured for spillable ordering
	if writer.config.OrderBy != OrderSpillable {
		t.Errorf("Expected OrderBy = OrderSpillable, got %v", writer.config.OrderBy)
	}

	testData := []map[string]any{
		{
			"_cardinalhq.trace_id":           "trace-1",
			"_cardinalhq.span_id":            "span-1",
			"_cardinalhq.start_time_unix_ns": int64(1000000),
			"operation_name":                 "operation-1",
		},
		{
			"_cardinalhq.trace_id":           "trace-1",
			"_cardinalhq.span_id":            "span-2",
			"_cardinalhq.start_time_unix_ns": int64(2000000),
			"operation_name":                 "operation-2",
		},
		{
			"_cardinalhq.trace_id":           "trace-2",
			"_cardinalhq.span_id":            "span-3",
			"_cardinalhq.start_time_unix_ns": int64(1500000),
			"operation_name":                 "operation-3",
		},
	}

	for _, row := range testData {
		if err := ValidateTracesRow(row); err != nil {
			t.Fatalf("Row validation failed: %v", err)
		}
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
		t.Fatalf("Expected 1 file, got %d", len(results))
	}

	// Check stats
	if stats, ok := results[0].Metadata.(TracesFileStats); ok {
		if stats.SlotID != slotID {
			t.Errorf("Expected slot ID %d, got %d", slotID, stats.SlotID)
		}
		if stats.SpanCount != 3 {
			t.Errorf("Expected 3 spans, got %d", stats.SpanCount)
		}
		if stats.TraceCount != 2 {
			t.Errorf("Expected 2 traces, got %d", stats.TraceCount)
		}
	} else {
		t.Error("Expected TracesFileStats metadata")
	}

	os.Remove(results[0].FileName)
}

func TestValidateMetricsRow(t *testing.T) {
	tests := []struct {
		name    string
		row     map[string]any
		wantErr bool
	}{
		{
			name:    "valid row",
			row:     map[string]any{"_cardinalhq.tid": int64(123)},
			wantErr: false,
		},
		{
			name:    "missing TID",
			row:     map[string]any{"value": float64(1.0)},
			wantErr: true,
		},
		{
			name:    "empty row",
			row:     map[string]any{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMetricsRow(tt.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateMetricsRow() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateLogsRow(t *testing.T) {
	tests := []struct {
		name    string
		row     map[string]any
		wantErr bool
	}{
		{
			name:    "valid row",
			row:     map[string]any{"_cardinalhq.timestamp": int64(123456)},
			wantErr: false,
		},
		{
			name:    "missing timestamp",
			row:     map[string]any{"message": "test"},
			wantErr: true,
		},
		{
			name:    "empty row",
			row:     map[string]any{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateLogsRow(tt.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateLogsRow() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateTracesRow(t *testing.T) {
	tests := []struct {
		name    string
		row     map[string]any
		wantErr bool
	}{
		{
			name: "valid row",
			row: map[string]any{
				"_cardinalhq.trace_id": "trace-123",
				"_cardinalhq.span_id":  "span-456",
			},
			wantErr: false,
		},
		{
			name: "missing trace_id",
			row: map[string]any{
				"_cardinalhq.span_id": "span-456",
			},
			wantErr: true,
		},
		{
			name: "missing span_id",
			row: map[string]any{
				"_cardinalhq.trace_id": "trace-123",
			},
			wantErr: true,
		},
		{
			name:    "empty row",
			row:     map[string]any{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTracesRow(tt.row)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTracesRow() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewCustomWriter(t *testing.T) {
	tmpdir := t.TempDir()

	nodes := map[string]parquet.Node{
		"id":   parquet.Int(64),
		"name": parquet.String(),
	}

	config := WriterConfig{
		BaseName:       "custom-test",
		TmpDir:         tmpdir,
		SchemaNodes:    nodes,
		TargetFileSize: 500,
		OrderBy:        OrderInMemory,
		OrderKeyFunc: func(row map[string]any) any {
			return row["id"].(int64)
		},
		SizeEstimator: NewFixedSizeEstimator(100),
	}

	writer, err := NewCustomWriter(config)
	if err != nil {
		t.Fatalf("Failed to create custom writer: %v", err)
	}
	defer writer.Abort()

	// Write data out of order to test custom ordering
	testData := []map[string]any{
		{"id": int64(3), "name": "third"},
		{"id": int64(1), "name": "first"},
		{"id": int64(2), "name": "second"},
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
		t.Fatalf("Expected 1 file, got %d", len(results))
	}

	// Verify ordering
	file, err := os.Open(results[0].FileName)
	if err != nil {
		t.Fatalf("Failed to open result file: %v", err)
	}
	defer file.Close()
	defer os.Remove(results[0].FileName)

	// Create schema for reader
	schema := parquet.NewSchema("custom-test", parquet.Group(nodes))
	reader := parquet.NewGenericReader[map[string]any](file, schema)
	defer reader.Close()

	expectedOrder := []int64{1, 2, 3}
	rowIdx := 0

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

		id := rows[0]["id"].(int64)
		if id != expectedOrder[rowIdx] {
			t.Errorf("Row %d has ID %d, expected %d", rowIdx, id, expectedOrder[rowIdx])
		}
		rowIdx++
	}
}
