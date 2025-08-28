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

package factories

import (
	"context"
	"os"
	"testing"
)

func TestNewTracesWriter(t *testing.T) {
	tmpdir := t.TempDir()

	slotID := int32(42)
	writer, err := NewTracesWriter("traces-test", tmpdir, 10000, slotID, 50)
	if err != nil {
		t.Fatalf("Failed to create traces writer: %v", err)
	}
	defer writer.Abort()

	// Verify it's configured correctly (no sorting now)

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
		// Check that we have the actual span and trace ID lists
		expectedSpanIDs := []string{"span-1", "span-2", "span-3"}
		expectedTraceIDs := []string{"trace-1", "trace-2"}

		if len(stats.SpanIDs) != 3 {
			t.Errorf("Expected 3 span IDs in list, got %d", len(stats.SpanIDs))
		}
		if len(stats.TraceIDs) != 2 {
			t.Errorf("Expected 2 trace IDs in list, got %d", len(stats.TraceIDs))
		}

		// Verify all expected span IDs are present (should be sorted)
		for i, expectedSpanID := range expectedSpanIDs {
			if i >= len(stats.SpanIDs) || stats.SpanIDs[i] != expectedSpanID {
				t.Errorf("Expected span ID %s at index %d, got %v", expectedSpanID, i, stats.SpanIDs)
			}
		}

		// Verify all expected trace IDs are present (should be sorted)
		for i, expectedTraceID := range expectedTraceIDs {
			if i >= len(stats.TraceIDs) || stats.TraceIDs[i] != expectedTraceID {
				t.Errorf("Expected trace ID %s at index %d, got %v", expectedTraceID, i, stats.TraceIDs)
			}
		}
	} else {
		t.Error("Expected TracesFileStats metadata")
	}

	os.Remove(results[0].FileName)
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

func TestTracesStatsAccumulator(t *testing.T) {
	slotID := int32(42)
	provider := &TracesStatsProvider{SlotID: slotID}
	accumulator := provider.NewAccumulator().(*TracesStatsAccumulator)

	// Add some test data
	testData := []map[string]any{
		{
			"_cardinalhq.trace_id":           "trace-1",
			"_cardinalhq.span_id":            "span-1",
			"_cardinalhq.start_time_unix_ns": int64(1000000),
		},
		{
			"_cardinalhq.trace_id":           "trace-1", // Same trace
			"_cardinalhq.span_id":            "span-2",  // Different span
			"_cardinalhq.start_time_unix_ns": int64(2000000),
		},
		{
			"_cardinalhq.trace_id":           "trace-2",     // Different trace
			"_cardinalhq.span_id":            "span-1",      // Same span ID as first row
			"_cardinalhq.start_time_unix_ns": int64(500000), // Earlier time (min)
		},
		{
			"_cardinalhq.trace_id":           "trace-2",      // Same trace as previous
			"_cardinalhq.span_id":            "span-3",       // Different span
			"_cardinalhq.start_time_unix_ns": int64(3000000), // Later time (max)
		},
	}

	for _, row := range testData {
		accumulator.Add(row)
	}

	stats := accumulator.Finalize().(TracesFileStats)

	// Verify basic counts
	if stats.SlotID != slotID {
		t.Errorf("Expected slot ID %d, got %d", slotID, stats.SlotID)
	}

	if stats.SpanCount != 3 { // 3 unique spans
		t.Errorf("Expected 3 unique spans, got %d", stats.SpanCount)
	}

	if stats.TraceCount != 2 { // 2 unique traces
		t.Errorf("Expected 2 unique traces, got %d", stats.TraceCount)
	}

	if stats.FirstTS != 500000 {
		t.Errorf("Expected first timestamp 500000, got %d", stats.FirstTS)
	}

	if stats.LastTS != 3000000 {
		t.Errorf("Expected last timestamp 3000000, got %d", stats.LastTS)
	}

	// Verify actual span and trace ID lists are sorted and complete
	expectedSpanIDs := []string{"span-1", "span-2", "span-3"}
	expectedTraceIDs := []string{"trace-1", "trace-2"}

	if len(stats.SpanIDs) != len(expectedSpanIDs) {
		t.Errorf("Expected %d span IDs in list, got %d", len(expectedSpanIDs), len(stats.SpanIDs))
	}

	if len(stats.TraceIDs) != len(expectedTraceIDs) {
		t.Errorf("Expected %d trace IDs in list, got %d", len(expectedTraceIDs), len(stats.TraceIDs))
	}

	for i, expectedSpanID := range expectedSpanIDs {
		if i >= len(stats.SpanIDs) || stats.SpanIDs[i] != expectedSpanID {
			t.Errorf("Expected span ID %s at index %d, got %v", expectedSpanID, i, stats.SpanIDs)
		}
	}

	for i, expectedTraceID := range expectedTraceIDs {
		if i >= len(stats.TraceIDs) || stats.TraceIDs[i] != expectedTraceID {
			t.Errorf("Expected trace ID %s at index %d, got %v", expectedTraceID, i, stats.TraceIDs)
		}
	}
}
