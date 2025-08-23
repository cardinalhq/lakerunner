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

func TestNewMetricsWriter(t *testing.T) {
	tmpdir := t.TempDir()

	writer, err := NewMetricsWriter("metrics-test", tmpdir, 200, 50.0) // Very small to force splitting
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
	expectedTIDs := [][]int64{
		{100}, // First file should have TID 100
		{200}, // Second file should have TID 200
	}

	for i, result := range results {
		if stats, ok := result.Metadata.(MetricsFileStats); ok {
			if stats.TIDCount != 1 {
				t.Errorf("Expected 1 TID per file, got %d", stats.TIDCount)
			}
			// Check that we have the actual TID list
			if len(stats.TIDs) != 1 {
				t.Errorf("Expected 1 TID in TIDs list, got %d", len(stats.TIDs))
			}
			// Check that the TID matches expected
			if len(expectedTIDs[i]) > 0 && (len(stats.TIDs) == 0 || stats.TIDs[0] != expectedTIDs[i][0]) {
				t.Errorf("File %d: Expected TID %d, got %v", i, expectedTIDs[i][0], stats.TIDs)
			}
		} else {
			t.Error("Expected MetricsFileStats metadata")
		}
		os.Remove(result.FileName)
	}
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

func TestMetricsStatsAccumulator(t *testing.T) {
	provider := &MetricsStatsProvider{}
	accumulator := provider.NewAccumulator().(*MetricsStatsAccumulator)

	// Add some test data
	testData := []map[string]any{
		{"_cardinalhq.tid": int64(100)},
		{"_cardinalhq.tid": int64(200)},
		{"_cardinalhq.tid": int64(100)}, // Duplicate TID
		{"_cardinalhq.tid": int64(50)},  // Min TID
		{"_cardinalhq.tid": int64(300)}, // Max TID
	}

	for _, row := range testData {
		accumulator.Add(row)
	}

	stats := accumulator.Finalize().(MetricsFileStats)

	// Verify basic counts
	if stats.TIDCount != 4 { // 4 unique TIDs
		t.Errorf("Expected 4 unique TIDs, got %d", stats.TIDCount)
	}

	if stats.MinTID != 50 {
		t.Errorf("Expected min TID 50, got %d", stats.MinTID)
	}

	if stats.MaxTID != 300 {
		t.Errorf("Expected max TID 300, got %d", stats.MaxTID)
	}

	// Verify actual TID list is sorted and complete
	expectedTIDs := []int64{50, 100, 200, 300}
	if len(stats.TIDs) != len(expectedTIDs) {
		t.Errorf("Expected %d TIDs in list, got %d", len(expectedTIDs), len(stats.TIDs))
	}

	for i, expectedTID := range expectedTIDs {
		if i >= len(stats.TIDs) || stats.TIDs[i] != expectedTID {
			t.Errorf("Expected TID %d at index %d, got %v", expectedTID, i, stats.TIDs)
		}
	}
}
