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

	writer, err := NewMetricsWriter("metrics-test", tmpdir, 200, 4) // Limit to 4 records per file
	if err != nil {
		t.Fatalf("Failed to create metrics writer: %v", err)
	}
	defer writer.Abort()

	// Test [metric name, TID] grouping - these should not be split across files
	testData := []map[string]any{
		{"_cardinalhq.name": "cpu.usage", "_cardinalhq.tid": int64(100), "_cardinalhq.timestamp": int64(1000), "value": float64(1.0)},
		{"_cardinalhq.name": "cpu.usage", "_cardinalhq.tid": int64(100), "_cardinalhq.timestamp": int64(2000), "value": float64(2.0)},
		{"_cardinalhq.name": "cpu.usage", "_cardinalhq.tid": int64(100), "_cardinalhq.timestamp": int64(3000), "value": float64(3.0)},
		{"_cardinalhq.name": "cpu.usage", "_cardinalhq.tid": int64(100), "_cardinalhq.timestamp": int64(4000), "value": float64(4.0)},    // This should exceed target size but stay with same key
		{"_cardinalhq.name": "memory.usage", "_cardinalhq.tid": int64(200), "_cardinalhq.timestamp": int64(5000), "value": float64(5.0)}, // New metric+TID, new file
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
	for i, result := range results {
		if stats, ok := result.Metadata.(MetricsFileStats); ok {
			// Check that we have fingerprints
			if len(stats.Fingerprints) == 0 {
				t.Errorf("Expected fingerprints in file %d, got 0", i)
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
			row:     map[string]any{"_cardinalhq.name": "cpu.usage", "_cardinalhq.tid": int64(123)},
			wantErr: false,
		},
		{
			name:    "missing metric name",
			row:     map[string]any{"_cardinalhq.tid": int64(123)},
			wantErr: true,
		},
		{
			name:    "missing TID",
			row:     map[string]any{"_cardinalhq.name": "cpu.usage"},
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

	// Add some test data with metric names and timestamps
	testData := []map[string]any{
		{"_cardinalhq.name": "cpu.usage", "_cardinalhq.timestamp": int64(1000)},
		{"_cardinalhq.name": "memory.usage", "_cardinalhq.timestamp": int64(2000)},
		{"_cardinalhq.name": "cpu.usage", "_cardinalhq.timestamp": int64(3000)},     // Duplicate metric name
		{"_cardinalhq.name": "disk.io", "_cardinalhq.timestamp": int64(500)},        // Earliest timestamp
		{"_cardinalhq.name": "network.bytes", "_cardinalhq.timestamp": int64(4000)}, // Latest timestamp
	}

	for _, row := range testData {
		accumulator.Add(row)
	}

	stats := accumulator.Finalize().(MetricsFileStats)

	// Verify timestamps
	if stats.FirstTS != 500 {
		t.Errorf("Expected firstTS 500, got %d", stats.FirstTS)
	}
	if stats.LastTS != 4000 {
		t.Errorf("Expected lastTS 4000, got %d", stats.LastTS)
	}

	// Verify we have fingerprints
	if len(stats.Fingerprints) == 0 {
		t.Error("Expected fingerprints, got none")
	}

}
