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

	"github.com/parquet-go/parquet-go"

	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
)

func TestNewLogsWriter(t *testing.T) {
	tmpdir := t.TempDir()

	writer, err := NewLogsWriter("logs-test", tmpdir, 10000, 150.0)
	if err != nil {
		t.Fatalf("Failed to create logs writer: %v", err)
	}
	defer writer.Abort()

	// Verify it's configured for spillable ordering
	if writer.Config().OrderBy != parquetwriter.OrderSpillable {
		t.Errorf("Expected OrderBy = OrderSpillable, got %v", writer.Config().OrderBy)
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

	// For verification, we'll use a schema discovered from the written data
	nodes := map[string]parquet.Node{
		"_cardinalhq.timestamp":   parquet.Int(64),
		"_cardinalhq.fingerprint": parquet.Int(64),
		"message":                 parquet.String(),
	}
	schema := parquet.NewSchema("logs-test", parquet.Group(nodes))
	reader := parquet.NewGenericReader[map[string]any](file, schema)
	defer reader.Close()

	var lastTimestamp int64
	rowCount := 0
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
		// Check that we have the actual fingerprint list
		expectedFingerprints := []int64{100, 200, 300, 400}
		if len(stats.Fingerprints) != 4 {
			t.Errorf("Expected 4 fingerprints in list, got %d", len(stats.Fingerprints))
		}
		// Verify all expected fingerprints are present (should be sorted)
		for i, expectedFP := range expectedFingerprints {
			if i >= len(stats.Fingerprints) || stats.Fingerprints[i] != expectedFP {
				t.Errorf("Expected fingerprint %d at index %d, got %v", expectedFP, i, stats.Fingerprints)
			}
		}
	} else {
		t.Error("Expected LogsFileStats metadata")
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

func TestLogsStatsAccumulator(t *testing.T) {
	provider := &LogsStatsProvider{}
	accumulator := provider.NewAccumulator().(*LogsStatsAccumulator)

	// Add some test data
	testData := []map[string]any{
		{"_cardinalhq.timestamp": int64(1000), "_cardinalhq.fingerprint": int64(100)},
		{"_cardinalhq.timestamp": int64(3000), "_cardinalhq.fingerprint": int64(300)},
		{"_cardinalhq.timestamp": int64(2000), "_cardinalhq.fingerprint": int64(200)},
		{"_cardinalhq.timestamp": int64(4000), "_cardinalhq.fingerprint": int64(100)}, // Duplicate fingerprint
	}

	for _, row := range testData {
		accumulator.Add(row)
	}

	stats := accumulator.Finalize().(LogsFileStats)

	// Verify basic counts
	if stats.FingerprintCount != 3 { // 3 unique fingerprints
		t.Errorf("Expected 3 unique fingerprints, got %d", stats.FingerprintCount)
	}

	if stats.FirstTS != 1000 {
		t.Errorf("Expected first timestamp 1000, got %d", stats.FirstTS)
	}

	if stats.LastTS != 4000 {
		t.Errorf("Expected last timestamp 4000, got %d", stats.LastTS)
	}

	// Verify actual fingerprint list is sorted and complete
	expectedFingerprints := []int64{100, 200, 300}
	if len(stats.Fingerprints) != len(expectedFingerprints) {
		t.Errorf("Expected %d fingerprints in list, got %d", len(expectedFingerprints), len(stats.Fingerprints))
	}

	for i, expectedFP := range expectedFingerprints {
		if i >= len(stats.Fingerprints) || stats.Fingerprints[i] != expectedFP {
			t.Errorf("Expected fingerprint %d at index %d, got %v", expectedFP, i, stats.Fingerprints)
		}
	}
}
