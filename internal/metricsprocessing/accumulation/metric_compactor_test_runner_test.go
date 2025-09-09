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

package accumulation

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestSeglog34Compaction tests the compaction of seglog-34 files
func TestSeglog34Compaction(t *testing.T) {
	// Skip if seglog-34 directory doesn't exist
	seglogDir := filepath.Join("..", "..", "..", "seglog-34")
	if _, err := os.Stat(seglogDir); os.IsNotExist(err) {
		t.Skip("seglog-34 directory not found, skipping test")
		return
	}

	ctx := context.Background()
	outputDir := "/tmp/test-compaction-output-unittest"

	// Clean up any existing output
	os.RemoveAll(outputDir)
	defer os.RemoveAll(outputDir)

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}

	// Create testable compactor
	compactor := NewTestableCompactor()

	// Process seglog-34 files
	result, err := compactor.CompactSeglog34(ctx, seglogDir, outputDir)
	if err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	// Print results for inspection
	t.Logf("Compaction Results:")
	t.Logf("  Input files: %s/source/*.parquet", seglogDir)
	t.Logf("  Output files: %d", len(result.OutputFiles))
	for i, file := range result.OutputFiles {
		t.Logf("    %d: %s", i+1, file)
	}
	t.Logf("  Total records: %d", result.TotalRecords)
	t.Logf("  Total file size: %d bytes", result.TotalFileSize)

	// Basic sanity checks
	if len(result.OutputFiles) == 0 {
		t.Error("No output files were created")
	}
	if result.TotalRecords == 0 {
		t.Error("No records were written to output files")
	}

	// Expected values based on seglog-34.json
	expectedSourceRecords := int64(45578)
	expectedDestRecords := int64(34291) // This is what the original compaction produced

	t.Logf("\nComparison:")
	t.Logf("  Expected source records: %d", expectedSourceRecords)
	t.Logf("  Expected dest records (original): %d", expectedDestRecords)
	t.Logf("  Our compaction produced: %d", result.TotalRecords)
	t.Logf("  Difference from original: %d", result.TotalRecords-expectedDestRecords)

	// Check if we match the original output
	if result.TotalRecords == expectedDestRecords {
		t.Logf("✅ Our compaction produces the SAME record count as the original")
	} else if result.TotalRecords == expectedSourceRecords {
		t.Logf("✅ Our compaction preserves ALL source records (no data loss)")
	} else {
		t.Logf("❌ Our compaction produces a different record count")
	}

	// The main goal is to see if our refactored logic produces the same result
	// If it produces 34,291 records, it matches the original (potentially buggy) behavior
	// If it produces 45,578 records, it may have fixed the data loss issue
}
