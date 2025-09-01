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

//go:build memoryanalysis

package filereader

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"
)

// TestDuckDBBatchedVolumeTest tests the batched DuckDB reader with
// larger batch sizes to see if we can achieve better memory efficiency
func TestDuckDBBatchedVolumeTest(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-721581701.parquet"

	// Test with larger batch sizes that showed better efficiency
	batchSizes := []int{500, 1000, 2000}

	for _, batchSize := range batchSizes {
		t.Run(fmt.Sprintf("DuckDB_Batched_Size%d", batchSize), func(t *testing.T) {
			runtime.GC()
			runtime.GC()
			time.Sleep(100 * time.Millisecond)

			baseline, err := getSystemMemStats()
			if err != nil {
				t.Fatalf("Failed to get baseline: %v", err)
			}

			reader, err := NewDuckDBParquetBatchedReader([]string{testFile}, batchSize)
			if err != nil {
				t.Fatalf("Failed to create reader: %v", err)
			}

			var totalRows int64
			batchCount := 0

			// Read all data
			for {
				batch, err := reader.Next(context.TODO())
				if err != nil {
					break // EOF
				}
				totalRows += int64(batch.Len())
				batchCount++
			}

			reader.Close()
			runtime.GC()
			runtime.GC()

			final, err := getSystemMemStats()
			if err != nil {
				t.Fatalf("Failed to get final stats: %v", err)
			}

			growth := final.ProcessRSS - baseline.ProcessRSS
			memPerRow := float64(growth) / float64(totalRows) / 1024

			t.Logf("Batch size %d: %d batches, %d rows total", batchSize, batchCount, totalRows)
			t.Logf("Total RSS growth: %.2f MB", float64(growth)/1024/1024)
			t.Logf("Memory per row: %.1f KB", memPerRow)

			if memPerRow < 5.0 {
				t.Logf("✅ Good efficiency (<5 KB per row)")
			} else if memPerRow < 20.0 {
				t.Logf("⚠️  Moderate efficiency (<20 KB per row)")
			} else {
				t.Logf("❌ Poor efficiency (>20 KB per row)")
			}
		})
	}
}
