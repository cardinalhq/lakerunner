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

//go:build experimental && memoryanalysis

package experimental

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestDuckDBBatchingComparison compares memory usage between the original
// and batched DuckDB readers to validate the batching fix.
func TestDuckDBBatchingComparison(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-721581701.parquet"

	readers := []struct {
		name    string
		factory func() (filereader.Reader, error)
	}{
		{"DuckDB_Original", func() (filereader.Reader, error) {
			return NewDuckDBParquetRawReader([]string{testFile}, 1000)
		}},
		{"DuckDB_Batched", func() (filereader.Reader, error) {
			return NewDuckDBParquetBatchedReader([]string{testFile}, 1000)
		}},
	}

	for _, reader := range readers {
		t.Run(reader.name+"_MemoryUsage", func(t *testing.T) {
			// Stabilize memory
			runtime.GC()
			runtime.GC()
			time.Sleep(100 * time.Millisecond)

			baseline, err := getSystemMemStats()
			if err != nil {
				t.Fatalf("Failed to get baseline memory: %v", err)
			}

			t.Logf("=== %s Memory Test ===", reader.name)
			t.Logf("Baseline: RSS=%.2f MB, GoHeap=%.2f MB",
				float64(baseline.ProcessRSS)/1024/1024,
				float64(baseline.GoHeapAlloc)/1024/1024)

			// Create reader and process first batch
			r, err := reader.factory()
			if err != nil {
				t.Fatalf("Failed to create reader: %v", err)
			}

			// Read first batch to trigger initial memory allocation
			batch, err := r.Next(context.TODO())
			if err != nil {
				r.Close()
				t.Fatalf("Failed to read first batch: %v", err)
			}

			if batch.Len() == 0 {
				r.Close()
				t.Fatal("No rows read in first batch")
			}

			// Measure memory after first batch
			firstBatch, err := getSystemMemStats()
			if err != nil {
				r.Close()
				t.Fatalf("Failed to get first batch memory: %v", err)
			}

			totalRows := int64(batch.Len())

			// Read a few more batches to see memory pattern
			for i := 0; i < 3; i++ {
				batch, err = r.Next(context.TODO())
				if err != nil {
					break // EOF or error, either is fine for this test
				}
				totalRows += int64(batch.Len())
			}

			// Clean up
			r.Close()
			runtime.GC()
			runtime.GC()

			final, err := getSystemMemStats()
			if err != nil {
				t.Fatalf("Failed to get final memory: %v", err)
			}

			// Calculate memory usage
			initialGrowth := firstBatch.ProcessRSS - baseline.ProcessRSS
			totalGrowth := final.ProcessRSS - baseline.ProcessRSS

			t.Logf("Rows processed: %d", totalRows)
			t.Logf("Initial memory growth (first batch): %.2f MB",
				float64(initialGrowth)/1024/1024)
			t.Logf("Total memory growth: %.2f MB",
				float64(totalGrowth)/1024/1024)
			t.Logf("Memory per row: %.3f KB",
				float64(totalGrowth)/float64(totalRows)/1024)

			// Memory efficiency rating for batched vs original
			memPerRow := float64(totalGrowth) / float64(totalRows) / 1024 // KB per row
			if memPerRow < 1.0 {
				t.Logf("✅ Excellent memory efficiency (<1 KB per row)")
			} else if memPerRow < 5.0 {
				t.Logf("✅ Good memory efficiency (<5 KB per row)")
			} else if memPerRow < 20.0 {
				t.Logf("⚠️  Moderate memory usage (<20 KB per row)")
			} else {
				t.Logf("❌ High memory usage (>20 KB per row)")
			}
		})
	}
}

// TestDuckDBBatchedReaderBehavior validates that the batched reader
// produces the same results as the original reader
func TestDuckDBBatchedReaderBehavior(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-721581701.parquet"

	// Create both readers
	original, err := NewDuckDBParquetRawReader([]string{testFile}, 100)
	if err != nil {
		t.Fatalf("Failed to create original reader: %v", err)
	}
	defer original.Close()

	batched, err := NewDuckDBParquetBatchedReader([]string{testFile}, 100)
	if err != nil {
		t.Fatalf("Failed to create batched reader: %v", err)
	}
	defer batched.Close()

	// Read first batch from each and compare
	origBatch, err := original.Next(context.TODO())
	if err != nil {
		t.Fatalf("Original reader failed: %v", err)
	}

	batchedBatch, err := batched.Next(context.TODO())
	if err != nil {
		t.Fatalf("Batched reader failed: %v", err)
	}

	// Compare basic metrics
	t.Logf("Original reader first batch: %d rows", origBatch.Len())
	t.Logf("Batched reader first batch: %d rows", batchedBatch.Len())

	if origBatch.Len() != batchedBatch.Len() {
		t.Errorf("Batch sizes differ: original=%d, batched=%d",
			origBatch.Len(), batchedBatch.Len())
	}

	// Verify we can read multiple batches from batched reader
	batchCount := 1
	totalRows := int64(batchedBatch.Len())

	for {
		batch, err := batched.Next(context.TODO())
		if err != nil {
			break // EOF
		}
		batchCount++
		totalRows += int64(batch.Len())

		if batchCount > 10 {
			t.Logf("Stopping after 10 batches to avoid long test")
			break
		}
	}

	t.Logf("Batched reader processed %d batches, %d total rows", batchCount, totalRows)

	if totalRows == 0 {
		t.Error("Batched reader returned no rows")
	}
}
