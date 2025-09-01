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
	"runtime"
	"testing"
	"time"
)

// TestDuckDBObjectCacheEffect tests if the object cache helps with
// memory efficiency over multiple iterations
func TestDuckDBObjectCacheEffect(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-721581701.parquet"
	iterations := 10

	readers := []struct {
		name    string
		factory func() (Reader, error)
	}{
		{"DuckDB_Original", func() (Reader, error) {
			return NewDuckDBParquetRawReader([]string{testFile}, 1000)
		}},
		{"DuckDB_Batched", func() (Reader, error) {
			return NewDuckDBParquetBatchedReader([]string{testFile}, 1000)
		}},
	}

	for _, reader := range readers {
		t.Run(reader.name+"_ObjectCache", func(t *testing.T) {
			runtime.GC()
			runtime.GC()
			time.Sleep(100 * time.Millisecond)

			baseline, err := getSystemMemStats()
			if err != nil {
				t.Fatalf("Failed to get baseline: %v", err)
			}

			t.Logf("=== %s with Object Cache ===", reader.name)
			t.Logf("Baseline RSS: %.2f MB", float64(baseline.ProcessRSS)/1024/1024)

			var totalRows int64

			// Run multiple iterations to see if object cache helps
			for i := 0; i < iterations; i++ {
				r, err := reader.factory()
				if err != nil {
					t.Fatalf("Iteration %d: Failed to create reader: %v", i, err)
				}

				// Read first batch only
				batch, err := r.Next(context.TODO())
				if err != nil {
					r.Close()
					t.Fatalf("Iteration %d: Failed to read batch: %v", i, err)
				}

				totalRows += int64(batch.Len())
				r.Close()

				// Check memory every few iterations
				if (i+1)%3 == 0 {
					current, _ := getSystemMemStats()
					currentGrowth := float64(current.ProcessRSS-baseline.ProcessRSS) / 1024 / 1024
					t.Logf("After iteration %d: RSS growth +%.2f MB", i+1, currentGrowth)
				}
			}

			runtime.GC()
			runtime.GC()
			time.Sleep(100 * time.Millisecond)

			final, err := getSystemMemStats()
			if err != nil {
				t.Fatalf("Failed to get final stats: %v", err)
			}

			totalGrowth := final.ProcessRSS - baseline.ProcessRSS
			avgMemPerIteration := float64(totalGrowth) / float64(iterations) / 1024 / 1024
			memPerRow := float64(totalGrowth) / float64(totalRows) / 1024

			t.Logf("Total iterations: %d", iterations)
			t.Logf("Total rows processed: %d", totalRows)
			t.Logf("Final RSS growth: %.2f MB", float64(totalGrowth)/1024/1024)
			t.Logf("Average memory per iteration: %.2f MB", avgMemPerIteration)
			t.Logf("Memory per row: %.1f KB", memPerRow)

			if avgMemPerIteration < 1.0 {
				t.Logf("✅ Excellent iteration efficiency (<1 MB per iteration)")
			} else if avgMemPerIteration < 5.0 {
				t.Logf("✅ Good iteration efficiency (<5 MB per iteration)")
			} else if avgMemPerIteration < 20.0 {
				t.Logf("⚠️  Moderate iteration efficiency (<20 MB per iteration)")
			} else {
				t.Logf("❌ Poor iteration efficiency (>20 MB per iteration)")
			}
		})
	}
}

// TestDuckDBObjectCacheLongRun tests object cache effect with more iterations
func TestDuckDBObjectCacheLongRun(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-721581701.parquet"
	iterations := 50

	t.Run("DuckDB_ObjectCache_50Iterations", func(t *testing.T) {
		runtime.GC()
		runtime.GC()
		time.Sleep(100 * time.Millisecond)

		baseline, err := getSystemMemStats()
		if err != nil {
			t.Fatalf("Failed to get baseline: %v", err)
		}

		t.Logf("Testing %d iterations with object cache enabled", iterations)

		var totalRows int64
		memorySnapshots := []float64{}

		for i := 0; i < iterations; i++ {
			reader, err := NewDuckDBParquetRawReader([]string{testFile}, 1000)
			if err != nil {
				t.Fatalf("Iteration %d: Failed to create reader: %v", i, err)
			}

			batch, err := reader.Next(context.TODO())
			if err != nil {
				reader.Close()
				t.Fatalf("Iteration %d: Failed to read batch: %v", i, err)
			}

			totalRows += int64(batch.Len())
			reader.Close()

			// Take memory snapshot every 10 iterations
			if (i+1)%10 == 0 {
				current, _ := getSystemMemStats()
				growth := float64(current.ProcessRSS-baseline.ProcessRSS) / 1024 / 1024
				memorySnapshots = append(memorySnapshots, growth)
				t.Logf("Iteration %d: RSS growth +%.2f MB", i+1, growth)
			}
		}

		runtime.GC()
		runtime.GC()

		final, _ := getSystemMemStats()
		finalGrowth := float64(final.ProcessRSS-baseline.ProcessRSS) / 1024 / 1024
		avgPerIteration := finalGrowth / float64(iterations)

		t.Logf("\n=== Final Results ===")
		t.Logf("Total iterations: %d", iterations)
		t.Logf("Total rows: %d", totalRows)
		t.Logf("Final RSS growth: %.2f MB", finalGrowth)
		t.Logf("Average per iteration: %.3f MB", avgPerIteration)

		// Check if memory growth is linear or plateaus
		if len(memorySnapshots) >= 2 {
			firstSnapshot := memorySnapshots[0]
			lastSnapshot := memorySnapshots[len(memorySnapshots)-1]
			growthRate := (lastSnapshot - firstSnapshot) / float64(len(memorySnapshots)-1) / 10

			t.Logf("Memory growth rate: %.3f MB per 10 iterations", growthRate*10)

			if growthRate < 0.1 {
				t.Logf("✅ Memory usage appears to plateau (object cache working)")
			} else if growthRate < 0.5 {
				t.Logf("✅ Slow memory growth (object cache helping)")
			} else {
				t.Logf("⚠️  Continued memory growth (object cache limited effect)")
			}
		}
	})
}
