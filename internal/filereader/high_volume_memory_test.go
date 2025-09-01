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
	"os"
	"runtime"
	"testing"
	"time"
)

// TestHighVolumeMemoryFootprint tests memory usage with 200 iterations
// to simulate processing millions of files in production.
func TestHighVolumeMemoryFootprint(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-721581701.parquet"
	iterations := 200

	readers := []struct {
		name    string
		factory func() (Reader, error)
	}{
		{"ParquetRaw", func() (Reader, error) {
			file, err := os.Open(testFile)
			if err != nil {
				return nil, err
			}
			stat, err := file.Stat()
			if err != nil {
				file.Close()
				return nil, err
			}
			reader, err := NewParquetRawReader(file, stat.Size(), 1000)
			if err != nil {
				file.Close()
				return nil, err
			}
			return &fileClosingReader{Reader: reader, file: file}, nil
		}},
		{"Arrow", func() (Reader, error) {
			file, err := os.Open(testFile)
			if err != nil {
				return nil, err
			}
			reader, err := NewArrowCookedReader(file, 1000)
			if err != nil {
				file.Close()
				return nil, err
			}
			return &fileClosingReader{Reader: reader, file: file}, nil
		}},
	}

	for _, reader := range readers {
		t.Run(reader.name+"_200Iterations", func(t *testing.T) {
			t.Logf("=== %s High Volume Test (%d iterations) ===", reader.name, iterations)

			// Establish stable baseline
			runtime.GC()
			runtime.GC()
			time.Sleep(100 * time.Millisecond)

			baseline, err := getSystemMemStats()
			if err != nil {
				t.Fatalf("Failed to get baseline memory stats: %v", err)
			}

			t.Logf("Baseline: RSS=%.2f MB, GoHeap=%.2f MB",
				float64(baseline.ProcessRSS)/1024/1024,
				float64(baseline.GoHeapAlloc)/1024/1024)

			var totalRows int64
			startTime := time.Now()

			// Progress tracking
			progressInterval := iterations / 10 // Report every 10%
			if progressInterval == 0 {
				progressInterval = 1
			}

			for i := 0; i < iterations; i++ {
				// Progress reporting
				if i%progressInterval == 0 && i > 0 {
					progress := float64(i) / float64(iterations) * 100
					elapsed := time.Since(startTime)

					// Get current memory stats for progress
					if current, err := getSystemMemStats(); err == nil {
						currentGrowth := float64(current.ProcessRSS-baseline.ProcessRSS) / 1024 / 1024
						t.Logf("Progress: %.0f%% complete (%d/%d), RSS growth: +%.2f MB, elapsed: %v",
							progress, i, iterations, currentGrowth, elapsed)
					}
				}

				reader, err := reader.factory()
				if err != nil {
					t.Fatalf("Iteration %d: Failed to create reader: %v", i, err)
				}

				// Process one batch to simulate real usage
				batch, err := reader.Next()
				if err != nil {
					reader.Close()
					t.Fatalf("Iteration %d: Failed to read batch: %v", i, err)
				}

				if batch.Len() == 0 {
					reader.Close()
					t.Fatalf("Iteration %d: No rows read", i)
				}

				totalRows += int64(batch.Len())

				// Clean up
				err = reader.Close()
				if err != nil {
					t.Logf("Warning: Iteration %d close error: %v", i, err)
				}

				// Periodic GC to prevent excessive heap growth
				if i%50 == 0 && i > 0 {
					runtime.GC()
				}
			}

			// Final aggressive cleanup
			runtime.GC()
			runtime.GC()
			runtime.GC()
			time.Sleep(200 * time.Millisecond)

			final, err := getSystemMemStats()
			if err != nil {
				t.Fatalf("Failed to get final memory stats: %v", err)
			}

			totalTime := time.Since(startTime)
			rssGrowth := final.ProcessRSS - baseline.ProcessRSS
			heapGrowth := int64(final.GoHeapAlloc - baseline.GoHeapAlloc)
			nativeGrowth := rssGrowth - heapGrowth

			avgRSSPerIteration := float64(rssGrowth) / float64(iterations) / 1024 / 1024
			avgNativePerIteration := float64(nativeGrowth) / float64(iterations) / 1024 / 1024

			t.Logf("\n=== Final Results ===")
			t.Logf("Total iterations: %d", iterations)
			t.Logf("Total rows processed: %d", totalRows)
			t.Logf("Total time: %v (%.2f ms/iteration)", totalTime, float64(totalTime.Nanoseconds())/float64(iterations)/1e6)
			t.Logf("Final RSS growth: %.2f MB", float64(rssGrowth)/1024/1024)
			t.Logf("Final Go heap growth: %.2f MB", float64(heapGrowth)/1024/1024)
			t.Logf("Final native growth: %.2f MB", float64(nativeGrowth)/1024/1024)
			t.Logf("Average RSS per iteration: %.3f MB", avgRSSPerIteration)
			t.Logf("Average native per iteration: %.3f MB", avgNativePerIteration)

			// Extrapolate to millions of files
			millionFileProjection := avgRSSPerIteration * 1000000
			t.Logf("\n=== Million File Projection ===")
			t.Logf("Projected RSS growth for 1M files: %.2f GB", millionFileProjection/1024)
			t.Logf("Projected native growth for 1M files: %.2f GB", (avgNativePerIteration*1000000)/1024)

			// Memory efficiency rating
			if avgRSSPerIteration < 0.1 {
				t.Logf("✅ Excellent memory efficiency (<0.1 MB per file)")
			} else if avgRSSPerIteration < 0.5 {
				t.Logf("✅ Good memory efficiency (<0.5 MB per file)")
			} else if avgRSSPerIteration < 1.0 {
				t.Logf("⚠️  Moderate memory usage (<1.0 MB per file)")
			} else if avgRSSPerIteration < 5.0 {
				t.Logf("⚠️  High memory usage (<5.0 MB per file)")
			} else {
				t.Logf("❌ Excessive memory usage (>5.0 MB per file)")
			}
		})
	}
}
