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
	"bytes"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// TestResourceCleanupValidation validates that all readers properly clean up resources
func TestResourceCleanupValidation(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-2176772462.parquet"

	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to load test file: %v", err)
	}

	absPath, err := filepath.Abs(testFile)
	if err != nil {
		t.Fatalf("Failed to get absolute path: %v", err)
	}

	t.Run("ParquetRaw_CleanupValidation", func(t *testing.T) {
		testReaderCleanup(t, "ParquetRaw", func() error {
			reader := bytes.NewReader(data)
			pr, err := NewParquetRawReader(reader, int64(len(data)), 1000)
			if err != nil {
				return err
			}

			// Read all data
			for {
				batch, err := pr.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				_ = batch
			}

			// Explicit close
			return pr.Close()
		})
	})

	t.Run("DuckDB_CleanupValidation", func(t *testing.T) {
		testReaderCleanup(t, "DuckDB", func() error {
			dr, err := NewDuckDBParquetRawReader([]string{absPath}, 1000)
			if err != nil {
				return err
			}

			// Read all data
			for {
				batch, err := dr.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				_ = batch
			}

			// Explicit close
			return dr.Close()
		})
	})

	t.Run("Arrow_CleanupValidation", func(t *testing.T) {
		testReaderCleanup(t, "Arrow", func() error {
			reader := bytes.NewReader(data)
			ar, err := NewArrowCookedReader(reader, 1000)
			if err != nil {
				return err
			}

			// Read all data
			for {
				batch, err := ar.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				_ = batch
			}

			// Explicit close
			return ar.Close()
		})
	})
}

// testReaderCleanup validates proper resource cleanup for a reader
func testReaderCleanup(t *testing.T, readerName string, readerFunc func() error) {
	// Establish stable baseline
	runtime.GC()
	runtime.GC()
	time.Sleep(200 * time.Millisecond) // Longer stabilization

	baseline, err := getSystemMemStats()
	if err != nil {
		t.Fatalf("Failed to get baseline: %v", err)
	}

	iterations := 5
	var measurements []SystemMemStats

	t.Logf("=== %s Resource Cleanup Validation ===", readerName)
	t.Logf("Baseline RSS: %.2f MB, Go Heap: %.2f MB",
		float64(baseline.ProcessRSS)/(1024*1024), float64(baseline.GoHeapAlloc)/(1024*1024))

	for i := 0; i < iterations; i++ {
		// Run reader
		if err := readerFunc(); err != nil {
			t.Fatalf("Iteration %d failed: %v", i, err)
		}

		// Force comprehensive cleanup
		runtime.GC()
		runtime.GC()
		runtime.GC()                       // Triple GC to ensure cleanup
		time.Sleep(100 * time.Millisecond) // Allow async cleanup

		// Measure after cleanup
		stats, err := getSystemMemStats()
		if err != nil {
			t.Logf("Warning: failed to get stats at iteration %d: %v", i, err)
			continue
		}
		measurements = append(measurements, stats)

		delta := calculateDelta(baseline, stats)
		t.Logf("After iteration %d: RSS=%+.2f MB, GoHeap=%+.2f MB, Native=%+.2f MB",
			i+1,
			float64(delta.ProcessRSSDelta)/(1024*1024),
			float64(delta.GoHeapDelta)/(1024*1024),
			float64(delta.ProcessRSSDelta-delta.GoHeapDelta)/(1024*1024))
	}

	if len(measurements) == 0 {
		t.Fatal("No measurements collected")
	}

	// Analyze cleanup effectiveness
	final := measurements[len(measurements)-1]
	finalDelta := calculateDelta(baseline, final)

	// Check if memory returns to baseline after multiple iterations
	rssLeakage := finalDelta.ProcessRSSDelta
	heapLeakage := finalDelta.GoHeapDelta

	t.Logf("\n=== Cleanup Analysis ===")
	t.Logf("Final RSS leakage: %+d bytes (%.2f MB)", rssLeakage, float64(rssLeakage)/(1024*1024))
	t.Logf("Final Go heap leakage: %+d bytes (%.2f MB)", heapLeakage, float64(heapLeakage)/(1024*1024))

	// Classify cleanup quality
	rssLeakageMB := float64(rssLeakage) / (1024 * 1024)
	if rssLeakageMB < 0.5 {
		t.Logf("✅ Excellent cleanup (<0.5 MB RSS leakage)")
	} else if rssLeakageMB < 2.0 {
		t.Logf("⚠️  Moderate cleanup (%.2f MB RSS leakage)", rssLeakageMB)
	} else {
		t.Logf("❌ Poor cleanup (%.2f MB RSS leakage - possible resource leak)", rssLeakageMB)
	}
}

// TestResourceCleanupWithExplicitDestructions tests cleanup with forced resource destruction
func TestResourceCleanupWithExplicitDestructions(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-2176772462.parquet"

	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to load test file: %v", err)
	}

	absPath, err := filepath.Abs(testFile)
	if err != nil {
		t.Fatalf("Failed to get absolute path: %v", err)
	}

	t.Run("DuckDB_ForceCleanup", func(t *testing.T) {
		runtime.GC()
		runtime.GC()
		time.Sleep(200 * time.Millisecond)

		baseline, _ := getSystemMemStats()

		// Create and destroy DuckDB readers multiple times with aggressive cleanup
		for i := 0; i < 3; i++ {
			func() {
				dr, err := NewDuckDBParquetRawReader([]string{absPath}, 1000)
				if err != nil {
					t.Fatalf("Failed to create DuckDB reader: %v", err)
				}
				defer func() {
					if err := dr.Close(); err != nil {
						t.Errorf("Close error: %v", err)
					}
					// Force additional cleanup attempts
					runtime.GC()
					runtime.GC()
					time.Sleep(50 * time.Millisecond)
				}()

				// Read some data
				batch, err := dr.Next()
				if err != nil && err != io.EOF {
					t.Fatalf("Read error: %v", err)
				}
				_ = batch
			}()

			// Measure after each cleanup
			current, _ := getSystemMemStats()
			delta := calculateDelta(baseline, current)
			t.Logf("After cleanup %d: RSS=%+.2f MB", i+1, float64(delta.ProcessRSSDelta)/(1024*1024))
		}
	})

	t.Run("Arrow_ForceCleanup", func(t *testing.T) {
		runtime.GC()
		runtime.GC()
		time.Sleep(200 * time.Millisecond)

		baseline, _ := getSystemMemStats()

		// Create and destroy Arrow readers multiple times with aggressive cleanup
		for i := 0; i < 3; i++ {
			func() {
				reader := bytes.NewReader(data)
				ar, err := NewArrowCookedReader(reader, 1000)
				if err != nil {
					t.Fatalf("Failed to create Arrow reader: %v", err)
				}
				defer func() {
					if err := ar.Close(); err != nil {
						t.Errorf("Close error: %v", err)
					}
					// Force additional cleanup attempts
					runtime.GC()
					runtime.GC()
					time.Sleep(50 * time.Millisecond)
				}()

				// Read some data
				batch, err := ar.Next()
				if err != nil && err != io.EOF {
					t.Fatalf("Read error: %v", err)
				}
				_ = batch
			}()

			// Measure after each cleanup
			current, _ := getSystemMemStats()
			delta := calculateDelta(baseline, current)
			t.Logf("After cleanup %d: RSS=%+.2f MB", i+1, float64(delta.ProcessRSSDelta)/(1024*1024))
		}
	})
}

// TestStabilizedMemoryMeasurement tests memory with longer stabilization periods
func TestStabilizedMemoryMeasurement(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-2176772462.parquet"

	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to load test file: %v", err)
	}

	absPath, err := filepath.Abs(testFile)
	if err != nil {
		t.Fatalf("Failed to get absolute path: %v", err)
	}

	readers := []struct {
		name string
		fn   func() error
	}{
		{"ParquetRaw", func() error {
			reader := bytes.NewReader(data)
			pr, err := NewParquetRawReader(reader, int64(len(data)), 1000)
			if err != nil {
				return err
			}
			defer pr.Close()

			for {
				batch, err := pr.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				_ = batch
			}
			return nil
		}},
		{"DuckDB", func() error {
			dr, err := NewDuckDBParquetRawReader([]string{absPath}, 1000)
			if err != nil {
				return err
			}
			defer dr.Close()

			for {
				batch, err := dr.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				_ = batch
			}
			return nil
		}},
		{"Arrow", func() error {
			reader := bytes.NewReader(data)
			ar, err := NewArrowCookedReader(reader, 1000)
			if err != nil {
				return err
			}
			defer ar.Close()

			for {
				batch, err := ar.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				_ = batch
			}
			return nil
		}},
	}

	for _, reader := range readers {
		t.Run(reader.name+"_Stabilized", func(t *testing.T) {
			// Extended stabilization period
			t.Logf("Stabilizing memory for %s reader...", reader.name)
			for i := 0; i < 5; i++ {
				runtime.GC()
				time.Sleep(100 * time.Millisecond)
			}

			// Multiple baseline measurements to ensure stability
			var baselines []SystemMemStats
			for i := 0; i < 3; i++ {
				if stats, err := getSystemMemStats(); err == nil {
					baselines = append(baselines, stats)
				}
				time.Sleep(50 * time.Millisecond)
			}

			if len(baselines) != 3 {
				t.Fatal("Failed to establish stable baseline")
			}

			// Check baseline stability
			firstBaseline := baselines[0]
			lastBaseline := baselines[len(baselines)-1]
			baselineDrift := calculateDelta(firstBaseline, lastBaseline)

			t.Logf("Baseline drift: RSS=%+d bytes, Heap=%+d bytes",
				baselineDrift.ProcessRSSDelta, baselineDrift.GoHeapDelta)

			if abs(baselineDrift.ProcessRSSDelta) > 1024*1024 { // >1MB drift
				t.Logf("⚠️  Large baseline drift detected: %.2f MB",
					float64(baselineDrift.ProcessRSSDelta)/(1024*1024))
			}

			// Use middle baseline for measurement
			baseline := baselines[1]

			// Execute reader
			if err := reader.fn(); err != nil {
				t.Fatalf("Reader execution failed: %v", err)
			}

			// Extended cleanup period
			t.Logf("Cleaning up resources...")
			for i := 0; i < 5; i++ {
				runtime.GC()
				time.Sleep(100 * time.Millisecond)
			}

			// Multiple post-execution measurements
			var finals []SystemMemStats
			for i := 0; i < 3; i++ {
				if stats, err := getSystemMemStats(); err == nil {
					finals = append(finals, stats)
				}
				time.Sleep(50 * time.Millisecond)
			}

			if len(finals) != 3 {
				t.Fatal("Failed to get stable final measurements")
			}

			// Use middle final measurement
			final := finals[1]
			delta := calculateDelta(baseline, final)

			// Check post-execution stability
			firstFinal := finals[0]
			lastFinal := finals[len(finals)-1]
			finalDrift := calculateDelta(firstFinal, lastFinal)

			t.Logf("Post-execution drift: RSS=%+d bytes, Heap=%+d bytes",
				finalDrift.ProcessRSSDelta, finalDrift.GoHeapDelta)

			t.Logf("\n=== Stabilized %s Memory Analysis ===", reader.name)
			t.Logf("RSS change: %+d bytes (%.2f MB)", delta.ProcessRSSDelta, float64(delta.ProcessRSSDelta)/(1024*1024))
			t.Logf("Go heap change: %+d bytes (%.2f MB)", delta.GoHeapDelta, float64(delta.GoHeapDelta)/(1024*1024))

			nativeMemory := delta.ProcessRSSDelta - delta.GoHeapDelta
			t.Logf("Native memory change: %+d bytes (%.2f MB)", nativeMemory, float64(nativeMemory)/(1024*1024))

			// Assess cleanup quality
			rssLeakMB := float64(delta.ProcessRSSDelta) / (1024 * 1024)
			if rssLeakMB < 1.0 {
				t.Logf("✅ Good cleanup (%.2f MB residual)", rssLeakMB)
			} else if rssLeakMB < 5.0 {
				t.Logf("⚠️  Moderate cleanup (%.2f MB residual)", rssLeakMB)
			} else {
				t.Logf("❌ Poor cleanup (%.2f MB residual - likely resource leak)", rssLeakMB)
			}
		})
	}
}

// TestSequentialReaderExecution tests readers run one after another to isolate effects
func TestSequentialReaderExecution(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-2176772462.parquet"

	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to load test file: %v", err)
	}

	absPath, err := filepath.Abs(testFile)
	if err != nil {
		t.Fatalf("Failed to get absolute path: %v", err)
	}

	// Long stabilization
	t.Logf("Initial stabilization...")
	for i := 0; i < 10; i++ {
		runtime.GC()
		time.Sleep(100 * time.Millisecond)
	}

	initialBaseline, err := getSystemMemStats()
	if err != nil {
		t.Fatalf("Failed to get initial baseline: %v", err)
	}

	t.Logf("Initial baseline: RSS=%.2f MB, Heap=%.2f MB",
		float64(initialBaseline.ProcessRSS)/(1024*1024),
		float64(initialBaseline.GoHeapAlloc)/(1024*1024))

	// Test ParquetRaw first
	t.Logf("\n--- Testing ParquetRaw Reader ---")
	reader := bytes.NewReader(data)
	pr, err := NewParquetRawReader(reader, int64(len(data)), 1000)
	if err != nil {
		t.Fatalf("Failed to create ParquetRaw reader: %v", err)
	}

	rowCount := 0
	for {
		batch, err := pr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ParquetRaw read error: %v", err)
		}
		if batch != nil {
			rowCount += batch.Len()
		}
	}
	pr.Close()

	// Cleanup and measure
	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(100 * time.Millisecond)
	}

	afterParquet, _ := getSystemMemStats()
	parquetDelta := calculateDelta(initialBaseline, afterParquet)
	t.Logf("ParquetRaw result: %d rows, RSS=%+.2f MB, Heap=%+.2f MB",
		rowCount, float64(parquetDelta.ProcessRSSDelta)/(1024*1024),
		float64(parquetDelta.GoHeapDelta)/(1024*1024))

	// Test DuckDB next
	t.Logf("\n--- Testing DuckDB Reader ---")
	dr, err := NewDuckDBParquetRawReader([]string{absPath}, 1000)
	if err != nil {
		t.Fatalf("Failed to create DuckDB reader: %v", err)
	}

	rowCount = 0
	for {
		batch, err := dr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("DuckDB read error: %v", err)
		}
		if batch != nil {
			rowCount += batch.Len()
		}
	}
	dr.Close()

	// Cleanup and measure
	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(100 * time.Millisecond)
	}

	afterDuckDB, _ := getSystemMemStats()
	duckdbDelta := calculateDelta(afterParquet, afterDuckDB)
	t.Logf("DuckDB result: %d rows, RSS=%+.2f MB, Heap=%+.2f MB",
		rowCount, float64(duckdbDelta.ProcessRSSDelta)/(1024*1024),
		float64(duckdbDelta.GoHeapDelta)/(1024*1024))

	// Test Arrow last
	t.Logf("\n--- Testing Arrow Reader ---")
	reader3 := bytes.NewReader(data)
	ar, err := NewArrowCookedReader(reader3, 1000)
	if err != nil {
		t.Fatalf("Failed to create Arrow reader: %v", err)
	}

	rowCount = 0
	for {
		batch, err := ar.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Arrow read error: %v", err)
		}
		if batch != nil {
			rowCount += batch.Len()
		}
	}
	ar.Close()

	// Final cleanup and measure
	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(100 * time.Millisecond)
	}

	afterArrow, _ := getSystemMemStats()
	arrowDelta := calculateDelta(afterDuckDB, afterArrow)
	overallDelta := calculateDelta(initialBaseline, afterArrow)

	t.Logf("Arrow result: %d rows, RSS=%+.2f MB, Heap=%+.2f MB",
		rowCount, float64(arrowDelta.ProcessRSSDelta)/(1024*1024),
		float64(arrowDelta.GoHeapDelta)/(1024*1024))

	t.Logf("\n=== Sequential Execution Summary ===")
	t.Logf("Overall RSS change: %+.2f MB", float64(overallDelta.ProcessRSSDelta)/(1024*1024))
	t.Logf("Overall heap change: %+.2f MB", float64(overallDelta.GoHeapDelta)/(1024*1024))
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
