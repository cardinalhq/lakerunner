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
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// SystemMemStats captures system-level memory usage
type SystemMemStats struct {
	ProcessRSS     int64  // Resident Set Size (physical memory)
	ProcessVSZ     int64  // Virtual memory size
	SystemMemTotal int64  // Total system memory
	SystemMemFree  int64  // Free system memory
	SystemMemUsed  int64  // Used system memory
	GoHeapAlloc    uint64 // Go heap allocation
	GoTotalAlloc   uint64 // Go total allocations
	Timestamp      time.Time
}

// getSystemMemStats captures comprehensive memory statistics
func getSystemMemStats() (SystemMemStats, error) {
	stats := SystemMemStats{
		Timestamp: time.Now(),
	}

	// Get Go runtime stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	stats.GoHeapAlloc = m.HeapAlloc
	stats.GoTotalAlloc = m.TotalAlloc

	// Get process memory usage via ps
	pid := os.Getpid()
	cmd := exec.Command("ps", "-o", "rss,vsz", "-p", strconv.Itoa(pid))
	output, err := cmd.Output()
	if err != nil {
		return stats, fmt.Errorf("failed to get process memory: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(lines) < 2 {
		return stats, fmt.Errorf("unexpected ps output: %s", output)
	}

	// Parse RSS and VSZ from ps output (in KB)
	fields := strings.Fields(lines[1])
	if len(fields) >= 2 {
		if rss, err := strconv.ParseInt(fields[0], 10, 64); err == nil {
			stats.ProcessRSS = rss * 1024 // Convert KB to bytes
		}
		if vsz, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
			stats.ProcessVSZ = vsz * 1024 // Convert KB to bytes
		}
	}

	// Get system memory info via vm_stat on macOS
	cmd = exec.Command("vm_stat")
	output, err = cmd.Output()
	if err == nil {
		stats.parseVMStat(string(output))
	}

	return stats, nil
}

// parseVMStat parses macOS vm_stat output to get system memory info
func (s *SystemMemStats) parseVMStat(vmstat string) {
	lines := strings.Split(vmstat, "\n")
	pageSize := int64(4096) // Default page size

	var freePages, usedPages int64

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "page size of") {
			// Extract page size from "Mach Virtual Memory Statistics: (page size of 4096 bytes)"
			parts := strings.Split(line, " ")
			for i, part := range parts {
				if part == "of" && i+1 < len(parts) {
					if size, err := strconv.ParseInt(parts[i+1], 10, 64); err == nil {
						pageSize = size
					}
					break
				}
			}
		} else if strings.HasPrefix(line, "Pages free:") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				if pages, err := strconv.ParseInt(strings.TrimRight(parts[2], "."), 10, 64); err == nil {
					freePages = pages
				}
			}
		} else if strings.HasPrefix(line, "Pages active:") || strings.HasPrefix(line, "Pages inactive:") ||
			strings.HasPrefix(line, "Pages wired down:") || strings.HasPrefix(line, "Pages occupied by compressor:") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				if pages, err := strconv.ParseInt(strings.TrimRight(parts[len(parts)-1], "."), 10, 64); err == nil {
					usedPages += pages
				}
			}
		}
	}

	s.SystemMemFree = freePages * pageSize
	s.SystemMemUsed = usedPages * pageSize
	s.SystemMemTotal = s.SystemMemFree + s.SystemMemUsed
}

// MemoryDelta represents the change in memory usage
type MemoryDelta struct {
	ProcessRSSDelta    int64
	ProcessVSZDelta    int64
	SystemMemUsedDelta int64
	GoHeapDelta        int64
	GoTotalAllocDelta  uint64
	Duration           time.Duration
}

// calculateDelta computes the memory usage delta
func calculateDelta(before, after SystemMemStats) MemoryDelta {
	return MemoryDelta{
		ProcessRSSDelta:    after.ProcessRSS - before.ProcessRSS,
		ProcessVSZDelta:    after.ProcessVSZ - before.ProcessVSZ,
		SystemMemUsedDelta: after.SystemMemUsed - before.SystemMemUsed,
		GoHeapDelta:        int64(after.GoHeapAlloc) - int64(before.GoHeapAlloc),
		GoTotalAllocDelta:  after.GoTotalAlloc - before.GoTotalAlloc,
		Duration:           after.Timestamp.Sub(before.Timestamp),
	}
}

// report provides comprehensive memory analysis
func (delta MemoryDelta) report(tb testing.TB, readerType string, rowsProcessed int64, colCount int) {
	tb.Logf("=== %s Total Memory Footprint Analysis ===", readerType)
	tb.Logf("Rows processed: %d, Columns: %d", rowsProcessed, colCount)
	tb.Logf("Duration: %v", delta.Duration)

	tb.Logf("--- Process Memory (RSS = Physical Memory Actually Used) ---")
	tb.Logf("RSS change: %+d bytes (%+.2f MB)", delta.ProcessRSSDelta, float64(delta.ProcessRSSDelta)/(1024*1024))
	tb.Logf("VSZ change: %+d bytes (%+.2f MB)", delta.ProcessVSZDelta, float64(delta.ProcessVSZDelta)/(1024*1024))

	tb.Logf("--- Go Heap Memory ---")
	tb.Logf("Go heap change: %+d bytes (%+.2f MB)", delta.GoHeapDelta, float64(delta.GoHeapDelta)/(1024*1024))
	tb.Logf("Go total alloc: %d bytes (%+.2f MB)", delta.GoTotalAllocDelta, float64(delta.GoTotalAllocDelta)/(1024*1024))

	tb.Logf("--- Native Memory (RSS - Go Heap) ---")
	nativeMemory := delta.ProcessRSSDelta - delta.GoHeapDelta
	tb.Logf("Native memory change: %+d bytes (%+.2f MB)", nativeMemory, float64(nativeMemory)/(1024*1024))

	if rowsProcessed > 0 {
		tb.Logf("--- Per-Row Memory Cost ---")
		tb.Logf("Total memory per row: %d bytes", delta.ProcessRSSDelta/rowsProcessed)
		tb.Logf("Go heap per row: %d bytes", delta.GoHeapDelta/rowsProcessed)
		tb.Logf("Native memory per row: %d bytes", nativeMemory/rowsProcessed)

		if colCount > 0 {
			totalFields := rowsProcessed * int64(colCount)
			tb.Logf("Total memory per field: %d bytes", delta.ProcessRSSDelta/totalFields)
		}
	}

	// Calculate efficiency ratios
	if delta.ProcessRSSDelta > 0 {
		goRatio := float64(delta.GoHeapDelta) / float64(delta.ProcessRSSDelta) * 100
		nativeRatio := float64(nativeMemory) / float64(delta.ProcessRSSDelta) * 100

		tb.Logf("--- Memory Distribution ---")
		tb.Logf("Go heap: %.1f%% of total memory", goRatio)
		tb.Logf("Native allocations: %.1f%% of total memory", nativeRatio)
	}
}

// TestTotalMemoryFootprintComparison measures total memory usage including native allocations
func TestTotalMemoryFootprintComparison(t *testing.T) {
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
		fn   func() (int64, error)
	}{
		{"ParquetRaw", func() (int64, error) {
			reader := bytes.NewReader(data)
			pr, err := NewParquetRawReader(reader, int64(len(data)), 1000)
			if err != nil {
				return 0, err
			}
			defer pr.Close()

			totalRows := int64(0)
			for {
				batch, err := pr.Next(context.TODO())
				if err == io.EOF {
					break
				}
				if err != nil {
					return 0, err
				}
				if batch != nil {
					totalRows += int64(batch.Len())
				}
			}
			return totalRows, nil
		}},
		{"DuckDB", func() (int64, error) {
			dr, err := NewDuckDBParquetRawReader([]string{absPath}, 1000)
			if err != nil {
				return 0, err
			}
			defer dr.Close()

			totalRows := int64(0)
			for {
				batch, err := dr.Next(context.TODO())
				if err == io.EOF {
					break
				}
				if err != nil {
					return 0, err
				}
				if batch != nil {
					totalRows += int64(batch.Len())
				}
			}
			return totalRows, nil
		}},
		{"Arrow", func() (int64, error) {
			reader := bytes.NewReader(data)
			ar, err := NewArrowCookedReader(context.TODO(), reader, 1000)
			if err != nil {
				return 0, err
			}
			defer ar.Close()

			totalRows := int64(0)
			for {
				batch, err := ar.Next(context.TODO())
				if err == io.EOF {
					break
				}
				if err != nil {
					return 0, err
				}
				if batch != nil {
					totalRows += int64(batch.Len())
				}
			}
			return totalRows, nil
		}},
	}

	// Test each reader individually to isolate memory usage
	for _, reader := range readers {
		t.Run(reader.name, func(t *testing.T) {
			// Force GC and wait to stabilize memory
			runtime.GC()
			runtime.GC()
			time.Sleep(100 * time.Millisecond)

			// Capture baseline
			before, err := getSystemMemStats()
			if err != nil {
				t.Fatalf("Failed to get baseline memory stats: %v", err)
			}

			// Run the reader
			rowsProcessed, err := reader.fn()
			if err != nil {
				t.Fatalf("Reader error: %v", err)
			}

			// Small delay to let any async cleanup finish
			time.Sleep(50 * time.Millisecond)

			// Capture after stats
			after, err := getSystemMemStats()
			if err != nil {
				t.Fatalf("Failed to get final memory stats: %v", err)
			}

			delta := calculateDelta(before, after)
			delta.report(t, reader.name, rowsProcessed, 55) // 55 columns

			if rowsProcessed != 214 {
				t.Errorf("Expected 214 rows, got %d", rowsProcessed)
			}
		})
	}
}

// BenchmarkTotalMemoryFootprint runs comprehensive memory profiling benchmarks
func BenchmarkTotalMemoryFootprint(b *testing.B) {
	testFile := "../../testdata/metrics/metrics-cooked-2176772462.parquet"

	data, err := os.ReadFile(testFile)
	if err != nil {
		b.Fatalf("Failed to load test file: %v", err)
	}

	absPath, err := filepath.Abs(testFile)
	if err != nil {
		b.Fatalf("Failed to get absolute path: %v", err)
	}

	readers := []struct {
		name string
		fn   func() (int64, error)
	}{
		{"ParquetRaw", func() (int64, error) {
			reader := bytes.NewReader(data)
			pr, err := NewParquetRawReader(reader, int64(len(data)), 1000)
			if err != nil {
				return 0, err
			}
			defer pr.Close()

			totalRows := int64(0)
			for {
				batch, err := pr.Next(context.TODO())
				if err == io.EOF {
					break
				}
				if err != nil {
					return 0, err
				}
				if batch != nil {
					totalRows += int64(batch.Len())
				}
			}
			return totalRows, nil
		}},
		{"DuckDB", func() (int64, error) {
			dr, err := NewDuckDBParquetRawReader([]string{absPath}, 1000)
			if err != nil {
				return 0, err
			}
			defer dr.Close()

			totalRows := int64(0)
			for {
				batch, err := dr.Next(context.TODO())
				if err == io.EOF {
					break
				}
				if err != nil {
					return 0, err
				}
				if batch != nil {
					totalRows += int64(batch.Len())
				}
			}
			return totalRows, nil
		}},
		{"Arrow", func() (int64, error) {
			reader := bytes.NewReader(data)
			ar, err := NewArrowCookedReader(context.TODO(), reader, 1000)
			if err != nil {
				return 0, err
			}
			defer ar.Close()

			totalRows := int64(0)
			for {
				batch, err := ar.Next(context.TODO())
				if err == io.EOF {
					break
				}
				if err != nil {
					return 0, err
				}
				if batch != nil {
					totalRows += int64(batch.Len())
				}
			}
			return totalRows, nil
		}},
	}

	for _, reader := range readers {
		b.Run(reader.name, func(b *testing.B) {
			b.ReportAllocs()

			// Stabilize memory before benchmark
			runtime.GC()
			runtime.GC()
			time.Sleep(100 * time.Millisecond)

			// Capture baseline memory
			baseline, err := getSystemMemStats()
			if err != nil {
				b.Fatalf("Failed to get baseline: %v", err)
			}

			b.ResetTimer()

			var maxDelta MemoryDelta
			totalRowsProcessed := int64(0)

			for i := 0; i < b.N; i++ {
				before, _ := getSystemMemStats()

				rowsProcessed, err := reader.fn()
				if err != nil {
					b.Fatalf("Reader error: %v", err)
				}
				totalRowsProcessed += rowsProcessed

				after, _ := getSystemMemStats()
				delta := calculateDelta(before, after)

				// Track maximum memory usage
				if delta.ProcessRSSDelta > maxDelta.ProcessRSSDelta {
					maxDelta = delta
				}
			}

			// Report overall statistics
			finalStats, _ := getSystemMemStats()
			overallDelta := calculateDelta(baseline, finalStats)

			b.Logf("=== Overall Memory Impact ===")
			b.Logf("Total rows processed across %d iterations: %d", b.N, totalRowsProcessed)
			overallDelta.report(b, reader.name, totalRowsProcessed, 55)

			b.Logf("=== Peak Memory Usage ===")
			maxDelta.report(b, reader.name+"_Peak", 214, 55)
		})
	}
}

// TestMemoryLeakDetection checks for memory leaks by running readers multiple times
func TestMemoryLeakDetection(t *testing.T) {
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
				batch, err := pr.Next(context.TODO())
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
				batch, err := dr.Next(context.TODO())
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
			ar, err := NewArrowCookedReader(context.TODO(), reader, 1000)
			if err != nil {
				return err
			}
			defer ar.Close()

			for {
				batch, err := ar.Next(context.TODO())
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
		t.Run(reader.name+"_LeakTest", func(t *testing.T) {
			// Stabilize memory
			runtime.GC()
			runtime.GC()
			time.Sleep(100 * time.Millisecond)

			baseline, err := getSystemMemStats()
			if err != nil {
				t.Fatalf("Failed to get baseline: %v", err)
			}

			// Run multiple iterations to detect leaks
			iterations := 10
			var measurements []SystemMemStats

			for i := 0; i < iterations; i++ {
				if err := reader.fn(); err != nil {
					t.Fatalf("Iteration %d failed: %v", i, err)
				}

				// Force GC between iterations
				runtime.GC()
				runtime.GC()
				time.Sleep(50 * time.Millisecond)

				stats, err := getSystemMemStats()
				if err != nil {
					t.Logf("Warning: failed to get stats at iteration %d: %v", i, err)
					continue
				}
				measurements = append(measurements, stats)
			}

			if len(measurements) == 0 {
				t.Fatal("No measurements collected")
			}

			// Analyze memory growth trends
			first := measurements[0]
			last := measurements[len(measurements)-1]
			totalDelta := calculateDelta(baseline, last)

			t.Logf("=== %s Leak Detection Results ===", reader.name)
			t.Logf("Iterations: %d", iterations)
			totalDelta.report(t, reader.name, int64(iterations)*214, 55)

			// Check for concerning growth patterns
			rssGrowth := last.ProcessRSS - first.ProcessRSS
			heapGrowth := int64(last.GoHeapAlloc) - int64(first.GoHeapAlloc)

			t.Logf("--- Growth Analysis ---")
			t.Logf("RSS growth over %d iterations: %+d bytes", iterations, rssGrowth)
			t.Logf("Go heap growth over %d iterations: %+d bytes", iterations, heapGrowth)

			// Flag potential leaks (>1MB growth per 10 iterations is concerning)
			if rssGrowth > 1024*1024 {
				t.Logf("⚠️  WARNING: Potential memory leak detected (RSS grew by %.2f MB)",
					float64(rssGrowth)/(1024*1024))
			} else {
				t.Logf("✅ No significant memory leak detected")
			}
		})
	}
}

// TestPeakMemoryUsage measures peak memory usage during processing
func TestPeakMemoryUsage(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-2176772462.parquet"

	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to load test file: %v", err)
	}

	absPath, err := filepath.Abs(testFile)
	if err != nil {
		t.Fatalf("Failed to get absolute path: %v", err)
	}

	t.Run("ParquetRaw_Peak", func(t *testing.T) {
		measurePeakMemory(t, "ParquetRaw", func() error {
			reader := bytes.NewReader(data)
			pr, err := NewParquetRawReader(reader, int64(len(data)), 1000)
			if err != nil {
				return err
			}
			defer pr.Close()

			for {
				batch, err := pr.Next(context.TODO())
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				_ = batch
			}
			return nil
		})
	})

	t.Run("DuckDB_Peak", func(t *testing.T) {
		measurePeakMemory(t, "DuckDB", func() error {
			dr, err := NewDuckDBParquetRawReader([]string{absPath}, 1000)
			if err != nil {
				return err
			}
			defer dr.Close()

			for {
				batch, err := dr.Next(context.TODO())
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				_ = batch
			}
			return nil
		})
	})

	t.Run("Arrow_Peak", func(t *testing.T) {
		measurePeakMemory(t, "Arrow", func() error {
			reader := bytes.NewReader(data)
			ar, err := NewArrowCookedReader(context.TODO(), reader, 1000)
			if err != nil {
				return err
			}
			defer ar.Close()

			for {
				batch, err := ar.Next(context.TODO())
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				_ = batch
			}
			return nil
		})
	})
}

// measurePeakMemory monitors memory usage during function execution
func measurePeakMemory(t *testing.T, readerName string, fn func() error) {
	// Stabilize memory
	runtime.GC()
	runtime.GC()
	time.Sleep(100 * time.Millisecond)

	baseline, err := getSystemMemStats()
	if err != nil {
		t.Fatalf("Failed to get baseline: %v", err)
	}

	// Monitor memory during execution
	done := make(chan error, 1)
	var peakDelta MemoryDelta

	// Start monitoring in background
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				if stats, err := getSystemMemStats(); err == nil {
					delta := calculateDelta(baseline, stats)
					if delta.ProcessRSSDelta > peakDelta.ProcessRSSDelta {
						peakDelta = delta
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Execute the function
	err = fn()
	done <- err

	if err != nil {
		t.Fatalf("Function execution failed: %v", err)
	}

	// Get final stats
	final, err := getSystemMemStats()
	if err != nil {
		t.Fatalf("Failed to get final stats: %v", err)
	}

	finalDelta := calculateDelta(baseline, final)

	t.Logf("=== %s Peak Memory Analysis ===", readerName)
	t.Logf("Peak RSS: %+d bytes (%.2f MB)", peakDelta.ProcessRSSDelta, float64(peakDelta.ProcessRSSDelta)/(1024*1024))
	t.Logf("Final RSS: %+d bytes (%.2f MB)", finalDelta.ProcessRSSDelta, float64(finalDelta.ProcessRSSDelta)/(1024*1024))
	t.Logf("Peak Go heap: %+d bytes (%.2f MB)", peakDelta.GoHeapDelta, float64(peakDelta.GoHeapDelta)/(1024*1024))
	t.Logf("Final Go heap: %+d bytes (%.2f MB)", finalDelta.GoHeapDelta, float64(finalDelta.GoHeapDelta)/(1024*1024))

	peakNative := peakDelta.ProcessRSSDelta - peakDelta.GoHeapDelta
	finalNative := finalDelta.ProcessRSSDelta - finalDelta.GoHeapDelta

	t.Logf("Peak native memory: %+d bytes (%.2f MB)", peakNative, float64(peakNative)/(1024*1024))
	t.Logf("Final native memory: %+d bytes (%.2f MB)", finalNative, float64(finalNative)/(1024*1024))
}
