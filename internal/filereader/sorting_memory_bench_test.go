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

package filereader

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// SortingMemoryStats captures memory and performance metrics during sorting.
type SortingMemoryStats struct {
	PeakHeapInuse  uint64
	PeakHeapAlloc  uint64
	TotalAlloc     uint64
	NumGC          uint32
	GCPauseTotalNs uint64
	Duration       time.Duration
	RowsProcessed  int64
}

func (s SortingMemoryStats) String() string {
	return fmt.Sprintf(
		"Peak Heap: %.2f MB, Total Alloc: %.2f MB, GC Runs: %d, GC Pause: %.2f ms, Duration: %v, Rows: %d",
		float64(s.PeakHeapInuse)/(1024*1024),
		float64(s.TotalAlloc)/(1024*1024),
		s.NumGC,
		float64(s.GCPauseTotalNs)/(1000*1000),
		s.Duration,
		s.RowsProcessed,
	)
}

// generateLogRows creates synthetic log rows with realistic service identifiers.
// The rows are intentionally unsorted to test sorting performance.
func generateLogRows(numRows int, numServices int) []pipeline.Row {
	rng := rand.New(rand.NewSource(42))
	rows := make([]pipeline.Row, numRows)

	// Generate a pool of service names to pick from
	services := make([]string, numServices)
	for i := range services {
		services[i] = fmt.Sprintf("service-%d-%s", i, randomString(rng, 10))
	}

	// Generate a pool of customer domains (some rows will have these)
	domains := make([]string, numServices/2)
	for i := range domains {
		domains[i] = fmt.Sprintf("customer-%d.example.com", i)
	}

	baseTime := time.Now().UnixMilli()

	for i := range rows {
		row := make(pipeline.Row, 20)

		// Randomize timestamp to ensure sorting is needed
		row[wkk.RowKeyCTimestamp] = baseTime + int64(rng.Intn(numRows*1000))

		// Assign service name
		row[wkk.RowKeyResourceServiceName] = services[rng.Intn(len(services))]

		// 30% of rows have customer domain (which takes priority in sorting)
		if rng.Float32() < 0.3 {
			row[wkk.RowKeyResourceCustomerDomain] = domains[rng.Intn(len(domains))]
		}

		// Add realistic log data
		row[wkk.RowKeyCMessage] = generateLogMessage(rng)
		row[wkk.RowKeyCLevel] = randomSeverity(rng)
		row[wkk.RowKeyCFingerprint] = rng.Int63()

		// Resource attributes
		row[wkk.NewRowKey("resource_host_name")] = fmt.Sprintf("host-%d", rng.Intn(100))
		row[wkk.NewRowKey("resource_k8s_pod_name")] = fmt.Sprintf("pod-%d", rng.Intn(1000))
		row[wkk.NewRowKey("resource_k8s_namespace")] = randomNamespace(rng)

		// Sparse attributes
		if rng.Float32() < 0.3 {
			row[wkk.NewRowKey("attr_http_method")] = randomHTTPMethod(rng)
			row[wkk.NewRowKey("attr_http_status")] = rng.Intn(600)
			row[wkk.NewRowKey("attr_http_url")] = fmt.Sprintf("/api/v1/resource/%d", rng.Intn(10000))
		}

		if rng.Float32() < 0.2 {
			row[wkk.NewRowKey("attr_error_message")] = generateErrorMessage(rng)
			row[wkk.NewRowKey("attr_stack_trace")] = generateStackTrace(rng)
		}

		rows[i] = row
	}

	return rows
}

func randomString(rng *rand.Rand, length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rng.Intn(len(charset))]
	}
	return string(b)
}

func generateLogMessage(rng *rand.Rand) string {
	messages := []string{
		"Request processed successfully in 123ms",
		"Database query executed: SELECT * FROM users WHERE id = 12345",
		"Cache miss for key user:12345:profile",
		"User authentication successful for user@example.com",
		"API request received from 192.168.1.100",
		"Background job completed: email_notifications batch=100",
		"Webhook delivered to https://example.com/webhook",
		"File uploaded: document.pdf size=1234567 bytes",
		"Email sent to user@example.com subject=Welcome",
		"Payment processed for order 12345 amount=$99.99",
	}
	return messages[rng.Intn(len(messages))]
}

func randomSeverity(rng *rand.Rand) string {
	severities := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	return severities[rng.Intn(len(severities))]
}

func randomNamespace(rng *rand.Rand) string {
	namespaces := []string{"backend", "frontend", "jobs", "monitoring", "default"}
	return namespaces[rng.Intn(len(namespaces))]
}

func randomHTTPMethod(rng *rand.Rand) string {
	methods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	return methods[rng.Intn(len(methods))]
}

func generateErrorMessage(rng *rand.Rand) string {
	errors := []string{
		"Connection timeout after 30000ms",
		"Invalid input: field 'email' must be a valid email address",
		"Database constraint violation: duplicate key value",
		"Resource not found: user with id 12345 does not exist",
		"Authentication failed: invalid credentials",
	}
	return errors[rng.Intn(len(errors))]
}

func generateStackTrace(rng *rand.Rand) string {
	return fmt.Sprintf(
		"at processRequest (app.js:%d)\nat handleRoute (router.js:%d)\nat server.js:%d",
		rng.Intn(500), rng.Intn(200), rng.Intn(100),
	)
}

// measureSortingMemory runs a sorting operation and captures memory statistics.
func measureSortingMemory(createReader func([]pipeline.Row) (Reader, error), rows []pipeline.Row) (SortingMemoryStats, error) {
	ctx := context.Background()

	// Force GC and get baseline
	runtime.GC()
	runtime.GC()
	time.Sleep(10 * time.Millisecond)

	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)

	// Track peak memory during operation using atomic operations
	var peakHeapInuse, peakHeapAlloc atomic.Uint64
	done := make(chan struct{})

	go func() {
		ticker := time.NewTicker(1 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				for {
					old := peakHeapInuse.Load()
					if m.HeapInuse <= old || peakHeapInuse.CompareAndSwap(old, m.HeapInuse) {
						break
					}
				}
				for {
					old := peakHeapAlloc.Load()
					if m.HeapAlloc <= old || peakHeapAlloc.CompareAndSwap(old, m.HeapAlloc) {
						break
					}
				}
			}
		}
	}()

	start := time.Now()

	// Create and run the sorting reader
	reader, err := createReader(rows)
	if err != nil {
		close(done)
		return SortingMemoryStats{}, err
	}

	// Consume all rows
	var rowsProcessed int64
	for {
		batch, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = reader.Close()
			close(done)
			return SortingMemoryStats{}, err
		}
		rowsProcessed += int64(batch.Len())
		pipeline.ReturnBatch(batch)
	}

	_ = reader.Close()
	duration := time.Since(start)

	close(done)
	time.Sleep(5 * time.Millisecond) // Let goroutine exit

	// Get final stats
	runtime.GC()
	var final runtime.MemStats
	runtime.ReadMemStats(&final)

	// Handle potential underflow if peak wasn't captured properly
	peakInuse := peakHeapInuse.Load()
	peakAlloc := peakHeapAlloc.Load()
	peakHeapDelta := uint64(0)
	if peakInuse > baseline.HeapInuse {
		peakHeapDelta = peakInuse - baseline.HeapInuse
	}
	peakAllocDelta := uint64(0)
	if peakAlloc > baseline.HeapAlloc {
		peakAllocDelta = peakAlloc - baseline.HeapAlloc
	}

	return SortingMemoryStats{
		PeakHeapInuse:  peakHeapDelta,
		PeakHeapAlloc:  peakAllocDelta,
		TotalAlloc:     final.TotalAlloc - baseline.TotalAlloc,
		NumGC:          final.NumGC - baseline.NumGC,
		GCPauseTotalNs: final.PauseTotalNs - baseline.PauseTotalNs,
		Duration:       duration,
		RowsProcessed:  rowsProcessed,
	}, nil
}

// TestCompareSortingMemory is a test that compares memory usage between sorting approaches.
// Run with: go test -v -run TestCompareSortingMemory -timeout 10m
func TestCompareSortingMemory(t *testing.T) {
	// Constrain to single core like production
	runtime.GOMAXPROCS(1)
	debug.SetGCPercent(50)

	rowCounts := []int{10000, 50000, 100000}
	numServices := 50 // Realistic number of unique services

	for _, numRows := range rowCounts {
		t.Run(fmt.Sprintf("%dk_rows", numRows/1000), func(t *testing.T) {
			t.Logf("Generating %d synthetic log rows with %d unique services...", numRows, numServices)
			rows := generateLogRows(numRows, numServices)

			// Test 1: Memory sorting with LogSortKeyProvider
			t.Run("MemorySort", func(t *testing.T) {
				stats, err := measureSortingMemory(func(rows []pipeline.Row) (Reader, error) {
					mock := NewMockReader(rows)
					return NewMemorySortingReader(mock, &LogSortKeyProvider{}, 1000)
				}, rows)
				if err != nil {
					t.Fatalf("Memory sorting failed: %v", err)
				}
				t.Logf("MemorySort: %s", stats)
			})

			// Test 2: Disk sorting with LogSortKeyProvider
			t.Run("DiskSort", func(t *testing.T) {
				stats, err := measureSortingMemory(func(rows []pipeline.Row) (Reader, error) {
					mock := NewMockReader(rows)
					return NewDiskSortingReader(mock, &LogSortKeyProvider{}, 1000)
				}, rows)
				if err != nil {
					t.Fatalf("Disk sorting failed: %v", err)
				}
				t.Logf("DiskSort: %s", stats)
			})
		})
	}
}

// BenchmarkSortingApproaches provides detailed benchmark metrics for sorting.
// Run with: go test -bench=BenchmarkSortingApproaches -benchmem -benchtime=3x
func BenchmarkSortingApproaches(b *testing.B) {
	runtime.GOMAXPROCS(1)
	debug.SetGCPercent(50)

	numRows := 100000
	numServices := 50
	rows := generateLogRows(numRows, numServices)

	b.Logf("Generated %d rows with %d unique services", numRows, numServices)

	b.Run("MemorySort", func(b *testing.B) {
		benchmarkSorting(b, rows, func(rows []pipeline.Row) (Reader, error) {
			mock := NewMockReader(rows)
			return NewMemorySortingReader(mock, &LogSortKeyProvider{}, 1000)
		})
	})

	b.Run("DiskSort", func(b *testing.B) {
		benchmarkSorting(b, rows, func(rows []pipeline.Row) (Reader, error) {
			mock := NewMockReader(rows)
			return NewDiskSortingReader(mock, &LogSortKeyProvider{}, 1000)
		})
	})
}

func benchmarkSorting(b *testing.B, rows []pipeline.Row, createReader func([]pipeline.Row) (Reader, error)) {
	ctx := context.Background()

	// Get baseline
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)

	var peakHeapInuse atomic.Uint64
	done := make(chan struct{})

	go func() {
		ticker := time.NewTicker(1 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				for {
					old := peakHeapInuse.Load()
					if m.HeapInuse <= old || peakHeapInuse.CompareAndSwap(old, m.HeapInuse) {
						break
					}
				}
			}
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader, err := createReader(rows)
		if err != nil {
			b.Fatal(err)
		}

		var rowsRead int64
		for {
			batch, err := reader.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			rowsRead += int64(batch.Len())
			pipeline.ReturnBatch(batch)
		}

		_ = reader.Close()

		if rowsRead != int64(len(rows)) {
			b.Fatalf("Expected %d rows, got %d", len(rows), rowsRead)
		}
	}

	close(done)

	runtime.GC()
	var final runtime.MemStats
	runtime.ReadMemStats(&final)

	// Report custom metrics
	peak := peakHeapInuse.Load()
	peakMB := float64(0)
	if peak > baseline.HeapInuse {
		peakMB = float64(peak-baseline.HeapInuse) / (1024 * 1024)
	}
	totalAllocMB := float64(final.TotalAlloc-baseline.TotalAlloc) / float64(b.N) / (1024 * 1024)
	gcRuns := float64(final.NumGC-baseline.NumGC) / float64(b.N)
	gcPauseMs := float64(final.PauseTotalNs-baseline.PauseTotalNs) / float64(b.N) / 1e6

	b.ReportMetric(peakMB, "peak_heap_MB")
	b.ReportMetric(totalAllocMB, "alloc_per_op_MB")
	b.ReportMetric(gcRuns, "gc_per_op")
	b.ReportMetric(gcPauseMs, "gc_pause_ms")
	b.ReportMetric(float64(len(rows))/b.Elapsed().Seconds()*float64(b.N), "rows/sec")
}

// BenchmarkSortingScaling tests how memory scales with row count.
// Run with: go test -bench=BenchmarkSortingScaling -benchtime=1x
func BenchmarkSortingScaling(b *testing.B) {
	runtime.GOMAXPROCS(1)
	debug.SetGCPercent(50)

	rowCounts := []int{10000, 25000, 50000, 100000, 200000}
	numServices := 50

	for _, numRows := range rowCounts {
		rows := generateLogRows(numRows, numServices)

		b.Run(fmt.Sprintf("Memory_%dk", numRows/1000), func(b *testing.B) {
			benchmarkSorting(b, rows, func(rows []pipeline.Row) (Reader, error) {
				mock := NewMockReader(rows)
				return NewMemorySortingReader(mock, &LogSortKeyProvider{}, 1000)
			})
		})

		b.Run(fmt.Sprintf("Disk_%dk", numRows/1000), func(b *testing.B) {
			benchmarkSorting(b, rows, func(rows []pipeline.Row) (Reader, error) {
				mock := NewMockReader(rows)
				return NewDiskSortingReader(mock, &LogSortKeyProvider{}, 1000)
			})
		})
	}
}
