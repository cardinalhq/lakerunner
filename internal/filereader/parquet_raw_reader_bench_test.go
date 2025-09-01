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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"testing"
)

// BenchmarkData holds preloaded test data to eliminate disk I/O from benchmarks
type BenchmarkData struct {
	Name      string
	Data      []byte
	Size      int64
	RowCount  int64
	FieldsAvg int
}

// Global benchmark data - loaded once and reused
var benchmarkFiles = map[string]*BenchmarkData{
	"small":  nil, // logs-cooked-0001.parquet (32 rows)
	"medium": nil, // metrics-cooked-0001.parquet (211 rows)
	"large":  nil, // logs-chqs3-0001.parquet (1807 rows)
}

// loadBenchmarkData preloads all test files into memory
func loadBenchmarkData(b *testing.B) {
	if benchmarkFiles["small"] != nil {
		return // Already loaded
	}

	testFiles := map[string]string{
		"small":  "../../testdata/logs/logs-cooked-0001.parquet",
		"medium": "../../testdata/metrics/metrics-cooked-0001.parquet",
		"large":  "../../testdata/logs/logs-chqs3-0001.parquet",
	}

	expectedRows := map[string]int64{
		"small":  32,
		"medium": 211,
		"large":  1807,
	}

	for key, filename := range testFiles {
		data, err := os.ReadFile(filename)
		if err != nil {
			b.Fatalf("Failed to load benchmark data %s: %v", filename, err)
		}

		// Count fields by reading a few rows
		reader := bytes.NewReader(data)
		parquetReader, err := NewParquetRawReader(reader, int64(len(data)), 1000)
		if err != nil {
			b.Fatalf("Failed to create reader for %s: %v", filename, err)
		}

		// Sample first few rows to get average field count
		batch, _ := parquetReader.Next(context.TODO())
		parquetReader.Close()

		totalFields := 0
		n := 0
		if batch != nil {
			n = batch.Len()
			for i := 0; i < n; i++ {
				totalFields += len(batch.Get(i))
			}
		}
		avgFields := 15 // default
		if n > 0 {
			avgFields = totalFields / n
		}

		benchmarkFiles[key] = &BenchmarkData{
			Name:      key,
			Data:      data,
			Size:      int64(len(data)),
			RowCount:  expectedRows[key],
			FieldsAvg: avgFields,
		}
	}
}

// createReaderFromData creates a parquet reader from preloaded data
func createReaderFromData(data *BenchmarkData) (*ParquetRawReader, error) {
	reader := bytes.NewReader(data.Data)
	return NewParquetRawReader(reader, data.Size, 1000)
}

// MemStats captures memory statistics before/after operations
type MemStats struct {
	AllocsBefore   uint64
	AllocsAfter    uint64
	BytesBefore    uint64
	BytesAfter     uint64
	GCCountBefore  uint32
	GCCountAfter   uint32
	HeapObjects    uint64
	HeapSizeBefore uint64
	HeapSizeAfter  uint64
}

// captureMemStatsBefore captures memory stats before an operation
func captureMemStatsBefore() MemStats {
	runtime.GC() // Force GC to get clean baseline
	runtime.GC() // Second GC to ensure everything is cleaned up

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return MemStats{
		AllocsBefore:   m.TotalAlloc,
		BytesBefore:    m.Alloc,
		GCCountBefore:  m.NumGC,
		HeapSizeBefore: m.HeapAlloc,
		HeapObjects:    m.HeapObjects,
	}
}

// captureMemStatsAfter captures memory stats after an operation
func (ms *MemStats) captureMemStatsAfter() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	ms.AllocsAfter = m.TotalAlloc
	ms.BytesAfter = m.Alloc
	ms.GCCountAfter = m.NumGC
	ms.HeapSizeAfter = m.HeapAlloc
}

// report prints detailed memory analysis
func (ms *MemStats) report(b *testing.B, operation string, rowsProcessed int64, fieldsPerRow int) {
	allocsDelta := ms.AllocsAfter - ms.AllocsBefore
	bytesDelta := int64(ms.BytesAfter) - int64(ms.BytesBefore)
	gcDelta := ms.GCCountAfter - ms.GCCountBefore
	heapDelta := int64(ms.HeapSizeAfter) - int64(ms.HeapSizeBefore)

	b.Logf("=== Memory Analysis: %s ===", operation)
	b.Logf("Rows processed: %d, Fields per row (avg): %d", rowsProcessed, fieldsPerRow)
	b.Logf("Total allocations: %d bytes (%d increase)", ms.AllocsAfter, allocsDelta)
	b.Logf("Current heap: %d bytes (%+d change)", ms.BytesAfter, bytesDelta)
	b.Logf("Heap objects: %d", ms.HeapObjects)
	b.Logf("GC runs during operation: %d", gcDelta)
	b.Logf("Heap size change: %+d bytes", heapDelta)

	if rowsProcessed > 0 {
		bytesPerRow := allocsDelta / uint64(rowsProcessed)
		heapPerRow := heapDelta / rowsProcessed
		b.Logf("Allocation per row: %d bytes", bytesPerRow)
		b.Logf("Heap change per row: %+d bytes", heapPerRow)

		if fieldsPerRow > 0 {
			bytesPerField := allocsDelta / uint64(rowsProcessed*int64(fieldsPerRow))
			b.Logf("Allocation per field: %d bytes", bytesPerField)
		}
	}
}

// BenchmarkParquetRawReader_Small benchmarks small file reading
func BenchmarkParquetRawReader_Small(b *testing.B) {
	loadBenchmarkData(b)
	data := benchmarkFiles["small"]

	b.ResetTimer()
	b.ReportAllocs()

	ms := captureMemStatsBefore()
	totalRows := int64(0)

	for i := 0; i < b.N; i++ {
		reader, err := createReaderFromData(data)
		if err != nil {
			b.Fatalf("Failed to create reader: %v", err)
		}

		rowsThisIter := int64(0)
		for {
			batch, err := reader.Next(context.TODO())
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("Read error: %v", err)
			}
			if batch != nil {
				rowsThisIter += int64(batch.Len())
			}
		}

		totalRows += rowsThisIter
		reader.Close()
	}

	ms.captureMemStatsAfter()
	ms.report(b, fmt.Sprintf("Small file (%s)", data.Name), totalRows, data.FieldsAvg)
}

// BenchmarkParquetRawReader_Medium benchmarks medium file reading
func BenchmarkParquetRawReader_Medium(b *testing.B) {
	loadBenchmarkData(b)
	data := benchmarkFiles["medium"]

	b.ResetTimer()
	b.ReportAllocs()

	ms := captureMemStatsBefore()
	totalRows := int64(0)

	for i := 0; i < b.N; i++ {
		reader, err := createReaderFromData(data)
		if err != nil {
			b.Fatalf("Failed to create reader: %v", err)
		}

		rowsThisIter := int64(0)
		for {
			batch, err := reader.Next(context.TODO())
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("Read error: %v", err)
			}
			if batch != nil {
				rowsThisIter += int64(batch.Len())
			}
		}

		totalRows += rowsThisIter
		reader.Close()
	}

	ms.captureMemStatsAfter()
	ms.report(b, fmt.Sprintf("Medium file (%s)", data.Name), totalRows, data.FieldsAvg)
}

// BenchmarkParquetRawReader_Large benchmarks large file reading
func BenchmarkParquetRawReader_Large(b *testing.B) {
	loadBenchmarkData(b)
	data := benchmarkFiles["large"]

	b.ResetTimer()
	b.ReportAllocs()

	ms := captureMemStatsBefore()
	totalRows := int64(0)

	for i := 0; i < b.N; i++ {
		reader, err := createReaderFromData(data)
		if err != nil {
			b.Fatalf("Failed to create reader: %v", err)
		}

		rowsThisIter := int64(0)
		for {
			batch, err := reader.Next(context.TODO())
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("Read error: %v", err)
			}
			if batch != nil {
				rowsThisIter += int64(batch.Len())
			}
		}

		totalRows += rowsThisIter
		reader.Close()
	}

	ms.captureMemStatsAfter()
	ms.report(b, fmt.Sprintf("Large file (%s)", data.Name), totalRows, data.FieldsAvg)
}

// BenchmarkParquetRawReader_BatchSizes tests different batch sizes
func BenchmarkParquetRawReader_BatchSizes(b *testing.B) {
	loadBenchmarkData(b)
	data := benchmarkFiles["medium"]

	batchSizes := []int{1, 10, 50, 100, 500}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			ms := captureMemStatsBefore()
			totalRows := int64(0)

			for i := 0; i < b.N; i++ {
				reader, err := createReaderFromData(data)
				if err != nil {
					b.Fatalf("Failed to create reader: %v", err)
				}

				rowsThisIter := int64(0)
				for {
					batch, err := reader.Next(context.TODO())
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatalf("Read error: %v", err)
					}
					if batch != nil {
						rowsThisIter += int64(batch.Len())
					}
				}

				totalRows += rowsThisIter
				reader.Close()
			}

			ms.captureMemStatsAfter()
			ms.report(b, fmt.Sprintf("Batch size %d", batchSize), totalRows, data.FieldsAvg)
		})
	}
}

// BenchmarkParquetRawReader_MemoryProfile profiles memory usage patterns
func BenchmarkParquetRawReader_MemoryProfile(b *testing.B) {
	loadBenchmarkData(b)
	data := benchmarkFiles["large"]

	b.ResetTimer()

	// Single iteration with detailed memory tracking
	reader, err := createReaderFromData(data)
	if err != nil {
		b.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	totalRows := int64(0)
	iterationCount := 0

	for {
		// Capture memory before each batch read
		var mBefore runtime.MemStats
		runtime.ReadMemStats(&mBefore)

		batch, err := reader.Next(context.TODO())
		iterationCount++

		n := 0
		if batch != nil {
			n = batch.Len()
			totalRows += int64(n)
		}

		// Capture memory after each batch read
		var mAfter runtime.MemStats
		runtime.ReadMemStats(&mAfter)

		allocDelta := mAfter.TotalAlloc - mBefore.TotalAlloc
		heapDelta := int64(mAfter.HeapAlloc) - int64(mBefore.HeapAlloc)

		if iterationCount <= 5 || iterationCount%10 == 0 {
			b.Logf("Batch %d: read %d rows, allocated %d bytes, heap change %+d bytes",
				iterationCount, n, allocDelta, heapDelta)
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			b.Fatalf("Read error: %v", err)
		}
	}

	b.Logf("Memory profile complete: %d batches, %d total rows", iterationCount, totalRows)
}

// BenchmarkParquetRawReader_GCPressure measures GC pressure
func BenchmarkParquetRawReader_GCPressure(b *testing.B) {
	loadBenchmarkData(b)
	data := benchmarkFiles["large"]

	b.ResetTimer()

	// Force clean state
	runtime.GC()
	runtime.GC()

	var initialStats runtime.MemStats
	runtime.ReadMemStats(&initialStats)

	reader, err := createReaderFromData(data)
	if err != nil {
		b.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	totalRows := int64(0)
	gcCheckInterval := 50 // Check GC stats every N batches
	batchCount := 0

	for {
		batch, err := reader.Next(context.TODO())
		batchCount++

		if batch != nil {
			totalRows += int64(batch.Len())
		}

		// Periodically check GC stats
		if batchCount%gcCheckInterval == 0 {
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)

			gcRuns := stats.NumGC - initialStats.NumGC
			gcPauseTotal := stats.PauseTotalNs - initialStats.PauseTotalNs
			avgPause := float64(0)
			if gcRuns > 0 {
				avgPause = float64(gcPauseTotal) / float64(gcRuns) / 1_000_000 // Convert to ms
			}

			b.Logf("After %d batches (%d rows): %d GC runs, avg pause %.2f ms, heap %d MB",
				batchCount, totalRows, gcRuns, avgPause, stats.HeapAlloc/(1024*1024))
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			b.Fatalf("Read error: %v", err)
		}
	}

	// Final GC stats
	var finalStats runtime.MemStats
	runtime.ReadMemStats(&finalStats)

	totalGCRuns := finalStats.NumGC - initialStats.NumGC
	totalGCPause := finalStats.PauseTotalNs - initialStats.PauseTotalNs
	avgGCPause := float64(totalGCPause) / float64(totalGCRuns) / 1_000_000

	b.Logf("=== GC Pressure Analysis ===")
	b.Logf("Total rows processed: %d", totalRows)
	b.Logf("Total GC runs: %d", totalGCRuns)
	b.Logf("Total GC pause time: %.2f ms", float64(totalGCPause)/1_000_000)
	b.Logf("Average GC pause: %.2f ms", avgGCPause)
	b.Logf("GC frequency: %.2f runs per 1000 rows", float64(totalGCRuns)*1000/float64(totalRows))
	b.Logf("Final heap size: %d MB", finalStats.HeapAlloc/(1024*1024))
}
