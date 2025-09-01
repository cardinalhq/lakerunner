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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// ReaderBenchmarkData holds preloaded test data and metadata
type ReaderBenchmarkData struct {
	Name     string
	FilePath string
	Data     []byte
	Size     int64
	RowCount int64
	ColCount int
}

// Global benchmark data - loaded once and reused across all reader benchmarks
var readerBenchmarkData *ReaderBenchmarkData

// loadReaderBenchmarkData preloads the largest metric parquet file for benchmarking
func loadReaderBenchmarkData(b *testing.B) *ReaderBenchmarkData {
	if readerBenchmarkData != nil {
		return readerBenchmarkData
	}

	// Use the largest metric file (by size) for comprehensive benchmarking
	testFile := "../../testdata/metrics/metrics-cooked-2176772462.parquet"

	data, err := os.ReadFile(testFile)
	if err != nil {
		b.Fatalf("Failed to load benchmark data %s: %v", testFile, err)
	}

	// Get actual row count from a quick read
	reader := bytes.NewReader(data)
	parquetReader, err := NewParquetRawReader(reader, int64(len(data)), 1000)
	if err != nil {
		b.Fatalf("Failed to create reader for %s: %v", testFile, err)
	}

	actualRowCount := int64(0)
	colCount := 0
	for {
		batch, err := parquetReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			b.Fatalf("Failed to count rows: %v", err)
		}
		if batch != nil {
			actualRowCount += int64(batch.Len())
			if colCount == 0 && batch.Len() > 0 {
				colCount = len(batch.Get(0)) // Get column count from first row
			}
		}
	}
	parquetReader.Close()

	readerBenchmarkData = &ReaderBenchmarkData{
		Name:     "metrics-large",
		FilePath: testFile,
		Data:     data,
		Size:     int64(len(data)),
		RowCount: actualRowCount,
		ColCount: colCount,
	}

	b.Logf("Loaded benchmark data: %s (%d bytes, %d rows, %d columns)",
		readerBenchmarkData.Name, readerBenchmarkData.Size, readerBenchmarkData.RowCount, readerBenchmarkData.ColCount)

	return readerBenchmarkData
}

// DetailedMemStats captures comprehensive memory statistics
type DetailedMemStats struct {
	AllocsBefore    uint64
	AllocsAfter     uint64
	BytesBefore     uint64
	BytesAfter      uint64
	HeapSizeBefore  uint64
	HeapSizeAfter   uint64
	StackSizeBefore uint64
	StackSizeAfter  uint64
	GCCountBefore   uint32
	GCCountAfter    uint32
	GCPauseBefore   uint64
	GCPauseAfter    uint64
	HeapObjects     uint64
	HeapReleased    uint64
}

// captureDetailedMemStatsBefore captures comprehensive memory stats before operation
func captureDetailedMemStatsBefore() DetailedMemStats {
	// Force GC to get clean baseline
	runtime.GC()
	runtime.GC()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return DetailedMemStats{
		AllocsBefore:    m.TotalAlloc,
		BytesBefore:     m.Alloc,
		HeapSizeBefore:  m.HeapAlloc,
		StackSizeBefore: m.StackInuse,
		GCCountBefore:   m.NumGC,
		GCPauseBefore:   m.PauseTotalNs,
		HeapObjects:     m.HeapObjects,
		HeapReleased:    m.HeapReleased,
	}
}

// captureDetailedMemStatsAfter captures comprehensive memory stats after operation
func (ms *DetailedMemStats) captureDetailedMemStatsAfter() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	ms.AllocsAfter = m.TotalAlloc
	ms.BytesAfter = m.Alloc
	ms.HeapSizeAfter = m.HeapAlloc
	ms.StackSizeAfter = m.StackInuse
	ms.GCCountAfter = m.NumGC
	ms.GCPauseAfter = m.PauseTotalNs
}

// reportDetailed prints comprehensive memory analysis
func (ms *DetailedMemStats) reportDetailed(b *testing.B, readerType string, data *ReaderBenchmarkData, rowsProcessed int64) {
	allocsDelta := ms.AllocsAfter - ms.AllocsBefore
	heapDelta := int64(ms.HeapSizeAfter) - int64(ms.HeapSizeBefore)
	stackDelta := int64(ms.StackSizeAfter) - int64(ms.StackSizeBefore)
	gcDelta := ms.GCCountAfter - ms.GCCountBefore
	pauseDelta := ms.GCPauseAfter - ms.GCPauseBefore

	b.Logf("=== %s Reader Memory Analysis ===", readerType)
	b.Logf("File: %s (%d bytes)", data.Name, data.Size)
	b.Logf("Rows processed: %d/%d (%.1f%%)", rowsProcessed, data.RowCount, float64(rowsProcessed)/float64(data.RowCount)*100)
	b.Logf("Columns per row: %d", data.ColCount)

	b.Logf("--- Memory Allocation ---")
	b.Logf("Total allocations: %d bytes (%d increase)", ms.AllocsAfter, allocsDelta)
	b.Logf("Current heap: %d bytes (%+d change)", ms.BytesAfter, heapDelta)
	b.Logf("Stack usage: %d bytes (%+d change)", ms.StackSizeAfter, stackDelta)
	b.Logf("Heap objects: %d", ms.HeapObjects)
	b.Logf("Heap released: %d bytes", ms.HeapReleased)

	b.Logf("--- Garbage Collection ---")
	b.Logf("GC runs: %d", gcDelta)
	avgPause := float64(0)
	if gcDelta > 0 {
		avgPause = float64(pauseDelta) / float64(gcDelta) / 1_000_000 // Convert to ms
	}
	b.Logf("GC pause total: %.2f ms (avg %.2f ms per run)", float64(pauseDelta)/1_000_000, avgPause)

	if rowsProcessed > 0 {
		b.Logf("--- Per-Row Metrics ---")
		b.Logf("Allocation per row: %d bytes", allocsDelta/uint64(rowsProcessed))
		b.Logf("Heap change per row: %+d bytes", heapDelta/rowsProcessed)

		if data.ColCount > 0 {
			totalFields := rowsProcessed * int64(data.ColCount)
			b.Logf("Allocation per field: %d bytes", allocsDelta/uint64(totalFields))
		}
	}

	b.Logf("--- Efficiency Metrics ---")
	if data.Size > 0 {
		efficiency := float64(allocsDelta) / float64(data.Size)
		b.Logf("Memory overhead ratio: %.2fx (allocated %d bytes for %d byte file)", efficiency, allocsDelta, data.Size)
	}
}

// BenchmarkParquetRawReader benchmarks the native parquet-go reader
func BenchmarkParquetRawReader(b *testing.B) {
	data := loadReaderBenchmarkData(b)

	b.ResetTimer()
	b.ReportAllocs()

	ms := captureDetailedMemStatsBefore()
	totalRows := int64(0)

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data.Data)
		parquetReader, err := NewParquetRawReader(reader, data.Size, 1000)
		if err != nil {
			b.Fatalf("Failed to create ParquetRawReader: %v", err)
		}

		rowsThisIter := int64(0)
		for {
			batch, err := parquetReader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("ParquetRawReader read error: %v", err)
			}
			if batch != nil {
				rowsThisIter += int64(batch.Len())
			}
		}

		if rowsThisIter != data.RowCount {
			b.Fatalf("Expected %d rows, got %d", data.RowCount, rowsThisIter)
		}

		totalRows += rowsThisIter
		parquetReader.Close()
	}

	ms.captureDetailedMemStatsAfter()
	ms.reportDetailed(b, "ParquetRaw", data, totalRows)
}

// BenchmarkDuckDBParquetReader benchmarks the DuckDB-based reader
func BenchmarkDuckDBParquetReader(b *testing.B) {
	data := loadReaderBenchmarkData(b)

	b.ResetTimer()
	b.ReportAllocs()

	ms := captureDetailedMemStatsBefore()
	totalRows := int64(0)

	for i := 0; i < b.N; i++ {
		// DuckDB reader works with file paths, so use the original file
		absPath, err := filepath.Abs(data.FilePath)
		if err != nil {
			b.Fatalf("Failed to get absolute path: %v", err)
		}

		duckdbReader, err := NewDuckDBParquetRawReader([]string{absPath}, 1000)
		if err != nil {
			b.Fatalf("Failed to create DuckDBParquetRawReader: %v", err)
		}

		rowsThisIter := int64(0)
		for {
			batch, err := duckdbReader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("DuckDBParquetRawReader read error: %v", err)
			}
			if batch != nil {
				rowsThisIter += int64(batch.Len())
			}
		}

		if rowsThisIter != data.RowCount {
			b.Fatalf("Expected %d rows, got %d", data.RowCount, rowsThisIter)
		}

		totalRows += rowsThisIter
		duckdbReader.Close()
	}

	ms.captureDetailedMemStatsAfter()
	ms.reportDetailed(b, "DuckDB", data, totalRows)
}

// BenchmarkArrowCookedReader benchmarks the Apache Arrow reader
func BenchmarkArrowCookedReader(b *testing.B) {
	data := loadReaderBenchmarkData(b)

	b.ResetTimer()
	b.ReportAllocs()

	ms := captureDetailedMemStatsBefore()
	totalRows := int64(0)

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data.Data)
		// Arrow reader needs a ReaderAtSeeker
		arrowReader, err := NewArrowCookedReader(reader, 1000)
		if err != nil {
			b.Fatalf("Failed to create ArrowCookedReader: %v", err)
		}

		rowsThisIter := int64(0)
		for {
			batch, err := arrowReader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("ArrowCookedReader read error: %v", err)
			}
			if batch != nil {
				rowsThisIter += int64(batch.Len())
			}
		}

		if rowsThisIter != data.RowCount {
			b.Fatalf("Expected %d rows, got %d", data.RowCount, rowsThisIter)
		}

		totalRows += rowsThisIter
		arrowReader.Close()
	}

	ms.captureDetailedMemStatsAfter()
	ms.reportDetailed(b, "Arrow", data, totalRows)
}

// BenchmarkAllReadersComparison runs all three readers in sequence for direct comparison
func BenchmarkAllReadersComparison(b *testing.B) {
	data := loadReaderBenchmarkData(b)

	readers := []struct {
		name string
		fn   func(b *testing.B, data *ReaderBenchmarkData) (int64, DetailedMemStats)
	}{
		{"ParquetRaw", benchmarkParquetRawReaderInternal},
		{"DuckDB", benchmarkDuckDBReaderInternal},
		{"Arrow", benchmarkArrowReaderInternal},
	}

	b.ResetTimer()

	for _, reader := range readers {
		b.Run(reader.name, func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				rowsProcessed, ms := reader.fn(b, data)
				if i == 0 { // Only report detailed stats for first iteration
					ms.reportDetailed(b, reader.name, data, rowsProcessed)
				}
			}
		})
	}
}

// Internal benchmark functions that return metrics for comparison

func benchmarkParquetRawReaderInternal(b *testing.B, data *ReaderBenchmarkData) (int64, DetailedMemStats) {
	ms := captureDetailedMemStatsBefore()

	reader := bytes.NewReader(data.Data)
	parquetReader, err := NewParquetRawReader(reader, data.Size, 1000)
	if err != nil {
		b.Fatalf("Failed to create ParquetRawReader: %v", err)
	}
	defer parquetReader.Close()

	totalRows := int64(0)
	for {
		batch, err := parquetReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			b.Fatalf("ParquetRawReader read error: %v", err)
		}
		if batch != nil {
			totalRows += int64(batch.Len())
		}
	}

	ms.captureDetailedMemStatsAfter()
	return totalRows, ms
}

func benchmarkDuckDBReaderInternal(b *testing.B, data *ReaderBenchmarkData) (int64, DetailedMemStats) {
	ms := captureDetailedMemStatsBefore()

	absPath, err := filepath.Abs(data.FilePath)
	if err != nil {
		b.Fatalf("Failed to get absolute path: %v", err)
	}

	duckdbReader, err := NewDuckDBParquetRawReader([]string{absPath}, 1000)
	if err != nil {
		b.Fatalf("Failed to create DuckDBParquetRawReader: %v", err)
	}
	defer duckdbReader.Close()

	totalRows := int64(0)
	for {
		batch, err := duckdbReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			b.Fatalf("DuckDBParquetRawReader read error: %v", err)
		}
		if batch != nil {
			totalRows += int64(batch.Len())
		}
	}

	ms.captureDetailedMemStatsAfter()
	return totalRows, ms
}

func benchmarkArrowReaderInternal(b *testing.B, data *ReaderBenchmarkData) (int64, DetailedMemStats) {
	ms := captureDetailedMemStatsBefore()

	reader := bytes.NewReader(data.Data)
	arrowReader, err := NewArrowCookedReader(reader, 1000)
	if err != nil {
		b.Fatalf("Failed to create ArrowCookedReader: %v", err)
	}
	defer arrowReader.Close()

	totalRows := int64(0)
	for {
		batch, err := arrowReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			b.Fatalf("ArrowCookedReader read error: %v", err)
		}
		if batch != nil {
			totalRows += int64(batch.Len())
		}
	}

	ms.captureDetailedMemStatsAfter()
	return totalRows, ms
}

// BenchmarkReadersWithDifferentBatchSizes tests all readers with various batch sizes
func BenchmarkReadersWithDifferentBatchSizes(b *testing.B) {
	data := loadReaderBenchmarkData(b)
	batchSizes := []int{1, 10, 100, 1000, 5000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			readers := []struct {
				name string
				fn   func() (int64, error)
			}{
				{"ParquetRaw", func() (int64, error) {
					reader := bytes.NewReader(data.Data)
					pr, err := NewParquetRawReader(reader, data.Size, batchSize)
					if err != nil {
						return 0, err
					}
					defer pr.Close()

					totalRows := int64(0)
					for {
						batch, err := pr.Next()
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
					absPath, err := filepath.Abs(data.FilePath)
					if err != nil {
						return 0, err
					}

					dr, err := NewDuckDBParquetRawReader([]string{absPath}, batchSize)
					if err != nil {
						return 0, err
					}
					defer dr.Close()

					totalRows := int64(0)
					for {
						batch, err := dr.Next()
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
					reader := bytes.NewReader(data.Data)
					ar, err := NewArrowCookedReader(reader, batchSize)
					if err != nil {
						return 0, err
					}
					defer ar.Close()

					totalRows := int64(0)
					for {
						batch, err := ar.Next()
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

					for i := 0; i < b.N; i++ {
						rowCount, err := reader.fn()
						if err != nil {
							b.Fatalf("%s reader error: %v", reader.name, err)
						}
						if rowCount != data.RowCount {
							b.Fatalf("Expected %d rows, got %d", data.RowCount, rowCount)
						}
					}
				})
			}
		})
	}
}

// BenchmarkReadersGCPressure analyzes GC pressure for all readers
func BenchmarkReadersGCPressure(b *testing.B) {
	data := loadReaderBenchmarkData(b)

	readers := []struct {
		name string
		fn   func() error
	}{
		{"ParquetRaw", func() error {
			reader := bytes.NewReader(data.Data)
			pr, err := NewParquetRawReader(reader, data.Size, 1000)
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
			absPath, err := filepath.Abs(data.FilePath)
			if err != nil {
				return err
			}

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
			reader := bytes.NewReader(data.Data)
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
		b.Run(reader.name, func(b *testing.B) {
			runtime.GC()
			runtime.GC()

			var initialStats runtime.MemStats
			runtime.ReadMemStats(&initialStats)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if err := reader.fn(); err != nil {
					b.Fatalf("%s reader error: %v", reader.name, err)
				}
			}

			var finalStats runtime.MemStats
			runtime.ReadMemStats(&finalStats)

			totalGCRuns := finalStats.NumGC - initialStats.NumGC
			totalGCPause := finalStats.PauseTotalNs - initialStats.PauseTotalNs

			b.Logf("=== %s GC Pressure Analysis ===", reader.name)
			b.Logf("Total GC runs: %d", totalGCRuns)
			b.Logf("Total GC pause time: %.2f ms", float64(totalGCPause)/1_000_000)
			if totalGCRuns > 0 {
				b.Logf("Average GC pause: %.2f ms", float64(totalGCPause)/float64(totalGCRuns)/1_000_000)
			}
			b.Logf("Final heap size: %d MB", finalStats.HeapAlloc/(1024*1024))
		})
	}
}
