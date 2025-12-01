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

package perftest

import (
	"bytes"
	"context"
	"os"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/pipeline"
)

// BenchmarkSyntheticData tests both backends with varying row counts.
func BenchmarkSyntheticData(b *testing.B) {
	// Set production constraints
	debug.SetGCPercent(50)
	runtime.GOMAXPROCS(1)
	// Original limit: 1538 MB
	// Pre-alloc overhead: 590 MB (measured) * 1.25 = 738 MB
	// Adjusted limit: 1538 + 738 = 2276 MB
	debug.SetMemoryLimit(2386112948) // 2276 MB

	// Pre-allocate ALL 400k rows once (reused across all tests)
	gen := NewSyntheticDataGenerator()
	allBatches := gen.GenerateBatches(400000, 1000)
	b.Logf("Pre-allocated 400 batches (400k total rows) for reuse")

	// Scan all batches once to discover complete schema
	schema := scanAllBatchesForSchema(allBatches)
	b.Logf("Scanned schema with %d columns", len(schema.Columns()))

	rowCounts := []int{
		10000,
		25000,
		50000,
		100000,
		200000,
		400000,
	}

	backends := []parquetwriter.BackendType{
		parquetwriter.BackendGoParquet,
		parquetwriter.BackendArrow,
	}

	for _, rowCount := range rowCounts {
		for _, backendType := range backends {
			name := benchName(rowCount, backendType)
			b.Run(name, func(b *testing.B) {
				benchmarkSynthetic(b, backendType, rowCount, allBatches, schema)
			})
		}
	}

	// Clean up pre-allocated batches
	for _, batch := range allBatches {
		pipeline.ReturnBatch(batch)
	}
}

func benchName(rows int, backend parquetwriter.BackendType) string {
	if rows >= 1000 {
		return string(backend) + "-" + formatRowCount(rows)
	}
	return string(backend)
}

func formatRowCount(n int) string {
	if n >= 1000 {
		return string(rune('0'+(n/1000)%10)) + "k"
	}
	return string(rune('0' + n))
}

func benchmarkSynthetic(b *testing.B, backendType parquetwriter.BackendType, rowCount int, allBatches []*pipeline.Batch, schema *filereader.ReaderSchema) {
	ctx := context.Background()

	// Calculate how many batches we need from the pre-allocated pool
	batchesNeeded := (rowCount + 999) / 1000 // Round up
	batches := allBatches[:batchesNeeded]

	b.Logf("Using first %d batches (%d rows) from pre-allocated pool", batchesNeeded, rowCount)

	// Get baseline memory stats BEFORE starting benchmark
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)

	// Track peak memory DURING backend processing
	var peakHeapInuse uint64
	var peakSys uint64
	done := make(chan struct{})

	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				if m.HeapInuse > peakHeapInuse {
					peakHeapInuse = m.HeapInuse
				}
				if m.Sys > peakSys {
					peakSys = m.Sys
				}
			}
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Create temp directory
		tmpDir, err := os.MkdirTemp("", "synthetic-bench-*")
		require.NoError(b, err)

		// Create backend
		config := parquetwriter.BackendConfig{
			Type:      backendType,
			TmpDir:    tmpDir,
			ChunkSize: 10000,
			Schema:    schema,
			StringConversionPrefixes: []string{
				"resource_",
				"scope_",
				"attr_",
			},
		}

		var backend parquetwriter.ParquetBackend
		switch backendType {
		case parquetwriter.BackendGoParquet:
			backend, err = parquetwriter.NewGoParquetBackend(config)
		case parquetwriter.BackendArrow:
			backend, err = parquetwriter.NewArrowBackend(config)
		default:
			b.Fatalf("Unknown backend type: %s", backendType)
		}
		require.NoError(b, err)

		// Write only the batches we need (first N batches)
		for _, batch := range batches {
			err := backend.WriteBatch(ctx, batch)
			require.NoError(b, err)
		}

		// Close and write to output buffer
		var outputBuf bytes.Buffer
		metadata, err := backend.Close(ctx, &outputBuf)
		require.NoError(b, err)

		// Report metrics
		b.ReportMetric(float64(metadata.RowCount), "rows")
		b.ReportMetric(float64(metadata.ColumnCount), "columns")
		b.ReportMetric(float64(outputBuf.Len()), "output_bytes")

		_ = os.RemoveAll(tmpDir)
	}

	close(done)
	time.Sleep(20 * time.Millisecond)

	// Get final stats
	runtime.GC()
	var final runtime.MemStats
	runtime.ReadMemStats(&final)

	// Calculate metrics
	totalAlloc := final.TotalAlloc - baseline.TotalAlloc
	gcRuns := final.NumGC - baseline.NumGC
	totalPauseNs := final.PauseTotalNs - baseline.PauseTotalNs

	// Calculate peak memory increase (subtract baseline to isolate backend overhead)
	peakHeapDelta := peakHeapInuse - baseline.HeapInuse
	peakSysDelta := peakSys - baseline.Sys

	// Calculate throughput
	logsPerIteration := float64(rowCount)
	avgSecsPerOp := b.Elapsed().Seconds() / float64(b.N)
	throughput := logsPerIteration / avgSecsPerOp

	b.ReportMetric(throughput, "logs/sec")
	b.ReportMetric(float64(peakHeapDelta)/(1024*1024), "peak_heap_delta_MB")
	b.ReportMetric(float64(peakSysDelta)/(1024*1024), "peak_rss_delta_MB")
	b.ReportMetric(float64(totalAlloc)/(1024*1024), "total_alloc_MB")
	b.ReportMetric(float64(gcRuns), "gc_runs")
	b.ReportMetric(float64(totalPauseNs)/(1000*1000), "gc_pause_ms")
}

// scanAllBatchesForSchema scans all batches to discover the complete schema.
// This properly handles sparse columns by tracking which columns have non-null values.
// It applies string conversion to match what the backends will do at write time.
func scanAllBatchesForSchema(batches []*pipeline.Batch) *filereader.ReaderSchema {
	builder := filereader.NewSchemaBuilder()

	// String conversion prefixes (must match backend config)
	stringPrefixes := []string{"resource_", "scope_", "attr_"}

	for _, batch := range batches {
		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			if row == nil {
				continue
			}
			for key, value := range row {
				fieldName := string(key.Value())

				// Apply string conversion if needed (match backend behavior)
				convertedValue := value
				for _, prefix := range stringPrefixes {
					if len(fieldName) >= len(prefix) && fieldName[:len(prefix)] == prefix {
						// Convert non-string values to string
						if value != nil {
							if _, isString := value.(string); !isString {
								convertedValue = "string_placeholder"
							}
						}
						break
					}
				}

				builder.AddValue(fieldName, convertedValue)
			}
		}
	}

	return builder.Build()
}
