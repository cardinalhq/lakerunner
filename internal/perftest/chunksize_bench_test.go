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
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/pipeline"
)

// BenchmarkChunkSizeComparison tests different chunk sizes with both backends
// to understand memory impact of delaying schema finalization.
//
// Tests chunk sizes: 10K, 25K, 50K rows
// Tests backends: go-parquet, arrow
// Uses real 400K row dataset for realistic schema evolution patterns
func BenchmarkChunkSizeComparison(b *testing.B) {
	// Find raw OTEL files
	files, err := filepath.Glob(filepath.Join(testDataDir, "raw", "logs_*.binpb.gz"))
	if err != nil || len(files) == 0 {
		b.Skip("No raw test data found. Run: ./scripts/download-perf-testdata.sh raw 10")
	}

	// Use the largest file for testing
	testFile := files[0]
	for _, f := range files {
		fInfo, _ := os.Stat(f)
		testInfo, _ := os.Stat(testFile)
		if fInfo.Size() > testInfo.Size() {
			testFile = f
		}
	}

	ctx := context.Background()

	// Force single-core operation
	oldMaxProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldMaxProcs)

	// Pre-load batches to exclude read overhead
	options := filereader.ReaderOptions{
		SignalType: filereader.SignalTypeLogs,
		BatchSize:  1000,
		OrgID:      "test-org",
	}

	reader, err := filereader.ReaderForFileWithOptions(testFile, options)
	if err != nil {
		b.Fatal(err)
	}

	var batches []*pipeline.Batch
	var totalLogs int64

	for {
		batch, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = reader.Close()
			b.Fatal(err)
		}
		batches = append(batches, batch)
		totalLogs += int64(batch.Len())
	}
	_ = reader.Close()

	b.Logf("Pre-loaded %d batches (%d logs) for chunk size tests", len(batches), totalLogs)

	// Test configurations: backend x chunk size
	testCases := []struct {
		backend   parquetwriter.BackendType
		chunkSize int64
	}{
		{parquetwriter.BackendGoParquet, 10000},
		{parquetwriter.BackendGoParquet, 25000},
		{parquetwriter.BackendGoParquet, 50000},
		{parquetwriter.BackendArrow, 10000},
		{parquetwriter.BackendArrow, 25000},
		{parquetwriter.BackendArrow, 50000},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("%s/chunk-%dk", tc.backend, tc.chunkSize/1000)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				tmpDir := b.TempDir()

				timer := NewTimer()
				sampler := NewMemorySampler(timer, 50*1000000) // Sample every 50ms
				sampler.Start()

				b.StartTimer()

				// Create backend with specific chunk size
				config := parquetwriter.BackendConfig{
					Type:      tc.backend,
					TmpDir:    tmpDir,
					ChunkSize: tc.chunkSize,
					StringConversionPrefixes: []string{
						"resource_",
						"scope_",
						"attr_",
					},
				}

				var backend parquetwriter.ParquetBackend
				if tc.backend == parquetwriter.BackendArrow {
					backend, err = parquetwriter.NewArrowBackend(config)
				} else {
					backend, err = parquetwriter.NewGoParquetBackend(config)
				}
				if err != nil {
					b.Fatal(err)
				}

				// Write all batches
				for _, batch := range batches {
					if err := backend.WriteBatch(ctx, batch); err != nil {
						backend.Abort()
						b.Fatal(err)
					}
					timer.AddLogs(int64(batch.Len()))
				}

				// Close and write to temp file
				outputPath := filepath.Join(tmpDir, "output.parquet")
				outputFile, err := os.Create(outputPath)
				if err != nil {
					backend.Abort()
					b.Fatal(err)
				}

				metadata, err := backend.Close(ctx, outputFile)
				if err != nil {
					_ = outputFile.Close()
					b.Fatal(err)
				}
				_ = outputFile.Close()

				// Get file size
				stat, _ := os.Stat(outputPath)
				fileSize := stat.Size()

				b.StopTimer()
				sampler.Stop()
				metrics := timer.Stop()

				if i == 0 {
					b.Logf("\n%s", metrics.Report(fmt.Sprintf("%s (chunk=%dk)", tc.backend, tc.chunkSize/1000)))
					b.Logf("Output: %d rows, %d columns, %d bytes (%.2f KB)", metadata.RowCount, metadata.ColumnCount, fileSize, float64(fileSize)/1024)
				}
			}

			logsPerSec := float64(totalLogs*int64(b.N)) / b.Elapsed().Seconds()
			b.ReportMetric(logsPerSec, "logs/sec")
		})
	}
}

// BenchmarkChunkSizeMemoryProfile runs single iterations for memory profiling
func BenchmarkChunkSizeMemoryProfile(b *testing.B) {
	files, err := filepath.Glob(filepath.Join(testDataDir, "raw", "logs_*.binpb.gz"))
	if err != nil || len(files) == 0 {
		b.Skip("No raw test data found. Run: ./scripts/download-perf-testdata.sh raw 10")
	}

	testFile := files[0]
	ctx := context.Background()

	// Force single-core
	oldMaxProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldMaxProcs)

	// Pre-load batches
	options := filereader.ReaderOptions{
		SignalType: filereader.SignalTypeLogs,
		BatchSize:  1000,
		OrgID:      "test-org",
	}

	reader, err := filereader.ReaderForFileWithOptions(testFile, options)
	if err != nil {
		b.Fatal(err)
	}

	var batches []*pipeline.Batch
	for {
		batch, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = reader.Close()
			b.Fatal(err)
		}
		batches = append(batches, batch)
	}
	_ = reader.Close()

	// Test Arrow with 50K chunk size
	testCases := []struct {
		backend   parquetwriter.BackendType
		chunkSize int64
	}{
		{parquetwriter.BackendArrow, 10000},
		{parquetwriter.BackendArrow, 50000},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("%s/chunk-%dk", tc.backend, tc.chunkSize/1000)
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				tmpDir := b.TempDir()

				config := parquetwriter.BackendConfig{
					Type:      tc.backend,
					TmpDir:    tmpDir,
					ChunkSize: tc.chunkSize,
					StringConversionPrefixes: []string{
						"resource_",
						"scope_",
						"attr_",
					},
				}

				backend, err := parquetwriter.NewArrowBackend(config)
				if err != nil {
					b.Fatal(err)
				}

				for _, batch := range batches {
					if err := backend.WriteBatch(ctx, batch); err != nil {
						backend.Abort()
						b.Fatal(err)
					}
				}

				outputFile, err := os.Create(filepath.Join(tmpDir, "output.parquet"))
				if err != nil {
					backend.Abort()
					b.Fatal(err)
				}

				_, err = backend.Close(ctx, outputFile)
				if err != nil {
					_ = outputFile.Close()
					b.Fatal(err)
				}
				_ = outputFile.Close()
			}
		})
	}
}
