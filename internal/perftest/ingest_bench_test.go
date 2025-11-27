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
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// BenchmarkPureIngestion measures ONLY the cost of reading raw OTEL files
// and converting them to pipeline.Row format. No writing, no processing.
// This isolates the ingestion component's CPU and memory impact.
func BenchmarkPureIngestion(b *testing.B) {
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
	stat, _ := os.Stat(testFile)
	fileSize := stat.Size()

	b.Logf("Using file: %s (%d bytes compressed)", filepath.Base(testFile), fileSize)

	// Force single-core operation to match production pattern
	oldMaxProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldMaxProcs)

	b.ResetTimer()

	var totalLogsProcessed int64
	var totalBytesDecompressed int64

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		timer := NewTimer()
		sampler := NewMemorySampler(timer, 50*1000000) // Sample every 50ms
		sampler.Start()

		// Create reader for raw OTEL log file
		options := filereader.ReaderOptions{
			SignalType: filereader.SignalTypeLogs,
			BatchSize:  1000,
			OrgID:      "test-org",
		}

		b.StartTimer()

		reader, err := filereader.ReaderForFileWithOptions(testFile, options)
		if err != nil {
			b.Fatal(err)
		}

		logsRead := int64(0)
		bytesProcessed := int64(0)

		for {
			batch, err := reader.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}

			logsRead += int64(batch.Len())
			timer.AddLogs(int64(batch.Len()))

			// Estimate uncompressed bytes by counting message sizes
			for i := 0; i < batch.Len(); i++ {
				row := batch.Get(i)
				if msg, ok := row[wkk.RowKeyCMessage].(string); ok {
					bytesProcessed += int64(len(msg))
				}
			}

			pipeline.ReturnBatch(batch)
		}

		totalLogsProcessed += logsRead
		totalBytesDecompressed += bytesProcessed

		_ = reader.Close()

		b.StopTimer()
		sampler.Stop()
		timer.AddBytes(bytesProcessed)
		metrics := timer.Stop()

		if i == 0 {
			compressionRatio := float64(bytesProcessed) / float64(fileSize)
			b.Logf("\n%s", metrics.Report("Pure Ingestion (Raw OTEL → pipeline.Row)"))
			b.Logf("Read %d logs, %d bytes decompressed (%.1fx compression)",
				logsRead, bytesProcessed, compressionRatio)
			b.Logf("Input: %d bytes compressed → %d bytes decompressed", fileSize, bytesProcessed)
		}
	}

	// Report metrics based on actual decompressed data
	b.ReportMetric(float64(totalLogsProcessed)/b.Elapsed().Seconds(), "logs/sec")
	b.ReportMetric(float64(totalBytesDecompressed)/b.Elapsed().Seconds()/1e6, "MB/sec")
	b.ReportMetric(float64(totalLogsProcessed)/b.Elapsed().Seconds(), "logs/sec/core") // Single core
}

// BenchmarkPureIngestionMultiFile measures ingestion across multiple files
// to understand the impact of opening/closing files
func BenchmarkPureIngestionMultiFile(b *testing.B) {
	files, err := filepath.Glob(filepath.Join(testDataDir, "raw", "logs_*.binpb.gz"))
	if err != nil || len(files) < 4 {
		b.Skip("Need at least 4 raw files. Run: ./scripts/download-perf-testdata.sh raw 10")
	}

	// Use first 4 files
	testFiles := files[:4]
	ctx := context.Background()

	var totalSize int64
	for _, f := range testFiles {
		stat, _ := os.Stat(f)
		totalSize += stat.Size()
	}

	b.Logf("Using %d files (%d bytes compressed total)", len(testFiles), totalSize)

	// Force single-core operation
	oldMaxProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldMaxProcs)

	b.ResetTimer()

	var totalLogsProcessed int64

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		timer := NewTimer()
		sampler := NewMemorySampler(timer, 50*1000000)
		sampler.Start()

		b.StartTimer()

		for _, testFile := range testFiles {
			options := filereader.ReaderOptions{
				SignalType: filereader.SignalTypeLogs,
				BatchSize:  1000,
				OrgID:      "test-org",
			}

			reader, err := filereader.ReaderForFileWithOptions(testFile, options)
			if err != nil {
				b.Fatal(err)
			}

			for {
				batch, err := reader.Next(ctx)
				if err == io.EOF {
					break
				}
				if err != nil {
					b.Fatal(err)
				}

				totalLogsProcessed += int64(batch.Len())
				timer.AddLogs(int64(batch.Len()))
				pipeline.ReturnBatch(batch)
			}

			_ = reader.Close()
		}

		b.StopTimer()
		sampler.Stop()
		metrics := timer.Stop()

		if i == 0 {
			b.Logf("\n%s", metrics.Report("Pure Ingestion (Multi-File)"))
		}
	}

	b.ReportMetric(float64(totalLogsProcessed)/b.Elapsed().Seconds(), "logs/sec")
	b.ReportMetric(float64(totalLogsProcessed)/b.Elapsed().Seconds(), "logs/sec/core")
}

// BenchmarkIngestionMemoryProfile runs a single iteration suitable for memory profiling
func BenchmarkIngestionMemoryProfile(b *testing.B) {
	files, err := filepath.Glob(filepath.Join(testDataDir, "raw", "logs_*.binpb.gz"))
	if err != nil || len(files) == 0 {
		b.Skip("No raw test data found. Run: ./scripts/download-perf-testdata.sh raw 10")
	}

	testFile := files[0]
	ctx := context.Background()

	// Force single-core operation
	oldMaxProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldMaxProcs)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		options := filereader.ReaderOptions{
			SignalType: filereader.SignalTypeLogs,
			BatchSize:  1000,
			OrgID:      "test-org",
		}

		reader, err := filereader.ReaderForFileWithOptions(testFile, options)
		if err != nil {
			b.Fatal(err)
		}

		for {
			batch, err := reader.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			pipeline.ReturnBatch(batch)
		}

		_ = reader.Close()
	}
}
