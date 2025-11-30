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
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
)

// BenchmarkE2ELogIngestion measures the complete end-to-end log ingestion pipeline
// using the same production code path:
// 1. Read raw OTEL files (binpb.gz format)
// 2. Translate rows through LogTranslator
// 3. Bin by dateint
// 4. Write to Parquet files
//
// This benchmark uses ProcessLogFiles which is the same code path as production.
func BenchmarkE2ELogIngestion(b *testing.B) {
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

	// Create temp dir for output
	tempDir := b.TempDir()

	b.ResetTimer()

	var totalLogsProcessed int64
	var totalBytesWritten int64

	// Create fingerprint manager (same as production)
	fingerprintManager := fingerprint.NewTenantManager(0.5)

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		timer := NewTimer()
		sampler := NewMemorySampler(timer, 50*1000000) // Sample every 50ms
		sampler.Start()

		outputDir := filepath.Join(tempDir, "output")
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			b.Fatal(err)
		}

		b.StartTimer()

		// Call the production ingestion function with fingerprinting enabled
		result, err := metricsprocessing.ProcessLogFiles(
			ctx,
			[]string{testFile},
			"bench-org",
			"bench-bucket",
			outputDir,
			10000,              // rpfEstimate
			fingerprintManager, // enable fingerprint calculation like production
		)
		if err != nil {
			b.Fatal(err)
		}

		totalLogsProcessed += result.TotalRecords
		totalBytesWritten += result.TotalOutputBytes

		b.StopTimer()
		sampler.Stop()
		timer.AddLogs(result.TotalRecords)
		timer.AddBytes(result.TotalOutputBytes)
		metrics := timer.Stop()

		if i == 0 {
			b.Logf("\n%s", metrics.Report("E2E Log Ingestion (OTEL → Parquet)"))
			b.Logf("Read %d logs, wrote %d bytes to %d parquet file(s)",
				result.TotalRecords, result.TotalOutputBytes, len(result.OutputFiles))
			b.Logf("Input: %d bytes compressed OTEL → Output: %d bytes Parquet (%.2fx size ratio)",
				fileSize, result.TotalOutputBytes, float64(result.TotalOutputBytes)/float64(fileSize))
		}

		// Clean up output files for next iteration
		_ = os.RemoveAll(outputDir)
	}

	// Report metrics
	b.ReportMetric(float64(totalLogsProcessed)/b.Elapsed().Seconds(), "logs/sec")
	b.ReportMetric(float64(totalBytesWritten)/b.Elapsed().Seconds()/1e6, "MB/sec")
	b.ReportMetric(float64(totalLogsProcessed)/b.Elapsed().Seconds(), "logs/sec/core") // Single core
}

// BenchmarkE2ELogIngestionMultiFile measures E2E ingestion across multiple files
func BenchmarkE2ELogIngestionMultiFile(b *testing.B) {
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

	// Create temp dir for output
	tempDir := b.TempDir()

	b.ResetTimer()

	var totalLogsProcessed int64
	var totalBytesWritten int64

	// Create fingerprint manager (same as production)
	fingerprintManager := fingerprint.NewTenantManager(0.5)

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		timer := NewTimer()
		sampler := NewMemorySampler(timer, 50*1000000)
		sampler.Start()

		outputDir := filepath.Join(tempDir, "output")
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			b.Fatal(err)
		}

		b.StartTimer()

		// Call the production ingestion function with multiple files and fingerprinting
		result, err := metricsprocessing.ProcessLogFiles(
			ctx,
			testFiles,
			"bench-org",
			"bench-bucket",
			outputDir,
			10000,
			fingerprintManager,
		)
		if err != nil {
			b.Fatal(err)
		}

		totalLogsProcessed += result.TotalRecords
		totalBytesWritten += result.TotalOutputBytes

		b.StopTimer()
		sampler.Stop()
		timer.AddLogs(result.TotalRecords)
		timer.AddBytes(result.TotalOutputBytes)
		metrics := timer.Stop()

		if i == 0 {
			b.Logf("\n%s", metrics.Report("E2E Log Ingestion (Multi-File)"))
			b.Logf("Read %d logs from %d files, wrote %d bytes to %d parquet file(s)",
				result.TotalRecords, len(testFiles), result.TotalOutputBytes, len(result.OutputFiles))
		}

		// Clean up output files
		_ = os.RemoveAll(outputDir)
	}

	b.ReportMetric(float64(totalLogsProcessed)/b.Elapsed().Seconds(), "logs/sec")
	b.ReportMetric(float64(totalBytesWritten)/b.Elapsed().Seconds()/1e6, "MB/sec")
	b.ReportMetric(float64(totalLogsProcessed)/b.Elapsed().Seconds(), "logs/sec/core")
}
