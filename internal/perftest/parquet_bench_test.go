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
	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// BenchmarkReadFingerprintWrite measures the full pipeline:
// Read OTEL → Row conversion → Fingerprinting → Parquet Writing
//
// This benchmark constrains to GOMAXPROCS=1 to match production
// (horizontal scaling pattern with single-core pods)
func BenchmarkReadFingerprintWrite(b *testing.B) {
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

	// Create fingerprinter and trie cluster manager ONCE (reused across iterations like production)
	fp := fingerprinter.NewFingerprinter()
	trieClusterManager := fingerprinter.NewTrieClusterManager(0.5)

	b.ResetTimer()

	var totalLogsProcessed int64
	var totalFingerprints int64
	var totalParquetFiles int

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		timer := NewTimer()
		sampler := NewMemorySampler(timer, 50*1000000) // Sample every 50ms
		sampler.Start()

		// Create temporary directory for this iteration
		tmpDir := b.TempDir()

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

		// Get schema from reader
		schema := reader.GetSchema()

		// Add chq_fingerprint column since we'll be adding fingerprints to rows
		schema.AddColumn(wkk.RowKeyCFingerprint, wkk.RowKeyCFingerprint, filereader.DataTypeInt64, true)

		// Create Parquet writer
		writer, err := factories.NewLogsWriter(tmpDir, schema, 100000, parquetwriter.DefaultBackend, "")
		if err != nil {
			_ = reader.Close()
			b.Fatal(err)
		}

		logsRead := int64(0)
		fingerprintsGenerated := int64(0)

		for {
			batch, err := reader.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = reader.Close()
				writer.Abort()
				b.Fatal(err)
			}

			logsRead += int64(batch.Len())
			timer.AddLogs(int64(batch.Len()))

			// Generate fingerprints for each log message
			for j := 0; j < batch.Len(); j++ {
				row := batch.Get(j)
				if msg, ok := row[wkk.RowKeyCMessage].(string); ok && msg != "" {
					fingerprintVal, _, err := fp.Fingerprint(msg, trieClusterManager)
					if err == nil && fingerprintVal != 0 {
						fingerprintsGenerated++
						// Add fingerprint to row for Parquet writing
						row[wkk.RowKeyCFingerprint] = fingerprintVal
					}
				}
			}

			// Write batch to Parquet
			if err := writer.WriteBatch(batch); err != nil {
				_ = reader.Close()
				writer.Abort()
				b.Fatal(err)
			}

			pipeline.ReturnBatch(batch)
		}

		// Close writer and get results
		results, err := writer.Close(ctx)
		if err != nil {
			_ = reader.Close()
			b.Fatal(err)
		}

		_ = reader.Close()

		b.StopTimer()
		sampler.Stop()
		metrics := timer.Stop()

		totalLogsProcessed += logsRead
		totalFingerprints += fingerprintsGenerated
		totalParquetFiles += len(results)

		if i == 0 {
			b.Logf("\n%s", metrics.Report("Read → Fingerprint → Write"))
			b.Logf("Read %d logs, generated %d fingerprints (%.1f%% coverage)",
				logsRead, fingerprintsGenerated, float64(fingerprintsGenerated)/float64(logsRead)*100)
			b.Logf("Created %d Parquet file(s)", len(results))
		}
	}

	// Report final metrics
	logsPerSec := float64(totalLogsProcessed) / b.Elapsed().Seconds()
	b.ReportMetric(logsPerSec, "logs/sec")
	b.ReportMetric(logsPerSec, "logs/sec/core") // Already single-core
	b.ReportMetric(float64(totalFingerprints), "fingerprints")
	b.ReportMetric(float64(totalParquetFiles), "parquet_files")
}

// BenchmarkParquetWriteOnly measures just the Parquet writing phase
// to isolate bottlenecks (pre-read data so we only measure write performance)
func BenchmarkParquetWriteOnly(b *testing.B) {
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

	// Pre-read all batches into memory to exclude read overhead
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

	// Get schema from reader before closing
	schema := reader.GetSchema()
	_ = reader.Close()

	b.Logf("Pre-loaded %d batches (%d logs) for write-only test", len(batches), totalLogs)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tmpDir := b.TempDir()

		timer := NewTimer()
		sampler := NewMemorySampler(timer, 50*1000000)
		sampler.Start()

		b.StartTimer()

		writer, err := factories.NewLogsWriter(tmpDir, schema, 100000, parquetwriter.DefaultBackend, "")
		if err != nil {
			b.Fatal(err)
		}

		for _, batch := range batches {
			if err := writer.WriteBatch(batch); err != nil {
				writer.Abort()
				b.Fatal(err)
			}
			timer.AddLogs(int64(batch.Len()))
		}

		results, err := writer.Close(ctx)
		if err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		sampler.Stop()
		metrics := timer.Stop()

		if i == 0 {
			b.Logf("\n%s", metrics.Report("Parquet Write Only"))
			b.Logf("Created %d Parquet file(s)", len(results))
		}
	}

	logsPerSec := float64(totalLogs*int64(b.N)) / b.Elapsed().Seconds()
	b.ReportMetric(logsPerSec, "logs/sec")
	b.ReportMetric(logsPerSec, "logs/sec/core")
}
