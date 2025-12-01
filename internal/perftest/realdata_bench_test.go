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
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/pipeline"
)

const testDataDir = "/tmp/lakerunner-perftest"

// BenchmarkRealDataParquetRead measures baseline reading performance with real data
func BenchmarkRealDataParquetRead(b *testing.B) {
	// Find cooked parquet files
	files, err := filepath.Glob(filepath.Join(testDataDir, "cooked", "*.parquet"))
	if err != nil || len(files) == 0 {
		b.Skip("No test data found. Run: ./scripts/download-perf-testdata.sh cooked 10")
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

	b.Logf("Using file: %s (%d bytes)", filepath.Base(testFile), fileSize)
	b.ResetTimer()

	var totalLogsProcessed int64

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		file, err := os.Open(testFile)
		if err != nil {
			b.Fatal(err)
		}
		stat, _ := file.Stat()

		timer := NewTimer()
		sampler := NewMemorySampler(timer, 100*1000000) // Sample every 100ms
		sampler.Start()
		b.StartTimer()

		// Use raw reader since the cooked files are DuckDB format
		reader, err := filereader.NewParquetRawReader(file, stat.Size(), 1000)
		if err != nil {
			_ = file.Close()
			b.Fatal(err)
		}

		logsRead := int64(0)
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
			pipeline.ReturnBatch(batch)
		}

		totalLogsProcessed += logsRead

		_ = reader.Close()
		_ = file.Close()

		b.StopTimer()
		sampler.Stop()
		timer.AddBytes(fileSize)
		metrics := timer.Stop()

		if i == 0 {
			b.Logf("\n%s", metrics.Report("Parquet Read (Real Data)"))
			b.Logf("Read %d logs from file", logsRead)
		}
	}

	cores := runtime.GOMAXPROCS(0)
	b.ReportMetric(float64(totalLogsProcessed)/b.Elapsed().Seconds(), "logs/sec")
	b.ReportMetric(float64(fileSize*int64(b.N))/b.Elapsed().Seconds()/1e6, "MB/sec")
	b.ReportMetric(float64(totalLogsProcessed)/b.Elapsed().Seconds()/float64(cores), "logs/sec/core")
}

// BenchmarkRealDataParquetWrite measures baseline writing performance with real data
func BenchmarkRealDataParquetWrite(b *testing.B) {
	files, err := filepath.Glob(filepath.Join(testDataDir, "cooked", "*.parquet"))
	if err != nil || len(files) == 0 {
		b.Skip("No test data found. Run: ./scripts/download-perf-testdata.sh cooked 10")
	}

	// Load batches from real file
	ctx := context.Background()
	testFile := files[0]
	batches, schema, totalLogs, totalBytes := loadBatchesFromFile(b, testFile)

	b.Logf("Loaded %d batches (%d logs, %d bytes) from %s", len(batches), totalLogs, totalBytes, filepath.Base(testFile))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tmpDir := b.TempDir()

		timer := NewTimer()
		sampler := NewMemorySampler(timer, 100*1000000)
		sampler.Start()
		b.StartTimer()

		writer, err := factories.NewLogsWriter(tmpDir, schema, totalLogs, parquetwriter.DefaultBackend)
		if err != nil {
			b.Fatal(err)
		}

		for _, batch := range batches {
			if err := writer.WriteBatch(batch); err != nil {
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
		timer.AddBytes(totalBytes)
		metrics := timer.Stop()

		if i == 0 {
			b.Logf("\n%s", metrics.Report("Parquet Write (Real Data)"))
			b.Logf("Output files: %d, total size: %d bytes", len(results), sumResultSizes(results))
		}
	}

	cores := runtime.GOMAXPROCS(0)
	b.ReportMetric(float64(totalLogs*int64(b.N))/b.Elapsed().Seconds(), "logs/sec")
	b.ReportMetric(float64(totalBytes*int64(b.N))/b.Elapsed().Seconds()/1e6, "MB/sec")
	b.ReportMetric(float64(totalLogs*int64(b.N))/b.Elapsed().Seconds()/float64(cores), "logs/sec/core")
}

// BenchmarkRealDataMergeSort measures merge sort with multiple real files
func BenchmarkRealDataMergeSort(b *testing.B) {
	files, err := filepath.Glob(filepath.Join(testDataDir, "cooked", "*.parquet"))
	if err != nil || len(files) < 4 {
		b.Skip("Need at least 4 test files. Run: ./scripts/download-perf-testdata.sh cooked 10")
	}

	// Use first 4 files
	testFiles := files[:4]
	ctx := context.Background()

	// Open readers for all files
	var readers []filereader.Reader
	var totalRecords int64
	var totalBytes int64

	for _, testFile := range testFiles {
		file, err := os.Open(testFile)
		if err != nil {
			b.Fatal(err)
		}
		defer func() { _ = file.Close() }()

		stat, _ := file.Stat()
		reader, err := filereader.NewCookedLogParquetReader(file, stat.Size(), 1000)
		if err != nil {
			b.Fatal(err)
		}
		defer func() { _ = reader.Close() }()

		readers = append(readers, reader)

		// Count records in this file
		logs, bytes := countLogsInFile(b, testFile)
		totalRecords += logs
		totalBytes += bytes
	}

	b.Logf("Merging %d files (%d total logs, %d total bytes)", len(testFiles), totalRecords, totalBytes)
	b.ResetTimer()

	timer := NewTimer()
	sampler := NewMemorySampler(timer, 100*1000000)
	sampler.Start()

	keyProvider := &filereader.TimestampSortKeyProvider{}
	mergeReader, err := filereader.NewMergesortReader(ctx, readers, keyProvider, 1000)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = mergeReader.Close() }()

	logsRead := int64(0)
	for {
		batch, err := mergeReader.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			b.Fatal(err)
		}
		logsRead += int64(batch.Len())
		timer.AddLogs(int64(batch.Len()))
		pipeline.ReturnBatch(batch)
	}

	b.StopTimer()
	sampler.Stop()
	timer.AddBytes(totalBytes)
	metrics := timer.Stop()

	b.Logf("\n%s", metrics.Report(fmt.Sprintf("Merge Sort (%d files, Real Data)", len(testFiles))))

	cores := runtime.GOMAXPROCS(0)
	b.ReportMetric(float64(logsRead)/b.Elapsed().Seconds(), "logs/sec")
	b.ReportMetric(float64(totalBytes)/b.Elapsed().Seconds()/1e6, "MB/sec")
	b.ReportMetric(float64(logsRead)/b.Elapsed().Seconds()/float64(cores), "logs/sec/core")
}

// Helper: countLogsInFile counts logs and estimates bytes in a parquet file
func countLogsInFile(tb testing.TB, filename string) (int64, int64) {
	tb.Helper()

	ctx := context.Background()
	file, err := os.Open(filename)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	stat, _ := file.Stat()
	reader, err := filereader.NewCookedLogParquetReader(file, stat.Size(), 1000)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = reader.Close() }()

	var logs int64
	for {
		batch, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			tb.Fatal(err)
		}
		logs += int64(batch.Len())
		pipeline.ReturnBatch(batch)
	}

	// Use file size as byte estimate
	return logs, stat.Size()
}

// Helper: loadBatchesFromFile loads all batches from a parquet file
func loadBatchesFromFile(tb testing.TB, filename string) ([]*pipeline.Batch, *filereader.ReaderSchema, int64, int64) {
	tb.Helper()

	ctx := context.Background()
	file, err := os.Open(filename)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = file.Close() }()

	stat, _ := file.Stat()
	reader, err := filereader.NewCookedLogParquetReader(file, stat.Size(), 1000)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = reader.Close() }()

	var batches []*pipeline.Batch
	var logs int64

	for {
		batch, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			tb.Fatal(err)
		}
		// Create a copy of the batch to keep
		batchCopy := pipeline.GetBatch()
		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			if row != nil {
				batchCopy.AppendRow(row)
			}
		}
		batches = append(batches, batchCopy)
		logs += int64(batchCopy.Len())
		pipeline.ReturnBatch(batch)
	}

	// Get schema from reader
	schema := reader.GetSchema()

	return batches, schema, logs, stat.Size()
}

// Helper: sumResultSizes sums file sizes from parquet writer results
func sumResultSizes(results []parquetwriter.Result) int64 {
	var total int64
	for _, r := range results {
		total += r.FileSize
	}
	return total
}
