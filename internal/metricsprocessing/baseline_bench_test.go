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

package metricsprocessing

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/perftest"
	"github.com/cardinalhq/lakerunner/internal/testdata"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// BenchmarkBaselineParquetRead measures current parquet reading performance
func BenchmarkBaselineParquetRead(b *testing.B) {
	ctx := context.Background()

	// Create test data
	tmpDir := b.TempDir()
	testFile, recordCount, totalBytes := createTestLogParquet(b, tmpDir, 100000)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		file, err := os.Open(testFile)
		if err != nil {
			b.Fatal(err)
		}
		stat, _ := file.Stat()

		timer := perftest.NewTimer()
		sampler := perftest.NewMemorySampler(timer, 100*1000000) // Sample every 100ms
		sampler.Start()
		b.StartTimer()

		reader, err := filereader.NewCookedLogParquetReader(file, stat.Size(), 1000)
		if err != nil {
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

		_ = reader.Close()
		_ = file.Close()

		b.StopTimer()
		sampler.Stop()
		timer.AddBytes(totalBytes)
		metrics := timer.Stop()

		if i == 0 {
			b.Logf("\n%s", metrics.Report("Parquet Read"))
		}
	}

	b.ReportMetric(float64(recordCount*int64(b.N))/b.Elapsed().Seconds(), "logs/sec")
	b.ReportMetric(float64(totalBytes*int64(b.N))/b.Elapsed().Seconds()/1e6, "MB/sec")
}

// BenchmarkBaselineParquetWrite measures current parquet writing performance
func BenchmarkBaselineParquetWrite(b *testing.B) {
	ctx := context.Background()

	// Generate test batches
	batches, schema, totalLogs, totalBytes := generateTestBatches(b, 100000)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tmpDir := b.TempDir()

		timer := perftest.NewTimer()
		sampler := perftest.NewMemorySampler(timer, 100*1000000)
		sampler.Start()
		b.StartTimer()

		writer, err := factories.NewLogsWriter(tmpDir, schema, 100000, parquetwriter.DefaultBackend)
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
			b.Logf("\n%s", metrics.Report("Parquet Write"))
			b.Logf("Output files: %d", len(results))
		}
	}

	b.ReportMetric(float64(totalLogs*int64(b.N))/b.Elapsed().Seconds(), "logs/sec")
	b.ReportMetric(float64(totalBytes*int64(b.N))/b.Elapsed().Seconds()/1e6, "MB/sec")
}

// BenchmarkBaselineMergeSort measures merge sort performance with 4 inputs
func BenchmarkBaselineMergeSort(b *testing.B) {
	ctx := context.Background()

	// Create 4 test files
	tmpDir := b.TempDir()
	var readers []filereader.Reader
	var totalRecords int64
	var totalBytes int64

	for i := 0; i < 4; i++ {
		testFile, records, bytes := createTestLogParquet(b, tmpDir, 25000)
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
		totalRecords += records
		totalBytes += bytes
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		timer := perftest.NewTimer()
		sampler := perftest.NewMemorySampler(timer, 100*1000000)
		sampler.Start()
		b.StartTimer()

		keyProvider := &filereader.LogSortKeyProvider{}
		mergeReader, err := filereader.NewMergesortReader(ctx, readers, keyProvider, 1000)
		if err != nil {
			b.Fatal(err)
		}

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

		_ = mergeReader.Close()

		b.StopTimer()
		sampler.Stop()
		timer.AddBytes(totalBytes)
		metrics := timer.Stop()

		if i == 0 {
			b.Logf("\n%s", metrics.Report("Merge Sort (4 inputs)"))
		}
	}

	b.ReportMetric(float64(totalRecords*int64(b.N))/b.Elapsed().Seconds(), "logs/sec")
}

// Helper: createTestLogParquet creates a test parquet file with log data
func createTestLogParquet(b *testing.B, tmpDir string, numLogs int) (filename string, records int64, bytes int64) {
	b.Helper()

	batches, schema, totalLogs, totalBytes := generateTestBatches(b, numLogs)

	writer, err := factories.NewLogsWriter(tmpDir, schema, int64(numLogs), parquetwriter.DefaultBackend)
	if err != nil {
		b.Fatal(err)
	}
	for _, batch := range batches {
		if err := writer.WriteBatch(batch); err != nil {
			b.Fatal(err)
		}
	}

	results, err := writer.Close(context.Background())
	if err != nil {
		b.Fatal(err)
	}

	if len(results) != 1 {
		b.Fatalf("Expected 1 result file, got %d", len(results))
	}

	return results[0].FileName, totalLogs, totalBytes
}

// Helper: generateTestBatches creates test batches with realistic log data
func generateTestBatches(b *testing.B, numLogs int) ([]*pipeline.Batch, *filereader.ReaderSchema, int64, int64) {
	b.Helper()

	// Create schema matching what testdata.GenerateLogBatch produces
	schema := filereader.NewReaderSchema()
	schema.AddColumn(wkk.RowKeyCTimestamp, wkk.RowKeyCTimestamp, filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.RowKeyCMessage, wkk.RowKeyCMessage, filereader.DataTypeString, true)
	schema.AddColumn(wkk.RowKeyCLevel, wkk.RowKeyCLevel, filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("service_name"), wkk.NewRowKey("service_name"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("host_name"), wkk.NewRowKey("host_name"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("container_name"), wkk.NewRowKey("container_name"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("k8s_namespace_name"), wkk.NewRowKey("k8s_namespace_name"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("k8s_pod_name"), wkk.NewRowKey("k8s_pod_name"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("k8s_cluster_name"), wkk.NewRowKey("k8s_cluster_name"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("deployment_environment"), wkk.NewRowKey("deployment_environment"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.RowKeyCFingerprint, wkk.RowKeyCFingerprint, filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("chq_id"), wkk.NewRowKey("chq_id"), filereader.DataTypeString, true)

	batchSize := 1000
	numBatches := (numLogs + batchSize - 1) / batchSize
	batches := make([]*pipeline.Batch, 0, numBatches)

	var totalLogs int64
	var totalBytes int64

	for batchNum := 0; batchNum < numBatches; batchNum++ {
		logsInBatch := batchSize
		if batchNum == numBatches-1 {
			logsInBatch = numLogs - (batchNum * batchSize)
		}

		batch := testdata.GenerateLogBatch(logsInBatch, batchNum*batchSize)
		batches = append(batches, batch)
		totalLogs += int64(logsInBatch)

		// Estimate bytes per log
		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			if msg, ok := row[wkk.RowKeyCMessage].(string); ok {
				totalBytes += int64(len(msg))
			}
		}
	}

	return batches, schema, totalLogs, totalBytes
}

// BenchmarkBaselineReadWritePipeline measures read → write pipeline
func BenchmarkBaselineReadWritePipeline(b *testing.B) {
	ctx := context.Background()

	// Create input file
	tmpDir := b.TempDir()
	inputFile, recordCount, totalBytes := createTestLogParquet(b, tmpDir, 100000)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		outputDir := b.TempDir()

		timer := perftest.NewStageTimer()
		sampler := perftest.NewMemorySampler(timer.TotalTimer, 100*1000000)
		sampler.Start()

		// Read stage
		timer.StartStage("read")
		file, err := os.Open(inputFile)
		if err != nil {
			b.Fatal(err)
		}
		stat, _ := file.Stat()
		reader, err := filereader.NewCookedLogParquetReader(file, stat.Size(), 1000)
		if err != nil {
			b.Fatal(err)
		}
		timer.EndStage("read", 0, 0)

		// Get schema from reader
		schema := reader.GetSchema()

		// Write stage
		timer.StartStage("write")
		writer, err := factories.NewLogsWriter(outputDir, schema, recordCount, parquetwriter.DefaultBackend)
		if err != nil {
			b.Fatal(err)
		}

		b.StartTimer()

		logsProcessed := int64(0)
		for {
			batch, err := reader.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}

			if err := writer.WriteBatch(batch); err != nil {
				b.Fatal(err)
			}
			logsProcessed += int64(batch.Len())
		}

		b.StopTimer()

		timer.EndStage("write", logsProcessed, totalBytes)

		timer.StartStage("close")
		_, err = writer.Close(ctx)
		if err != nil {
			b.Fatal(err)
		}
		_ = reader.Close()
		_ = file.Close()
		timer.EndStage("close", 0, 0)

		sampler.Stop()

		if i == 0 {
			b.Logf("\n%s", timer.StageReport())
		}
	}

	b.ReportMetric(float64(recordCount*int64(b.N))/b.Elapsed().Seconds(), "logs/sec")
	b.ReportMetric(float64(totalBytes*int64(b.N))/b.Elapsed().Seconds()/1e6, "MB/sec")
}

func BenchmarkBaselineComponents(b *testing.B) {
	for _, cores := range []int{1, 2, 4} {
		cores := cores
		b.Run(fmt.Sprintf("Read-%dcores", cores), func(b *testing.B) {
			// GOMAXPROCS setting would go here if we want to limit cores
			BenchmarkBaselineParquetRead(b)
		})

		b.Run(fmt.Sprintf("Write-%dcores", cores), func(b *testing.B) {
			BenchmarkBaselineParquetWrite(b)
		})
	}
}
