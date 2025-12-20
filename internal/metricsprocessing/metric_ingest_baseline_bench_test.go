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
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// getSampleMetricFile returns a path to a sample metric file in /tmp/metrics
// or skips the test if not available.
func getSampleMetricFile(t testing.TB) string {
	testFile := "/tmp/metrics/metrics_0000.binpb.gz"
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skipf("Test file does not exist: %s", testFile)
	}
	return testFile
}

// getSampleMetricFiles returns paths to multiple sample metric files.
func getSampleMetricFiles(t testing.TB, count int) []string {
	files := make([]string, 0, count)
	for i := range count {
		testFile := fmt.Sprintf("/tmp/metrics/metrics_%04d.binpb.gz", i)
		if _, err := os.Stat(testFile); os.IsNotExist(err) {
			if len(files) == 0 {
				t.Skipf("No test files found in /tmp/metrics/")
			}
			break
		}
		files = append(files, testFile)
	}
	return files
}

// BenchmarkMetricIngest_1_GzipRead benchmarks just reading and gunzipping the file.
func BenchmarkMetricIngest_1_GzipRead(b *testing.B) {
	testFile := getSampleMetricFile(b)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		file, err := os.Open(testFile)
		require.NoError(b, err)

		gzipReader, err := gzip.NewReader(file)
		require.NoError(b, err)

		data, err := io.ReadAll(gzipReader)
		require.NoError(b, err)

		b.SetBytes(int64(len(data)))

		_ = gzipReader.Close()
		_ = file.Close()
	}
}

// BenchmarkMetricIngest_2_ProtoReader benchmarks the IngestProtoMetricsReader alone.
func BenchmarkMetricIngest_2_ProtoReader(b *testing.B) {
	testFile := getSampleMetricFile(b)
	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ctx := context.Background()

		reader, err := createMetricProtoReader(testFile, filereader.ReaderOptions{
			OrgID: orgID.String(),
		})
		require.NoError(b, err)

		rowCount := int64(0)
		for {
			batch, readErr := reader.Next(ctx)
			if readErr != nil {
				if batch != nil {
					pipeline.ReturnBatch(batch)
				}
				break
			}
			rowCount += int64(batch.Len())
			pipeline.ReturnBatch(batch)
		}

		b.ReportMetric(float64(rowCount), "rows/op")
		_ = reader.Close()
	}
}

// BenchmarkMetricIngest_3_WithTranslation benchmarks proto reader + MetricTranslator.
func BenchmarkMetricIngest_3_WithTranslation(b *testing.B) {
	testFile := getSampleMetricFile(b)
	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ctx := context.Background()

		reader, err := createMetricProtoReader(testFile, filereader.ReaderOptions{
			OrgID: orgID.String(),
		})
		require.NoError(b, err)

		translator := &MetricTranslator{
			OrgID:    orgID.String(),
			Bucket:   "test-bucket",
			ObjectID: "test-object",
		}
		translatingReader, err := filereader.NewTranslatingReader(reader, translator, 1000)
		require.NoError(b, err)

		rowCount := int64(0)
		for {
			batch, readErr := translatingReader.Next(ctx)
			if readErr != nil {
				if batch != nil {
					pipeline.ReturnBatch(batch)
				}
				break
			}
			rowCount += int64(batch.Len())
			pipeline.ReturnBatch(batch)
		}

		b.ReportMetric(float64(rowCount), "rows/op")
		_ = translatingReader.Close()
	}
}

// BenchmarkMetricIngest_4_WithDiskSort benchmarks proto reader + translation + disk sorting.
func BenchmarkMetricIngest_4_WithDiskSort(b *testing.B) {
	testFile := getSampleMetricFile(b)
	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ctx := context.Background()

		reader, err := createMetricProtoReader(testFile, filereader.ReaderOptions{
			OrgID: orgID.String(),
		})
		require.NoError(b, err)

		translator := &MetricTranslator{
			OrgID:    orgID.String(),
			Bucket:   "test-bucket",
			ObjectID: "test-object",
		}
		translatingReader, err := filereader.NewTranslatingReader(reader, translator, 1000)
		require.NoError(b, err)

		keyProvider := filereader.GetCurrentMetricSortKeyProvider()
		sortedReader, err := filereader.NewDiskSortingReader(translatingReader, keyProvider, 1000)
		require.NoError(b, err)

		rowCount := int64(0)
		for {
			batch, readErr := sortedReader.Next(ctx)
			if readErr != nil {
				if batch != nil {
					pipeline.ReturnBatch(batch)
				}
				break
			}
			rowCount += int64(batch.Len())
			pipeline.ReturnBatch(batch)
		}

		b.ReportMetric(float64(rowCount), "rows/op")
		_ = sortedReader.Close()
	}
}

// BenchmarkMetricIngest_5_FullPipeline benchmarks the complete metric ingest path.
// This mirrors what MetricIngestProcessor.createReaderStack + createUnifiedReader does.
func BenchmarkMetricIngest_5_FullPipeline(b *testing.B) {
	testFile := getSampleMetricFile(b)
	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ctx := context.Background()

		// Stage 1: Proto reader
		reader, err := createMetricProtoReader(testFile, filereader.ReaderOptions{
			OrgID: orgID.String(),
		})
		require.NoError(b, err)

		// Stage 2: Translation
		translator := &MetricTranslator{
			OrgID:    orgID.String(),
			Bucket:   "test-bucket",
			ObjectID: "test-object",
		}
		translatingReader, err := filereader.NewTranslatingReader(reader, translator, 1000)
		require.NoError(b, err)

		// Stage 3: Disk sorting
		keyProvider := filereader.GetCurrentMetricSortKeyProvider()
		sortedReader, err := filereader.NewDiskSortingReader(translatingReader, keyProvider, 1000)
		require.NoError(b, err)

		// Stage 4: Aggregation (10s window as in production)
		aggregatingReader, err := filereader.NewAggregatingMetricsReader(sortedReader, 10000, 1000)
		require.NoError(b, err)

		rowCount := int64(0)
		for {
			batch, readErr := aggregatingReader.Next(ctx)
			if readErr != nil {
				if batch != nil {
					pipeline.ReturnBatch(batch)
				}
				break
			}
			rowCount += int64(batch.Len())
			pipeline.ReturnBatch(batch)
		}

		b.ReportMetric(float64(rowCount), "rows/op")
		_ = aggregatingReader.Close()
	}
}

// BenchmarkMetricIngest_6_FullPipelineWithWrite benchmarks the complete path including parquet write.
func BenchmarkMetricIngest_6_FullPipelineWithWrite(b *testing.B) {
	testFile := getSampleMetricFile(b)
	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tmpDir := b.TempDir()
		ctx := context.Background()

		// Stage 1: Proto reader
		reader, err := createMetricProtoReader(testFile, filereader.ReaderOptions{
			OrgID: orgID.String(),
		})
		require.NoError(b, err)

		// Stage 2: Translation
		translator := &MetricTranslator{
			OrgID:    orgID.String(),
			Bucket:   "test-bucket",
			ObjectID: "test-object",
		}
		translatingReader, err := filereader.NewTranslatingReader(reader, translator, 1000)
		require.NoError(b, err)

		// Stage 3: Disk sorting
		keyProvider := filereader.GetCurrentMetricSortKeyProvider()
		sortedReader, err := filereader.NewDiskSortingReader(translatingReader, keyProvider, 1000)
		require.NoError(b, err)

		// Stage 4: Aggregation (10s window as in production)
		aggregatingReader, err := filereader.NewAggregatingMetricsReader(sortedReader, 10000, 1000)
		require.NoError(b, err)

		// Get schema and add required columns
		schema := aggregatingReader.GetSchema()
		schema.AddColumn(wkk.RowKeyCCustomerID, wkk.RowKeyCCustomerID, filereader.DataTypeString, true)
		schema.AddColumn(wkk.RowKeyCTelemetryType, wkk.RowKeyCTelemetryType, filereader.DataTypeString, true)
		schema.AddColumn(wkk.RowKeyCTID, wkk.RowKeyCTID, filereader.DataTypeInt64, true)
		schema.AddColumn(wkk.RowKeyCID, wkk.RowKeyCID, filereader.DataTypeString, true)

		// Stage 5: Write to parquet
		writer, err := factories.NewMetricsWriter(tmpDir, schema, 1000)
		require.NoError(b, err)

		rowCount := int64(0)
		for {
			batch, readErr := aggregatingReader.Next(ctx)
			if readErr != nil {
				if batch != nil {
					pipeline.ReturnBatch(batch)
				}
				break
			}
			err = writer.WriteBatch(batch)
			require.NoError(b, err)
			rowCount += int64(batch.Len())
			pipeline.ReturnBatch(batch)
		}

		results, err := writer.Close(ctx)
		require.NoError(b, err)

		var totalSize int64
		for _, r := range results {
			totalSize += r.FileSize
		}

		b.ReportMetric(float64(rowCount), "rows/op")
		b.ReportMetric(float64(totalSize), "bytes_written/op")
		_ = aggregatingReader.Close()
	}
}

// BenchmarkMetricIngest_MultiFileMerge benchmarks merging multiple files.
func BenchmarkMetricIngest_MultiFileMerge(b *testing.B) {
	files := getSampleMetricFiles(b, 5) // Test with 5 files
	if len(files) < 2 {
		b.Skip("Need at least 2 test files for merge benchmark")
	}
	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ctx := context.Background()

		// Create readers for each file
		var readers []filereader.Reader
		for _, file := range files {
			reader, err := createMetricProtoReader(file, filereader.ReaderOptions{
				OrgID: orgID.String(),
			})
			require.NoError(b, err)

			translator := &MetricTranslator{
				OrgID:    orgID.String(),
				Bucket:   "test-bucket",
				ObjectID: filepath.Base(file),
			}
			translatingReader, err := filereader.NewTranslatingReader(reader, translator, 1000)
			require.NoError(b, err)

			keyProvider := filereader.GetCurrentMetricSortKeyProvider()
			sortedReader, err := filereader.NewDiskSortingReader(translatingReader, keyProvider, 1000)
			require.NoError(b, err)

			readers = append(readers, sortedReader)
		}

		// Create mergesort reader
		keyProvider := filereader.GetCurrentMetricSortKeyProvider()
		mergedReader, err := filereader.NewMergesortReader(ctx, readers, keyProvider, 1000)
		require.NoError(b, err)

		// Add aggregation
		aggregatingReader, err := filereader.NewAggregatingMetricsReader(mergedReader, 10000, 1000)
		require.NoError(b, err)

		rowCount := int64(0)
		for {
			batch, readErr := aggregatingReader.Next(ctx)
			if readErr != nil {
				if batch != nil {
					pipeline.ReturnBatch(batch)
				}
				break
			}
			rowCount += int64(batch.Len())
			pipeline.ReturnBatch(batch)
		}

		b.ReportMetric(float64(rowCount), "rows/op")
		b.ReportMetric(float64(len(files)), "files/op")
		_ = aggregatingReader.Close()
	}
}

// BenchmarkMetricIngest_MemoryProfile profiles memory allocation patterns.
func BenchmarkMetricIngest_MemoryProfile(b *testing.B) {
	testFile := getSampleMetricFile(b)
	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		runtime.GC()
		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)
		b.StartTimer()

		ctx := context.Background()

		reader, _ := createMetricProtoReader(testFile, filereader.ReaderOptions{OrgID: orgID.String()})
		translator := &MetricTranslator{OrgID: orgID.String(), Bucket: "test-bucket", ObjectID: "test-object"}
		translatingReader, _ := filereader.NewTranslatingReader(reader, translator, 1000)
		keyProvider := filereader.GetCurrentMetricSortKeyProvider()
		sortedReader, _ := filereader.NewDiskSortingReader(translatingReader, keyProvider, 1000)
		aggregatingReader, _ := filereader.NewAggregatingMetricsReader(sortedReader, 10000, 1000)

		rowCount := int64(0)
		for {
			batch, err := aggregatingReader.Next(ctx)
			if err != nil {
				if batch != nil {
					pipeline.ReturnBatch(batch)
				}
				break
			}
			rowCount += int64(batch.Len())
			pipeline.ReturnBatch(batch)
		}
		_ = aggregatingReader.Close()

		b.StopTimer()
		runtime.GC()
		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)

		// Report memory metrics
		b.ReportMetric(float64(memAfter.TotalAlloc-memBefore.TotalAlloc)/float64(rowCount), "bytes_alloc/row")
		b.ReportMetric(float64(memAfter.HeapAlloc)/1024/1024, "heap_MB")
		b.StartTimer()
	}
}

// TestMetricFileCardinality analyzes the cardinality of test metric files.
func TestMetricFileCardinality(t *testing.T) {
	testFile := getSampleMetricFile(t)
	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	ctx := context.Background()

	// Stage 1: Just proto reader
	reader, err := createMetricProtoReader(testFile, filereader.ReaderOptions{
		OrgID: orgID.String(),
	})
	require.NoError(t, err)

	protoRows := int64(0)
	uniqueNames := make(map[string]int)
	for {
		batch, readErr := reader.Next(ctx)
		if readErr != nil {
			if batch != nil {
				pipeline.ReturnBatch(batch)
			}
			break
		}
		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			protoRows++
			if name, ok := row[wkk.RowKeyCName].(string); ok {
				uniqueNames[name]++
			}
		}
		pipeline.ReturnBatch(batch)
	}
	_ = reader.Close()

	t.Logf("Proto reader: %d rows, %d unique metric names", protoRows, len(uniqueNames))

	// Stage 2: With translation (adds TID)
	reader2, err := createMetricProtoReader(testFile, filereader.ReaderOptions{
		OrgID: orgID.String(),
	})
	require.NoError(t, err)

	translator := &MetricTranslator{
		OrgID:    orgID.String(),
		Bucket:   "test-bucket",
		ObjectID: "test-object",
	}
	translatingReader, err := filereader.NewTranslatingReader(reader2, translator, 1000)
	require.NoError(t, err)

	translatedRows := int64(0)
	uniqueTIDs := make(map[int64]int)
	uniqueTimestamps := make(map[int64]int)
	for {
		batch, readErr := translatingReader.Next(ctx)
		if readErr != nil {
			if batch != nil {
				pipeline.ReturnBatch(batch)
			}
			break
		}
		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			translatedRows++
			if tid, ok := row[wkk.RowKeyCTID].(int64); ok {
				uniqueTIDs[tid]++
			}
			if ts, ok := row[wkk.RowKeyCTimestamp].(int64); ok {
				uniqueTimestamps[ts]++
			}
		}
		pipeline.ReturnBatch(batch)
	}
	_ = translatingReader.Close()

	t.Logf("After translation: %d rows, %d unique TIDs, %d unique timestamps", translatedRows, len(uniqueTIDs), len(uniqueTimestamps))

	// Stage 3: Full pipeline
	reader3, err := createMetricProtoReader(testFile, filereader.ReaderOptions{
		OrgID: orgID.String(),
	})
	require.NoError(t, err)

	translator3 := &MetricTranslator{
		OrgID:    orgID.String(),
		Bucket:   "test-bucket",
		ObjectID: "test-object",
	}
	translatingReader3, err := filereader.NewTranslatingReader(reader3, translator3, 1000)
	require.NoError(t, err)

	keyProvider := filereader.GetCurrentMetricSortKeyProvider()
	sortedReader, err := filereader.NewDiskSortingReader(translatingReader3, keyProvider, 1000)
	require.NoError(t, err)

	aggregatingReader, err := filereader.NewAggregatingMetricsReader(sortedReader, 10000, 1000)
	require.NoError(t, err)

	aggregatedRows := int64(0)
	for {
		batch, readErr := aggregatingReader.Next(ctx)
		if readErr != nil {
			if batch != nil {
				pipeline.ReturnBatch(batch)
			}
			break
		}
		aggregatedRows += int64(batch.Len())
		pipeline.ReturnBatch(batch)
	}
	_ = aggregatingReader.Close()

	t.Logf("After aggregation: %d rows", aggregatedRows)
	t.Logf("Expected: ~%d rows (unique TIDs * unique timestamps)", len(uniqueTIDs)*len(uniqueTimestamps))
}

// BenchmarkMetricIngest_BatchSizes tests impact of different batch sizes.
func BenchmarkMetricIngest_BatchSizes(b *testing.B) {
	testFile := getSampleMetricFile(b)
	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	batchSizes := []int{100, 500, 1000, 2000, 5000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				ctx := context.Background()

				reader, _ := createMetricProtoReader(testFile, filereader.ReaderOptions{
					OrgID:     orgID.String(),
					BatchSize: batchSize,
				})
				translator := &MetricTranslator{OrgID: orgID.String(), Bucket: "test-bucket", ObjectID: "test-object"}
				translatingReader, _ := filereader.NewTranslatingReader(reader, translator, batchSize)
				keyProvider := filereader.GetCurrentMetricSortKeyProvider()
				sortedReader, _ := filereader.NewDiskSortingReader(translatingReader, keyProvider, batchSize)
				aggregatingReader, _ := filereader.NewAggregatingMetricsReader(sortedReader, 10000, batchSize)

				for {
					batch, err := aggregatingReader.Next(ctx)
					if err != nil {
						if batch != nil {
							pipeline.ReturnBatch(batch)
						}
						break
					}
					pipeline.ReturnBatch(batch)
				}
				_ = aggregatingReader.Close()
			}
		})
	}
}

// BenchmarkMetricIngest_FullPipeline_MemorySort benchmarks the full pipeline using in-memory sorting.
func BenchmarkMetricIngest_FullPipeline_MemorySort(b *testing.B) {
	testFile := getSampleMetricFile(b)
	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ctx := context.Background()

		// Combined proto reader + translation + in-memory sorting
		reader, err := createSortingMetricProtoReader(testFile, orgID.String(), "test-bucket", "test-object")
		require.NoError(b, err)

		// Aggregation (10s window as in production)
		aggregatingReader, err := filereader.NewAggregatingMetricsReader(reader, 10000, 1000)
		require.NoError(b, err)

		rowCount := int64(0)
		for {
			batch, readErr := aggregatingReader.Next(ctx)
			if readErr != nil {
				if batch != nil {
					pipeline.ReturnBatch(batch)
				}
				break
			}
			rowCount += int64(batch.Len())
			pipeline.ReturnBatch(batch)
		}

		b.ReportMetric(float64(rowCount), "rows/op")
		_ = aggregatingReader.Close()
	}
}

// BenchmarkMetricIngest_SortingReader_Comparison compares disk-based sorting vs in-memory sorting.
func BenchmarkMetricIngest_SortingReader_Comparison(b *testing.B) {
	testFile := getSampleMetricFile(b)
	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	b.Run("DiskSort", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ctx := context.Background()

			reader, _ := createMetricProtoReader(testFile, filereader.ReaderOptions{
				OrgID: orgID.String(),
			})
			translator := &MetricTranslator{OrgID: orgID.String(), Bucket: "test-bucket", ObjectID: "test-object"}
			translatingReader, _ := filereader.NewTranslatingReader(reader, translator, 1000)
			keyProvider := filereader.GetCurrentMetricSortKeyProvider()
			sortedReader, _ := filereader.NewDiskSortingReader(translatingReader, keyProvider, 1000)

			var rows int64
			for {
				batch, err := sortedReader.Next(ctx)
				if err != nil {
					if batch != nil {
						pipeline.ReturnBatch(batch)
					}
					break
				}
				rows += int64(batch.Len())
				pipeline.ReturnBatch(batch)
			}
			_ = sortedReader.Close()
			b.ReportMetric(float64(rows), "rows/op")
		}
	})

	b.Run("MemorySort", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			ctx := context.Background()

			reader, _ := createSortingMetricProtoReader(testFile, orgID.String(), "test-bucket", "test-object")

			var rows int64
			for {
				batch, err := reader.Next(ctx)
				if err != nil {
					if batch != nil {
						pipeline.ReturnBatch(batch)
					}
					break
				}
				rows += int64(batch.Len())
				pipeline.ReturnBatch(batch)
			}
			_ = reader.Close()
			b.ReportMetric(float64(rows), "rows/op")
		}
	})
}
