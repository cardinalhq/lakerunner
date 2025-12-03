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
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/exemplars"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/pipeline"
)

// BenchmarkProcessMetricsWithTimeBinning benchmarks the core metrics processing with time binning
func BenchmarkProcessMetricsWithTimeBinning(b *testing.B) {
	testFile := filepath.Join("..", "..", "testdata", "metrics", "metrics_187312485.binpb.gz")

	// Verify test file exists
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Fatalf("Test file does not exist: %s", testFile)
	}

	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tmpDir := b.TempDir()
		ctx := context.Background()

		// Copy test file to temp location to simulate downloaded segment
		testData, err := os.ReadFile(testFile)
		require.NoError(b, err)

		segmentFile := filepath.Join(tmpDir, "segment.binpb.gz")
		err = os.WriteFile(segmentFile, testData, 0644)
		require.NoError(b, err)

		// Create mock processor
		processor := &MetricIngestProcessor{
			exemplarProcessor: exemplars.NewProcessor(exemplars.DefaultConfig()),
		}

		// Create reader stack
		reader, err := processor.createReaderStack(segmentFile, orgID.String(), "test-bucket", "test-object")
		require.NoError(b, err)
		defer func() { _ = reader.Close() }()

		// Create unified reader
		unifiedReader, err := processor.createUnifiedReader(ctx, []filereader.Reader{reader})
		require.NoError(b, err)
		defer func() { _ = unifiedReader.Close() }()

		// Create storage profile
		storageProfile := storageprofile.StorageProfile{
			OrganizationID: orgID,
			CollectorName:  "test-collector",
			Bucket:         "test-bucket",
			InstanceNum:    1,
		}

		// Create mock store
		mockStore := &benchMockMetricIngestStore{orgID: orgID}
		processor.store = mockStore

		// Benchmark the time binning process
		_, err = processor.processRowsWithTimeBinning(ctx, unifiedReader, tmpDir, storageProfile)
		require.NoError(b, err)
	}
}

// BenchmarkWriteFromMetricReader benchmarks the batch writing process for metrics
func BenchmarkWriteFromMetricReader(b *testing.B) {
	testFile := filepath.Join("..", "..", "testdata", "metrics", "metrics_187312485.binpb.gz")

	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Fatalf("Test file does not exist: %s", testFile)
	}

	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tmpDir := b.TempDir()
		ctx := context.Background()

		// Create reader
		reader, err := createMetricProtoReader(testFile, filereader.ReaderOptions{
			OrgID: orgID.String(),
		})
		require.NoError(b, err)
		defer func() { _ = reader.Close() }()

		// Create translator
		translator := &MetricTranslator{
			OrgID:    orgID.String(),
			Bucket:   "test-bucket",
			ObjectID: "test-object",
		}
		translatingReader, err := filereader.NewTranslatingReader(reader, translator, 1000)
		require.NoError(b, err)
		defer func() { _ = translatingReader.Close() }()

		// Get schema from reader
		schema := translatingReader.GetSchema()

		// Create writer
		writer, err := factories.NewMetricsWriter(tmpDir, schema, 1000)
		require.NoError(b, err)

		// Benchmark the write operation
		err = writeFromMetricReader(ctx, translatingReader, writer)
		require.NoError(b, err)

		_, err = writer.Close(ctx)
		require.NoError(b, err)
	}
}

// BenchmarkMetricBatchProcessing benchmarks batch allocation and processing patterns
func BenchmarkMetricBatchProcessing(b *testing.B) {
	testFile := filepath.Join("..", "..", "testdata", "metrics", "metrics_187312485.binpb.gz")

	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Fatalf("Test file does not exist: %s", testFile)
	}

	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	batchSizes := []int{100, 500, 1000, 5000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize-%d", batchSize), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				ctx := context.Background()

				// Create reader with specific batch size
				options := filereader.ReaderOptions{
					SignalType: filereader.SignalTypeMetrics,
					BatchSize:  batchSize,
					OrgID:      orgID.String(),
				}

				reader, err := filereader.ReaderForFileWithOptions(testFile, options)
				require.NoError(b, err)
				defer func() { _ = reader.Close() }()

				// Process batches
				batchCount := 0
				for {
					batch, err := reader.Next(ctx)
					if err != nil {
						if err.Error() == "EOF" {
							break
						}
						b.Fatalf("Failed to read batch: %v", err)
					}
					batchCount++
					pipeline.ReturnBatch(batch)
				}
			}
		})
	}
}

// BenchmarkMetricMemoryLeaks checks for batch leaks with pool stats
func BenchmarkMetricMemoryLeaks(b *testing.B) {
	testFile := filepath.Join("..", "..", "testdata", "metrics", "metrics_187312485.binpb.gz")

	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Fatalf("Test file does not exist: %s", testFile)
	}

	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	// Get initial pool stats
	initialStats := pipeline.GlobalBatchPoolStats()
	b.Logf("Initial pool stats - Allocations: %d, Gets: %d, Puts: %d, Leaked: %d",
		initialStats.Allocations, initialStats.Gets, initialStats.Puts, initialStats.LeakedBatches())

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tmpDir := b.TempDir()
		ctx := context.Background()

		// Create reader
		reader, err := createMetricProtoReader(testFile, filereader.ReaderOptions{
			OrgID: orgID.String(),
		})
		require.NoError(b, err)

		// Create translator
		translator := &MetricTranslator{
			OrgID:    orgID.String(),
			Bucket:   "test-bucket",
			ObjectID: "test-object",
		}
		translatingReader, err := filereader.NewTranslatingReader(reader, translator, 1000)
		require.NoError(b, err)

		// Get schema from reader
		schema := translatingReader.GetSchema()

		// Create writer
		writer, err := factories.NewMetricsWriter(tmpDir, schema, 1000)
		require.NoError(b, err)

		// Process all batches
		for {
			batch, err := translatingReader.Next(ctx)
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				b.Fatalf("Failed to read batch: %v", err)
			}

			err = writer.WriteBatch(batch)
			require.NoError(b, err)

			pipeline.ReturnBatch(batch)
		}

		_, err = writer.Close(ctx)
		require.NoError(b, err)

		_ = translatingReader.Close()
		_ = reader.Close()
	}

	// Force GC and check final pool stats
	runtime.GC()
	finalStats := pipeline.GlobalBatchPoolStats()
	b.Logf("Final pool stats - Allocations: %d, Gets: %d, Puts: %d, Leaked: %d",
		finalStats.Allocations, finalStats.Gets, finalStats.Puts, finalStats.LeakedBatches())

	leakDelta := finalStats.LeakedBatches() - initialStats.LeakedBatches()
	if leakDelta > 0 {
		b.Logf("WARNING: Detected %d leaked batches", leakDelta)
	}
}

// BenchmarkMetricDetailedMemoryProfile provides granular memory profiling during processing
func BenchmarkMetricDetailedMemoryProfile(b *testing.B) {
	testFile := filepath.Join("..", "..", "testdata", "metrics", "metrics_187312485.binpb.gz")

	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Fatalf("Test file does not exist: %s", testFile)
	}

	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		runtime.GC()
		var memBefore runtime.MemStats
		runtime.ReadMemStats(&memBefore)
		b.StartTimer()

		tmpDir := b.TempDir()
		ctx := context.Background()

		// Track memory at each stage
		stages := []string{}
		memStats := []runtime.MemStats{}

		// Stage 1: Create reader
		reader, err := createMetricProtoReader(testFile, filereader.ReaderOptions{
			OrgID: orgID.String(),
		})
		require.NoError(b, err)
		defer func() { _ = reader.Close() }()

		runtime.GC()
		var mem1 runtime.MemStats
		runtime.ReadMemStats(&mem1)
		stages = append(stages, "After reader creation")
		memStats = append(memStats, mem1)

		// Stage 2: Create translator
		translator := &MetricTranslator{
			OrgID:    orgID.String(),
			Bucket:   "test-bucket",
			ObjectID: "test-object",
		}
		translatingReader, err := filereader.NewTranslatingReader(reader, translator, 1000)
		require.NoError(b, err)
		defer func() { _ = translatingReader.Close() }()

		runtime.GC()
		var mem2 runtime.MemStats
		runtime.ReadMemStats(&mem2)
		stages = append(stages, "After translator creation")
		memStats = append(memStats, mem2)

		// Get schema from reader
		schema := translatingReader.GetSchema()

		// Stage 3: Create writer
		writer, err := factories.NewMetricsWriter(tmpDir, schema, 1000)
		require.NoError(b, err)

		runtime.GC()
		var mem3 runtime.MemStats
		runtime.ReadMemStats(&mem3)
		stages = append(stages, "After writer creation")
		memStats = append(memStats, mem3)

		// Stage 4: Process batches
		batchCount := 0
		for {
			batch, err := translatingReader.Next(ctx)
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				b.Fatalf("Failed to read batch: %v", err)
			}

			err = writer.WriteBatch(batch)
			require.NoError(b, err)

			pipeline.ReturnBatch(batch)
			batchCount++

			// Check memory every 10 batches
			if batchCount%10 == 0 {
				runtime.GC()
				var memMid runtime.MemStats
				runtime.ReadMemStats(&memMid)
				stages = append(stages, fmt.Sprintf("After %d batches", batchCount))
				memStats = append(memStats, memMid)
			}
		}

		// Stage 5: Close writer
		_, err = writer.Close(ctx)
		require.NoError(b, err)

		b.StopTimer()
		runtime.GC()
		var memAfter runtime.MemStats
		runtime.ReadMemStats(&memAfter)
		stages = append(stages, "After completion")
		memStats = append(memStats, memAfter)

		// Report memory usage at each stage
		b.Logf("\n=== Memory Profile for iteration %d ===", i+1)
		b.Logf("Initial: HeapAlloc=%d MB, HeapObjects=%d",
			memBefore.HeapAlloc/1024/1024, memBefore.HeapObjects)

		for idx, stage := range stages {
			m := memStats[idx]
			b.Logf("%s: HeapAlloc=%d MB, HeapObjects=%d, TotalAlloc=%d MB",
				stage,
				m.HeapAlloc/1024/1024,
				m.HeapObjects,
				m.TotalAlloc/1024/1024)
		}

		b.Logf("Total batches processed: %d", batchCount)
		b.StartTimer()
	}
}

// BenchmarkMetricAggregation benchmarks the aggregation pipeline specifically
func BenchmarkMetricAggregation(b *testing.B) {
	testFile := filepath.Join("..", "..", "testdata", "metrics", "metrics_187312485.binpb.gz")

	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Fatalf("Test file does not exist: %s", testFile)
	}

	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	aggregationWindows := []int{5000, 10000, 30000, 60000} // 5s, 10s, 30s, 60s

	for _, windowMs := range aggregationWindows {
		b.Run(fmt.Sprintf("Window-%dms", windowMs), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				ctx := context.Background()

				// Create base reader
				reader, err := createMetricProtoReader(testFile, filereader.ReaderOptions{
					OrgID: orgID.String(),
				})
				require.NoError(b, err)
				defer func() { _ = reader.Close() }()

				// Add translation
				translator := &MetricTranslator{
					OrgID:    orgID.String(),
					Bucket:   "test-bucket",
					ObjectID: "test-object",
				}
				translatingReader, err := filereader.NewTranslatingReader(reader, translator, 1000)
				require.NoError(b, err)
				defer func() { _ = translatingReader.Close() }()

				// Add aggregation with specified window
				aggregatingReader, err := filereader.NewAggregatingMetricsReader(translatingReader, int64(windowMs), 1000)
				require.NoError(b, err)
				defer func() { _ = aggregatingReader.Close() }()

				// Process all batches to benchmark aggregation
				totalRows := 0
				for {
					batch, err := aggregatingReader.Next(ctx)
					if err != nil {
						if err.Error() == "EOF" {
							break
						}
						b.Fatalf("Failed to read batch: %v", err)
					}

					totalRows += batch.Len()
					pipeline.ReturnBatch(batch)
				}

				b.Logf("Processed %d aggregated rows with %dms window", totalRows, windowMs)
			}
		})
	}
}

// benchMockMetricIngestStore is a mock implementation for benchmarking
type benchMockMetricIngestStore struct {
	orgID uuid.UUID
}

func (m *benchMockMetricIngestStore) GetMetricEstimate(ctx context.Context, organizationID uuid.UUID, frequencyMs int32) int64 {
	return 1000 // Return a reasonable estimate
}

func (m *benchMockMetricIngestStore) InsertMetricSegmentsBatch(ctx context.Context, segments []lrdb.InsertMetricSegmentParams) error {
	return nil // No-op for benchmarking
}

func (m *benchMockMetricIngestStore) BatchUpsertExemplarMetrics(ctx context.Context, batch []lrdb.BatchUpsertExemplarMetricsParams) *lrdb.BatchUpsertExemplarMetricsBatchResults {
	return &lrdb.BatchUpsertExemplarMetricsBatchResults{} // No-op for benchmarking
}

func (m *benchMockMetricIngestStore) UpsertServiceIdentifier(ctx context.Context, arg lrdb.UpsertServiceIdentifierParams) (lrdb.UpsertServiceIdentifierRow, error) {
	return lrdb.UpsertServiceIdentifierRow{}, nil // No-op for benchmarking
}

func (m *benchMockMetricIngestStore) KafkaOffsetsAfter(ctx context.Context, params lrdb.KafkaOffsetsAfterParams) ([]int64, error) {
	return nil, nil // No-op for benchmarking
}

func (m *benchMockMetricIngestStore) CleanupKafkaOffsets(ctx context.Context, params lrdb.CleanupKafkaOffsetsParams) (int64, error) {
	return 0, nil // No-op for benchmarking
}

func (m *benchMockMetricIngestStore) InsertKafkaOffsets(ctx context.Context, params lrdb.InsertKafkaOffsetsParams) error {
	return nil // No-op for benchmarking
}

func (m *benchMockMetricIngestStore) WorkQueueClaim(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
	return lrdb.WorkQueue{}, nil // No-op for benchmarking
}

func (m *benchMockMetricIngestStore) WorkQueueComplete(ctx context.Context, arg lrdb.WorkQueueCompleteParams) error {
	return nil // No-op for benchmarking
}

func (m *benchMockMetricIngestStore) WorkQueueFail(ctx context.Context, arg lrdb.WorkQueueFailParams) (int32, error) {
	return 0, nil // No-op for benchmarking
}

func (m *benchMockMetricIngestStore) WorkQueueHeartbeat(ctx context.Context, arg lrdb.WorkQueueHeartbeatParams) error {
	return nil // No-op for benchmarking
}

func (m *benchMockMetricIngestStore) WorkQueueDepthAll(ctx context.Context) ([]lrdb.WorkQueueDepthAllRow, error) {
	return []lrdb.WorkQueueDepthAllRow{}, nil // No-op for benchmarking
}

// Helper function to write from reader (similar to logs benchmark)
func writeFromMetricReader(ctx context.Context, reader filereader.Reader, writer interface{ WriteBatch(*pipeline.Batch) error }) error {
	for {
		batch, err := reader.Next(ctx)
		if err != nil {
			if err.Error() == "EOF" {
				return nil
			}
			return err
		}

		if err := writer.WriteBatch(batch); err != nil {
			pipeline.ReturnBatch(batch)
			return err
		}

		pipeline.ReturnBatch(batch)
	}
}
