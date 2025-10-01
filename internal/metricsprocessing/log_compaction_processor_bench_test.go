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
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/exemplars"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// BenchmarkProcessLogsWithSorting benchmarks the core log processing with sorting
func BenchmarkProcessLogsWithSorting(b *testing.B) {
	testFile := filepath.Join("..", "..", "testdata", "logs", "logs_160396104.binpb")

	// Verify test file exists
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Fatalf("Test file does not exist: %s", testFile)
	}

	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001")

	// Create mock segments pointing to our test file
	segments := []lrdb.LogSeg{
		{
			OrganizationID: orgID,
			Dateint:        20240101,
			SegmentID:      1,
			InstanceNum:    0,
			TsRange: pgtype.Range[pgtype.Int8]{
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Lower:     pgtype.Int8{Int64: 1704067200000, Valid: true}, // 2024-01-01 00:00:00
				Upper:     pgtype.Int8{Int64: 1704153600000, Valid: true}, // 2024-01-02 00:00:00
				Valid:     true,
			},
			RecordCount: 1000,
			FileSize:    487169,
			Published:   true,
			Compacted:   false,
		},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		tmpDir := b.TempDir()

		// Copy test file to temp location to simulate downloaded segment
		testData, err := os.ReadFile(testFile)
		require.NoError(b, err)

		segmentFile := filepath.Join(tmpDir, "segment.binpb")
		err = os.WriteFile(segmentFile, testData, 0644)
		require.NoError(b, err)

		params := logProcessingParams{
			TmpDir:         tmpDir,
			StorageClient:  &benchMockStorageClient{localFile: segmentFile},
			OrganizationID: orgID,
			StorageProfile: storageprofile.StorageProfile{
				CollectorName: "test-collector",
				Bucket:        "test-bucket",
			},
			ActiveSegments: segments,
			MaxRecords:     10000,
		}

		ctx := context.Background()
		_, err = processLogsWithSorting(ctx, params)
		require.NoError(b, err)
	}
}

// BenchmarkWriteFromReader benchmarks the batch writing process with memory tracking
func BenchmarkWriteFromReader(b *testing.B) {
	testFile := filepath.Join("..", "..", "testdata", "logs", "logs_160396104.binpb")

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
		exemplarProcessor := exemplars.NewProcessor(exemplars.DefaultConfig())
		reader, err := createLogReader(testFile, orgID.String(), exemplarProcessor)
		require.NoError(b, err)
		defer func() { _ = reader.Close() }()

		// Create translator
		translator := NewLogTranslator(orgID.String(), "test-bucket", "test-object", nil)
		translatingReader, err := filereader.NewTranslatingReader(reader, translator, 1000)
		require.NoError(b, err)
		defer func() { _ = translatingReader.Close() }()

		// Create writer
		writer, err := factories.NewLogsWriter(tmpDir, 10000)
		require.NoError(b, err)

		// Benchmark the write operation
		err = writeFromReader(ctx, translatingReader, writer)
		require.NoError(b, err)

		_, err = writer.Close(ctx)
		require.NoError(b, err)
	}
}

// BenchmarkBatchProcessing benchmarks batch allocation and processing patterns
func BenchmarkBatchProcessing(b *testing.B) {
	testFile := filepath.Join("..", "..", "testdata", "logs", "logs_160396104.binpb")

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
					SignalType: filereader.SignalTypeLogs,
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

// BenchmarkMemoryLeaks checks for batch leaks with pool stats
func BenchmarkMemoryLeaks(b *testing.B) {
	testFile := filepath.Join("..", "..", "testdata", "logs", "logs_160396104.binpb")

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
		exemplarProcessor := exemplars.NewProcessor(exemplars.DefaultConfig())
		reader, err := createLogReader(testFile, orgID.String(), exemplarProcessor)
		require.NoError(b, err)

		// Create translator
		translator := NewLogTranslator(orgID.String(), "test-bucket", "test-object", nil)
		translatingReader, err := filereader.NewTranslatingReader(reader, translator, 1000)
		require.NoError(b, err)

		// Create writer
		writer, err := factories.NewLogsWriter(tmpDir, 10000)
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

// BenchmarkDetailedMemoryProfile provides granular memory profiling during processing
func BenchmarkDetailedMemoryProfile(b *testing.B) {
	testFile := filepath.Join("..", "..", "testdata", "logs", "logs_160396104.binpb")

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
		exemplarProcessor := exemplars.NewProcessor(exemplars.DefaultConfig())
		reader, err := createLogReader(testFile, orgID.String(), exemplarProcessor)
		require.NoError(b, err)
		defer func() { _ = reader.Close() }()

		runtime.GC()
		var mem1 runtime.MemStats
		runtime.ReadMemStats(&mem1)
		stages = append(stages, "After reader creation")
		memStats = append(memStats, mem1)

		// Stage 2: Create translator
		translator := NewLogTranslator(orgID.String(), "test-bucket", "test-object", nil)
		translatingReader, err := filereader.NewTranslatingReader(reader, translator, 1000)
		require.NoError(b, err)
		defer func() { _ = translatingReader.Close() }()

		runtime.GC()
		var mem2 runtime.MemStats
		runtime.ReadMemStats(&mem2)
		stages = append(stages, "After translator creation")
		memStats = append(memStats, mem2)

		// Stage 3: Create writer
		writer, err := factories.NewLogsWriter(tmpDir, 10000)
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

// benchMockStorageClient is a mock implementation for benchmarking
type benchMockStorageClient struct {
	localFile string
}

func (m *benchMockStorageClient) DownloadObject(ctx context.Context, tmpdir, bucket, key string) (string, int64, bool, error) {
	// Copy the local test file to a temp file in tmpdir
	data, err := os.ReadFile(m.localFile)
	if err != nil {
		return "", 0, false, err
	}
	tempFile := filepath.Join(tmpdir, "downloaded_"+filepath.Base(key))
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return "", 0, false, err
	}
	info, err := os.Stat(tempFile)
	if err != nil {
		return "", 0, false, err
	}
	return tempFile, info.Size(), false, nil
}

func (m *benchMockStorageClient) UploadObject(ctx context.Context, bucket, key, localPath string) error {
	return nil
}

func (m *benchMockStorageClient) DeleteObject(ctx context.Context, bucket, key string) error {
	return nil
}

func (m *benchMockStorageClient) DeleteObjects(ctx context.Context, bucket string, keys []string) ([]string, error) {
	return keys, nil
}

// createLogReader is a helper that matches the one from cmd/debug
func createLogReader(filename, orgID string, exemplarProcessor *exemplars.Processor) (filereader.Reader, error) {
	options := filereader.ReaderOptions{
		SignalType: filereader.SignalTypeLogs,
		BatchSize:  1000,
		OrgID:      orgID,
	}

	return filereader.ReaderForFileWithOptions(filename, options)
}
