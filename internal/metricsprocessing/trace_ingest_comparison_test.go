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
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/pipeline"
)

// PipelineStats captures performance metrics for a pipeline run
type PipelineStats struct {
	Name          string
	Duration      time.Duration
	Allocations   uint64
	TotalAlloc    uint64
	HeapInUse     uint64
	RecordCount   int64
	OutputFiles   int
	OutputColumns []string
}

// TestTraceIngestPipelineComparison compares old (filereader/parquetwriter) and new (DuckDB) trace ingestion pipelines
func TestTraceIngestPipelineComparison(t *testing.T) {
	// Find trace test files
	testFiles := findTraceTestFiles(t)
	if len(testFiles) == 0 {
		t.Skip("No trace test files found")
	}

	t.Logf("Testing with %d trace files", len(testFiles))

	ctx := context.Background()
	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001").String()

	// Run old pipeline
	oldStats, oldOutput, err := runOldPipeline(ctx, t, testFiles, orgID)
	require.NoError(t, err)

	// Run new DuckDB pipeline
	newStats, newOutput, err := runDuckDBPipeline(ctx, t, testFiles, orgID)
	require.NoError(t, err)

	// Print comparison report
	printComparisonReport(t, oldStats, newStats, oldOutput, newOutput)

	// Verify correctness
	verifyOutputCorrectness(t, oldOutput, newOutput)
}

func findTraceTestFiles(t *testing.T) []string {
	// Use only a single test file for comparison.
	// The old pipeline cannot handle multiple files with different attribute schemas
	// because it determines schema from the first file and fails on new columns.
	// DuckDB handles this automatically via schema merging.
	testdataFile := filepath.Join("..", "..", "testdata", "traces", "otel-traces.binpb.gz")
	if _, err := os.Stat(testdataFile); err == nil {
		return []string{testdataFile}
	}

	// Fallback: try first file from /tmp/x if testdata doesn't exist
	tmpXFiles, _ := filepath.Glob("/tmp/x/traces*.binpb.gz")
	if len(tmpXFiles) > 0 {
		return []string{tmpXFiles[0]}
	}

	return nil
}

func runOldPipeline(ctx context.Context, t *testing.T, inputFiles []string, orgID string) (*PipelineStats, string, error) {
	tmpDir := t.TempDir()

	// Force GC and get initial memory stats
	runtime.GC()
	var startMem runtime.MemStats
	runtime.ReadMemStats(&startMem)

	startTime := time.Now()

	// Create readers for each file
	var readers []filereader.Reader
	for _, file := range inputFiles {
		reader, err := filereader.ReaderForFileWithOptions(file, filereader.ReaderOptions{
			SignalType: filereader.SignalTypeTraces,
			BatchSize:  1000,
			OrgID:      orgID,
		})
		if err != nil {
			return nil, "", fmt.Errorf("create reader for %s: %w", file, err)
		}
		readers = append(readers, reader)
	}

	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()

	// Get schema from first reader
	schema := readers[0].GetSchema()

	// Create parquet writer
	outputFile := filepath.Join(tmpDir, "output.parquet")
	writer, err := factories.NewTracesWriter(tmpDir, schema, 1000000, parquetwriter.DefaultBackend)
	if err != nil {
		return nil, "", fmt.Errorf("create writer: %w", err)
	}

	// Process all rows
	var totalRows int64
	for _, reader := range readers {
		for {
			batch, readErr := reader.Next(ctx)
			if readErr != nil && readErr != io.EOF {
				if batch != nil {
					pipeline.ReturnBatch(batch)
				}
				return nil, "", fmt.Errorf("read error: %w", readErr)
			}

			if batch != nil {
				totalRows += int64(batch.Len())
				if err := writer.WriteBatch(batch); err != nil {
					pipeline.ReturnBatch(batch)
					return nil, "", fmt.Errorf("write error: %w", err)
				}
				pipeline.ReturnBatch(batch)
			}

			if readErr == io.EOF {
				break
			}
		}
	}

	results, err := writer.Close(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("close writer: %w", err)
	}

	duration := time.Since(startTime)

	// Get memory stats
	runtime.GC()
	var endMem runtime.MemStats
	runtime.ReadMemStats(&endMem)

	// Get output file if any
	if len(results) > 0 {
		outputFile = results[0].FileName
	}

	// Get schema columns
	var columns []string
	for _, col := range schema.Columns() {
		columns = append(columns, string(col.Name.Value()))
	}
	sort.Strings(columns)

	return &PipelineStats{
		Name:          "Old Pipeline (filereader/parquetwriter)",
		Duration:      duration,
		Allocations:   endMem.Mallocs - startMem.Mallocs,
		TotalAlloc:    endMem.TotalAlloc - startMem.TotalAlloc,
		HeapInUse:     endMem.HeapInuse,
		RecordCount:   totalRows,
		OutputFiles:   len(results),
		OutputColumns: columns,
	}, outputFile, nil
}

func runDuckDBPipeline(ctx context.Context, t *testing.T, inputFiles []string, orgID string) (*PipelineStats, string, error) {
	tmpDir := t.TempDir()

	// Copy files to temp directory
	var localFiles []string
	for _, file := range inputFiles {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, "", fmt.Errorf("read %s: %w", file, err)
		}
		localFile := filepath.Join(tmpDir, filepath.Base(file))
		if err := os.WriteFile(localFile, data, 0644); err != nil {
			return nil, "", fmt.Errorf("write %s: %w", localFile, err)
		}
		localFiles = append(localFiles, localFile)
	}

	// Force GC and get initial memory stats
	runtime.GC()
	var startMem runtime.MemStats
	runtime.ReadMemStats(&startMem)

	startTime := time.Now()

	result, err := processTraceIngestWithDuckDB(ctx, localFiles, orgID, tmpDir)
	if err != nil {
		return nil, "", fmt.Errorf("DuckDB processing: %w", err)
	}

	duration := time.Since(startTime)

	// Get memory stats
	runtime.GC()
	var endMem runtime.MemStats
	runtime.ReadMemStats(&endMem)

	// Get output file and columns
	var outputFile string
	var columns []string

	for _, bin := range result.DateintBins {
		outputFile = bin.OutputFile
		// Get schema from output file
		cols, err := getParquetColumns(ctx, bin.OutputFile)
		if err != nil {
			t.Logf("Warning: failed to get parquet columns: %v", err)
		} else {
			columns = cols
		}
		break
	}
	sort.Strings(columns)

	return &PipelineStats{
		Name:          "New Pipeline (DuckDB)",
		Duration:      duration,
		Allocations:   endMem.Mallocs - startMem.Mallocs,
		TotalAlloc:    endMem.TotalAlloc - startMem.TotalAlloc,
		HeapInUse:     endMem.HeapInuse,
		RecordCount:   result.TotalRows,
		OutputFiles:   len(result.DateintBins),
		OutputColumns: columns,
	}, outputFile, nil
}

func getParquetColumns(ctx context.Context, filePath string) ([]string, error) {
	db, err := duckdbx.NewDB()
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	// Use DESCRIBE to get actual data columns, not parquet_schema which includes metadata entries like duckdb_schema
	query := fmt.Sprintf("SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('%s'))", filePath)
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}
	return columns, rows.Err()
}

func printComparisonReport(t *testing.T, old, new *PipelineStats, oldOutput, newOutput string) {
	fmt.Println()
	fmt.Println("=" + strings.Repeat("=", 79))
	fmt.Println("TRACE INGESTION PIPELINE COMPARISON REPORT")
	fmt.Println("=" + strings.Repeat("=", 79))
	fmt.Println()

	// Performance comparison
	fmt.Println("PERFORMANCE METRICS")
	fmt.Println("-" + strings.Repeat("-", 79))
	fmt.Printf("%-40s %20s %20s\n", "Metric", old.Name[:15]+"...", new.Name[:15]+"...")
	fmt.Println("-" + strings.Repeat("-", 79))

	speedup := float64(old.Duration) / float64(new.Duration)
	fmt.Printf("%-40s %20s %20s (%.2fx)\n", "Execution Time",
		old.Duration.Round(time.Millisecond),
		new.Duration.Round(time.Millisecond),
		speedup)

	fmt.Printf("%-40s %20d %20d\n", "Records Processed", old.RecordCount, new.RecordCount)
	fmt.Printf("%-40s %20d %20d\n", "Output Files", old.OutputFiles, new.OutputFiles)
	fmt.Println()

	// Memory comparison
	fmt.Println("MEMORY METRICS")
	fmt.Println("-" + strings.Repeat("-", 79))
	memReduction := 100 - (float64(new.TotalAlloc)/float64(old.TotalAlloc))*100
	fmt.Printf("%-40s %15s MB %15s MB (%.1f%% reduction)\n", "Total Allocations",
		formatMB(old.TotalAlloc),
		formatMB(new.TotalAlloc),
		memReduction)

	fmt.Printf("%-40s %20d %20d\n", "Allocation Count", old.Allocations, new.Allocations)
	fmt.Printf("%-40s %15s MB %15s MB\n", "Heap In Use",
		formatMB(old.HeapInUse),
		formatMB(new.HeapInUse))
	fmt.Println()

	// Schema comparison
	fmt.Println("SCHEMA COMPARISON")
	fmt.Println("-" + strings.Repeat("-", 79))
	fmt.Printf("%-40s %20d %20d\n", "Column Count", len(old.OutputColumns), len(new.OutputColumns))

	// Find common and different columns
	oldColSet := make(map[string]bool)
	for _, col := range old.OutputColumns {
		oldColSet[col] = true
	}
	newColSet := make(map[string]bool)
	for _, col := range new.OutputColumns {
		newColSet[col] = true
	}

	var onlyOld, onlyNew, common []string
	for col := range oldColSet {
		if newColSet[col] {
			common = append(common, col)
		} else {
			onlyOld = append(onlyOld, col)
		}
	}
	for col := range newColSet {
		if !oldColSet[col] {
			onlyNew = append(onlyNew, col)
		}
	}
	sort.Strings(onlyOld)
	sort.Strings(onlyNew)
	sort.Strings(common)

	fmt.Printf("%-40s %20d\n", "Common Columns", len(common))
	if len(onlyOld) > 0 {
		fmt.Printf("%-40s %s\n", "Only in Old Pipeline:", strings.Join(onlyOld, ", "))
	}
	if len(onlyNew) > 0 {
		fmt.Printf("%-40s %s\n", "Only in New Pipeline:", strings.Join(onlyNew, ", "))
	}
	fmt.Println()
	fmt.Println("=" + strings.Repeat("=", 79))
}

func formatMB(bytes uint64) string {
	return fmt.Sprintf("%.2f", float64(bytes)/(1024*1024))
}

func verifyOutputCorrectness(t *testing.T, oldOutput, newOutput string) {
	ctx := context.Background()

	// Both paths should produce output
	if oldOutput == "" || newOutput == "" {
		t.Log("Cannot verify correctness: one or both outputs are empty")
		return
	}

	// Compare row counts
	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	oldCount, err := getRowCount(ctx, conn, oldOutput)
	require.NoError(t, err)

	newCount, err := getRowCount(ctx, conn, newOutput)
	require.NoError(t, err)

	assert.Equal(t, oldCount, newCount, "Row counts should match between old and new pipelines")
	t.Logf("Row count verification: old=%d, new=%d", oldCount, newCount)
}

func getRowCount(ctx context.Context, conn *sql.Conn, filePath string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", filePath)

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()

	var count int64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return 0, err
		}
	}
	return count, nil
}

// BenchmarkTraceIngestPipelines benchmarks both pipelines
func BenchmarkTraceIngestPipelines(b *testing.B) {
	testFile := filepath.Join("..", "..", "testdata", "traces", "otel-traces.binpb.gz")
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skip("Test file not found")
	}

	ctx := context.Background()
	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001").String()

	b.Run("OldPipeline", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tmpDir := b.TempDir()

			reader, err := filereader.ReaderForFileWithOptions(testFile, filereader.ReaderOptions{
				SignalType: filereader.SignalTypeTraces,
				BatchSize:  1000,
				OrgID:      orgID,
			})
			if err != nil {
				b.Fatal(err)
			}

			schema := reader.GetSchema()
			writer, err := factories.NewTracesWriter(tmpDir, schema, 1000000, parquetwriter.DefaultBackend)
			if err != nil {
				_ = reader.Close()
				b.Fatal(err)
			}

			for {
				batch, readErr := reader.Next(ctx)
				if batch != nil {
					_ = writer.WriteBatch(batch)
					pipeline.ReturnBatch(batch)
				}
				if readErr == io.EOF {
					break
				}
				if readErr != nil {
					break
				}
			}

			_, _ = writer.Close(ctx)
			_ = reader.Close()
		}
	})

	b.Run("DuckDBPipeline", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			tmpDir := b.TempDir()

			data, _ := os.ReadFile(testFile)
			localFile := filepath.Join(tmpDir, "segment.binpb.gz")
			_ = os.WriteFile(localFile, data, 0644)

			_, _ = processTraceIngestWithDuckDB(ctx, []string{localFile}, orgID, tmpDir)
		}
	})
}
