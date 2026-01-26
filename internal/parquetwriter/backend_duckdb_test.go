// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package parquetwriter

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// TestDuckDBBackendBasic verifies basic DuckDB backend functionality.
func TestDuckDBBackendBasic(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create test schema
	schema := filereader.NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("id"), wkk.NewRowKey("id"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("name"), wkk.NewRowKey("name"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("value"), wkk.NewRowKey("value"), filereader.DataTypeFloat64, true)
	schema.AddColumn(wkk.NewRowKey("active"), wkk.NewRowKey("active"), filereader.DataTypeBool, true)

	config := BackendConfig{
		Type:      BackendDuckDB,
		TmpDir:    tmpDir,
		ChunkSize: 1000,
		Schema:    schema,
	}

	backend, err := NewDuckDBBackend(config)
	require.NoError(t, err)
	require.Equal(t, "duckdb", backend.Name())

	// Write test data
	batch := pipeline.GetBatch()
	batch.AppendRow(pipeline.Row{
		wkk.NewRowKey("id"):     int64(1),
		wkk.NewRowKey("name"):   "Alice",
		wkk.NewRowKey("value"):  1.5,
		wkk.NewRowKey("active"): true,
	})
	batch.AppendRow(pipeline.Row{
		wkk.NewRowKey("id"):     int64(2),
		wkk.NewRowKey("name"):   "Bob",
		wkk.NewRowKey("value"):  2.7,
		wkk.NewRowKey("active"): false,
	})

	err = backend.WriteBatch(ctx, batch)
	require.NoError(t, err)
	pipeline.ReturnBatch(batch)

	// Close and write to file
	outFile, err := os.Create(tmpDir + "/duckdb_output.parquet")
	require.NoError(t, err)

	metadata, err := backend.Close(ctx, outFile)
	require.NoError(t, err)
	require.NoError(t, outFile.Close())

	require.Equal(t, int64(2), metadata.RowCount)
	require.Equal(t, 4, metadata.ColumnCount)

	// Verify file is readable
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM read_parquet('%s') ORDER BY id", tmpDir+"/duckdb_output.parquet"))
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var count int
	for rows.Next() {
		count++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 2, count)
}

// TestArrowVsDuckDBComparison compares Arrow and DuckDB backend outputs.
func TestArrowVsDuckDBComparison(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create test schema with various types
	schema := filereader.NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("id"), wkk.NewRowKey("id"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("timestamp"), wkk.NewRowKey("timestamp"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("message"), wkk.NewRowKey("message"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("level"), wkk.NewRowKey("level"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("value"), wkk.NewRowKey("value"), filereader.DataTypeFloat64, true)
	schema.AddColumn(wkk.NewRowKey("active"), wkk.NewRowKey("active"), filereader.DataTypeBool, true)
	// String conversion columns
	schema.AddColumn(wkk.NewRowKey("resource_service"), wkk.NewRowKey("resource_service"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("attr_user_id"), wkk.NewRowKey("attr_user_id"), filereader.DataTypeString, true)

	// Create test data
	batch := pipeline.GetBatch()
	defer pipeline.ReturnBatch(batch)

	testData := []map[string]any{
		{
			"id":               int64(1),
			"timestamp":        int64(1000),
			"message":          "first message",
			"level":            "INFO",
			"value":            1.5,
			"active":           true,
			"resource_service": "frontend",
			"attr_user_id":     int64(100),
		},
		{
			"id":               int64(2),
			"timestamp":        int64(2000),
			"message":          "second message",
			"level":            "WARN",
			"value":            2.7,
			"active":           false,
			"resource_service": "backend",
			"attr_user_id":     int64(200),
		},
		{
			"id":               int64(3),
			"timestamp":        int64(3000),
			"message":          "third message",
			"level":            "ERROR",
			"value":            3.9,
			"active":           true,
			"resource_service": "database",
			"attr_user_id":     int64(300),
		},
	}

	for _, testRow := range testData {
		row := batch.AddRow()
		for k, v := range testRow {
			row[wkk.NewRowKey(k)] = v
		}
	}

	stringConversionPrefixes := []string{"resource_", "scope_", "attr_"}

	// Write with Arrow backend
	arrowConfig := BackendConfig{
		Type:                     BackendArrow,
		TmpDir:                   tmpDir,
		Schema:                   schema,
		ChunkSize:                10000,
		StringConversionPrefixes: stringConversionPrefixes,
	}
	arrowBackend, err := NewArrowBackend(arrowConfig)
	require.NoError(t, err)

	err = arrowBackend.WriteBatch(ctx, batch)
	require.NoError(t, err)

	arrowFile, err := os.Create(tmpDir + "/arrow.parquet")
	require.NoError(t, err)

	_, err = arrowBackend.Close(ctx, arrowFile)
	require.NoError(t, err)
	require.NoError(t, arrowFile.Close())

	// Write with DuckDB backend
	duckdbConfig := BackendConfig{
		Type:                     BackendDuckDB,
		TmpDir:                   tmpDir,
		Schema:                   schema,
		ChunkSize:                10000,
		StringConversionPrefixes: stringConversionPrefixes,
	}
	duckdbBackend, err := NewDuckDBBackend(duckdbConfig)
	require.NoError(t, err)

	err = duckdbBackend.WriteBatch(ctx, batch)
	require.NoError(t, err)

	duckdbFile, err := os.Create(tmpDir + "/duckdb.parquet")
	require.NoError(t, err)

	_, err = duckdbBackend.Close(ctx, duckdbFile)
	require.NoError(t, err)
	require.NoError(t, duckdbFile.Close())

	// Read back both files with DuckDB and compare
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	arrowRows := readParquetFile(t, db, tmpDir+"/arrow.parquet")
	duckdbRows := readParquetFile(t, db, tmpDir+"/duckdb.parquet")

	// Compare row counts
	require.Equal(t, len(arrowRows), len(duckdbRows), "Row counts should match")
	require.Equal(t, 3, len(arrowRows), "Should have 3 rows")

	// Compare each row field by field
	for i := 0; i < len(arrowRows); i++ {
		arrowRow := arrowRows[i]
		duckdbRow := duckdbRows[i]

		require.Equal(t, len(arrowRow), len(duckdbRow),
			"Row %d: column counts should match", i)

		for colName, arrowValue := range arrowRow {
			duckdbValue, ok := duckdbRow[colName]
			require.True(t, ok, "Row %d: column %s missing in duckdb output", i, colName)

			require.Equal(t, arrowValue, duckdbValue,
				"Row %d, Column %s: values should match\n  arrow: %v (%T)\n  duckdb: %v (%T)",
				i, colName, arrowValue, arrowValue, duckdbValue, duckdbValue)
		}
	}

	t.Logf("Arrow and DuckDB backends produced identical output (%d rows verified)", len(arrowRows))
}

// TestArrowVsDuckDBLargeDataset tests with a larger synthetic dataset.
func TestArrowVsDuckDBLargeDataset(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create schema
	schema := filereader.NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("id"), wkk.NewRowKey("id"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("timestamp"), wkk.NewRowKey("timestamp"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("message"), wkk.NewRowKey("message"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("value"), wkk.NewRowKey("value"), filereader.DataTypeFloat64, true)
	schema.AddColumn(wkk.NewRowKey("resource_service"), wkk.NewRowKey("resource_service"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("attr_count"), wkk.NewRowKey("attr_count"), filereader.DataTypeString, true)

	numRows := 10000

	// Create batch
	batch := pipeline.GetBatch()
	defer pipeline.ReturnBatch(batch)

	for i := 0; i < numRows; i++ {
		row := batch.AddRow()
		row[wkk.NewRowKey("id")] = int64(i)
		row[wkk.NewRowKey("timestamp")] = int64(1000 + i*10)
		row[wkk.NewRowKey("message")] = fmt.Sprintf("message-%d", i)
		row[wkk.NewRowKey("value")] = float64(i) * 1.5
		row[wkk.NewRowKey("resource_service")] = fmt.Sprintf("service-%d", i%5)
		row[wkk.NewRowKey("attr_count")] = int64(i * 2)
	}

	stringConversionPrefixes := []string{"resource_", "scope_", "attr_"}

	// Write with Arrow
	arrowConfig := BackendConfig{
		Type:                     BackendArrow,
		TmpDir:                   tmpDir,
		Schema:                   schema,
		ChunkSize:                10000,
		StringConversionPrefixes: stringConversionPrefixes,
	}
	arrowBackend, err := NewArrowBackend(arrowConfig)
	require.NoError(t, err)

	err = arrowBackend.WriteBatch(ctx, batch)
	require.NoError(t, err)

	arrowFile, err := os.Create(tmpDir + "/arrow_large.parquet")
	require.NoError(t, err)

	arrowMeta, err := arrowBackend.Close(ctx, arrowFile)
	require.NoError(t, err)
	require.NoError(t, arrowFile.Close())

	// Write with DuckDB
	duckdbConfig := BackendConfig{
		Type:                     BackendDuckDB,
		TmpDir:                   tmpDir,
		Schema:                   schema,
		ChunkSize:                10000,
		StringConversionPrefixes: stringConversionPrefixes,
	}
	duckdbBackend, err := NewDuckDBBackend(duckdbConfig)
	require.NoError(t, err)

	err = duckdbBackend.WriteBatch(ctx, batch)
	require.NoError(t, err)

	duckdbFile, err := os.Create(tmpDir + "/duckdb_large.parquet")
	require.NoError(t, err)

	duckdbMeta, err := duckdbBackend.Close(ctx, duckdbFile)
	require.NoError(t, err)
	require.NoError(t, duckdbFile.Close())

	require.Equal(t, arrowMeta.RowCount, duckdbMeta.RowCount, "Row counts should match")

	// Get file sizes
	arrowInfo, _ := os.Stat(tmpDir + "/arrow_large.parquet")
	duckdbInfo, _ := os.Stat(tmpDir + "/duckdb_large.parquet")

	t.Logf("Arrow file size: %d bytes", arrowInfo.Size())
	t.Logf("DuckDB file size: %d bytes", duckdbInfo.Size())

	// Read back and verify using DuckDB
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	arrowRows := readParquetFile(t, db, tmpDir+"/arrow_large.parquet")
	duckdbRows := readParquetFile(t, db, tmpDir+"/duckdb_large.parquet")

	require.Equal(t, len(arrowRows), len(duckdbRows), "Row counts should match")
	require.Equal(t, numRows, len(arrowRows), "Should have all rows")

	// Sample comparison
	samplesToCheck := []int{0, numRows / 4, numRows / 2, numRows - 1}
	for _, i := range samplesToCheck {
		arrowRow := arrowRows[i]
		duckdbRow := duckdbRows[i]

		for colName, arrowValue := range arrowRow {
			duckdbValue := duckdbRow[colName]
			require.Equal(t, arrowValue, duckdbValue,
				"Row %d, Column %s: values should match", i, colName)
		}
	}

	t.Logf("Large dataset comparison passed (%d rows)", numRows)
}

// TestDuckDBBackendWithNulls verifies proper null handling.
func TestDuckDBBackendWithNulls(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	schema := filereader.NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("id"), wkk.NewRowKey("id"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("name"), wkk.NewRowKey("name"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("optional_field"), wkk.NewRowKey("optional_field"), filereader.DataTypeString, true)

	config := BackendConfig{
		Type:      BackendDuckDB,
		TmpDir:    tmpDir,
		ChunkSize: 1000,
		Schema:    schema,
	}

	backend, err := NewDuckDBBackend(config)
	require.NoError(t, err)

	// Write rows with some nulls
	batch := pipeline.GetBatch()
	batch.AppendRow(pipeline.Row{
		wkk.NewRowKey("id"):             int64(1),
		wkk.NewRowKey("name"):           "Alice",
		wkk.NewRowKey("optional_field"): "has value",
	})
	batch.AppendRow(pipeline.Row{
		wkk.NewRowKey("id"):   int64(2),
		wkk.NewRowKey("name"): "Bob",
		// optional_field is null
	})
	batch.AppendRow(pipeline.Row{
		wkk.NewRowKey("id"):             int64(3),
		wkk.NewRowKey("name"):           "Charlie",
		wkk.NewRowKey("optional_field"): "another value",
	})

	err = backend.WriteBatch(ctx, batch)
	require.NoError(t, err)
	pipeline.ReturnBatch(batch)

	outFile, err := os.Create(tmpDir + "/nulls.parquet")
	require.NoError(t, err)

	metadata, err := backend.Close(ctx, outFile)
	require.NoError(t, err)
	require.NoError(t, outFile.Close())

	require.Equal(t, int64(3), metadata.RowCount)

	// Verify nulls are preserved
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var nullCount int
	err = db.QueryRow(fmt.Sprintf(
		"SELECT COUNT(*) FROM read_parquet('%s') WHERE optional_field IS NULL",
		tmpDir+"/nulls.parquet")).Scan(&nullCount)
	require.NoError(t, err)
	require.Equal(t, 1, nullCount, "Should have 1 null value")
}

// TestDuckDBBackendAbort verifies abort cleanup.
func TestDuckDBBackendAbort(t *testing.T) {
	tmpDir := t.TempDir()

	schema := filereader.NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("id"), wkk.NewRowKey("id"), filereader.DataTypeInt64, true)

	config := BackendConfig{
		Type:      BackendDuckDB,
		TmpDir:    tmpDir,
		ChunkSize: 1000,
		Schema:    schema,
	}

	backend, err := NewDuckDBBackend(config)
	require.NoError(t, err)

	// Write some data
	batch := pipeline.GetBatch()
	batch.AppendRow(pipeline.Row{
		wkk.NewRowKey("id"): int64(1),
	})
	err = backend.WriteBatch(context.Background(), batch)
	require.NoError(t, err)
	pipeline.ReturnBatch(batch)

	// Abort instead of close
	backend.Abort()

	// Verify backend is aborted
	require.True(t, backend.aborted)
}

// BenchmarkBackendComparison benchmarks Arrow vs DuckDB.
func BenchmarkBackendComparison(b *testing.B) {
	tmpDir := b.TempDir()
	ctx := context.Background()

	schema := filereader.NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("id"), wkk.NewRowKey("id"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("timestamp"), wkk.NewRowKey("timestamp"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("message"), wkk.NewRowKey("message"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("value"), wkk.NewRowKey("value"), filereader.DataTypeFloat64, true)
	schema.AddColumn(wkk.NewRowKey("resource_service"), wkk.NewRowKey("resource_service"), filereader.DataTypeString, true)

	rowsPerBatch := 1000
	numBatches := 100

	stringConversionPrefixes := []string{"resource_", "scope_", "attr_"}

	// Prepare test data
	batches := make([]*pipeline.Batch, numBatches)
	for bi := 0; bi < numBatches; bi++ {
		batch := pipeline.GetBatch()
		for i := 0; i < rowsPerBatch; i++ {
			rowIdx := bi*rowsPerBatch + i
			row := batch.AddRow()
			row[wkk.NewRowKey("id")] = int64(rowIdx)
			row[wkk.NewRowKey("timestamp")] = int64(1000 + rowIdx*10)
			row[wkk.NewRowKey("message")] = fmt.Sprintf("message-%d", rowIdx)
			row[wkk.NewRowKey("value")] = float64(rowIdx) * 1.5
			row[wkk.NewRowKey("resource_service")] = fmt.Sprintf("service-%d", rowIdx%5)
		}
		batches[bi] = batch
	}
	defer func() {
		for _, batch := range batches {
			pipeline.ReturnBatch(batch)
		}
	}()

	b.Run("Arrow", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			config := BackendConfig{
				Type:                     BackendArrow,
				TmpDir:                   tmpDir,
				Schema:                   schema,
				ChunkSize:                10000,
				StringConversionPrefixes: stringConversionPrefixes,
			}
			backend, err := NewArrowBackend(config)
			if err != nil {
				b.Fatal(err)
			}

			for _, batch := range batches {
				if err := backend.WriteBatch(ctx, batch); err != nil {
					b.Fatal(err)
				}
			}

			outFile, err := os.CreateTemp(tmpDir, "arrow-*.parquet")
			if err != nil {
				b.Fatal(err)
			}
			_, err = backend.Close(ctx, outFile)
			_ = outFile.Close()
			_ = os.Remove(outFile.Name())
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("DuckDB", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			config := BackendConfig{
				Type:                     BackendDuckDB,
				TmpDir:                   tmpDir,
				Schema:                   schema,
				ChunkSize:                10000,
				StringConversionPrefixes: stringConversionPrefixes,
			}
			backend, err := NewDuckDBBackend(config)
			if err != nil {
				b.Fatal(err)
			}

			for _, batch := range batches {
				if err := backend.WriteBatch(ctx, batch); err != nil {
					b.Fatal(err)
				}
			}

			outFile, err := os.CreateTemp(tmpDir, "duckdb-*.parquet")
			if err != nil {
				b.Fatal(err)
			}
			_, err = backend.Close(ctx, outFile)
			_ = outFile.Close()
			_ = os.Remove(outFile.Name())
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// TestBackendPerformanceComparison runs a detailed performance comparison.
func TestBackendPerformanceComparison(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	tmpDir := t.TempDir()
	ctx := context.Background()

	schema := filereader.NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("id"), wkk.NewRowKey("id"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("timestamp"), wkk.NewRowKey("timestamp"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("message"), wkk.NewRowKey("message"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("value"), wkk.NewRowKey("value"), filereader.DataTypeFloat64, true)
	schema.AddColumn(wkk.NewRowKey("active"), wkk.NewRowKey("active"), filereader.DataTypeBool, true)
	schema.AddColumn(wkk.NewRowKey("resource_service"), wkk.NewRowKey("resource_service"), filereader.DataTypeString, true)

	numRows := 100000
	batchSize := 10000
	stringConversionPrefixes := []string{"resource_", "scope_", "attr_"}

	// Prepare batches
	var batches []*pipeline.Batch
	for start := 0; start < numRows; start += batchSize {
		batch := pipeline.GetBatch()
		end := start + batchSize
		if end > numRows {
			end = numRows
		}
		for i := start; i < end; i++ {
			row := batch.AddRow()
			row[wkk.NewRowKey("id")] = int64(i)
			row[wkk.NewRowKey("timestamp")] = int64(1000 + i*10)
			row[wkk.NewRowKey("message")] = fmt.Sprintf("log message with some content for row %d that has reasonable length", i)
			row[wkk.NewRowKey("value")] = float64(i) * 1.5
			row[wkk.NewRowKey("active")] = i%2 == 0
			row[wkk.NewRowKey("resource_service")] = fmt.Sprintf("service-%d", i%10)
		}
		batches = append(batches, batch)
	}
	defer func() {
		for _, batch := range batches {
			pipeline.ReturnBatch(batch)
		}
	}()

	type result struct {
		name        string
		duration    time.Duration
		fileSize    int64
		allocBefore uint64
		allocAfter  uint64
	}

	var results []result

	// Test Arrow backend
	{
		var memStatsBefore, memStatsAfter runtime.MemStats
		runtime.ReadMemStats(&memStatsBefore)
		runtime.GC()

		start := time.Now()
		config := BackendConfig{
			Type:                     BackendArrow,
			TmpDir:                   tmpDir,
			Schema:                   schema,
			ChunkSize:                50000,
			StringConversionPrefixes: stringConversionPrefixes,
		}
		backend, err := NewArrowBackend(config)
		require.NoError(t, err)

		for _, batch := range batches {
			err := backend.WriteBatch(ctx, batch)
			require.NoError(t, err)
		}

		outFile, err := os.Create(tmpDir + "/arrow_perf.parquet")
		require.NoError(t, err)

		_, err = backend.Close(ctx, outFile)
		require.NoError(t, err)
		require.NoError(t, outFile.Close())
		duration := time.Since(start)

		runtime.GC()
		runtime.ReadMemStats(&memStatsAfter)

		info, _ := os.Stat(tmpDir + "/arrow_perf.parquet")
		results = append(results, result{
			name:        "Arrow",
			duration:    duration,
			fileSize:    info.Size(),
			allocBefore: memStatsBefore.TotalAlloc,
			allocAfter:  memStatsAfter.TotalAlloc,
		})
	}

	// Test DuckDB backend
	{
		var memStatsBefore, memStatsAfter runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&memStatsBefore)

		start := time.Now()
		config := BackendConfig{
			Type:                     BackendDuckDB,
			TmpDir:                   tmpDir,
			Schema:                   schema,
			ChunkSize:                50000,
			StringConversionPrefixes: stringConversionPrefixes,
		}
		backend, err := NewDuckDBBackend(config)
		require.NoError(t, err)

		for _, batch := range batches {
			err := backend.WriteBatch(ctx, batch)
			require.NoError(t, err)
		}

		outFile, err := os.Create(tmpDir + "/duckdb_perf.parquet")
		require.NoError(t, err)

		_, err = backend.Close(ctx, outFile)
		require.NoError(t, err)
		require.NoError(t, outFile.Close())
		duration := time.Since(start)

		runtime.GC()
		runtime.ReadMemStats(&memStatsAfter)

		info, _ := os.Stat(tmpDir + "/duckdb_perf.parquet")
		results = append(results, result{
			name:        "DuckDB",
			duration:    duration,
			fileSize:    info.Size(),
			allocBefore: memStatsBefore.TotalAlloc,
			allocAfter:  memStatsAfter.TotalAlloc,
		})
	}

	// Print results
	t.Logf("\n=== Performance Comparison (%d rows) ===\n", numRows)
	for _, r := range results {
		allocDelta := r.allocAfter - r.allocBefore
		t.Logf("%s:\n", r.name)
		t.Logf("  Duration:     %v\n", r.duration)
		t.Logf("  File size:    %d bytes (%.2f MB)\n", r.fileSize, float64(r.fileSize)/1024/1024)
		t.Logf("  Alloc delta:  %d bytes (%.2f MB)\n", allocDelta, float64(allocDelta)/1024/1024)
		t.Logf("  Throughput:   %.0f rows/sec\n", float64(numRows)/r.duration.Seconds())
	}

	// Verify data equivalence
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	arrowRows := readParquetFile(t, db, tmpDir+"/arrow_perf.parquet")
	duckdbRows := readParquetFile(t, db, tmpDir+"/duckdb_perf.parquet")

	require.Equal(t, len(arrowRows), len(duckdbRows), "Row counts should match")
	require.Equal(t, numRows, len(arrowRows), "Should have all rows")

	// Spot check a few rows
	for _, i := range []int{0, numRows / 2, numRows - 1} {
		for colName, arrowValue := range arrowRows[i] {
			duckdbValue := duckdbRows[i][colName]
			require.Equal(t, arrowValue, duckdbValue,
				"Row %d, Column %s mismatch", i, colName)
		}
	}

	t.Logf("\nData verified: Arrow and DuckDB outputs are identical")
}
