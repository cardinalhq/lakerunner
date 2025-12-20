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

package parquetwriter

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/file"
	pqmetadata "github.com/apache/arrow-go/v18/parquet/metadata"
	goparquet "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// TestArrowBackendStatistics verifies that the Arrow backend writes parquet files
// with statistics (min/max values) set correctly. This is critical because we rely
// on these statistics to determine if columns contain non-null values when reading.
func TestArrowBackendStatistics(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test schema with various column types
	schema := filereader.NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("id"), wkk.NewRowKey("id"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("name"), wkk.NewRowKey("name"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("score"), wkk.NewRowKey("score"), filereader.DataTypeFloat64, true)
	schema.AddColumn(wkk.NewRowKey("active"), wkk.NewRowKey("active"), filereader.DataTypeBool, true)
	schema.AddColumn(wkk.NewRowKey("nullable_field"), wkk.NewRowKey("nullable_field"), filereader.DataTypeString, true)

	// Create backend
	config := BackendConfig{
		Type:      BackendArrow,
		TmpDir:    tmpDir,
		ChunkSize: 100,
		Schema:    schema,
	}

	backend, err := NewArrowBackend(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Write test data with some nulls
	batch := pipeline.GetBatch()

	// Row 1: id, name, score, active (no nullable_field)
	batch.AppendRow(pipeline.Row{
		wkk.NewRowKey("id"):     int64(1),
		wkk.NewRowKey("name"):   "Alice",
		wkk.NewRowKey("score"):  95.5,
		wkk.NewRowKey("active"): true,
	})

	// Row 2: all fields including nullable_field
	batch.AppendRow(pipeline.Row{
		wkk.NewRowKey("id"):             int64(2),
		wkk.NewRowKey("name"):           "Bob",
		wkk.NewRowKey("score"):          87.3,
		wkk.NewRowKey("active"):         false,
		wkk.NewRowKey("nullable_field"): "has value",
	})

	// Row 3: id, name, score, active (no nullable_field)
	batch.AppendRow(pipeline.Row{
		wkk.NewRowKey("id"):     int64(3),
		wkk.NewRowKey("name"):   "Charlie",
		wkk.NewRowKey("score"):  92.1,
		wkk.NewRowKey("active"): true,
	})

	err = backend.WriteBatch(ctx, batch)
	require.NoError(t, err)
	pipeline.ReturnBatch(batch)

	// Close backend and get the parquet file
	var buf bytes.Buffer
	metadata, err := backend.Close(context.Background(), &buf)
	require.NoError(t, err)
	require.NotNil(t, metadata)

	// Write to a temp file so we can read it with Arrow
	tmpFile, err := os.CreateTemp(tmpDir, "test-*.parquet")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	_, err = tmpFile.Write(buf.Bytes())
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	// Read the parquet file and verify statistics
	f, err := os.Open(tmpFile.Name())
	require.NoError(t, err)
	// Note: NewParquetReader takes ownership of the file and will close it

	pf, err := file.NewParquetReader(f, file.WithReadProps(nil))
	require.NoError(t, err)
	defer func() { require.NoError(t, pf.Close()) }()

	// Verify we have row groups
	require.Greater(t, pf.NumRowGroups(), 0, "Parquet file should have at least one row group")

	// Check statistics for each column
	rgMeta := pf.MetaData().RowGroup(0)
	require.Equal(t, int64(3), rgMeta.NumRows(), "Row group should have 3 rows")

	// Map column names to their indices
	columnMap := make(map[string]int)
	for i := 0; i < pf.MetaData().Schema.NumColumns(); i++ {
		col := pf.MetaData().Schema.Column(i)
		columnMap[col.Name()] = i
	}

	// Verify statistics for each column
	t.Run("id column has statistics", func(t *testing.T) {
		colIdx, ok := columnMap["id"]
		require.True(t, ok, "id column should exist")

		colChunk, err := rgMeta.ColumnChunk(colIdx)
		require.NoError(t, err)

		stats, err := colChunk.Statistics()
		require.NoError(t, err)
		require.NotNil(t, stats, "id column should have statistics")

		require.True(t, stats.HasMinMax(), "id column should have min/max values")
		require.True(t, stats.HasNullCount(), "id column should have null count")
		require.Equal(t, int64(0), stats.NullCount(), "id column should have no nulls")

		// Verify min/max values are correct (we wrote 1, 2, 3)
		minBytes := stats.EncodeMin()
		maxBytes := stats.EncodeMax()
		colDescr := stats.Descr()
		minVal := pqmetadata.GetStatValue(colDescr.PhysicalType(), minBytes)
		maxVal := pqmetadata.GetStatValue(colDescr.PhysicalType(), maxBytes)
		require.Equal(t, int64(1), minVal, "id min should be 1")
		require.Equal(t, int64(3), maxVal, "id max should be 3")
	})

	t.Run("name column has statistics", func(t *testing.T) {
		colIdx, ok := columnMap["name"]
		require.True(t, ok, "name column should exist")

		colChunk, err := rgMeta.ColumnChunk(colIdx)
		require.NoError(t, err)

		stats, err := colChunk.Statistics()
		require.NoError(t, err)
		require.NotNil(t, stats, "name column should have statistics")

		require.True(t, stats.HasMinMax(), "name column should have min/max values")
		require.True(t, stats.HasNullCount(), "name column should have null count")
		require.Equal(t, int64(0), stats.NullCount(), "name column should have no nulls")

		// Verify min/max values are correct (we wrote "Alice", "Bob", "Charlie")
		// String comparison is lexicographic
		minBytes := stats.EncodeMin()
		maxBytes := stats.EncodeMax()
		colDescr := stats.Descr()
		minVal := pqmetadata.GetStatValue(colDescr.PhysicalType(), minBytes)
		maxVal := pqmetadata.GetStatValue(colDescr.PhysicalType(), maxBytes)
		require.Equal(t, "Alice", string(minVal.([]byte)), "name min should be 'Alice'")
		require.Equal(t, "Charlie", string(maxVal.([]byte)), "name max should be 'Charlie'")
	})

	t.Run("score column has statistics", func(t *testing.T) {
		colIdx, ok := columnMap["score"]
		require.True(t, ok, "score column should exist")

		colChunk, err := rgMeta.ColumnChunk(colIdx)
		require.NoError(t, err)

		stats, err := colChunk.Statistics()
		require.NoError(t, err)
		require.NotNil(t, stats, "score column should have statistics")

		require.True(t, stats.HasMinMax(), "score column should have min/max values")
		require.True(t, stats.HasNullCount(), "score column should have null count")
		require.Equal(t, int64(0), stats.NullCount(), "score column should have no nulls")

		// Verify min/max values are correct (we wrote 95.5, 87.3, 92.1)
		minBytes := stats.EncodeMin()
		maxBytes := stats.EncodeMax()
		colDescr := stats.Descr()
		minVal := pqmetadata.GetStatValue(colDescr.PhysicalType(), minBytes)
		maxVal := pqmetadata.GetStatValue(colDescr.PhysicalType(), maxBytes)
		require.Equal(t, 87.3, minVal.(float64), "score min should be 87.3")
		require.Equal(t, 95.5, maxVal.(float64), "score max should be 95.5")
	})

	t.Run("active column has statistics", func(t *testing.T) {
		colIdx, ok := columnMap["active"]
		require.True(t, ok, "active column should exist")

		colChunk, err := rgMeta.ColumnChunk(colIdx)
		require.NoError(t, err)

		stats, err := colChunk.Statistics()
		require.NoError(t, err)
		require.NotNil(t, stats, "active column should have statistics")

		require.True(t, stats.HasMinMax(), "active column should have min/max values")
		require.True(t, stats.HasNullCount(), "active column should have null count")
		require.Equal(t, int64(0), stats.NullCount(), "active column should have no nulls")

		// Verify min/max values are correct (we wrote true, false, true)
		// For boolean, false < true
		minBytes := stats.EncodeMin()
		maxBytes := stats.EncodeMax()
		colDescr := stats.Descr()
		minVal := pqmetadata.GetStatValue(colDescr.PhysicalType(), minBytes)
		maxVal := pqmetadata.GetStatValue(colDescr.PhysicalType(), maxBytes)
		require.Equal(t, false, minVal.(bool), "active min should be false")
		require.Equal(t, true, maxVal.(bool), "active max should be true")
	})

	t.Run("nullable_field column has statistics showing nulls", func(t *testing.T) {
		colIdx, ok := columnMap["nullable_field"]
		require.True(t, ok, "nullable_field column should exist")

		colChunk, err := rgMeta.ColumnChunk(colIdx)
		require.NoError(t, err)

		stats, err := colChunk.Statistics()
		require.NoError(t, err)
		require.NotNil(t, stats, "nullable_field column should have statistics")

		// This column has 2 nulls and 1 non-null value
		require.True(t, stats.HasNullCount(), "nullable_field column should have null count")
		require.Equal(t, int64(2), stats.NullCount(), "nullable_field should have 2 nulls")

		// Should still have min/max for the one non-null value ("has value")
		require.True(t, stats.HasMinMax(), "nullable_field should have min/max even with some nulls")
		minBytes := stats.EncodeMin()
		maxBytes := stats.EncodeMax()
		colDescr := stats.Descr()
		minVal := pqmetadata.GetStatValue(colDescr.PhysicalType(), minBytes)
		maxVal := pqmetadata.GetStatValue(colDescr.PhysicalType(), maxBytes)
		require.Equal(t, "has value", string(minVal.([]byte)), "nullable_field min should be 'has value'")
		require.Equal(t, "has value", string(maxVal.([]byte)), "nullable_field max should be 'has value'")
	})
}

// TestArrowBackendAllNullColumn verifies that when we write a column that only contains
// nulls, the statistics correctly reflect this, and the reader can detect it.
// This tests the round-trip: write with schema HasNonNull=true, but only write nulls,
// then read back and verify statistics show it's actually all-null.
func TestArrowBackendAllNullColumn(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a test schema with a column we'll never populate (only nulls)
	schema := filereader.NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("id"), wkk.NewRowKey("id"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("name"), wkk.NewRowKey("name"), filereader.DataTypeString, true)
	// This column is marked as HasNonNull=true in the schema, but we'll only write nulls
	schema.AddColumn(wkk.NewRowKey("never_populated"), wkk.NewRowKey("never_populated"), filereader.DataTypeString, true)

	config := BackendConfig{
		Type:      BackendArrow,
		TmpDir:    tmpDir,
		ChunkSize: 100,
		Schema:    schema,
	}

	backend, err := NewArrowBackend(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Write test data - never include "never_populated" field (will be null)
	batch := pipeline.GetBatch()
	batch.AppendRow(pipeline.Row{
		wkk.NewRowKey("id"):   int64(1),
		wkk.NewRowKey("name"): "Alice",
		// never_populated is omitted (null)
	})
	batch.AppendRow(pipeline.Row{
		wkk.NewRowKey("id"):   int64(2),
		wkk.NewRowKey("name"): "Bob",
		// never_populated is omitted (null)
	})
	batch.AppendRow(pipeline.Row{
		wkk.NewRowKey("id"):   int64(3),
		wkk.NewRowKey("name"): "Charlie",
		// never_populated is omitted (null)
	})

	err = backend.WriteBatch(ctx, batch)
	require.NoError(t, err)
	pipeline.ReturnBatch(batch)

	// Close backend and get the parquet file
	var buf bytes.Buffer
	metadata, err := backend.Close(context.Background(), &buf)
	require.NoError(t, err)
	require.NotNil(t, metadata)

	// Write to a temp file so we can read it back
	tmpFile, err := os.CreateTemp(tmpDir, "test-*.parquet")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	_, err = tmpFile.Write(buf.Bytes())
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	// Read the parquet file using our Arrow reader
	f, err := os.Open(tmpFile.Name())
	require.NoError(t, err)
	// Note: NewIngestLogParquetReader takes ownership of the file and will close it

	// Use our Arrow reader to extract schema with statistics
	arrowReader, err := filereader.NewIngestLogParquetReader(ctx, f, 1000)
	require.NoError(t, err)
	defer func() { require.NoError(t, arrowReader.Close()) }()

	extractedSchema := arrowReader.GetSchema()
	require.NotNil(t, extractedSchema)

	// Verify the schema correctly identifies the all-null column
	columns := extractedSchema.Columns()

	var foundNeverPopulated bool
	for _, col := range columns {
		if col.Name.Value() == "never_populated" {
			foundNeverPopulated = true
			// The column should be marked as HasNonNull=false because statistics
			// show it only contains nulls (no min/max set, or null count == total rows)
			require.False(t, col.HasNonNull,
				"never_populated column should be detected as all-null based on statistics")
		}
	}

	require.True(t, foundNeverPopulated, "never_populated column should exist in schema")

	// Also verify the other columns are correctly marked as having data
	for _, col := range columns {
		if col.Name.Value() == "id" || col.Name.Value() == "name" {
			require.True(t, col.HasNonNull,
				"%s column should be marked as having non-null values", col.Name.Value())
		}
	}
}

// TestArrowBackendStatisticsEnabled verifies that statistics are enabled by default
// in the Arrow parquet writer configuration.
func TestArrowBackendStatisticsEnabled(t *testing.T) {
	tmpDir := t.TempDir()

	schema := filereader.NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("test_col"), wkk.NewRowKey("test_col"), filereader.DataTypeInt64, true)

	config := BackendConfig{
		Type:      BackendArrow,
		TmpDir:    tmpDir,
		ChunkSize: 100,
		Schema:    schema,
	}

	backend, err := NewArrowBackend(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Write minimal data
	batch := pipeline.GetBatch()
	batch.AppendRow(pipeline.Row{
		wkk.NewRowKey("test_col"): int64(42),
	})
	err = backend.WriteBatch(ctx, batch)
	require.NoError(t, err)
	pipeline.ReturnBatch(batch)

	var buf bytes.Buffer
	_, err = backend.Close(context.Background(), &buf)
	require.NoError(t, err)

	// Verify the file has statistics
	tmpFile, err := os.CreateTemp(tmpDir, "test-*.parquet")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	_, err = tmpFile.Write(buf.Bytes())
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	f, err := os.Open(tmpFile.Name())
	require.NoError(t, err)
	// Note: NewParquetReader takes ownership of the file and will close it

	pf, err := file.NewParquetReader(f)
	require.NoError(t, err)
	defer func() { require.NoError(t, pf.Close()) }()

	require.Greater(t, pf.NumRowGroups(), 0)

	rgMeta := pf.MetaData().RowGroup(0)
	colChunk, err := rgMeta.ColumnChunk(0)
	require.NoError(t, err)

	stats, err := colChunk.Statistics()
	require.NoError(t, err)
	require.NotNil(t, stats, "Statistics should be present by default")
	require.True(t, stats.HasMinMax(), "Min/max should be set by default")
}

// TestArrowBackendBinaryDataReadByGoParquet writes binary data with Arrow and
// reads it back with go-parquet to verify cross-library compatibility.
func TestArrowBackendBinaryDataReadByGoParquet(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a schema with binary data column (like chq_sketch)
	schema := filereader.NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("id"), wkk.NewRowKey("id"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("name"), wkk.NewRowKey("name"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("sketch"), wkk.NewRowKey("sketch"), filereader.DataTypeBytes, true)

	config := BackendConfig{
		Type:      BackendArrow,
		TmpDir:    tmpDir,
		ChunkSize: 100,
		Schema:    schema,
	}

	backend, err := NewArrowBackend(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Write test data with binary column
	batch := pipeline.GetBatch()
	batch.AppendRow(pipeline.Row{
		wkk.NewRowKey("id"):     int64(1),
		wkk.NewRowKey("name"):   "test1",
		wkk.NewRowKey("sketch"): []byte{0x01, 0x02, 0x03, 0x04},
	})
	batch.AppendRow(pipeline.Row{
		wkk.NewRowKey("id"):     int64(2),
		wkk.NewRowKey("name"):   "test2",
		wkk.NewRowKey("sketch"): []byte{0x05, 0x06, 0x07, 0x08, 0x09},
	})
	batch.AppendRow(pipeline.Row{
		wkk.NewRowKey("id"):   int64(3),
		wkk.NewRowKey("name"): "test3",
		// No sketch - should be null
	})

	err = backend.WriteBatch(ctx, batch)
	require.NoError(t, err)
	pipeline.ReturnBatch(batch)

	// Close backend and write to file
	tmpFile, err := os.CreateTemp(tmpDir, "arrow-binary-*.parquet")
	require.NoError(t, err)
	filePath := tmpFile.Name()

	_, err = backend.Close(ctx, tmpFile)
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	// Now read it back with go-parquet
	goParquetFile, err := os.Open(filePath)
	require.NoError(t, err)
	defer func() { _ = goParquetFile.Close() }()

	stat, err := goParquetFile.Stat()
	require.NoError(t, err)

	// Use go-parquet to read the file
	pf, err := goparquet.OpenFile(goParquetFile, stat.Size())
	require.NoError(t, err, "go-parquet should be able to read Arrow-written file")

	// Read rows using RowGroups
	for _, rg := range pf.RowGroups() {
		rows := rg.Rows()
		defer func() { _ = rows.Close() }()

		row := make(goparquet.Row, 3) // 3 columns
		for {
			n, err := rows.ReadRows([]goparquet.Row{row})
			if n == 0 || err != nil {
				break
			}
			t.Logf("Read row: %v", row)
		}
	}
}

// TestArrowBackendLargeDataReadByGoParquet writes enough data to create multiple pages
// and verifies go-parquet can read all pages correctly.
func TestArrowBackendLargeDataReadByGoParquet(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a schema with various column types including binary
	schema := filereader.NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("id"), wkk.NewRowKey("id"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("timestamp"), wkk.NewRowKey("timestamp"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("name"), wkk.NewRowKey("name"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("value"), wkk.NewRowKey("value"), filereader.DataTypeFloat64, true)
	schema.AddColumn(wkk.NewRowKey("sketch"), wkk.NewRowKey("sketch"), filereader.DataTypeBytes, true)
	schema.AddColumn(wkk.NewRowKey("resource_service"), wkk.NewRowKey("resource_service"), filereader.DataTypeString, true)

	// Use small chunk size to force multiple flushes
	config := BackendConfig{
		Type:      BackendArrow,
		TmpDir:    tmpDir,
		ChunkSize: 100, // Small chunks to force multiple pages
		Schema:    schema,
	}

	backend, err := NewArrowBackend(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Write many rows to create multiple pages
	numRows := 1000
	for batchStart := 0; batchStart < numRows; batchStart += 50 {
		batch := pipeline.GetBatch()
		for i := batchStart; i < batchStart+50 && i < numRows; i++ {
			row := pipeline.Row{
				wkk.NewRowKey("id"):        int64(i),
				wkk.NewRowKey("timestamp"): int64(1000000 + i),
				wkk.NewRowKey("name"):      fmt.Sprintf("metric_%d", i),
				wkk.NewRowKey("value"):     float64(i) * 1.5,
			}
			// Add binary data for some rows
			if i%3 == 0 {
				row[wkk.NewRowKey("sketch")] = []byte{byte(i % 256), byte((i + 1) % 256), byte((i + 2) % 256)}
			}
			// Add resource_service for some rows
			if i%2 == 0 {
				row[wkk.NewRowKey("resource_service")] = fmt.Sprintf("service_%d", i%10)
			}
			batch.AppendRow(row)
		}
		err = backend.WriteBatch(ctx, batch)
		require.NoError(t, err)
		pipeline.ReturnBatch(batch)
	}

	// Close backend and write to file
	tmpFile, err := os.CreateTemp(tmpDir, "arrow-large-*.parquet")
	require.NoError(t, err)
	filePath := tmpFile.Name()

	metadata, err := backend.Close(ctx, tmpFile)
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	t.Logf("Wrote %d rows to %s", metadata.RowCount, filePath)

	// Now read it back with go-parquet
	goParquetFile, err := os.Open(filePath)
	require.NoError(t, err)
	defer func() { _ = goParquetFile.Close() }()

	stat, err := goParquetFile.Stat()
	require.NoError(t, err)

	// Use go-parquet to read the file
	pf, err := goparquet.OpenFile(goParquetFile, stat.Size())
	require.NoError(t, err, "go-parquet should be able to read Arrow-written file")

	t.Logf("File has %d row groups, %d rows total", len(pf.RowGroups()), pf.NumRows())
	t.Logf("Schema: %s", pf.Schema())

	// Count all rows
	totalRows := int64(0)
	for rgIdx, rg := range pf.RowGroups() {
		t.Logf("RowGroup %d: %d rows", rgIdx, rg.NumRows())
		rows := rg.Rows()

		row := make(goparquet.Row, 6) // 6 columns
		rowsInGroup := 0
		for {
			n, err := rows.ReadRows([]goparquet.Row{row})
			if n == 0 {
				if err != nil {
					t.Logf("  Error reading row: %v", err)
				}
				break
			}
			if err != nil {
				t.Logf("  Read error at row %d: %v", rowsInGroup, err)
			}
			rowsInGroup++
			totalRows++
		}
		t.Logf("  Read %d rows from group %d", rowsInGroup, rgIdx)
		_ = rows.Close()
	}

	require.Equal(t, int64(numRows), totalRows, "should read all rows")
	t.Logf("Successfully read %d rows", totalRows)
}

// TestArrowBackendSingleBatchReadByGoParquet writes exactly one batch
// to isolate whether the issue is multi-batch or single-batch.
func TestArrowBackendSingleBatchReadByGoParquet(t *testing.T) {
	tmpDir := t.TempDir()

	schema := filereader.NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("id"), wkk.NewRowKey("id"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("name"), wkk.NewRowKey("name"), filereader.DataTypeString, true)

	// Chunk size larger than our data - should be one batch
	config := BackendConfig{
		Type:      BackendArrow,
		TmpDir:    tmpDir,
		ChunkSize: 1000,
		Schema:    schema,
	}

	backend, err := NewArrowBackend(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Write exactly 50 rows - should be one batch, one row group
	batch := pipeline.GetBatch()
	for i := 0; i < 50; i++ {
		batch.AppendRow(pipeline.Row{
			wkk.NewRowKey("id"):   int64(i),
			wkk.NewRowKey("name"): fmt.Sprintf("name_%d", i),
		})
	}
	err = backend.WriteBatch(ctx, batch)
	require.NoError(t, err)
	pipeline.ReturnBatch(batch)

	// Close
	tmpFile, err := os.CreateTemp(tmpDir, "single-batch-*.parquet")
	require.NoError(t, err)
	filePath := tmpFile.Name()

	metadata, err := backend.Close(ctx, tmpFile)
	require.NoError(t, err)
	require.NoError(t, tmpFile.Close())

	t.Logf("Wrote %d rows", metadata.RowCount)

	// First verify with Arrow's own reader and inspect metadata
	arrowPF, err := file.OpenParquetFile(filePath, false)
	require.NoError(t, err)
	t.Logf("Arrow reader: %d row groups, %d rows", arrowPF.NumRowGroups(), arrowPF.NumRows())

	// Inspect the metadata
	fileMeta := arrowPF.MetaData()
	t.Logf("Arrow metadata: version=%v, created_by=%s", fileMeta.Version(), fileMeta.GetCreatedBy())

	for rgIdx := 0; rgIdx < arrowPF.NumRowGroups(); rgIdx++ {
		rgMeta := fileMeta.RowGroup(rgIdx)
		t.Logf("Arrow RG %d: %d rows, %d cols", rgIdx, rgMeta.NumRows(), rgMeta.NumColumns())
		for colIdx := 0; colIdx < rgMeta.NumColumns(); colIdx++ {
			ccMeta, err := rgMeta.ColumnChunk(colIdx)
			require.NoError(t, err)
			t.Logf("  Col %d: encoding=%v, num_values=%d, dict_page_offset=%d, data_page_offset=%d",
				colIdx, ccMeta.Encodings(), ccMeta.NumValues(),
				ccMeta.DictionaryPageOffset(), ccMeta.DataPageOffset())
		}
	}
	_ = arrowPF.Close()

	// Read with go-parquet
	goParquetFile, err := os.Open(filePath)
	require.NoError(t, err)
	defer func() { _ = goParquetFile.Close() }()

	stat, err := goParquetFile.Stat()
	require.NoError(t, err)

	pf, err := goparquet.OpenFile(goParquetFile, stat.Size())
	require.NoError(t, err)

	t.Logf("File has %d row groups, %d rows", len(pf.RowGroups()), pf.NumRows())

	// Inspect column chunks
	pfSchema := pf.Schema()
	for rgIdx, rg := range pf.RowGroups() {
		t.Logf("Row group %d:", rgIdx)
		for ccIdx, cc := range rg.ColumnChunks() {
			colIdx := cc.Column()
			colPath := pfSchema.Columns()[colIdx]
			t.Logf("  Column %d: name=%s, type=%v",
				ccIdx, strings.Join(colPath, "."), cc.Type())
		}
	}

	totalRows := int64(0)
	for rgIdx, rg := range pf.RowGroups() {
		rows := rg.Rows()
		row := make(goparquet.Row, 2)
		for {
			n, err := rows.ReadRows([]goparquet.Row{row})
			if n == 0 {
				if err != nil {
					t.Logf("Error in row group %d: %v", rgIdx, err)
				}
				break
			}
			totalRows++
		}
		_ = rows.Close()
	}

	// Save the file for debugging if test fails
	if totalRows != 50 {
		debugFile := "/tmp/arrow-debug.parquet"
		data, _ := os.ReadFile(filePath)
		_ = os.WriteFile(debugFile, data, 0644)
		t.Logf("Debug file saved at: %s", debugFile)
	}

	require.Equal(t, int64(50), totalRows)
}
