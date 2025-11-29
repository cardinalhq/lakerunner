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
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/file"
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
	schema.AddColumn(wkk.NewRowKey("id"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("name"), filereader.DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("score"), filereader.DataTypeFloat64, true)
	schema.AddColumn(wkk.NewRowKey("active"), filereader.DataTypeBool, true)
	schema.AddColumn(wkk.NewRowKey("nullable_field"), filereader.DataTypeString, true)

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
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write(buf.Bytes())
	require.NoError(t, err)
	tmpFile.Close()

	// Read the parquet file and verify statistics
	f, err := os.Open(tmpFile.Name())
	require.NoError(t, err)
	defer f.Close()

	pf, err := file.NewParquetReader(f, file.WithReadProps(nil))
	require.NoError(t, err)
	defer pf.Close()

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

		// Should still have min/max for the one non-null value
		require.True(t, stats.HasMinMax(), "nullable_field should have min/max even with some nulls")
	})
}

// TestArrowBackendStatisticsEnabled verifies that statistics are enabled by default
// in the Arrow parquet writer configuration.
func TestArrowBackendStatisticsEnabled(t *testing.T) {
	tmpDir := t.TempDir()

	schema := filereader.NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("test_col"), filereader.DataTypeInt64, true)

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
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.Write(buf.Bytes())
	require.NoError(t, err)
	tmpFile.Close()

	f, err := os.Open(tmpFile.Name())
	require.NoError(t, err)
	defer f.Close()

	pf, err := file.NewParquetReader(f)
	require.NoError(t, err)
	defer pf.Close()

	require.Greater(t, pf.NumRowGroups(), 0)

	rgMeta := pf.MetaData().RowGroup(0)
	colChunk, err := rgMeta.ColumnChunk(0)
	require.NoError(t, err)

	stats, err := colChunk.Statistics()
	require.NoError(t, err)
	require.NotNil(t, stats, "Statistics should be present by default")
	require.True(t, stats.HasMinMax(), "Min/max should be set by default")
}
