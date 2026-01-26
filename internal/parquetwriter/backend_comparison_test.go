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
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// TestBackendOutputComparison writes identical data with both backends and verifies
// the output is identical row-by-row using DuckDB to read back the parquet files.
func TestBackendOutputComparison(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create test schema with various types including string conversion
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
	schema.AddColumn(wkk.NewRowKey("attr_count"), wkk.NewRowKey("attr_count"), filereader.DataTypeString, true)

	// Create test data batch
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
			"attr_user_id":     int64(100), // Will be converted to string
			"attr_count":       int64(5),   // Will be converted to string
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
			"attr_count":       int64(10),
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
			"attr_count":       int64(15),
		},
	}

	for _, testRow := range testData {
		row := batch.AddRow()
		for k, v := range testRow {
			row[wkk.NewRowKey(k)] = v
		}
	}

	stringConversionPrefixes := []string{"resource_", "scope_", "attr_"}

	// Write with go-parquet backend
	goParquetConfig := BackendConfig{
		Type:                     BackendGoParquet,
		TmpDir:                   tmpDir,
		Schema:                   schema,
		ChunkSize:                10000,
		StringConversionPrefixes: stringConversionPrefixes,
	}
	goParquetBackend, err := NewGoParquetBackend(goParquetConfig)
	require.NoError(t, err)

	err = goParquetBackend.WriteBatch(ctx, batch)
	require.NoError(t, err)

	goParquetFile, err := os.Create(tmpDir + "/goparquet.parquet")
	require.NoError(t, err)

	_, err = goParquetBackend.Close(ctx, goParquetFile)
	require.NoError(t, err)
	require.NoError(t, goParquetFile.Close())

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

	// Read back both files with DuckDB and compare
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	// Read go-parquet output
	goParquetRows := readParquetFile(t, db, tmpDir+"/goparquet.parquet")
	// Read arrow output
	arrowRows := readParquetFile(t, db, tmpDir+"/arrow.parquet")

	// Compare row counts
	require.Equal(t, len(goParquetRows), len(arrowRows), "Row counts should match")
	require.Equal(t, 3, len(goParquetRows), "Should have 3 rows")

	// Compare each row
	for i := 0; i < len(goParquetRows); i++ {
		goRow := goParquetRows[i]
		arrowRow := arrowRows[i]

		// Compare column counts
		require.Equal(t, len(goRow), len(arrowRow),
			"Row %d: column counts should match (go-parquet: %d, arrow: %d)",
			i, len(goRow), len(arrowRow))

		// Compare each column value
		for colName, goValue := range goRow {
			arrowValue, ok := arrowRow[colName]
			require.True(t, ok, "Row %d: column %s missing in arrow output", i, colName)

			require.Equal(t, goValue, arrowValue,
				"Row %d, Column %s: values should match\n  go-parquet: %v (%T)\n  arrow: %v (%T)",
				i, colName, goValue, goValue, arrowValue, arrowValue)
		}
	}

	t.Logf("✓ Both backends produced identical output (%d rows verified)", len(goParquetRows))
}

// readParquetFile reads a parquet file with DuckDB and returns rows as []map[string]any
func readParquetFile(t *testing.T, db *sql.DB, filePath string) []map[string]any {
	query := fmt.Sprintf("SELECT * FROM read_parquet('%s') ORDER BY id", filePath)
	rows, err := db.Query(query)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	columnNames, err := rows.Columns()
	require.NoError(t, err)

	var results []map[string]any

	for rows.Next() {
		// Create slice of interface{} to hold values
		values := make([]any, len(columnNames))
		valuePtrs := make([]any, len(columnNames))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		err := rows.Scan(valuePtrs...)
		require.NoError(t, err)

		// Build map for this row
		rowMap := make(map[string]any)
		for i, colName := range columnNames {
			rowMap[colName] = values[i]
		}
		results = append(results, rowMap)
	}

	require.NoError(t, rows.Err())
	return results
}

// TestBackendOutputComparison_LargeDataset tests with a larger synthetic dataset
func TestBackendOutputComparison_LargeDataset(t *testing.T) {
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

	// Create 1000 rows
	batch := pipeline.GetBatch()
	defer pipeline.ReturnBatch(batch)

	for i := 0; i < 1000; i++ {
		row := batch.AddRow()
		row[wkk.NewRowKey("id")] = int64(i)
		row[wkk.NewRowKey("timestamp")] = int64(1000 + i*10)
		row[wkk.NewRowKey("message")] = fmt.Sprintf("message-%d", i)
		row[wkk.NewRowKey("value")] = float64(i) * 1.5
		row[wkk.NewRowKey("resource_service")] = fmt.Sprintf("service-%d", i%5)
		row[wkk.NewRowKey("attr_count")] = int64(i * 2) // Will be converted to string
	}

	stringConversionPrefixes := []string{"resource_", "scope_", "attr_"}

	// Write with both backends
	goParquetConfig := BackendConfig{
		Type:                     BackendGoParquet,
		TmpDir:                   tmpDir,
		Schema:                   schema,
		ChunkSize:                10000,
		StringConversionPrefixes: stringConversionPrefixes,
	}
	goParquetBackend, err := NewGoParquetBackend(goParquetConfig)
	require.NoError(t, err)

	err = goParquetBackend.WriteBatch(ctx, batch)
	require.NoError(t, err)

	goParquetFile, err := os.Create(tmpDir + "/goparquet_large.parquet")
	require.NoError(t, err)

	_, err = goParquetBackend.Close(ctx, goParquetFile)
	require.NoError(t, err)
	require.NoError(t, goParquetFile.Close())

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

	_, err = arrowBackend.Close(ctx, arrowFile)
	require.NoError(t, err)
	require.NoError(t, arrowFile.Close())

	// Read back and compare using DuckDB
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	goParquetRows := readParquetFile(t, db, tmpDir+"/goparquet_large.parquet")
	arrowRows := readParquetFile(t, db, tmpDir+"/arrow_large.parquet")

	require.Equal(t, len(goParquetRows), len(arrowRows), "Row counts should match")
	require.Equal(t, 1000, len(goParquetRows), "Should have 1000 rows")

	// Sample comparison: check first, middle, and last rows
	samplesToCheck := []int{0, 499, 999}
	for _, i := range samplesToCheck {
		goRow := goParquetRows[i]
		arrowRow := arrowRows[i]

		for colName, goValue := range goRow {
			arrowValue := arrowRow[colName]
			require.Equal(t, goValue, arrowValue,
				"Row %d, Column %s: values should match", i, colName)
		}
	}

	t.Logf("✓ Both backends produced identical output (%d rows, sampled 3 for verification)", len(goParquetRows))
}

// TestBackendOutputComparison_StringConversion specifically tests string conversion
func TestBackendOutputComparison_StringConversion(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create schema with columns that will be converted to strings
	schema := filereader.NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("id"), wkk.NewRowKey("id"), filereader.DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("attr_int"), wkk.NewRowKey("attr_int"), filereader.DataTypeString, true)       // int -> string
	schema.AddColumn(wkk.NewRowKey("attr_float"), wkk.NewRowKey("attr_float"), filereader.DataTypeString, true)   // float -> string
	schema.AddColumn(wkk.NewRowKey("attr_bool"), wkk.NewRowKey("attr_bool"), filereader.DataTypeString, true)     // bool -> string
	schema.AddColumn(wkk.NewRowKey("resource_id"), wkk.NewRowKey("resource_id"), filereader.DataTypeString, true) // int -> string

	batch := pipeline.GetBatch()
	defer pipeline.ReturnBatch(batch)

	// Add rows with various types that need conversion
	testCases := []map[string]any{
		{
			"id":          int64(1),
			"attr_int":    int64(42),
			"attr_float":  float64(3.14),
			"attr_bool":   true,
			"resource_id": int64(999),
		},
		{
			"id":          int64(2),
			"attr_int":    int64(-100),
			"attr_float":  float64(-2.71),
			"attr_bool":   false,
			"resource_id": int64(1000),
		},
	}

	for _, testCase := range testCases {
		row := batch.AddRow()
		for k, v := range testCase {
			row[wkk.NewRowKey(k)] = v
		}
	}

	stringConversionPrefixes := []string{"resource_", "scope_", "attr_"}

	// Write with both backends
	for _, backendType := range []BackendType{BackendGoParquet, BackendArrow} {
		config := BackendConfig{
			Type:                     backendType,
			TmpDir:                   tmpDir,
			Schema:                   schema,
			ChunkSize:                10000,
			StringConversionPrefixes: stringConversionPrefixes,
		}

		var backend ParquetBackend
		var err error
		if backendType == BackendGoParquet {
			backend, err = NewGoParquetBackend(config)
		} else {
			backend, err = NewArrowBackend(config)
		}
		require.NoError(t, err)

		err = backend.WriteBatch(ctx, batch)
		require.NoError(t, err)

		filePath := fmt.Sprintf("%s/%s_conversion.parquet", tmpDir, backendType)
		file, err := os.Create(filePath)
		require.NoError(t, err)

		_, err = backend.Close(ctx, file)
		require.NoError(t, err)
		require.NoError(t, file.Close())
	}

	// Read back and verify all string conversions match
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	goParquetRows := readParquetFile(t, db, tmpDir+"/go-parquet_conversion.parquet")
	arrowRows := readParquetFile(t, db, tmpDir+"/arrow_conversion.parquet")

	require.Equal(t, 2, len(goParquetRows), "Should have 2 rows")
	require.Equal(t, len(goParquetRows), len(arrowRows), "Row counts should match")

	// Verify conversions
	expectedConversions := []map[string]string{
		{
			"attr_int":    "42",
			"attr_float":  "3.140000",
			"attr_bool":   "true",
			"resource_id": "999",
		},
		{
			"attr_int":    "-100",
			"attr_float":  "-2.710000",
			"attr_bool":   "false",
			"resource_id": "1000",
		},
	}

	for i := 0; i < len(goParquetRows); i++ {
		goRow := goParquetRows[i]
		arrowRow := arrowRows[i]
		expected := expectedConversions[i]

		for colName, expectedValue := range expected {
			goValue := goRow[colName]
			arrowValue := arrowRow[colName]

			// Both should match expected string value
			require.Equal(t, expectedValue, goValue,
				"Row %d, Column %s: go-parquet should produce %q", i, colName, expectedValue)
			require.Equal(t, expectedValue, arrowValue,
				"Row %d, Column %s: arrow should produce %q", i, colName, expectedValue)
			require.Equal(t, goValue, arrowValue,
				"Row %d, Column %s: backends should match", i, colName)
		}
	}

	t.Logf("✓ String conversion is identical across both backends")
}
