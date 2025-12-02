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

package filereader

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// TestIngestLogParquetReader_NestedColumnNameCollision tests that a parquet file with
// both a top-level column and a nested column with the same leaf name but different
// types is handled correctly.
//
// This is a regression test for a bug where nested columns (e.g., "response.status")
// would override the type of top-level columns with the same leaf name ("status")
// in the schema type map, causing type conversion errors.
//
// Example structure:
//   - status: STRING (top-level)
//   - response.status: INT32 (nested in struct)
//
// The bug caused the reader to expect INT64 for the top-level "status" column
// (because the nested INT32 overwrote it), then fail when trying to read a STRING.
func TestIngestLogParquetReader_NestedColumnNameCollision(t *testing.T) {
	ctx := context.Background()

	// Create parquet data with conflicting column names
	data := createParquetWithNestedNameCollision(t)

	// Create reader
	reader, err := NewIngestLogParquetReader(ctx, bytes.NewReader(data), 1000)
	require.NoError(t, err, "Failed to create reader - this may indicate the nested column name collision bug")
	defer func() { _ = reader.Close() }()

	// Expected values for verification
	expectedStatuses := []string{"ok", "error", "warning"}
	expectedTimestamps := []int64{1000, 2000, 3000}
	expectedMessages := []string{"msg1", "msg2", "msg3"}

	// Read all rows and verify content
	var rowIndex int
	for {
		batch, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "Failed to read batch - type conversion error indicates regression")

		for i := range batch.Len() {
			row := batch.Get(i)
			require.Less(t, rowIndex, len(expectedStatuses), "More rows than expected")

			// Verify top-level "status" is a string (not converted to int64)
			statusVal := row[wkk.NewRowKey("status")]
			require.IsType(t, "", statusVal, "status should be string type, not int64")
			require.Equal(t, expectedStatuses[rowIndex], statusVal, "status value mismatch")

			// Verify timestamp
			tsVal := row[wkk.NewRowKey("timestamp")]
			require.IsType(t, int64(0), tsVal, "timestamp should be int64")
			require.Equal(t, expectedTimestamps[rowIndex], tsVal, "timestamp value mismatch")

			// Verify message
			msgVal := row[wkk.NewRowKey("message")]
			require.IsType(t, "", msgVal, "message should be string")
			require.Equal(t, expectedMessages[rowIndex], msgVal, "message value mismatch")

			rowIndex++
		}
		pipeline.ReturnBatch(batch)
	}

	require.Equal(t, 3, rowIndex, "Should have read all 3 rows")
}

// TestBuildParquetTypeMap_NestedColumnNameCollision verifies that buildParquetTypeMap
// correctly handles nested columns with the same leaf name as top-level columns.
func TestBuildParquetTypeMap_NestedColumnNameCollision(t *testing.T) {
	// Create parquet data with conflicting column names
	data := createParquetWithNestedNameCollision(t)

	// Open as parquet file to test buildParquetTypeMap directly
	pf, err := file.NewParquetReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer func() { _ = pf.Close() }()

	// Build type map
	typeMap := buildParquetTypeMap(pf)

	// Verify top-level "status" is STRING, not INT64
	// The key should be "status" (not overwritten by nested column)
	require.Contains(t, typeMap, "status", "Should have top-level status column")
	require.Equal(t, DataTypeString, typeMap["status"],
		"Top-level status should be STRING, not overwritten by nested INT32")

	// Verify nested "response.status" has its own key with underscores
	require.Contains(t, typeMap, "response_status",
		"Should have nested response_status column with underscored path")
	require.Equal(t, DataTypeInt64, typeMap["response_status"],
		"Nested response.status should be INT64 (promoted from INT32)")

	// Verify we have both columns (no collision/overwrite)
	require.NotEqual(t, typeMap["status"], typeMap["response_status"],
		"Top-level and nested columns should have different types")
}

// createParquetWithNestedNameCollision creates a parquet file with:
//   - timestamp: INT64 (for sorting)
//   - status: STRING (top-level)
//   - response: STRUCT with:
//   - status: INT32 (nested, same leaf name as top-level but different type)
//   - code: INT32
func createParquetWithNestedNameCollision(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer

	// Build schema with nested struct having same-named column
	responseType := arrow.StructOf(
		arrow.Field{Name: "status", Type: arrow.PrimitiveTypes.Int32},
		arrow.Field{Name: "code", Type: arrow.PrimitiveTypes.Int32},
	)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64},
		{Name: "status", Type: arrow.BinaryTypes.String}, // Top-level STRING
		{Name: "message", Type: arrow.BinaryTypes.String},
		{Name: "response", Type: responseType}, // Nested struct with INT32 "status"
	}, nil)

	// Create parquet writer
	writerProps := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))
	arrowWriterProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	writer, err := pqarrow.NewFileWriter(schema, &buf, writerProps, arrowWriterProps)
	require.NoError(t, err)

	// Build record batch with test data
	pool := memory.NewGoAllocator()
	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	// Add test rows
	timestamps := []int64{1000, 2000, 3000}
	statuses := []string{"ok", "error", "warning"} // Top-level STRING status
	messages := []string{"msg1", "msg2", "msg3"}
	responseStatuses := []int32{200, 500, 400} // Nested INT32 status
	responseCodes := []int32{0, 1, 2}

	for i := range timestamps {
		b.Field(0).(*array.Int64Builder).Append(timestamps[i])
		b.Field(1).(*array.StringBuilder).Append(statuses[i])
		b.Field(2).(*array.StringBuilder).Append(messages[i])

		// Build struct for response field
		sb := b.Field(3).(*array.StructBuilder)
		sb.Append(true)
		sb.FieldBuilder(0).(*array.Int32Builder).Append(responseStatuses[i])
		sb.FieldBuilder(1).(*array.Int32Builder).Append(responseCodes[i])
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	err = writer.Write(rec)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	return buf.Bytes()
}
