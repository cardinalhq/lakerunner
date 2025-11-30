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
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// TestArrowRawReader_StringCopying verifies that strings are properly copied
// and don't reference freed Arrow buffer memory after record release
func TestArrowRawReader_StringCopying(t *testing.T) {
	ctx := context.Background()

	// Create a simple parquet file with strings
	var buf bytes.Buffer

	// Build schema with string field
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "message", Type: arrow.BinaryTypes.String},
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
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

	// Add multiple rows to test batch processing
	testStrings := []string{
		"first message",
		"second message with longer content",
		"third",
		"fourth message with special chars: ™€£",
		"fifth message that is very long to test memory handling of larger strings that might be allocated differently",
	}

	for i, msg := range testStrings {
		b.Field(0).(*array.StringBuilder).Append(msg)
		b.Field(1).(*array.Int64Builder).Append(int64(i))
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	err = writer.Write(rec)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Now read it back with ArrowRawReader
	reader, err := NewIngestLogParquetReader(ctx, bytes.NewReader(buf.Bytes()), 2) // Small batch size to test multiple batches
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Collect all messages
	var collectedMessages []string
	var collectedIDs []int64

	for {
		batch, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		// Store the batch data
		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)

			// Extract message and ID
			if msg, ok := row[wkk.NewRowKey("message")].(string); ok {
				collectedMessages = append(collectedMessages, msg)
			}
			if id, ok := row[wkk.NewRowKey("id")].(int64); ok {
				collectedIDs = append(collectedIDs, id)
			}
		}

		// Force garbage collection to potentially expose use-after-free bugs
		// if we're still referencing Arrow buffer memory
		for i := 0; i < 3; i++ {
			_ = make([]byte, 1024*1024) // Allocate memory to trigger GC
		}
	}

	// Verify all strings are correctly preserved
	assert.Equal(t, testStrings, collectedMessages, "All strings should be correctly preserved after Arrow record release")
	assert.Equal(t, []int64{0, 1, 2, 3, 4}, collectedIDs, "All IDs should be correctly preserved")
}

// TestArrowRawReader_BinaryCopying verifies that binary data is properly copied
func TestArrowRawReader_BinaryCopying(t *testing.T) {
	ctx := context.Background()

	// Create a simple parquet file with binary data
	var buf bytes.Buffer

	// Build schema with binary field
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "data", Type: arrow.BinaryTypes.Binary},
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
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

	// Add binary data
	testData := [][]byte{
		{0x00, 0x01, 0x02, 0x03},
		{0xFF, 0xFE, 0xFD},
		{0xDE, 0xAD, 0xBE, 0xEF},
		make([]byte, 1024), // Larger binary data
	}

	for i, data := range testData {
		b.Field(0).(*array.BinaryBuilder).Append(data)
		b.Field(1).(*array.Int64Builder).Append(int64(i))
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	err = writer.Write(rec)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Now read it back with ArrowRawReader
	reader, err := NewIngestLogParquetReader(ctx, bytes.NewReader(buf.Bytes()), 2)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// Collect all data
	var collectedData [][]byte

	for {
		batch, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)

			if data, ok := row[wkk.NewRowKey("data")].([]byte); ok {
				// Make a copy to ensure we're not comparing the same slice
				copied := make([]byte, len(data))
				copy(copied, data)
				collectedData = append(collectedData, copied)
			}
		}
	}

	// Verify all binary data is correctly preserved
	require.Equal(t, len(testData), len(collectedData), "Should have collected all binary data")
	for i := range testData {
		assert.Equal(t, testData[i], collectedData[i], "Binary data at index %d should match", i)
	}
}

// TestArrowRawReader_NestedStringCopying verifies strings in nested structures are copied
func TestArrowRawReader_NestedStringCopying(t *testing.T) {
	ctx := context.Background()

	// Create a parquet file with nested structures containing strings
	var buf bytes.Buffer

	// Build schema with list of strings
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
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

	// Add lists of strings
	listBuilder := b.Field(0).(*array.ListBuilder)
	valueBuilder := listBuilder.ValueBuilder().(*array.StringBuilder)

	// pipeline.Row 1: ["tag1", "tag2", "tag3"]
	listBuilder.Append(true)
	valueBuilder.Append("tag1")
	valueBuilder.Append("tag2")
	valueBuilder.Append("tag3")
	b.Field(1).(*array.Int64Builder).Append(1)

	// pipeline.Row 2: ["alpha", "beta"]
	listBuilder.Append(true)
	valueBuilder.Append("alpha")
	valueBuilder.Append("beta")
	b.Field(1).(*array.Int64Builder).Append(2)

	rec := b.NewRecordBatch()
	defer rec.Release()

	err = writer.Write(rec)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read it back
	reader, err := NewIngestLogParquetReader(ctx, bytes.NewReader(buf.Bytes()), 10)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	batch, err := reader.Next(ctx)
	require.NoError(t, err)

	// Verify first row
	row1 := batch.Get(0)
	tags1, ok := row1[wkk.NewRowKey("tags")].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"tag1", "tag2", "tag3"}, tags1)

	// Verify second row
	row2 := batch.Get(1)
	tags2, ok := row2[wkk.NewRowKey("tags")].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"alpha", "beta"}, tags2)

	// Force GC to ensure strings are truly copied
	for i := 0; i < 3; i++ {
		_ = make([]byte, 1024*1024)
	}

	// Re-verify strings are still valid after potential GC
	assert.Equal(t, []any{"tag1", "tag2", "tag3"}, tags1)
	assert.Equal(t, []any{"alpha", "beta"}, tags2)
}
