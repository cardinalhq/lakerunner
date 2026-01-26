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

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// createParquetWithMaps creates a test parquet file with map fields to test schema extraction
func createParquetWithMaps(t *testing.T, records []arrow.RecordBatch) []byte {
	t.Helper()

	var buf bytes.Buffer
	props := pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(memory.DefaultAllocator))
	writer, err := pqarrow.NewFileWriter(records[0].Schema(), &buf, parquet.NewWriterProperties(), props)
	require.NoError(t, err)

	for _, rec := range records {
		err = writer.Write(rec)
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	return buf.Bytes()
}

// TestIngestLogParquetReader_SchemaExtraction_Maps tests that map fields are fully flattened
// and all keys discovered across all rows appear in the schema.
func TestIngestLogParquetReader_SchemaExtraction_Maps(t *testing.T) {
	t.Skip("TODO: Re-enable when map flattening is implemented")
	mem := memory.NewGoAllocator()

	// Create schema with a map field
	mapType := arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "attributes", Type: mapType, Nullable: true},
	}, nil)

	// Build first record with map keys: "service", "environment"
	builder1 := array.NewRecordBuilder(mem, schema)
	defer builder1.Release()

	builder1.Field(0).(*array.Int64Builder).Append(1)

	mapBuilder1 := builder1.Field(1).(*array.MapBuilder)
	mapBuilder1.Append(true)
	mapBuilder1.KeyBuilder().(*array.StringBuilder).Append("service")
	mapBuilder1.ItemBuilder().(*array.StringBuilder).Append("api")
	mapBuilder1.KeyBuilder().(*array.StringBuilder).Append("environment")
	mapBuilder1.ItemBuilder().(*array.StringBuilder).Append("prod")

	rec1 := builder1.NewRecordBatch()
	defer rec1.Release()

	// Build second record with DIFFERENT map keys: "service", "region"
	builder2 := array.NewRecordBuilder(mem, schema)
	defer builder2.Release()

	builder2.Field(0).(*array.Int64Builder).Append(2)

	mapBuilder2 := builder2.Field(1).(*array.MapBuilder)
	mapBuilder2.Append(true)
	mapBuilder2.KeyBuilder().(*array.StringBuilder).Append("service")
	mapBuilder2.ItemBuilder().(*array.StringBuilder).Append("web")
	mapBuilder2.KeyBuilder().(*array.StringBuilder).Append("region")
	mapBuilder2.ItemBuilder().(*array.StringBuilder).Append("us-east-1")

	rec2 := builder2.NewRecordBatch()
	defer rec2.Release()

	// Create parquet file
	parquetData := createParquetWithMaps(t, []arrow.RecordBatch{rec1, rec2})

	// Read with IngestLogParquetReader
	reader, err := NewIngestLogParquetReader(context.Background(), bytes.NewReader(parquetData), 1000)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	readerSchema := reader.GetSchema()
	require.NotNil(t, readerSchema)

	// Verify schema contains ALL map keys from BOTH rows
	assert.True(t, readerSchema.HasColumn("id"), "Schema should have 'id' column")
	assert.True(t, readerSchema.HasColumn("attributes_service"), "Schema should have 'attributes_service' from map")
	assert.True(t, readerSchema.HasColumn("attributes_environment"), "Schema should have 'attributes_environment' from row 1")
	assert.True(t, readerSchema.HasColumn("attributes_region"), "Schema should have 'attributes_region' from row 2")

	// Read all rows and verify they match the schema
	ctx := context.Background()
	allRows := make([]pipeline.Row, 0)

	for {
		batch, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := range batch.Len() {
			row := batch.Get(i)

			// Verify all keys in row exist in schema
			for key := range row {
				keyStr := wkk.RowKeyValue(key)
				assert.True(t, readerSchema.HasColumn(keyStr),
					"Row contains key %q not in schema", keyStr)
			}

			allRows = append(allRows, row)
		}

		pipeline.ReturnBatch(batch)
	}

	require.Equal(t, 2, len(allRows), "Should have read 2 rows")

	// Verify row 1 has correct values
	row1 := allRows[0]
	assert.Equal(t, int64(1), row1[wkk.NewRowKey("id")])
	assert.Equal(t, "api", row1[wkk.NewRowKey("attributes_service")])
	assert.Equal(t, "prod", row1[wkk.NewRowKey("attributes_environment")])
	assert.Nil(t, row1[wkk.NewRowKey("attributes_region")], "Row 1 should not have 'region'")

	// Verify row 2 has correct values
	row2 := allRows[1]
	assert.Equal(t, int64(2), row2[wkk.NewRowKey("id")])
	assert.Equal(t, "web", row2[wkk.NewRowKey("attributes_service")])
	assert.Nil(t, row2[wkk.NewRowKey("attributes_environment")], "Row 2 should not have 'environment'")
	assert.Equal(t, "us-east-1", row2[wkk.NewRowKey("attributes_region")])
}

// TestIngestLogParquetReader_SchemaExtraction_NestedMaps tests deeply nested map structures.
func TestIngestLogParquetReader_SchemaExtraction_NestedMaps(t *testing.T) {
	t.Skip("TODO: Re-enable when map/struct flattening is implemented")
	mem := memory.NewGoAllocator()

	// Create schema with nested struct containing a map
	mapType := arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
	structType := arrow.StructOf(
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "tags", Type: mapType, Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "resource", Type: structType, Nullable: true},
	}, nil)

	// Build first record
	builder1 := array.NewRecordBuilder(mem, schema)
	defer builder1.Release()

	builder1.Field(0).(*array.Int64Builder).Append(1)

	structBuilder1 := builder1.Field(1).(*array.StructBuilder)
	structBuilder1.Append(true)
	structBuilder1.FieldBuilder(0).(*array.StringBuilder).Append("service-a")

	mapBuilder1 := structBuilder1.FieldBuilder(1).(*array.MapBuilder)
	mapBuilder1.Append(true)
	mapBuilder1.KeyBuilder().(*array.StringBuilder).Append("version")
	mapBuilder1.ItemBuilder().(*array.StringBuilder).Append("1.0")
	mapBuilder1.KeyBuilder().(*array.StringBuilder).Append("tier")
	mapBuilder1.ItemBuilder().(*array.StringBuilder).Append("premium")

	rec1 := builder1.NewRecordBatch()
	defer rec1.Release()

	// Build second record with different map keys
	builder2 := array.NewRecordBuilder(mem, schema)
	defer builder2.Release()

	builder2.Field(0).(*array.Int64Builder).Append(2)

	structBuilder2 := builder2.Field(1).(*array.StructBuilder)
	structBuilder2.Append(true)
	structBuilder2.FieldBuilder(0).(*array.StringBuilder).Append("service-b")

	mapBuilder2 := structBuilder2.FieldBuilder(1).(*array.MapBuilder)
	mapBuilder2.Append(true)
	mapBuilder2.KeyBuilder().(*array.StringBuilder).Append("version")
	mapBuilder2.ItemBuilder().(*array.StringBuilder).Append("2.0")
	mapBuilder2.KeyBuilder().(*array.StringBuilder).Append("region")
	mapBuilder2.ItemBuilder().(*array.StringBuilder).Append("us-west-2")

	rec2 := builder2.NewRecordBatch()
	defer rec2.Release()

	// Create parquet file
	parquetData := createParquetWithMaps(t, []arrow.RecordBatch{rec1, rec2})

	// Read with IngestLogParquetReader
	reader, err := NewIngestLogParquetReader(context.Background(), bytes.NewReader(parquetData), 1000)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	readerSchema := reader.GetSchema()
	require.NotNil(t, readerSchema)

	// Verify schema contains flattened nested paths
	assert.True(t, readerSchema.HasColumn("id"))
	assert.True(t, readerSchema.HasColumn("resource_name"), "Struct field should be flattened")
	assert.True(t, readerSchema.HasColumn("resource_tags_version"), "Nested map key 'version' should be flattened")
	assert.True(t, readerSchema.HasColumn("resource_tags_tier"), "Nested map key 'tier' from row 1")
	assert.True(t, readerSchema.HasColumn("resource_tags_region"), "Nested map key 'region' from row 2")

	// Read all rows and verify
	ctx := context.Background()
	allRows := make([]pipeline.Row, 0)

	for {
		batch, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := range batch.Len() {
			row := batch.Get(i)

			// Verify all keys in row exist in schema
			for key := range row {
				keyStr := wkk.RowKeyValue(key)
				assert.True(t, readerSchema.HasColumn(keyStr),
					"Row contains key %q not in schema", keyStr)
			}

			allRows = append(allRows, row)
		}

		pipeline.ReturnBatch(batch)
	}

	require.Equal(t, 2, len(allRows))

	// Verify row values
	row1 := allRows[0]
	assert.Equal(t, int64(1), row1[wkk.NewRowKey("id")])
	assert.Equal(t, "service-a", row1[wkk.NewRowKey("resource_name")])
	assert.Equal(t, "1.0", row1[wkk.NewRowKey("resource_tags_version")])
	assert.Equal(t, "premium", row1[wkk.NewRowKey("resource_tags_tier")])
	assert.Nil(t, row1[wkk.NewRowKey("resource_tags_region")])

	row2 := allRows[1]
	assert.Equal(t, int64(2), row2[wkk.NewRowKey("id")])
	assert.Equal(t, "service-b", row2[wkk.NewRowKey("resource_name")])
	assert.Equal(t, "2.0", row2[wkk.NewRowKey("resource_tags_version")])
	assert.Nil(t, row2[wkk.NewRowKey("resource_tags_tier")])
	assert.Equal(t, "us-west-2", row2[wkk.NewRowKey("resource_tags_region")])
}

// TestIngestLogParquetReader_SchemaExtraction_Lists tests that list fields are preserved (not flattened).
func TestIngestLogParquetReader_SchemaExtraction_Lists(t *testing.T) {
	t.Skip("TODO: Re-enable when list handling is implemented")
	mem := memory.NewGoAllocator()

	// Create schema with list field
	listType := arrow.ListOf(arrow.BinaryTypes.String)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "tags", Type: listType, Nullable: true},
	}, nil)

	// Build record with list values
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).Append(1)

	listBuilder := builder.Field(1).(*array.ListBuilder)
	listBuilder.Append(true)
	listBuilder.ValueBuilder().(*array.StringBuilder).Append("tag1")
	listBuilder.ValueBuilder().(*array.StringBuilder).Append("tag2")
	listBuilder.ValueBuilder().(*array.StringBuilder).Append("tag3")

	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Create parquet file
	parquetData := createParquetWithMaps(t, []arrow.RecordBatch{rec})

	// Read with IngestLogParquetReader
	reader, err := NewIngestLogParquetReader(context.Background(), bytes.NewReader(parquetData), 1000)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	readerSchema := reader.GetSchema()
	require.NotNil(t, readerSchema)

	// Verify schema has list field (not flattened)
	assert.True(t, readerSchema.HasColumn("id"))
	assert.True(t, readerSchema.HasColumn("tags"), "List field should be in schema as-is")
	assert.Equal(t, DataTypeAny, readerSchema.GetColumnType("tags"), "List should have Any type")

	// Read and verify row
	ctx := context.Background()
	batch, err := reader.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, batch.Len())

	row := batch.Get(0)
	tags := row[wkk.NewRowKey("tags")]
	require.NotNil(t, tags)

	// Verify list is preserved as []any
	tagsList, ok := tags.([]any)
	require.True(t, ok, "List should be []any type")
	require.Equal(t, 3, len(tagsList))
	assert.Equal(t, "tag1", tagsList[0])
	assert.Equal(t, "tag2", tagsList[1])
	assert.Equal(t, "tag3", tagsList[2])

	// Verify all row keys are in schema
	for key := range row {
		keyStr := wkk.RowKeyValue(key)
		assert.True(t, readerSchema.HasColumn(keyStr),
			"Row key %q not in schema", keyStr)
	}

	pipeline.ReturnBatch(batch)
}

// TestIngestLogParquetReader_SchemaExtraction_CompleteMatch tests the critical requirement:
// Every field in every row MUST be in the schema, and the schema MUST accurately reflect
// all columns that will appear across all rows.
func TestIngestLogParquetReader_SchemaExtraction_CompleteMatch(t *testing.T) {
	t.Skip("TODO: Re-enable when struct flattening is implemented")
	mem := memory.NewGoAllocator()

	// Create a complex schema mixing structs, maps, and primitives
	mapType := arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64)
	structType := arrow.StructOf(
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "metadata", Type: mapType, Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "simple", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "resource", Type: structType, Nullable: true},
	}, nil)

	// Create 3 records with varying map keys
	records := make([]arrow.RecordBatch, 3)

	// Record 1: metadata keys "count", "total"
	builder1 := array.NewRecordBuilder(mem, schema)
	builder1.Field(0).(*array.Int64Builder).Append(1)
	builder1.Field(1).(*array.StringBuilder).Append("first")
	structBuilder1 := builder1.Field(2).(*array.StructBuilder)
	structBuilder1.Append(true)
	structBuilder1.FieldBuilder(0).(*array.StringBuilder).Append("resource-1")
	mapBuilder1 := structBuilder1.FieldBuilder(1).(*array.MapBuilder)
	mapBuilder1.Append(true)
	mapBuilder1.KeyBuilder().(*array.StringBuilder).Append("count")
	mapBuilder1.ItemBuilder().(*array.Int64Builder).Append(10)
	mapBuilder1.KeyBuilder().(*array.StringBuilder).Append("total")
	mapBuilder1.ItemBuilder().(*array.Int64Builder).Append(100)
	records[0] = builder1.NewRecordBatch()

	// Record 2: metadata keys "count", "average"
	builder2 := array.NewRecordBuilder(mem, schema)
	builder2.Field(0).(*array.Int64Builder).Append(2)
	builder2.Field(1).(*array.StringBuilder).Append("second")
	structBuilder2 := builder2.Field(2).(*array.StructBuilder)
	structBuilder2.Append(true)
	structBuilder2.FieldBuilder(0).(*array.StringBuilder).Append("resource-2")
	mapBuilder2 := structBuilder2.FieldBuilder(1).(*array.MapBuilder)
	mapBuilder2.Append(true)
	mapBuilder2.KeyBuilder().(*array.StringBuilder).Append("count")
	mapBuilder2.ItemBuilder().(*array.Int64Builder).Append(20)
	mapBuilder2.KeyBuilder().(*array.StringBuilder).Append("average")
	mapBuilder2.ItemBuilder().(*array.Int64Builder).Append(50)
	records[1] = builder2.NewRecordBatch()

	// Record 3: metadata keys "total", "max"
	builder3 := array.NewRecordBuilder(mem, schema)
	builder3.Field(0).(*array.Int64Builder).Append(3)
	builder3.Field(1).(*array.StringBuilder).Append("third")
	structBuilder3 := builder3.Field(2).(*array.StructBuilder)
	structBuilder3.Append(true)
	structBuilder3.FieldBuilder(0).(*array.StringBuilder).Append("resource-3")
	mapBuilder3 := structBuilder3.FieldBuilder(1).(*array.MapBuilder)
	mapBuilder3.Append(true)
	mapBuilder3.KeyBuilder().(*array.StringBuilder).Append("total")
	mapBuilder3.ItemBuilder().(*array.Int64Builder).Append(200)
	mapBuilder3.KeyBuilder().(*array.StringBuilder).Append("max")
	mapBuilder3.ItemBuilder().(*array.Int64Builder).Append(75)
	records[2] = builder3.NewRecordBatch()

	defer func() {
		for _, rec := range records {
			rec.Release()
		}
		builder1.Release()
		builder2.Release()
		builder3.Release()
	}()

	// Create parquet file
	parquetData := createParquetWithMaps(t, records)

	// Read with IngestLogParquetReader
	reader, err := NewIngestLogParquetReader(context.Background(), bytes.NewReader(parquetData), 1000)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	readerSchema := reader.GetSchema()
	require.NotNil(t, readerSchema)

	// Verify schema contains ALL columns from ALL rows
	expectedColumns := []string{
		"id",
		"simple",
		"resource_name",
		"resource_metadata_count",   // from rows 1, 2
		"resource_metadata_total",   // from rows 1, 3
		"resource_metadata_average", // from row 2
		"resource_metadata_max",     // from row 3
	}

	for _, col := range expectedColumns {
		assert.True(t, readerSchema.HasColumn(col),
			"Schema missing expected column %q", col)
	}

	// Read all rows and perform the CRITICAL check:
	// Every key in every row MUST exist in the schema
	ctx := context.Background()
	rowsChecked := 0

	for {
		batch, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := range batch.Len() {
			row := batch.Get(i)
			rowsChecked++

			// CRITICAL: Every key in this row MUST be in the schema
			for key := range row {
				keyStr := wkk.RowKeyValue(key)
				assert.True(t, readerSchema.HasColumn(keyStr),
					"Row %d contains key %q that is NOT in schema - SCHEMA IS INCOMPLETE",
					rowsChecked, keyStr)
			}
		}

		pipeline.ReturnBatch(batch)
	}

	assert.Equal(t, 3, rowsChecked, "Should have checked all 3 rows")
}

// TestIngestLogParquetReader_SchemaExtraction_EmptyMaps tests handling of empty maps.
func TestIngestLogParquetReader_SchemaExtraction_EmptyMaps(t *testing.T) {
	t.Skip("TODO: Re-enable when map flattening is implemented")
	mem := memory.NewGoAllocator()

	// Create schema with map
	mapType := arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "attributes", Type: mapType, Nullable: true},
	}, nil)

	// Record 1: empty map
	builder1 := array.NewRecordBuilder(mem, schema)
	defer builder1.Release()
	builder1.Field(0).(*array.Int64Builder).Append(1)
	mapBuilder1 := builder1.Field(1).(*array.MapBuilder)
	mapBuilder1.Append(true) // Append empty map
	rec1 := builder1.NewRecordBatch()
	defer rec1.Release()

	// Record 2: map with keys
	builder2 := array.NewRecordBuilder(mem, schema)
	defer builder2.Release()
	builder2.Field(0).(*array.Int64Builder).Append(2)
	mapBuilder2 := builder2.Field(1).(*array.MapBuilder)
	mapBuilder2.Append(true)
	mapBuilder2.KeyBuilder().(*array.StringBuilder).Append("key1")
	mapBuilder2.ItemBuilder().(*array.StringBuilder).Append("value1")
	rec2 := builder2.NewRecordBatch()
	defer rec2.Release()

	// Create parquet file
	parquetData := createParquetWithMaps(t, []arrow.RecordBatch{rec1, rec2})

	// Read with IngestLogParquetReader
	reader, err := NewIngestLogParquetReader(context.Background(), bytes.NewReader(parquetData), 1000)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	readerSchema := reader.GetSchema()
	require.NotNil(t, readerSchema)

	// Schema should include the key from record 2
	assert.True(t, readerSchema.HasColumn("id"))
	assert.True(t, readerSchema.HasColumn("attributes_key1"))

	// Read and verify all rows
	ctx := context.Background()
	batch, err := reader.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, batch.Len())

	// Verify all row keys are in schema
	for i := range batch.Len() {
		row := batch.Get(i)
		for key := range row {
			keyStr := wkk.RowKeyValue(key)
			assert.True(t, readerSchema.HasColumn(keyStr),
				"Row key %q not in schema", keyStr)
		}
	}

	pipeline.ReturnBatch(batch)
}

// TestIngestLogParquetReader_SchemaExtraction_NestedStructs tests deeply nested struct flattening.
func TestIngestLogParquetReader_SchemaExtraction_NestedStructs(t *testing.T) {
	t.Skip("TODO: Re-enable when struct flattening is implemented")
	mem := memory.NewGoAllocator()

	// Create schema with struct -> struct nesting (2 levels)
	innerStructType := arrow.StructOf(
		arrow.Field{Name: "city", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "state", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	outerStructType := arrow.StructOf(
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "location", Type: innerStructType, Nullable: true},
	)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "user", Type: outerStructType, Nullable: true},
	}, nil)

	// Build record
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).Append(1)

	userBuilder := builder.Field(1).(*array.StructBuilder)
	userBuilder.Append(true)
	userBuilder.FieldBuilder(0).(*array.StringBuilder).Append("Alice")

	locationBuilder := userBuilder.FieldBuilder(1).(*array.StructBuilder)
	locationBuilder.Append(true)
	locationBuilder.FieldBuilder(0).(*array.StringBuilder).Append("Seattle")
	locationBuilder.FieldBuilder(1).(*array.StringBuilder).Append("WA")

	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Create parquet file
	parquetData := createParquetWithMaps(t, []arrow.RecordBatch{rec})

	// Read with IngestLogParquetReader
	reader, err := NewIngestLogParquetReader(context.Background(), bytes.NewReader(parquetData), 1000)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	readerSchema := reader.GetSchema()
	require.NotNil(t, readerSchema)

	// Verify all nested struct fields are flattened
	assert.True(t, readerSchema.HasColumn("id"))
	assert.True(t, readerSchema.HasColumn("user_name"), "First level struct field")
	assert.True(t, readerSchema.HasColumn("user_location_city"), "Second level struct field")
	assert.True(t, readerSchema.HasColumn("user_location_state"), "Second level struct field")

	// Read and verify row
	ctx := context.Background()
	batch, err := reader.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, batch.Len())

	row := batch.Get(0)
	assert.Equal(t, int64(1), row[wkk.NewRowKey("id")])
	assert.Equal(t, "Alice", row[wkk.NewRowKey("user_name")])
	assert.Equal(t, "Seattle", row[wkk.NewRowKey("user_location_city")])
	assert.Equal(t, "WA", row[wkk.NewRowKey("user_location_state")])

	// Verify all row keys are in schema
	for key := range row {
		keyStr := wkk.RowKeyValue(key)
		assert.True(t, readerSchema.HasColumn(keyStr),
			"Row key %q not in schema", keyStr)
	}

	pipeline.ReturnBatch(batch)
}

// TestIngestLogParquetReader_SchemaExtraction_ListOfStructs tests lists containing struct elements.
func TestIngestLogParquetReader_SchemaExtraction_ListOfStructs(t *testing.T) {
	t.Skip("TODO: Re-enable when list handling is implemented")
	mem := memory.NewGoAllocator()

	// Create schema with list of structs
	structType := arrow.StructOf(
		arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	)
	listType := arrow.ListOf(structType)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "items", Type: listType, Nullable: true},
	}, nil)

	// Build record with list of structs
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	builder.Field(0).(*array.Int64Builder).Append(1)

	listBuilder := builder.Field(1).(*array.ListBuilder)
	listBuilder.Append(true)

	structBuilder := listBuilder.ValueBuilder().(*array.StructBuilder)

	// First struct in list
	structBuilder.Append(true)
	structBuilder.FieldBuilder(0).(*array.StringBuilder).Append("item1")
	structBuilder.FieldBuilder(1).(*array.Int64Builder).Append(100)

	// Second struct in list
	structBuilder.Append(true)
	structBuilder.FieldBuilder(0).(*array.StringBuilder).Append("item2")
	structBuilder.FieldBuilder(1).(*array.Int64Builder).Append(200)

	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Create parquet file
	parquetData := createParquetWithMaps(t, []arrow.RecordBatch{rec})

	// Read with IngestLogParquetReader
	reader, err := NewIngestLogParquetReader(context.Background(), bytes.NewReader(parquetData), 1000)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	readerSchema := reader.GetSchema()
	require.NotNil(t, readerSchema)

	// Lists are NOT flattened - stored as-is
	assert.True(t, readerSchema.HasColumn("id"))
	assert.True(t, readerSchema.HasColumn("items"), "List field should be in schema as-is")
	assert.Equal(t, DataTypeAny, readerSchema.GetColumnType("items"), "List should have Any type")

	// Read and verify row
	ctx := context.Background()
	batch, err := reader.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, batch.Len())

	row := batch.Get(0)
	items := row[wkk.NewRowKey("items")]
	require.NotNil(t, items)

	// Verify list is preserved as []any containing maps (converted structs)
	itemsList, ok := items.([]any)
	require.True(t, ok, "List should be []any type")
	require.Equal(t, 2, len(itemsList))

	// Each struct in the list becomes a map[string]any
	item1, ok := itemsList[0].(map[string]any)
	require.True(t, ok, "List element should be map[string]any")
	assert.Equal(t, "item1", item1["name"])
	assert.Equal(t, int64(100), item1["value"])

	item2, ok := itemsList[1].(map[string]any)
	require.True(t, ok, "List element should be map[string]any")
	assert.Equal(t, "item2", item2["name"])
	assert.Equal(t, int64(200), item2["value"])

	// Verify all row keys are in schema
	for key := range row {
		keyStr := wkk.RowKeyValue(key)
		assert.True(t, readerSchema.HasColumn(keyStr),
			"Row key %q not in schema", keyStr)
	}

	pipeline.ReturnBatch(batch)
}
