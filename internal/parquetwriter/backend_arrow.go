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
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// ArrowColumnBuilder builds a single column incrementally as rows arrive.
type ArrowColumnBuilder struct {
	name     string
	dataType arrow.DataType
	builder  array.Builder
}

// ArrowBackend implements ParquetBackend using Apache Arrow with streaming writes.
// This backend writes record batches to Parquet incrementally, releasing memory as it goes.
type ArrowBackend struct {
	config    BackendConfig
	columns   map[wkk.RowKey]*ArrowColumnBuilder
	rowCount  int64
	allocator memory.Allocator

	// Streaming write state
	parquetWriter      *pqarrow.FileWriter
	tmpFilePath        string
	tmpFile            *os.File
	schema             *arrow.Schema
	schemaFinalized    bool
	rowsSinceLastFlush int64

	// Configuration
	chunkSize          int64    // Rows per record batch
	conversionPrefixes []string // Cached prefixes for string conversion
}

// NewArrowBackend creates a new Arrow-based backend with streaming writes.
// The schema must be provided upfront and cannot be nil. All columns are created immediately.
// Columns marked as all-null (HasNonNull=false) are filtered out automatically.
func NewArrowBackend(config BackendConfig) (*ArrowBackend, error) {
	if config.Schema == nil {
		return nil, fmt.Errorf("schema is required and cannot be nil")
	}

	chunkSize := config.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 10000 // Default chunk size
	}

	allocator := memory.DefaultAllocator
	columns := make(map[wkk.RowKey]*ArrowColumnBuilder)

	// Get string conversion prefixes
	conversionPrefixes := config.StringConversionPrefixes
	if len(conversionPrefixes) == 0 {
		conversionPrefixes = DefaultStringConversionPrefixes
	}

	// Create columns from schema, filtering out all-null columns
	for _, col := range config.Schema.Columns() {
		// Skip columns that are all null (HasNonNull=false)
		if !col.HasNonNull {
			continue
		}

		// Determine Arrow type - apply string conversion if field name matches prefixes
		var arrowType arrow.DataType
		fieldName := wkk.RowKeyValue(col.Name)
		shouldConvert := false
		for _, prefix := range conversionPrefixes {
			if strings.HasPrefix(fieldName, prefix) {
				shouldConvert = true
				break
			}
		}

		if shouldConvert {
			// Convert to string type to match value conversion at write time
			arrowType = arrow.BinaryTypes.String
		} else {
			arrowType = readerDataTypeToArrow(col.DataType)
		}

		colBuilder := &ArrowColumnBuilder{
			name:     fieldName,
			dataType: arrowType,
			builder:  array.NewBuilder(allocator, arrowType),
		}
		colBuilder.builder.Reserve(int(chunkSize))
		columns[col.Name] = colBuilder
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("schema has no non-null columns")
	}

	return &ArrowBackend{
		config:             config,
		columns:            columns,
		allocator:          allocator,
		chunkSize:          chunkSize,
		conversionPrefixes: config.StringConversionPrefixes,
		schemaFinalized:    false,
		rowsSinceLastFlush: 0,
	}, nil
}

// readerDataTypeToArrow converts a filereader.DataType to an Arrow DataType.
func readerDataTypeToArrow(dt filereader.DataType) arrow.DataType {
	switch dt {
	case filereader.DataTypeBool:
		return arrow.FixedWidthTypes.Boolean
	case filereader.DataTypeInt64:
		return arrow.PrimitiveTypes.Int64
	case filereader.DataTypeFloat64:
		return arrow.PrimitiveTypes.Float64
	case filereader.DataTypeString:
		return arrow.BinaryTypes.String
	case filereader.DataTypeBytes:
		return arrow.BinaryTypes.Binary
	case filereader.DataTypeAny:
		// For complex types, use string representation
		return arrow.BinaryTypes.String
	default:
		// Default to string for unknown types
		return arrow.BinaryTypes.String
	}
}

// Name returns the backend name.
func (b *ArrowBackend) Name() string {
	return "arrow"
}

// shouldConvertToString checks if a field should be converted to string.
func (b *ArrowBackend) shouldConvertToString(fieldName string) bool {
	for _, prefix := range b.conversionPrefixes {
		if strings.HasPrefix(fieldName, prefix) {
			return true
		}
	}
	return false
}

// convertToStringIfNeeded converts a value to string if needed.
func (b *ArrowBackend) convertToStringIfNeeded(fieldName string, value any) any {
	if !b.shouldConvertToString(fieldName) {
		return value
	}

	if value == nil {
		return nil
	}

	if _, ok := value.(string); ok {
		return value
	}

	// Convert based on type
	switch v := value.(type) {
	case int:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case uint:
		return fmt.Sprintf("%d", v)
	case uint32:
		return fmt.Sprintf("%d", v)
	case uint64:
		return fmt.Sprintf("%d", v)
	case float32:
		return fmt.Sprintf("%f", v)
	case float64:
		return fmt.Sprintf("%f", v)
	case bool:
		return fmt.Sprintf("%t", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// WriteBatch appends a batch of rows to the in-memory columns.
// All columns must be defined in the schema provided at construction.
// Rows with unexpected columns will return an error.
func (b *ArrowBackend) WriteBatch(ctx context.Context, batch *pipeline.Batch) error {
	numRows := batch.Len()

	// Initialize presence tracking for ALL schema columns
	columnPresence := make(map[wkk.RowKey][]bool)
	for key := range b.columns {
		columnPresence[key] = make([]bool, numRows)
	}

	// First pass: validate columns and track presence
	for i := range numRows {
		row := batch.Get(i)
		if row == nil {
			continue
		}

		for key := range row {
			if _, exists := b.columns[key]; !exists {
				// Column not in schema - reject the row
				return fmt.Errorf("row contains unexpected column '%s' not in schema (row %d). Schema must be complete upfront",
					wkk.RowKeyValue(key), b.rowCount+int64(i))
			}

			// Mark this column as present in this row
			columnPresence[key][i] = true
		}
	}

	// Second pass: append values to each column builder
	for key, colBuilder := range b.columns {
		presence := columnPresence[key]
		for i := range numRows {
			row := batch.Get(i)
			if row == nil || !presence[i] {
				colBuilder.builder.AppendNull()
			} else {
				value := row[key]
				// Apply string conversion if needed
				convertedValue := b.convertToStringIfNeeded(colBuilder.name, value)
				if err := b.appendValue(colBuilder, convertedValue); err != nil {
					return fmt.Errorf("failed to append value to column %s at row %d: %w", colBuilder.name, b.rowCount+int64(i), err)
				}
			}
		}
	}

	b.rowCount += int64(numRows)
	b.rowsSinceLastFlush += int64(numRows)

	// Check if we should flush record batch to Parquet
	if b.rowsSinceLastFlush >= b.chunkSize {
		if err := b.flushRecordBatch(); err != nil {
			return fmt.Errorf("failed to flush record batch: %w", err)
		}
	}

	return nil
}

// flushRecordBatch writes accumulated rows as a RecordBatch to Parquet and releases memory.
func (b *ArrowBackend) flushRecordBatch() error {
	if b.rowsSinceLastFlush == 0 {
		return nil // Nothing to flush
	}

	// Finalize schema on first flush
	if !b.schemaFinalized {
		if err := b.finalizeSchema(); err != nil {
			return fmt.Errorf("failed to finalize schema: %w", err)
		}
	}

	// Build record batch from current builders
	fields := b.schema.Fields()
	arrays := make([]arrow.Array, len(fields))

	for i, field := range fields {
		// Find column by name
		var col *ArrowColumnBuilder
		for _, c := range b.columns {
			if c.name == field.Name {
				col = c
				break
			}
		}
		if col == nil {
			return fmt.Errorf("column %s not found in builders", field.Name)
		}

		// Finalize builder to create array
		arrays[i] = col.builder.NewArray()
	}

	// Create record batch
	recordBatch := array.NewRecordBatch(b.schema, arrays, b.rowsSinceLastFlush)
	defer recordBatch.Release()

	// Write to Parquet
	if err := b.parquetWriter.Write(recordBatch); err != nil {
		return fmt.Errorf("failed to write record batch: %w", err)
	}

	// Create new builders for next batch
	for _, col := range b.columns {
		col.builder = array.NewBuilder(b.allocator, col.dataType)
		col.builder.Reserve(int(b.chunkSize))
	}

	b.rowsSinceLastFlush = 0
	return nil
}

// finalizeSchema creates the Arrow schema and initializes the Parquet writer.
func (b *ArrowBackend) finalizeSchema() error {
	if b.schemaFinalized {
		return nil
	}

	// Sort columns by name for consistent schema
	keys := make([]wkk.RowKey, 0, len(b.columns))
	for key := range b.columns {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return wkk.RowKeyValue(keys[i]) < wkk.RowKeyValue(keys[j])
	})

	// Build Arrow schema
	fields := make([]arrow.Field, len(keys))
	for i, key := range keys {
		col := b.columns[key]
		fields[i] = arrow.Field{Name: col.name, Type: col.dataType, Nullable: true}
	}
	b.schema = arrow.NewSchema(fields, nil)

	// Create temporary file for Parquet output
	tmpFile, err := os.CreateTemp(b.config.TmpDir, "arrow-*.parquet")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	b.tmpFile = tmpFile
	b.tmpFilePath = tmpFile.Name()

	// Configure Parquet writer properties:
	// - Disable dictionary encoding: Arrow's dictionary pages aren't read correctly by parquet-go
	// - Data Page V1: Required for DuckDB compatibility (V2 causes TProtocolException errors)
	// - Required root repetition: Needed for parquet-go compatibility (REPEATED causes
	//   incorrect interpretation of definition levels)
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(false),
		parquet.WithDataPageVersion(parquet.DataPageV1),
		parquet.WithRootRepetition(parquet.Repetitions.Required),
	)

	arrowProps := pqarrow.DefaultWriterProps()

	// Create Parquet file writer
	parquetWriter, err := pqarrow.NewFileWriter(b.schema, tmpFile, writerProps, arrowProps)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	b.parquetWriter = parquetWriter

	b.schemaFinalized = true
	return nil
}

// Close finalizes the Parquet file and copies it to the output writer.
func (b *ArrowBackend) Close(ctx context.Context, writer io.Writer) (*BackendMetadata, error) {
	// Flush any remaining rows
	if b.rowsSinceLastFlush > 0 {
		if err := b.flushRecordBatch(); err != nil {
			return nil, fmt.Errorf("failed to flush final record batch: %w", err)
		}
	}

	// Close the Parquet writer (this also closes the underlying file)
	if b.parquetWriter != nil {
		if err := b.parquetWriter.Close(); err != nil {
			return nil, fmt.Errorf("failed to close parquet writer: %w", err)
		}
		b.parquetWriter = nil
		b.tmpFile = nil // File was closed by parquet writer
	}

	// Copy temp file to output writer
	if b.tmpFilePath != "" {
		tmpFile, err := os.Open(b.tmpFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to open temp file for reading: %w", err)
		}
		defer func() {
			_ = tmpFile.Close()
			_ = os.Remove(b.tmpFilePath)
		}()

		if _, err := io.Copy(writer, tmpFile); err != nil {
			return nil, fmt.Errorf("failed to copy temp file to output: %w", err)
		}
	}

	columnCount := len(b.columns)

	// Release column builders
	b.releaseColumns()

	return &BackendMetadata{
		RowCount:    b.rowCount,
		ColumnCount: columnCount,
		Extra: map[string]any{
			"chunk_size": b.chunkSize,
			"streaming":  true,
		},
	}, nil
}

// Abort cleans up resources without writing output.
func (b *ArrowBackend) Abort() {
	b.releaseColumns()

	if b.parquetWriter != nil {
		_ = b.parquetWriter.Close()
	}

	if b.tmpFile != nil {
		_ = b.tmpFile.Close()
	}

	if b.tmpFilePath != "" {
		_ = os.Remove(b.tmpFilePath)
	}
}

// appendValue appends a Go value to the appropriate Arrow builder.
func (b *ArrowBackend) appendValue(col *ArrowColumnBuilder, value any) error {
	if value == nil {
		col.builder.AppendNull()
		return nil
	}

	switch builder := col.builder.(type) {
	case *array.BooleanBuilder:
		if v, ok := value.(bool); ok {
			builder.Append(v)
		} else {
			return fmt.Errorf("type mismatch: expected bool, got %T", value)
		}
	case *array.Int64Builder:
		switch v := value.(type) {
		case int64:
			builder.Append(v)
		case int:
			builder.Append(int64(v))
		case int32:
			builder.Append(int64(v))
		default:
			return fmt.Errorf("type mismatch: expected int64, got %T", value)
		}
	case *array.Uint64Builder:
		switch v := value.(type) {
		case uint64:
			builder.Append(v)
		case uint:
			builder.Append(uint64(v))
		case uint32:
			builder.Append(uint64(v))
		default:
			return fmt.Errorf("type mismatch: expected uint64, got %T", value)
		}
	case *array.Float64Builder:
		switch v := value.(type) {
		case float64:
			builder.Append(v)
		case float32:
			builder.Append(float64(v))
		default:
			return fmt.Errorf("type mismatch: expected float64, got %T", value)
		}
	case *array.StringBuilder:
		if v, ok := value.(string); ok {
			builder.Append(v)
		} else {
			return fmt.Errorf("type mismatch: expected string, got %T", value)
		}
	case *array.BinaryBuilder:
		if v, ok := value.([]byte); ok {
			builder.Append(v)
		} else {
			return fmt.Errorf("type mismatch: expected []byte, got %T", value)
		}
	case *array.TimestampBuilder:
		switch v := value.(type) {
		case int64:
			builder.Append(arrow.Timestamp(v))
		case arrow.Timestamp:
			builder.Append(v)
		default:
			return fmt.Errorf("type mismatch: expected timestamp, got %T", value)
		}
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}

	return nil
}

// releaseColumns releases all column builders.
func (b *ArrowBackend) releaseColumns() {
	for _, col := range b.columns {
		if col.builder != nil {
			col.builder.Release()
		}
	}
}
