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
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

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
	chunkSize int64 // Rows per record batch
}

// NewArrowBackend creates a new Arrow-based backend with streaming writes.
func NewArrowBackend(config BackendConfig) (*ArrowBackend, error) {
	chunkSize := config.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 10000 // Default chunk size
	}

	return &ArrowBackend{
		config:             config,
		columns:            make(map[wkk.RowKey]*ArrowColumnBuilder),
		allocator:          memory.DefaultAllocator,
		chunkSize:          chunkSize,
		schemaFinalized:    false,
		rowsSinceLastFlush: 0,
	}, nil
}

// Name returns the backend name.
func (b *ArrowBackend) Name() string {
	return "arrow"
}

// WriteBatch appends a batch of rows to the in-memory columns.
func (b *ArrowBackend) WriteBatch(ctx context.Context, batch *pipeline.Batch) error {
	numRows := batch.Len()

	// Initialize presence tracking for ALL existing columns
	columnPresence := make(map[wkk.RowKey][]bool)
	for key := range b.columns {
		columnPresence[key] = make([]bool, numRows)
	}

	// First pass: discover new columns and track presence
	for i := range numRows {
		row := batch.Get(i)
		if row == nil {
			continue
		}

		for key, value := range row {
			if _, exists := b.columns[key]; !exists {
				// New column discovered
				if b.schemaFinalized {
					return fmt.Errorf("cannot add new column %s after schema is finalized", wkk.RowKeyValue(key))
				}

				col, err := b.createColumn(key, value)
				if err != nil {
					return fmt.Errorf("failed to create column %s: %w", wkk.RowKeyValue(key), err)
				}
				b.columns[key] = col

				// Backfill nulls for rows already processed
				if b.rowsSinceLastFlush > 0 {
					for range b.rowsSinceLastFlush {
						col.builder.AppendNull()
					}
				}

				// Initialize presence tracking for this new column
				presence := make([]bool, numRows)
				columnPresence[key] = presence
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
				if err := b.appendValue(colBuilder, value); err != nil {
					return fmt.Errorf("failed to append value to column %s: %w", colBuilder.name, err)
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

	// Configure Parquet writer properties
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(true),
	)

	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
	)

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

	// Calculate schema fingerprint
	var fingerprint string
	if b.schema != nil {
		fingerprint = b.calculateSchemaFingerprint(b.schema)
	}

	columnCount := len(b.columns)

	// Release column builders
	b.releaseColumns()

	return &BackendMetadata{
		RowCount:          b.rowCount,
		ColumnCount:       columnCount,
		SchemaFingerprint: fingerprint,
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

// createColumn creates a new column builder for the given key and sample value.
func (b *ArrowBackend) createColumn(key wkk.RowKey, sampleValue any) (*ArrowColumnBuilder, error) {
	dataType := inferArrowType(sampleValue)

	col := &ArrowColumnBuilder{
		name:     wkk.RowKeyValue(key),
		dataType: dataType,
		builder:  array.NewBuilder(b.allocator, dataType),
	}

	// Reserve space for chunk size to avoid reallocation
	col.builder.Reserve(int(b.chunkSize))

	return col, nil
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

// calculateSchemaFingerprint generates a deterministic hash of the schema.
func (b *ArrowBackend) calculateSchemaFingerprint(schema *arrow.Schema) string {
	h := sha256.New()

	// Sort fields by name for deterministic ordering
	fields := schema.Fields()
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	})

	for _, field := range fields {
		h.Write([]byte(field.Name))
		h.Write([]byte("\n"))
		h.Write([]byte(field.Type.String()))
		h.Write([]byte("\n"))
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

// inferArrowType infers the Arrow data type from a Go value.
func inferArrowType(value any) arrow.DataType {
	switch value.(type) {
	case bool:
		return arrow.FixedWidthTypes.Boolean
	case int, int32, int64:
		return arrow.PrimitiveTypes.Int64
	case float32, float64:
		return arrow.PrimitiveTypes.Float64
	case string:
		return arrow.BinaryTypes.String
	default:
		// Default to string for unknown types
		return arrow.BinaryTypes.String
	}
}
