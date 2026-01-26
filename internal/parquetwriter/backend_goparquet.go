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
	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/schemabuilder"
	"github.com/cardinalhq/lakerunner/pipeline"
)

// GoParquetBackend implements ParquetBackend using parquet-go, writing directly to Parquet.
// This is the current production implementation.
type GoParquetBackend struct {
	config BackendConfig

	// Parquet writer state
	tmpFile       *os.File
	parquetWriter *parquet.GenericWriter[map[string]any]

	// Pre-built schema from ReaderSchema
	parquetSchema *parquet.Schema
	// Map of column names that should be in every row
	expectedColumns map[string]bool

	// Cached column types for fast lookup (avoids iterating schema for every field)
	nonStringColumns   map[string]bool // columns that should NOT be converted to string
	conversionPrefixes []string
	convertToString    map[string]bool // pre-computed map of fields that need string conversion

	// Metrics
	rowCount int64
}

// NewGoParquetBackend creates a new go-parquet backend.
// The schema must be provided upfront and cannot be nil.
// All columns are validated against the schema during writes.
// Columns marked as all-null (HasNonNull=false) are filtered out automatically.
func NewGoParquetBackend(config BackendConfig) (*GoParquetBackend, error) {
	if config.Schema == nil {
		return nil, fmt.Errorf("schema is required and cannot be nil")
	}

	// Build parquet schema from reader schema
	nodes, err := schemabuilder.BuildFromReaderSchema(config.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to build parquet schema: %w", err)
	}

	if len(nodes) == 0 {
		return nil, fmt.Errorf("schema has no non-null columns")
	}

	parquetSchema := parquet.NewSchema("lakerunner", parquet.Group(nodes))

	// Build map of expected column names for validation
	expectedColumns := make(map[string]bool, len(nodes))
	for name := range nodes {
		expectedColumns[name] = true
	}

	// Build cache of columns that should NOT be converted to strings
	// (int64, float64, bool types) - avoids iterating schema for every field
	nonStringColumns := make(map[string]bool)
	for _, col := range config.Schema.Columns() {
		fieldName := string(col.Name.Value())
		switch col.DataType {
		case filereader.DataTypeInt64, filereader.DataTypeFloat64, filereader.DataTypeBool:
			nonStringColumns[fieldName] = true
		}
	}

	// Pre-compute which fields need string conversion based on prefixes
	// This avoids checking prefixes for every field on every row
	convertToString := make(map[string]bool)
	for _, col := range config.Schema.Columns() {
		fieldName := string(col.Name.Value())

		// Skip if it's a non-string column type
		if nonStringColumns[fieldName] {
			continue
		}

		// Check if field matches any conversion prefix
		for _, prefix := range config.StringConversionPrefixes {
			if strings.HasPrefix(fieldName, prefix) {
				convertToString[fieldName] = true
				break
			}
		}
	}

	return &GoParquetBackend{
		config:             config,
		parquetSchema:      parquetSchema,
		expectedColumns:    expectedColumns,
		nonStringColumns:   nonStringColumns,
		conversionPrefixes: config.StringConversionPrefixes,
		convertToString:    convertToString,
	}, nil
}

// Name returns the backend name.
func (b *GoParquetBackend) Name() string {
	return "go-parquet"
}

// WriteBatch writes a batch of rows directly to Parquet.
// All columns must be defined in the schema provided at construction.
// Rows with unexpected columns will return an error.
func (b *GoParquetBackend) WriteBatch(ctx context.Context, batch *pipeline.Batch) error {
	// Initialize Parquet writer on first write
	if b.parquetWriter == nil {
		if err := b.initParquetWriter(); err != nil {
			return fmt.Errorf("failed to initialize parquet writer: %w", err)
		}
	}

	// Pre-allocate slice for entire batch (nil rows are rare, small overhead if present)
	rowMaps := make([]map[string]any, 0, batch.Len())

	// Process each row and build row maps
	for i := 0; i < batch.Len(); i++ {
		row := batch.Get(i)
		if row == nil {
			continue
		}

		// Convert row to map[string]any and validate against schema
		rowMap := make(map[string]any, len(b.expectedColumns))
		for key, value := range row {
			fieldName := string(key.Value())

			// Validate column is in schema
			if !b.expectedColumns[fieldName] {
				return fmt.Errorf("row contains unexpected column '%s' not in schema (row %d). Schema must be complete upfront",
					fieldName, b.rowCount)
			}

			// Apply string conversion if needed
			convertedValue := b.convertToStringIfNeeded(fieldName, value)
			rowMap[fieldName] = convertedValue
		}

		rowMaps = append(rowMaps, rowMap)
		b.rowCount++
	}

	// Write entire batch to Parquet in one call
	if _, err := b.parquetWriter.Write(rowMaps); err != nil {
		return fmt.Errorf("failed to write batch to parquet: %w", err)
	}

	return nil
}

// Close finalizes the Parquet file and writes it to the output writer.
func (b *GoParquetBackend) Close(ctx context.Context, writer io.Writer) (*BackendMetadata, error) {
	if b.parquetWriter == nil {
		// No data written
		return &BackendMetadata{
			RowCount:    0,
			ColumnCount: 0,
		}, nil
	}

	// Close the Parquet writer (flushes all data)
	if err := b.parquetWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to close parquet writer: %w", err)
	}
	b.parquetWriter = nil

	// Close temp file and reopen for reading
	tmpFileName := b.tmpFile.Name()
	if err := b.tmpFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close temp file: %w", err)
	}

	// Copy temp file to output writer
	tmpFile, err := os.Open(tmpFileName)
	if err != nil {
		return nil, fmt.Errorf("failed to reopen temp file: %w", err)
	}
	defer func() { _ = tmpFile.Close() }()

	if _, err := io.Copy(writer, tmpFile); err != nil {
		return nil, fmt.Errorf("failed to copy parquet data to output: %w", err)
	}

	// Cleanup temp file
	b.cleanupTempFile()

	return &BackendMetadata{
		RowCount:    b.rowCount,
		ColumnCount: len(b.parquetSchema.Fields()),
	}, nil
}

// Abort cleans up resources without writing output.
func (b *GoParquetBackend) Abort() {
	if b.parquetWriter != nil {
		_ = b.parquetWriter.Close()
		b.parquetWriter = nil
	}
	b.cleanupTempFile()
}

// initParquetWriter creates a temp file and initializes the Parquet writer.
func (b *GoParquetBackend) initParquetWriter() error {
	tmpFile, err := os.CreateTemp(b.config.TmpDir, "goparquet-*.parquet")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	// Create Parquet writer config
	writerConfig, err := parquet.NewWriterConfig(schemabuilder.WriterOptions(b.config.TmpDir, b.parquetSchema)...)
	if err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
		return fmt.Errorf("failed to create writer config: %w", err)
	}

	// Create Parquet writer
	parquetWriter := parquet.NewGenericWriter[map[string]any](tmpFile, writerConfig)

	b.tmpFile = tmpFile
	b.parquetWriter = parquetWriter

	return nil
}

// cleanupTempFile removes the temporary Parquet file.
func (b *GoParquetBackend) cleanupTempFile() {
	if b.tmpFile != nil {
		_ = b.tmpFile.Close()
		_ = os.Remove(b.tmpFile.Name())
		b.tmpFile = nil
	}
}

// shouldConvertToString checks if a field should be converted to string.
// Uses pre-computed map for O(1) lookup instead of checking prefixes on every call.
func (b *GoParquetBackend) shouldConvertToString(fieldName string) bool {
	return b.convertToString[fieldName]
}

// convertToStringIfNeeded converts a value to string if needed.
func (b *GoParquetBackend) convertToStringIfNeeded(fieldName string, value any) any {
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
