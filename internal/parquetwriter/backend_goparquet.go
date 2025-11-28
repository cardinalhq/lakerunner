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
	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/cardinalhq/lakerunner/internal/parquetwriter/schemabuilder"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/rowcodec"
)

// GoParquetBackend implements ParquetBackend using segmentio/parquet-go with CBOR buffering.
// This is the current production implementation.
type GoParquetBackend struct {
	config BackendConfig

	// CBOR buffering
	codec      rowcodec.Codec
	bufferFile *os.File
	encoder    rowcodec.Encoder

	// Pre-built schema from ReaderSchema
	parquetSchema *parquet.Schema
	// Map of column names that should be in every row
	expectedColumns map[string]bool

	// Metrics
	rowCount           int64
	conversionPrefixes []string
}

// NewGoParquetBackend creates a new go-parquet backend.
// The schema must be provided upfront and cannot be nil.
// All columns are validated against the schema during writes.
// Columns marked as all-null (HasNonNull=false) are filtered out automatically.
func NewGoParquetBackend(config BackendConfig) (*GoParquetBackend, error) {
	if config.Schema == nil {
		return nil, fmt.Errorf("schema is required and cannot be nil")
	}

	codec, err := rowcodec.New(rowcodec.TypeDefault)
	if err != nil {
		return nil, fmt.Errorf("failed to create codec: %w", err)
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

	return &GoParquetBackend{
		config:             config,
		codec:              codec,
		parquetSchema:      parquetSchema,
		expectedColumns:    expectedColumns,
		conversionPrefixes: config.StringConversionPrefixes,
	}, nil
}

// Name returns the backend name.
func (b *GoParquetBackend) Name() string {
	return "go-parquet"
}

// WriteBatch writes a batch of rows to the CBOR buffer.
// All columns must be defined in the schema provided at construction.
// Rows with unexpected columns will return an error.
func (b *GoParquetBackend) WriteBatch(ctx context.Context, batch *pipeline.Batch) error {
	// Initialize buffer file on first write
	if b.bufferFile == nil {
		if err := b.startNewBufferFile(); err != nil {
			return fmt.Errorf("failed to start buffer file: %w", err)
		}
	}

	// Process each row
	for i := 0; i < batch.Len(); i++ {
		row := batch.Get(i)
		if row == nil {
			continue
		}

		// Convert row to map[string]any for codec and validate against schema
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

		// Encode to CBOR buffer
		if err := b.encoder.Encode(rowMap); err != nil {
			return fmt.Errorf("failed to encode row: %w", err)
		}

		b.rowCount++
	}

	return nil
}

// Close finalizes the Parquet file and writes it to the output writer.
func (b *GoParquetBackend) Close(ctx context.Context, writer io.Writer) (*BackendMetadata, error) {
	if b.bufferFile == nil {
		// No data written
		return &BackendMetadata{
			RowCount:    0,
			ColumnCount: 0,
		}, nil
	}

	// Finalize buffer file (encoder is set to nil, not closed)
	b.encoder = nil

	// Sync the buffer file
	if err := b.bufferFile.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync buffer file: %w", err)
	}

	// Use pre-built schema (already created in constructor)
	// Stream from CBOR buffer to Parquet
	metadata, err := b.streamBinaryToParquet(writer, b.parquetSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to write parquet: %w", err)
	}

	// Cleanup buffer file
	b.cleanupBufferFile()

	return metadata, nil
}

// Abort cleans up resources without writing output.
func (b *GoParquetBackend) Abort() {
	b.cleanupBufferFile()
}

// startNewBufferFile creates a new temporary file for CBOR buffering.
func (b *GoParquetBackend) startNewBufferFile() error {
	tmpFile, err := os.CreateTemp(b.config.TmpDir, "goparquet-buffer-*.cbor")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	encoder := b.codec.NewEncoder(tmpFile)

	b.bufferFile = tmpFile
	b.encoder = encoder
	// Schema is already built in constructor, no need to reinitialize

	return nil
}

// streamBinaryToParquet reads from CBOR buffer and writes Parquet.
func (b *GoParquetBackend) streamBinaryToParquet(output io.Writer, schema *parquet.Schema) (*BackendMetadata, error) {
	// Close and reopen buffer file for reading
	bufferFileName := b.bufferFile.Name()
	if err := b.bufferFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close buffer file: %w", err)
	}

	bufferFile, err := os.Open(bufferFileName)
	if err != nil {
		return nil, fmt.Errorf("failed to reopen buffer file: %w", err)
	}
	defer func() { _ = bufferFile.Close() }()

	// Create Parquet writer config
	writerConfig, err := parquet.NewWriterConfig(schemabuilder.WriterOptions(b.config.TmpDir, schema)...)
	if err != nil {
		return nil, fmt.Errorf("failed to create writer config: %w", err)
	}

	// Create Parquet writer
	writer := parquet.NewGenericWriter[map[string]any](output, writerConfig)
	defer func() { _ = writer.Close() }()

	// Decode and write rows
	decoder := b.codec.NewDecoder(bufferFile)
	rowsWritten := int64(0)

	row := make(map[string]any) // Reuse for all decodes
	for {
		err := decoder.Decode(row)
		if err != nil {
			if err == io.EOF {
				break
			}
			// Handle EOF from map length read
			if err.Error() == "read map length: EOF" {
				break
			}
			return nil, fmt.Errorf("failed to decode row: %w", err)
		}

		if _, err := writer.Write([]map[string]any{row}); err != nil {
			return nil, fmt.Errorf("failed to write row to parquet: %w", err)
		}
		rowsWritten++

		// Clear map for next decode (reuse)
		for k := range row {
			delete(row, k)
		}
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close parquet writer: %w", err)
	}

	// Calculate schema fingerprint for comparison
	fingerprint := b.calculateSchemaFingerprint(schema)

	return &BackendMetadata{
		RowCount:          rowsWritten,
		ColumnCount:       len(schema.Fields()),
		SchemaFingerprint: fingerprint,
		Extra: map[string]any{
			"buffer_file_used": true,
		},
	}, nil
}

// cleanupBufferFile removes the temporary CBOR file.
func (b *GoParquetBackend) cleanupBufferFile() {
	if b.bufferFile != nil {
		_ = b.bufferFile.Close()
		_ = os.Remove(b.bufferFile.Name())
		b.bufferFile = nil
	}
}

// shouldConvertToString checks if a field should be converted to string.
func (b *GoParquetBackend) shouldConvertToString(fieldName string) bool {
	for _, prefix := range b.conversionPrefixes {
		if strings.HasPrefix(fieldName, prefix) {
			return true
		}
	}
	return false
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

// calculateSchemaFingerprint creates a deterministic hash of the schema.
func (b *GoParquetBackend) calculateSchemaFingerprint(schema *parquet.Schema) string {
	// Get field names only (types can vary in representation between backends)
	fields := schema.Fields()
	fieldNames := make([]string, len(fields))
	for i, field := range fields {
		fieldNames[i] = field.Name()
	}
	sort.Strings(fieldNames)

	// Hash the sorted field list
	h := sha256.New()
	for _, name := range fieldNames {
		h.Write([]byte(name))
		h.Write([]byte("\n"))
	}

	return fmt.Sprintf("%x", h.Sum(nil))[:16]
}
