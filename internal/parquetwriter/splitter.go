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
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/schemabuilder"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/rowcodec"
)

// FileSplitter manages splitting data into multiple output files based on
// size constraints and grouping requirements.
type FileSplitter struct {
	config             WriterConfig
	currentRows        int64
	currentGroup       any
	conversionPrefixes []string // Cached prefixes for string conversion

	// Binary buffering for schema evolution
	codec        rowcodec.Codec
	bufferFile   *os.File
	encoder      rowcodec.Encoder
	currentStats StatsAccumulator

	// Dynamic schema management per file
	currentSchema *schemabuilder.SchemaBuilder

	// Results tracking
	results []Result
	closed  bool
}

// NewFileSplitter creates a new file splitter with the given configuration.
func NewFileSplitter(config WriterConfig) *FileSplitter {
	// Use default codec (Binary for compatibility, can use TypeCBOR for better performance)
	codec, err := rowcodec.New(rowcodec.TypeDefault)
	if err != nil {
		// This should never happen with our static configuration
		panic(fmt.Sprintf("failed to create codec: %v", err))
	}

	return &FileSplitter{
		config:             config,
		codec:              codec,
		results:            make([]Result, 0),
		conversionPrefixes: config.GetStringConversionPrefixes(),
	}
}

// shouldConvertToString checks if a field name matches any of the configured prefixes
// that require string conversion.
func (s *FileSplitter) shouldConvertToString(fieldName string) bool {
	for _, prefix := range s.conversionPrefixes {
		if strings.HasPrefix(fieldName, prefix) {
			return true
		}
	}
	return false
}

// convertToStringIfNeeded converts a value to string if the field name matches
// one of the configured prefixes. Otherwise, returns the value unchanged.
func (s *FileSplitter) convertToStringIfNeeded(fieldName string, value any) any {
	if !s.shouldConvertToString(fieldName) {
		return value
	}

	// Handle nil values
	if value == nil {
		return nil
	}

	// If already a string, return as-is
	if _, ok := value.(string); ok {
		return value
	}

	// Convert to string based on type
	switch v := value.(type) {
	case int64:
		return strconv.FormatInt(v, 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int:
		return strconv.Itoa(v)
	case uint64:
		return strconv.FormatUint(v, 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case bool:
		return strconv.FormatBool(v)
	case []byte:
		return string(v)
	default:
		// For any other type (including slices, maps, etc.), use fmt.Sprint
		return fmt.Sprint(v)
	}
}

// convertFingerprintToInt64 converts _cardinalhq_fingerprint from string to int64 if needed.
// The fingerprint field must always be int64 in the schema.
func (s *FileSplitter) convertFingerprintToInt64(value any) (any, error) {
	if value == nil {
		return nil, nil
	}

	// If already int64, return as-is
	if v, ok := value.(int64); ok {
		return v, nil
	}

	// Convert string to int64
	if str, ok := value.(string); ok {
		val, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse fingerprint string %q as int64: %w", str, err)
		}
		return val, nil
	}

	// For other numeric types, convert to int64
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case uint64:
		if v > uint64(9223372036854775807) { // max int64
			return nil, fmt.Errorf("fingerprint value %d exceeds int64 max", v)
		}
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint:
		if v > uint(9223372036854775807) {
			return nil, fmt.Errorf("fingerprint value %d exceeds int64 max", v)
		}
		return int64(v), nil
	default:
		return nil, fmt.Errorf("unsupported fingerprint type: %T", value)
	}
}

// WriteBatchRows efficiently writes multiple rows from a pipeline batch.
// Rows are buffered to binary files to allow schema evolution across batches.
func (s *FileSplitter) WriteBatchRows(ctx context.Context, batch *pipeline.Batch) error {
	if s.closed {
		return ErrWriterClosed
	}

	if batch == nil {
		return fmt.Errorf("batch cannot be nil")
	}

	// Count actual rows first (excluding nil rows)
	actualRowCount := 0
	for i := 0; i < batch.Len(); i++ {
		if batch.Get(i) != nil {
			actualRowCount++
		}
	}

	// Return early if no actual rows to process
	if actualRowCount == 0 {
		return nil
	}

	// Check if we need to split files BEFORE processing this batch
	// Skip splitting if RecordsPerFile is NoRecordLimitPerFile (unlimited mode)
	projectedRows := s.currentRows + int64(actualRowCount)
	if s.bufferFile != nil && s.config.RecordsPerFile != NoRecordLimitPerFile && s.config.RecordsPerFile > 0 && projectedRows > s.config.RecordsPerFile {
		// Finish current file first
		if err := s.finishCurrentFile(); err != nil {
			return fmt.Errorf("finish current file before split: %w", err)
		}
	}

	// Start a new binary buffer file if we don't have one
	if s.bufferFile == nil {
		if err := s.startNewBufferFile(); err != nil {
			return fmt.Errorf("start new buffer file: %w", err)
		}
	}

	// Process and buffer all rows
	for i := 0; i < batch.Len(); i++ {
		row := batch.Get(i)
		if row == nil {
			continue
		}

		// Convert pipeline.Row to map[string]any efficiently
		stringRow := make(map[string]any, len(row)+1) // +1 for _cardinalhq_id
		for key, value := range row {
			fieldName := string(key.Value())
			// Apply string conversion for fields with configured prefixes
			convertedValue := s.convertToStringIfNeeded(fieldName, value)

			// Special handling for _cardinalhq_fingerprint: must always be int64
			if fieldName == "_cardinalhq_fingerprint" {
				var err error
				convertedValue, err = s.convertFingerprintToInt64(convertedValue)
				if err != nil {
					return fmt.Errorf("convert fingerprint for row %d: %w", i, err)
				}
			}

			stringRow[fieldName] = convertedValue
		}

		stringRow["_cardinalhq_id"] = idgen.NextBase32ID()

		// Add to schema builder for evolution tracking
		if err := s.currentSchema.AddRow(stringRow); err != nil {
			return fmt.Errorf("schema validation failed: %w", err)
		}

		// Encode and write row to buffer
		if err := s.encoder.Encode(stringRow); err != nil {
			return fmt.Errorf("encode row: %w", err)
		}

		// Update stats and tracking
		if s.currentStats != nil {
			s.currentStats.Add(stringRow)
		}
		s.currentRows++

		// Update group tracking
		if s.config.GroupKeyFunc != nil {
			s.currentGroup = s.config.GroupKeyFunc(stringRow)
		}
	}

	return nil
}

// startNewBufferFile creates a new buffer file for row accumulation.
// The schema will be built dynamically as rows are added.
func (s *FileSplitter) startNewBufferFile() error {
	// Create the binary buffer file
	file, err := os.CreateTemp(s.config.TmpDir, "*.bin")
	if err != nil {
		return fmt.Errorf("create binary temp file: %w", err)
	}

	// Initialize a new schema builder for this file
	s.currentSchema = schemabuilder.NewSchemaBuilder()

	// Initialize stats accumulator if provider is configured
	var stats StatsAccumulator
	if s.config.StatsProvider != nil {
		stats = s.config.StatsProvider.NewAccumulator()
	}

	// Create binary encoder for writing rows
	s.bufferFile = file
	s.encoder = s.codec.NewEncoder(file)
	s.currentStats = stats
	s.currentRows = 0
	// currentGroup will be set when first row is written

	return nil
}

// streamBinaryToParquet streams all buffered binary data to a new parquet file.
// This creates the final parquet file with the evolved schema.
func (s *FileSplitter) streamBinaryToParquet() (string, error) {
	// Build the final schema from all accumulated rows
	nodes, err := s.currentSchema.Build()
	if err != nil {
		return "", fmt.Errorf("failed to build schema: %w", err)
	}
	if len(nodes) == 0 {
		return "", fmt.Errorf("no columns discovered for schema")
	}

	schema := parquet.NewSchema("lakerunner", parquet.Group(nodes))

	// Create the final parquet output file
	parquetFile, err := os.CreateTemp(s.config.TmpDir, "*.parquet")
	if err != nil {
		return "", fmt.Errorf("create parquet temp file: %w", err)
	}
	defer func() { _ = parquetFile.Close() }()

	// Create parquet writer with optimized settings
	writerConfig, err := parquet.NewWriterConfig(schemabuilder.WriterOptions(s.config.TmpDir, schema)...)
	if err != nil {
		return "", fmt.Errorf("create writer config: %w", err)
	}

	parquetWriter := parquet.NewGenericWriter[map[string]any](parquetFile, writerConfig)

	// Close the binary encoder and sync file before reading
	if s.encoder != nil {
		s.encoder = nil
	}

	// Sync the file to ensure all data is written to disk
	if err := s.bufferFile.Sync(); err != nil {
		return "", fmt.Errorf("sync buffer file: %w", err)
	}

	// Get file size for debugging
	stat, err := s.bufferFile.Stat()
	if err != nil {
		return "", fmt.Errorf("failed to stat buffer file: %w", err)
	}
	if stat.Size() == 0 {
		return "", fmt.Errorf("buffer file is empty - no data was written")
	}

	// Close and reopen buffer file for reading
	bufferFileName := s.bufferFile.Name()
	if err := s.bufferFile.Close(); err != nil {
		return "", fmt.Errorf("close buffer file for writing: %w", err)
	}

	bufferFile, err := os.Open(bufferFileName)
	if err != nil {
		return "", fmt.Errorf("reopen buffer file for reading: %w", err)
	}
	defer func() { _ = bufferFile.Close() }()

	// Create decoder to read back the buffered rows
	decoder := s.codec.NewDecoder(bufferFile)

	// Stream all rows to parquet
	row := make(map[string]any) // Reuse this map for all decodes
	for {
		err := decoder.Decode(row)
		if err != nil {
			if err == io.EOF {
				break // End of file reached
			}
			// Handle EOF that comes from trying to read map length when no more data
			if err.Error() == "read map length: EOF" {
				break
			}
			return "", fmt.Errorf("decode row: %w", err)
		}

		// Write the row to parquet
		if _, err := parquetWriter.Write([]map[string]any{row}); err != nil {
			return "", fmt.Errorf("write row to parquet: %w", err)
		}
	}

	// Close parquet writer to finalize the file
	if err := parquetWriter.Close(); err != nil {
		return "", fmt.Errorf("close parquet writer: %w", err)
	}

	return parquetFile.Name(), nil
}

// finishCurrentFile streams buffered binary data to parquet and adds to results.
func (s *FileSplitter) finishCurrentFile() error {
	if s.bufferFile == nil {
		return nil // No file to finish
	}

	// Only create parquet if we have rows
	if s.currentRows == 0 {
		s.cleanupCurrentBufferFile()
		return nil
	}

	// Stream binary data to final parquet file
	parquetFileName, err := s.streamBinaryToParquet()
	if err != nil {
		s.cleanupCurrentBufferFile()
		return fmt.Errorf("stream binary to parquet: %w", err)
	}

	// Get file size
	info, err := os.Stat(parquetFileName)
	var fileSize int64 = -1
	if err == nil {
		fileSize = info.Size()
	}

	// Collect stats
	var metadata any
	if s.currentStats != nil {
		metadata = s.currentStats.Finalize()
	}

	// Add to results
	s.results = append(s.results, Result{
		FileName:    parquetFileName,
		RecordCount: s.currentRows,
		FileSize:    fileSize,
		Metadata:    metadata,
	})

	// Clean up buffer file and reset state
	s.cleanupCurrentBufferFile()

	return nil
}

// cleanupCurrentBufferFile removes the binary buffer file and resets state.
func (s *FileSplitter) cleanupCurrentBufferFile() {
	if s.bufferFile != nil {
		bufferFileName := s.bufferFile.Name()
		_ = s.bufferFile.Close()
		_ = os.Remove(bufferFileName)
		s.bufferFile = nil
	}

	s.encoder = nil
	s.currentStats = nil
	s.currentRows = 0
	s.currentSchema = nil
}

// Close finishes the current file and returns all results.
func (s *FileSplitter) Close(ctx context.Context) ([]Result, error) {
	if s.closed {
		return s.results, nil
	}
	s.closed = true

	// Finish any current file
	if err := s.finishCurrentFile(); err != nil {
		return s.results, fmt.Errorf("finish current file: %w", err)
	}

	return s.results, nil
}

// Abort cleans up any current file and temporary resources.
func (s *FileSplitter) Abort() {
	s.closed = true

	// Clean up current binary buffer file
	s.cleanupCurrentBufferFile()

	// Clean up any completed result files too
	for _, result := range s.results {
		_ = os.Remove(result.FileName)
	}
	s.results = nil
}
