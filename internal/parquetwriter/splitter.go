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
	"os"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"

	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/schemabuilder"
	"github.com/cardinalhq/lakerunner/pipeline"
)

// FileSplitter manages splitting data into multiple output files based on
// size constraints and grouping requirements.
type FileSplitter struct {
	config             WriterConfig
	currentRows        int64
	currentGroup       any
	conversionPrefixes []string // Cached prefixes for string conversion

	// Parquet writer state
	tmpFile       *os.File
	parquetWriter *parquet.GenericWriter[map[string]any]
	currentStats  StatsAccumulator

	// Pre-built schema from config (used for all files)
	parquetSchema *parquet.Schema
	// Map of expected column names for validation
	expectedColumns map[string]bool

	// Results tracking
	results []Result
	closed  bool
}

// NewFileSplitter creates a new file splitter with the given configuration.
func NewFileSplitter(config WriterConfig) *FileSplitter {
	// Build parquet schema from reader schema
	// Schema is already validated by config.Validate()
	nodes, err := schemabuilder.BuildFromReaderSchema(config.Schema)
	if err != nil {
		panic(fmt.Sprintf("failed to build parquet schema: %v", err))
	}

	// Apply string conversion to schema: any column matching configured prefixes
	// must be converted to string type in the parquet schema, since we convert
	// the actual values to strings at write time
	conversionPrefixes := config.GetStringConversionPrefixes()
	for nodeName := range nodes {
		shouldConvert := false
		for _, prefix := range conversionPrefixes {
			if strings.HasPrefix(nodeName, prefix) {
				shouldConvert = true
				break
			}
		}
		if shouldConvert {
			// Replace with string node, preserving optional/required status
			nodes[nodeName] = parquet.Optional(parquet.String())
		}
	}

	// Add synthetic chq_id column to schema (injected by WriteBatchRows)
	nodes["chq_id"] = parquet.Optional(parquet.String())

	parquetSchema := parquet.NewSchema("lakerunner", parquet.Group(nodes))

	// Build map of expected column names for validation
	expectedColumns := make(map[string]bool, len(nodes))
	for name := range nodes {
		expectedColumns[name] = true
	}

	return &FileSplitter{
		config:             config,
		parquetSchema:      parquetSchema,
		expectedColumns:    expectedColumns,
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

// convertFingerprintToInt64 converts chq_fingerprint from string to int64 if needed.
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

	// File splitting logic (BETWEEN batches only, never within a batch):
	//
	// Splitting happens BEFORE processing each batch and only if:
	// 1. We have an existing file (s.parquetWriter != nil)
	// 2. RecordsPerFile limit is set (not unlimited mode)
	// 3. Adding this batch would exceed RecordsPerFile
	//
	// With NoSplitGroups enabled:
	// - Split ONLY if the new batch has a different group key than current file
	// - If same group, continue writing to same file even if exceeding RecordsPerFile
	// - This keeps groups together while respecting file size limits at group boundaries
	//
	// Without NoSplitGroups:
	// - Split whenever RecordsPerFile would be exceeded
	//
	// Note: Splitting never happens WITHIN a batch. If a batch contains multiple groups
	// or exceeds limits, all rows still go into the current file. This ensures efficient
	// batch processing without mid-batch file switches.
	shouldSplit := false
	if s.parquetWriter != nil && s.config.RecordsPerFile != NoRecordLimitPerFile && s.config.RecordsPerFile > 0 {
		projectedRows := s.currentRows + int64(actualRowCount)
		if projectedRows > s.config.RecordsPerFile {
			// Check if NoSplitGroups is enabled
			if s.config.NoSplitGroups && s.config.GroupKeyFunc != nil {
				// Peek at first non-nil row to get the group for this batch
				var batchGroup any
				for i := 0; i < batch.Len(); i++ {
					row := batch.Get(i)
					if row != nil {
						// Convert to map to extract group key
						tempRow := make(map[string]any, len(row))
						for key, value := range row {
							tempRow[string(key.Value())] = value
						}
						batchGroup = s.config.GroupKeyFunc(tempRow)
						break
					}
				}

				// Only split if the group changed (or if we don't have a current group yet)
				if s.currentGroup == nil || batchGroup != s.currentGroup {
					shouldSplit = true
				}
				// Otherwise, continue writing to same file even though we exceed RecordsPerFile
			} else {
				// NoSplitGroups not enabled, split based on row count only
				shouldSplit = true
			}
		}
	}

	if shouldSplit {
		// Finish current file first
		if err := s.finishCurrentFile(); err != nil {
			return fmt.Errorf("finish current file before split: %w", err)
		}
	}

	// Start a new Parquet writer if we don't have one
	if s.parquetWriter == nil {
		if err := s.startNewParquetWriter(); err != nil {
			return fmt.Errorf("start new parquet writer: %w", err)
		}
	}

	// Process and buffer all rows
	for i := 0; i < batch.Len(); i++ {
		row := batch.Get(i)
		if row == nil {
			continue
		}

		// Convert pipeline.Row to map[string]any efficiently
		stringRow := make(map[string]any, len(row)+1) // +1 for chq_id
		for key, value := range row {
			fieldName := string(key.Value())
			// Apply string conversion for fields with configured prefixes
			convertedValue := s.convertToStringIfNeeded(fieldName, value)

			// Special handling for chq_fingerprint: must always be int64
			if fieldName == "chq_fingerprint" {
				var err error
				convertedValue, err = s.convertFingerprintToInt64(convertedValue)
				if err != nil {
					return fmt.Errorf("convert fingerprint for row %d: %w", i, err)
				}
			}

			stringRow[fieldName] = convertedValue
		}

		stringRow["chq_id"] = idgen.NextBase32ID()

		// Validate all columns are in schema
		for key := range stringRow {
			if !s.expectedColumns[key] {
				return fmt.Errorf("row contains unexpected column '%s' not in schema. Schema must be complete upfront", key)
			}
		}

		// Write row directly to Parquet
		if _, err := s.parquetWriter.Write([]map[string]any{stringRow}); err != nil {
			// Add debug info about the row that failed
			debugInfo := fmt.Sprintf("\nRow that failed to write (row %d):\n", i)
			for k, v := range stringRow {
				debugInfo += fmt.Sprintf("  %s: %v (%T)\n", k, v, v)
			}
			return fmt.Errorf("write row to parquet: %w%s", err, debugInfo)
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

// startNewParquetWriter creates a new temp file and Parquet writer.
func (s *FileSplitter) startNewParquetWriter() error {
	// Create temp Parquet file
	tmpFile, err := os.CreateTemp(s.config.TmpDir, "*.parquet")
	if err != nil {
		return fmt.Errorf("create parquet temp file: %w", err)
	}

	writerConfig, err := parquet.NewWriterConfig(schemabuilder.WriterOptions(s.config.TmpDir, s.parquetSchema)...)
	if err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
		return fmt.Errorf("create writer config: %w", err)
	}

	parquetWriter := parquet.NewGenericWriter[map[string]any](tmpFile, writerConfig)

	// Initialize stats accumulator if provider is configured
	var stats StatsAccumulator
	if s.config.StatsProvider != nil {
		stats = s.config.StatsProvider.NewAccumulator()
	}

	s.tmpFile = tmpFile
	s.parquetWriter = parquetWriter
	s.currentStats = stats
	s.currentRows = 0
	// currentGroup will be set when first row is written

	return nil
}

// finalizeParquetFile closes the Parquet writer and returns the file name.
func (s *FileSplitter) finalizeParquetFile() (string, error) {
	// Close the Parquet writer (flushes all data)
	if err := s.parquetWriter.Close(); err != nil {
		return "", fmt.Errorf("close parquet writer: %w", err)
	}
	s.parquetWriter = nil

	// Close temp file (CHECK return code since we're keeping this file)
	tmpFileName := s.tmpFile.Name()
	if err := s.tmpFile.Close(); err != nil {
		return "", fmt.Errorf("close parquet temp file: %w", err)
	}
	s.tmpFile = nil

	return tmpFileName, nil
}

// finishCurrentFile finalizes the current Parquet file and adds to results.
func (s *FileSplitter) finishCurrentFile() error {
	if s.parquetWriter == nil {
		return nil // No file to finish
	}

	// Only finalize if we have rows
	if s.currentRows == 0 {
		s.cleanupCurrentFile()
		return nil
	}

	// Finalize Parquet file
	parquetFileName, err := s.finalizeParquetFile()
	if err != nil {
		s.cleanupCurrentFile()
		return fmt.Errorf("finalize parquet file: %w", err)
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

	// Reset state (file already closed)
	s.currentStats = nil
	s.currentRows = 0

	return nil
}

// cleanupCurrentFile removes the current Parquet file and resets state.
func (s *FileSplitter) cleanupCurrentFile() {
	if s.parquetWriter != nil {
		_ = s.parquetWriter.Close()
		s.parquetWriter = nil
	}

	if s.tmpFile != nil {
		tmpFileName := s.tmpFile.Name()
		_ = s.tmpFile.Close()
		_ = os.Remove(tmpFileName)
		s.tmpFile = nil
	}

	s.currentStats = nil
	s.currentRows = 0
	// Schema is reused across files, no need to reset
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

	// Clean up current file
	s.cleanupCurrentFile()

	// Clean up any completed result files too
	for _, result := range s.results {
		_ = os.Remove(result.FileName)
	}
	s.results = nil
}
