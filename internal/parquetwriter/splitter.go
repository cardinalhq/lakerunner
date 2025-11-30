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

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// FileSplitter manages splitting data into multiple output files based on
// size constraints and grouping requirements.
type FileSplitter struct {
	config       WriterConfig
	currentRows  int64
	currentGroup any

	// Parquet backend state
	backend      ParquetBackend
	tmpFile      *os.File // For final file rename
	currentStats StatsAccumulator

	// Results tracking
	results []Result
	closed  bool
}

// NewFileSplitter creates a new file splitter with the given configuration.
func NewFileSplitter(config WriterConfig) *FileSplitter {
	return &FileSplitter{
		config:  config,
		results: make([]Result, 0),
	}
}

// convertFingerprintToInt64 converts chq_fingerprint from string to int64 if needed.
// Some live data accidentally uses string for this field, but we always want int64.
func convertFingerprintToInt64(value any) (any, error) {
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
		if v > 9223372036854775807 { // max int64
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
		if v > 9223372036854775807 {
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
	// 1. We have an existing file (s.backend != nil)
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
	if s.backend != nil && s.config.RecordsPerFile != NoRecordLimitPerFile && s.config.RecordsPerFile > 0 {
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
	if s.backend == nil {
		if err := s.startNewParquetWriter(); err != nil {
			return fmt.Errorf("start new parquet writer: %w", err)
		}
	}

	// Build column type map once for efficient lookups
	int64Columns := make(map[wkk.RowKey]bool)
	for _, col := range s.config.Schema.Columns() {
		if col.DataType == filereader.DataTypeInt64 {
			int64Columns[col.Name] = true
		}
	}

	// Process and prepare all rows in the batch
	for i := 0; i < batch.Len(); i++ {
		row := batch.Get(i)
		if row == nil {
			continue
		}

		// Convert int64 fields that might be strings (defensive measure for messy live data)
		for key, value := range row {
			if int64Columns[key] && value != nil {
				if str, ok := value.(string); ok {
					converted, err := convertFingerprintToInt64(str)
					if err != nil {
						return fmt.Errorf("convert %s (int64 field) from string for row %d: %w", string(key.Value()), i, err)
					}
					row[key] = converted
				}
			}
		}

		// Add chq_id to the row
		row[wkk.RowKeyCID] = idgen.NextBase32ID()

		// Update stats and tracking
		if s.currentStats != nil {
			stringRow := pipeline.ToStringMap(row)
			s.currentStats.Add(stringRow)
		}
		s.currentRows++

		// Update group tracking
		if s.config.GroupKeyFunc != nil {
			stringRow := pipeline.ToStringMap(row)
			s.currentGroup = s.config.GroupKeyFunc(stringRow)
		}
	}

	// Write entire batch to backend at once (much more efficient than row-by-row)
	if err := s.backend.WriteBatch(context.Background(), batch); err != nil {
		return fmt.Errorf("write batch to backend: %w", err)
	}

	return nil
}

// startNewParquetWriter creates a new temp file and backend.
func (s *FileSplitter) startNewParquetWriter() error {
	// Create temp Parquet file
	tmpFile, err := os.CreateTemp(s.config.TmpDir, "*.parquet")
	if err != nil {
		return fmt.Errorf("create parquet temp file: %w", err)
	}

	// Create backend configuration
	backendConfig := BackendConfig{
		Type:                     s.config.GetBackendType(),
		TmpDir:                   s.config.TmpDir,
		Schema:                   s.config.Schema,
		ChunkSize:                s.config.GetChunkSize(),
		StringConversionPrefixes: s.config.GetStringConversionPrefixes(),
	}

	// Create the appropriate backend
	var backend ParquetBackend
	switch backendConfig.Type {
	case BackendArrow:
		backend, err = NewArrowBackend(backendConfig)
	case BackendGoParquet:
		backend, err = NewGoParquetBackend(backendConfig)
	default:
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
		return fmt.Errorf("unknown backend type: %s", backendConfig.Type)
	}

	if err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
		return fmt.Errorf("create backend: %w", err)
	}

	// Initialize stats accumulator if provider is configured
	var stats StatsAccumulator
	if s.config.StatsProvider != nil {
		stats = s.config.StatsProvider.NewAccumulator()
	}

	s.tmpFile = tmpFile
	s.backend = backend
	s.currentStats = stats
	s.currentRows = 0
	// currentGroup will be set when first row is written

	return nil
}

// finalizeParquetFile closes the backend and returns the file name.
func (s *FileSplitter) finalizeParquetFile() (string, error) {
	ctx := context.Background()

	// Close the backend (flushes all data to tmpFile)
	if _, err := s.backend.Close(ctx, s.tmpFile); err != nil {
		return "", fmt.Errorf("close backend: %w", err)
	}
	s.backend = nil

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
	if s.backend == nil {
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
	if s.backend != nil {
		s.backend.Abort()
		s.backend = nil
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
