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
	"sort"

	"github.com/parquet-go/parquet-go"
)

// FileSplitter manages splitting data into multiple output files based on
// size constraints and grouping requirements.
type FileSplitter struct {
	config        WriterConfig
	currentSize   int64
	currentRows   int64
	currentGroup  any

	// Current file being written
	currentFile   *os.File
	currentWriter *parquet.GenericWriter[map[string]any]
	currentStats  StatsAccumulator

	// Schema management
	seenColumns  map[string]bool
	parquetNodes map[string]parquet.Node

	// Results tracking
	results []Result
	closed  bool
}

// NewFileSplitter creates a new file splitter with the given configuration.
func NewFileSplitter(config WriterConfig) *FileSplitter {
	return &FileSplitter{
		config:       config,
		seenColumns:  make(map[string]bool),
		parquetNodes: make(map[string]parquet.Node),
		results:      make([]Result, 0),
	}
}

// ShouldSplit determines if a new file should be started for the given row.
func (s *FileSplitter) ShouldSplit(row map[string]any) bool {
	if s.currentWriter == nil {
		return false // No current file, so we can't split
	}

	estimatedRowSize := int64(s.config.BytesPerRecord)
	projectedSize := s.currentSize + estimatedRowSize

	// If we would exceed target size, consider splitting
	if projectedSize > s.config.TargetFileSize {
		// But don't split if NoSplitGroups is set and we're in the same group
		if s.config.NoSplitGroups && s.config.GroupKeyFunc != nil {
			newGroup := s.config.GroupKeyFunc(row)
			if newGroup == s.currentGroup {
				return false // Keep in same file to preserve group integrity
			}
		}
		return true
	}

	return false
}

// WriteRow writes a single row to the current file, splitting if necessary.
func (s *FileSplitter) WriteRow(ctx context.Context, row map[string]any) error {
	if s.closed {
		return ErrWriterClosed
	}

	// Validate row against schema
	if err := s.validateRow(row); err != nil {
		return fmt.Errorf("%w: %v", ErrSchemaViolation, err)
	}

	// Check if we need to split to a new file
	if s.ShouldSplit(row) {
		if err := s.finishCurrentFile(); err != nil {
			return fmt.Errorf("finish current file: %w", err)
		}
	}

	// Start a new file if we don't have one
	if s.currentWriter == nil {
		if err := s.startNewFile(); err != nil {
			return fmt.Errorf("start new file: %w", err)
		}
	}

	// Write the row
	if err := s.writeRowToCurrentFile(row); err != nil {
		return fmt.Errorf("%w: %v", ErrWriteFailed, err)
	}

	return nil
}

// validateRow checks that the row is valid for our schema.
func (s *FileSplitter) validateRow(row map[string]any) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}

	// Check that all columns in the row are defined in the schema
	for colName := range row {
		if _, exists := s.config.SchemaNodes[colName]; !exists {
			return fmt.Errorf("column %q not in schema", colName)
		}
	}

	return nil
}

// startNewFile creates a new output file and initializes the writer.
func (s *FileSplitter) startNewFile() error {
	// Create the output file
	file, err := os.CreateTemp(s.config.TmpDir, s.config.BaseName+"-*.parquet")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	// Build the schema from seen columns (or all columns if no data written yet)
	var schemaColumns []string
	if len(s.seenColumns) > 0 {
		schemaColumns = make([]string, 0, len(s.seenColumns))
		for col := range s.seenColumns {
			schemaColumns = append(schemaColumns, col)
		}
	} else {
		// First file - include all schema columns
		schemaColumns = make([]string, 0, len(s.config.SchemaNodes))
		for col := range s.config.SchemaNodes {
			schemaColumns = append(schemaColumns, col)
		}
	}
	sort.Strings(schemaColumns)

	// Build parquet schema
	nodes := make(map[string]parquet.Node, len(schemaColumns))
	for _, col := range schemaColumns {
		nodes[col] = s.config.SchemaNodes[col]
	}
	schema := parquet.NewSchema(s.config.BaseName, parquet.Group(nodes))

	// Create parquet writer with optimized settings
	writerConfig, err := parquet.NewWriterConfig(WriterOptions(s.config.TmpDir, schema)...)
	if err != nil {
		file.Close()
		os.Remove(file.Name())
		return fmt.Errorf("create writer config: %w", err)
	}

	writer := parquet.NewGenericWriter[map[string]any](file, writerConfig)

	// Initialize stats accumulator if provider is configured
	var stats StatsAccumulator
	if s.config.StatsProvider != nil {
		stats = s.config.StatsProvider.NewAccumulator()
	}

	// Set up group tracking if needed
	var currentGroup any
	if s.config.GroupKeyFunc != nil && len(s.results) > 0 {
		// We'll set this when we write the first row to this file
		currentGroup = nil
	}

	s.currentFile = file
	s.currentWriter = writer
	s.currentStats = stats
	s.currentSize = 0
	s.currentRows = 0
	s.currentGroup = currentGroup
	s.parquetNodes = nodes

	return nil
}

// writeRowToCurrentFile writes a row to the current file and updates tracking.
func (s *FileSplitter) writeRowToCurrentFile(row map[string]any) error {
	// Filter row to only include columns in this file's schema
	filteredRow := make(map[string]any, len(s.parquetNodes))
	for col := range s.parquetNodes {
		if value, exists := row[col]; exists {
			filteredRow[col] = value
			if value != nil {
				s.seenColumns[col] = true
			}
		}
	}

	// Write to parquet
	if _, err := s.currentWriter.Write([]map[string]any{filteredRow}); err != nil {
		return fmt.Errorf("write to parquet: %w", err)
	}

	// Update stats
	if s.currentStats != nil {
		s.currentStats.Add(filteredRow)
	}

	// Update tracking
	s.currentRows++
	s.currentSize += int64(s.config.BytesPerRecord)

	// Update group tracking
	if s.config.GroupKeyFunc != nil {
		s.currentGroup = s.config.GroupKeyFunc(row)
	}

	return nil
}

// finishCurrentFile closes the current file and adds it to results.
func (s *FileSplitter) finishCurrentFile() error {
	if s.currentWriter == nil {
		return nil // No file to finish
	}

	// Close the parquet writer
	if err := s.currentWriter.Close(); err != nil {
		return fmt.Errorf("close parquet writer: %w", err)
	}
	s.currentWriter = nil

	// Close the file
	if err := s.currentFile.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}

	// Get file size
	info, err := os.Stat(s.currentFile.Name())
	var fileSize int64 = -1
	if err == nil {
		fileSize = info.Size()
	}

	// Collect stats
	var metadata any
	if s.currentStats != nil {
		metadata = s.currentStats.Finalize()
	}

	// Add to results if we wrote any rows
	if s.currentRows > 0 {
		s.results = append(s.results, Result{
			FileName:    s.currentFile.Name(),
			RecordCount: s.currentRows,
			FileSize:    fileSize,
			Metadata:    metadata,
		})
	} else {
		// Remove empty file
		os.Remove(s.currentFile.Name())
	}

	s.currentFile = nil
	s.currentStats = nil
	s.currentSize = 0
	s.currentRows = 0

	return nil
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

	if s.currentWriter != nil {
		s.currentWriter.Close()
		s.currentWriter = nil
	}

	if s.currentFile != nil {
		s.currentFile.Close()
		os.Remove(s.currentFile.Name())
		s.currentFile = nil
	}

	// Clean up any completed result files too
	for _, result := range s.results {
		os.Remove(result.FileName)
	}
	s.results = nil
}
