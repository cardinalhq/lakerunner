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

	"github.com/parquet-go/parquet-go"

	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/schemabuilder"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
)

// FileSplitter manages splitting data into multiple output files based on
// size constraints and grouping requirements.
type FileSplitter struct {
	config       WriterConfig
	currentRows  int64
	currentGroup any

	// Current file being written
	currentFile   *os.File
	currentWriter *parquet.GenericWriter[map[string]any]
	currentStats  StatsAccumulator

	// Dynamic schema management per file
	currentSchema *schemabuilder.SchemaBuilder

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

// WriteBatchRows efficiently writes multiple rows from a pipeline batch.
// This preserves string interning and avoids per-row map conversions.
func (s *FileSplitter) WriteBatchRows(ctx context.Context, batch *pipeline.Batch) error {
	if s.closed {
		return ErrWriterClosed
	}

	if batch == nil {
		return fmt.Errorf("batch cannot be nil")
	}

	// Convert all rows to map[string]any for writing to parquet
	stringRows := make([]map[string]any, 0, batch.Len())

	for i := 0; i < batch.Len(); i++ {
		row := batch.Get(i)
		if row == nil {
			continue
		}

		// Convert pipeline.Row to map[string]any efficiently
		stringRow := make(map[string]any, len(row)+1) // +1 for _cardinalhq.id
		for key, value := range row {
			stringRow[string(key.Value())] = value
		}

		// Add "_cardinalhq.id" column with a base32-encoded flake ID
		stringRow["_cardinalhq.id"] = idgen.NextBase32ID()

		stringRows = append(stringRows, stringRow)
	}

	if len(stringRows) == 0 {
		return nil // No valid rows to write
	}

	// Check if we need to split files based on record count
	projectedRows := s.currentRows + int64(len(stringRows))
	if s.currentWriter != nil && projectedRows > s.config.RecordsPerFile {
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

	// Add all rows to schema builder first
	for _, stringRow := range stringRows {
		if err := s.currentSchema.AddRow(stringRow); err != nil {
			return fmt.Errorf("schema validation failed: %w", err)
		}
	}

	// If this is the first batch for this file, we need to create the parquet writer
	if s.currentWriter == nil {
		if err := s.createParquetWriter(); err != nil {
			return fmt.Errorf("create parquet writer: %w", err)
		}
	}

	// Write all rows to parquet at once
	if _, err := s.currentWriter.Write(stringRows); err != nil {
		return fmt.Errorf("write batch to parquet: %w", err)
	}

	// Update stats and tracking
	for _, stringRow := range stringRows {
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

// startNewFile creates a new output file and initializes the writer.
// The schema will be built dynamically as rows are added.
func (s *FileSplitter) startNewFile() error {
	// Create the output file
	file, err := os.CreateTemp(s.config.TmpDir, s.config.BaseName+"-*.parquet")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	// Initialize a new schema builder for this file
	s.currentSchema = schemabuilder.NewSchemaBuilder()

	// Initialize stats accumulator if provider is configured
	var stats StatsAccumulator
	if s.config.StatsProvider != nil {
		stats = s.config.StatsProvider.NewAccumulator()
	}

	s.currentFile = file
	s.currentWriter = nil
	s.currentStats = stats
	s.currentRows = 0
	// currentGroup will be set when first row is written

	return nil
}

// createParquetWriter creates the parquet writer once we have schema from the first row.
func (s *FileSplitter) createParquetWriter() error {
	// Build the schema from accumulated column information
	nodes := s.currentSchema.Build()
	if len(nodes) == 0 {
		return fmt.Errorf("no columns discovered for schema")
	}

	schema := parquet.NewSchema(s.config.BaseName, parquet.Group(nodes))

	// Create parquet writer with optimized settings
	writerConfig, err := parquet.NewWriterConfig(schemabuilder.WriterOptions(s.config.TmpDir, schema)...)
	if err != nil {
		return fmt.Errorf("create writer config: %w", err)
	}

	s.currentWriter = parquet.NewGenericWriter[map[string]any](s.currentFile, writerConfig)
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
