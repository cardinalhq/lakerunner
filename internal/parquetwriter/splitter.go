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
	"encoding/gob"
	"fmt"
	"io"
	"os"

	"github.com/parquet-go/parquet-go"

	gobconfig "github.com/cardinalhq/lakerunner/internal/gob"
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

	// Gob buffering for schema evolution
	gobConfig    *gobconfig.Config
	gobFile      *os.File
	gobEncoder   *gob.Encoder
	currentStats StatsAccumulator

	// Dynamic schema management per file
	currentSchema *schemabuilder.SchemaBuilder

	// Results tracking
	results []Result
	closed  bool
}

// NewFileSplitter creates a new file splitter with the given configuration.
func NewFileSplitter(config WriterConfig) *FileSplitter {
	gobConfig, err := gobconfig.NewConfig()
	if err != nil {
		// This should never happen with our static configuration
		panic(fmt.Sprintf("failed to create gob config: %v", err))
	}

	return &FileSplitter{
		config:    config,
		gobConfig: gobConfig,
		results:   make([]Result, 0),
	}
}

// WriteBatchRows efficiently writes multiple rows from a pipeline batch.
// Rows are buffered to gob files to allow schema evolution across batches.
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
	projectedRows := s.currentRows + int64(actualRowCount)
	if s.gobFile != nil && projectedRows > s.config.RecordsPerFile {
		// Finish current file first
		if err := s.finishCurrentFile(); err != nil {
			return fmt.Errorf("finish current file before split: %w", err)
		}
	}

	// Start a new gob buffer file if we don't have one
	if s.gobFile == nil {
		if err := s.startNewGobFile(); err != nil {
			return fmt.Errorf("start new gob file: %w", err)
		}
	}

	// Process and buffer all rows to gob
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

		stringRow["_cardinalhq.id"] = idgen.NextBase32ID()

		// Add to schema builder for evolution tracking
		if err := s.currentSchema.AddRow(stringRow); err != nil {
			return fmt.Errorf("schema validation failed: %w", err)
		}

		// Encode and write row to gob buffer
		if err := s.gobEncoder.Encode(stringRow); err != nil {
			return fmt.Errorf("encode row to gob: %w", err)
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

// startNewGobFile creates a new gob buffer file for row accumulation.
// The schema will be built dynamically as rows are added.
func (s *FileSplitter) startNewGobFile() error {
	// Create the gob buffer file
	file, err := os.CreateTemp(s.config.TmpDir, "buffer-*.gob")
	if err != nil {
		return fmt.Errorf("create gob temp file: %w", err)
	}

	// Initialize a new schema builder for this file
	s.currentSchema = schemabuilder.NewSchemaBuilder()

	// Initialize stats accumulator if provider is configured
	var stats StatsAccumulator
	if s.config.StatsProvider != nil {
		stats = s.config.StatsProvider.NewAccumulator()
	}

	// Create gob encoder for writing rows
	s.gobFile = file
	s.gobEncoder = s.gobConfig.NewEncoder(file)
	s.currentStats = stats
	s.currentRows = 0
	// currentGroup will be set when first row is written

	return nil
}

// streamGobToParquet streams all buffered gob data to a new parquet file.
// This creates the final parquet file with the evolved schema.
func (s *FileSplitter) streamGobToParquet() (string, error) {
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
	defer parquetFile.Close()

	// Create parquet writer with optimized settings
	writerConfig, err := parquet.NewWriterConfig(schemabuilder.WriterOptions(s.config.TmpDir, schema)...)
	if err != nil {
		return "", fmt.Errorf("create writer config: %w", err)
	}

	parquetWriter := parquet.NewGenericWriter[map[string]any](parquetFile, writerConfig)

	// Close the gob encoder and reopen file for reading
	if s.gobEncoder != nil {
		// Note: We don't close s.gobFile here as we need it for reading
		s.gobEncoder = nil
	}

	// Reopen gob file for reading
	if err := s.gobFile.Close(); err != nil {
		return "", fmt.Errorf("close gob file for writing: %w", err)
	}

	gobFile, err := os.Open(s.gobFile.Name())
	if err != nil {
		return "", fmt.Errorf("reopen gob file for reading: %w", err)
	}
	defer gobFile.Close()

	// Create gob decoder to read back the buffered rows
	gobDecoder := s.gobConfig.NewDecoder(gobFile)

	// Stream all rows from gob to parquet
	for {
		var row map[string]any

		if err := gobDecoder.Decode(&row); err != nil {
			if err == io.EOF {
				break // End of file reached
			}
			return "", fmt.Errorf("decode gob row: %w", err)
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

// finishCurrentFile streams buffered gob data to parquet and adds to results.
func (s *FileSplitter) finishCurrentFile() error {
	if s.gobFile == nil {
		return nil // No file to finish
	}

	// Only create parquet if we have rows
	if s.currentRows == 0 {
		s.cleanupCurrentGobFile()
		return nil
	}

	// Stream gob data to final parquet file
	parquetFileName, err := s.streamGobToParquet()
	if err != nil {
		s.cleanupCurrentGobFile()
		return fmt.Errorf("stream gob to parquet: %w", err)
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

	// Clean up gob file and reset state
	s.cleanupCurrentGobFile()

	return nil
}

// cleanupCurrentGobFile removes the gob buffer file and resets state.
func (s *FileSplitter) cleanupCurrentGobFile() {
	if s.gobFile != nil {
		gobFileName := s.gobFile.Name()
		s.gobFile.Close()
		os.Remove(gobFileName)
		s.gobFile = nil
	}

	s.gobEncoder = nil
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

	// Clean up current gob buffer file
	s.cleanupCurrentGobFile()

	// Clean up any completed result files too
	for _, result := range s.results {
		os.Remove(result.FileName)
	}
	s.results = nil
}
