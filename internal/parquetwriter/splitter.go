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

	"github.com/fxamacker/cbor/v2"
	"github.com/parquet-go/parquet-go"

	cborconfig "github.com/cardinalhq/lakerunner/internal/cbor"
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

	// CBOR buffering for schema evolution
	cborConfig   *cborconfig.Config
	cborFile     *os.File
	cborEncoder  *cbor.Encoder
	currentStats StatsAccumulator

	// Dynamic schema management per file
	currentSchema *schemabuilder.SchemaBuilder

	// Results tracking
	results []Result
	closed  bool
}

// NewFileSplitter creates a new file splitter with the given configuration.
func NewFileSplitter(config WriterConfig) *FileSplitter {
	cborConfig, err := cborconfig.NewConfig()
	if err != nil {
		// This should never happen with our static configuration
		panic(fmt.Sprintf("failed to create CBOR config: %v", err))
	}

	return &FileSplitter{
		config:     config,
		cborConfig: cborConfig,
		results:    make([]Result, 0),
	}
}

// WriteBatchRows efficiently writes multiple rows from a pipeline batch.
// Rows are buffered to CBOR files to allow schema evolution across batches.
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
	if s.cborFile != nil && projectedRows > s.config.RecordsPerFile {
		// Finish current file first
		if err := s.finishCurrentFile(); err != nil {
			return fmt.Errorf("finish current file before split: %w", err)
		}
	}

	// Start a new CBOR buffer file if we don't have one
	if s.cborFile == nil {
		if err := s.startNewCBORFile(); err != nil {
			return fmt.Errorf("start new CBOR file: %w", err)
		}
	}

	// Process and buffer all rows to CBOR
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

		// Encode and write row to CBOR buffer
		if err := s.cborEncoder.Encode(stringRow); err != nil {
			return fmt.Errorf("encode row to CBOR: %w", err)
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

// startNewCBORFile creates a new CBOR buffer file for row accumulation.
// The schema will be built dynamically as rows are added.
func (s *FileSplitter) startNewCBORFile() error {
	// Create the CBOR buffer file
	file, err := os.CreateTemp(s.config.TmpDir, "buffer-*.cbor")
	if err != nil {
		return fmt.Errorf("create CBOR temp file: %w", err)
	}

	// Initialize a new schema builder for this file
	s.currentSchema = schemabuilder.NewSchemaBuilder()

	// Initialize stats accumulator if provider is configured
	var stats StatsAccumulator
	if s.config.StatsProvider != nil {
		stats = s.config.StatsProvider.NewAccumulator()
	}

	// Create CBOR encoder for writing rows
	s.cborFile = file
	s.cborEncoder = s.cborConfig.NewEncoder(file)
	s.currentStats = stats
	s.currentRows = 0
	// currentGroup will be set when first row is written

	return nil
}

// streamCBORToParquet streams all buffered CBOR data to a new parquet file.
// This creates the final parquet file with the evolved schema.
func (s *FileSplitter) streamCBORToParquet() (string, error) {
	// Build the final schema from all accumulated rows
	nodes := s.currentSchema.Build()
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

	// Close the CBOR encoder and reopen file for reading
	if s.cborEncoder != nil {
		// Note: We don't close s.cborFile here as we need it for reading
		s.cborEncoder = nil
	}

	// Reopen CBOR file for reading
	if err := s.cborFile.Close(); err != nil {
		return "", fmt.Errorf("close CBOR file for writing: %w", err)
	}

	cborFile, err := os.Open(s.cborFile.Name())
	if err != nil {
		return "", fmt.Errorf("reopen CBOR file for reading: %w", err)
	}
	defer cborFile.Close()

	// Create CBOR decoder to read back the buffered rows
	cborDecoder := s.cborConfig.NewDecoder(cborFile)

	// Stream all rows from CBOR to parquet
	for {
		var row map[string]any
		if err := cborDecoder.Decode(&row); err != nil {
			if err == io.EOF {
				break // End of file reached
			}
			return "", fmt.Errorf("decode CBOR row: %w", err)
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

// finishCurrentFile streams buffered CBOR data to parquet and adds to results.
func (s *FileSplitter) finishCurrentFile() error {
	if s.cborFile == nil {
		return nil // No file to finish
	}

	// Only create parquet if we have rows
	if s.currentRows == 0 {
		s.cleanupCurrentCBORFile()
		return nil
	}

	// Stream CBOR data to final parquet file
	parquetFileName, err := s.streamCBORToParquet()
	if err != nil {
		s.cleanupCurrentCBORFile()
		return fmt.Errorf("stream CBOR to parquet: %w", err)
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

	// Clean up CBOR file and reset state
	s.cleanupCurrentCBORFile()

	return nil
}

// cleanupCurrentCBORFile removes the CBOR buffer file and resets state.
func (s *FileSplitter) cleanupCurrentCBORFile() {
	if s.cborFile != nil {
		cborFileName := s.cborFile.Name()
		s.cborFile.Close()
		os.Remove(cborFileName)
		s.cborFile = nil
	}

	s.cborEncoder = nil
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

	// Clean up current CBOR buffer file
	s.cleanupCurrentCBORFile()

	// Clean up any completed result files too
	for _, result := range s.results {
		os.Remove(result.FileName)
	}
	s.results = nil
}
