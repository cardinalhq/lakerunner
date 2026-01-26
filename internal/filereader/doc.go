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

// Package filereader provides generic file reading for structured data formats
// with composable readers, merge-sort capabilities, and pluggable data transformation.
//
// # Overview
//
// The filereader package provides streaming row-by-row access to various telemetry
// file formats. All readers implement a common interface and can be composed together
// for complex data processing patterns like merge-sort operations across multiple files.
// Data transformation is handled by separate translator components for maximum flexibility.
//
// # Core Interfaces
//
// All file format readers return raw data without transformation:
//
//	type Row map[string]any
//
//	type Batch struct {
//	    Rows []Row
//	}
//
//	type Reader interface {
//	    Next() (*Batch, error)        // Returns next batch or io.EOF when exhausted
//	    Close() error
//	    TotalRowsReturned() int64     // Total rows successfully returned so far
//	}
//
//	type RowTranslator interface {
//	    TranslateRow(row *Row) error  // Transforms row in-place
//	}
//
// Use translators for data processing and TranslatingReader for composition.
//
// # Format Readers
//
// All format readers return raw, untransformed data from files:
//
//   - ParquetRawReader: Generic Parquet files using parquet-go/parquet-go (requires io.ReaderAt)
//   - JSONLinesReader: Streams JSON objects line-by-line from any io.ReadCloser
//   - IngestProtoLogsReader: Raw OTEL log records from protobuf
//   - ProtoTracesReader: Raw OTEL span data from protobuf
//
// Example usage:
//
//	// For compressed JSON, pass in a gzip reader:
//	gzReader, err := gzip.NewReader(file)
//	if err != nil {
//	    return err
//	}
//
//	reader, err := NewJSONLinesReader(gzReader, 1000)
//	if err != nil {
//	    return err
//	}
//	defer reader.Close()
//
//	for {
//	    batch, err := reader.Next(ctx)
//	    if err != nil {
//	        if errors.Is(err, io.EOF) {
//	            break
//	        }
//	        return err
//	    }
//	    if batch != nil {
//	        // process raw row data in batch.Rows
//	        for _, row := range batch.Rows {
//	            // process each row
//	        }
//	    }
//	}
//
// # Data Translation
//
// Use translators to transform raw data:
//
//	// Create a simple translator that adds tags
//	translator := NewTagsTranslator(map[string]string{
//	    "source": "myapp",
//	    "env": "prod",
//	})
//
//	// Wrap any reader with translation
//	translatingReader := NewTranslatingReader(rawReader, translator)
//
//	// Chain multiple translators
//	chain := NewChainTranslator(
//	    NewTagsTranslator(someTags),
//	    customTranslator,  // Implement your own RowTranslator
//	)
//	reader := NewTranslatingReader(rawReader, chain)
//
// Built-in translators:
//
//   - NoopTranslator: Pass-through (no transformation)
//   - TagsTranslator: Adds static tags to rows
//   - ChainTranslator: Applies multiple translators in sequence
//
// Implement custom translators by satisfying the RowTranslator interface.
//
// # Composite Readers
//
// # Sorting Readers
//
// Choose the appropriate sorting reader based on dataset size and memory constraints:
//
// MemorySortingReader - For smaller datasets (high memory usage, no disk I/O):
//
//	reader := NewMemorySortingReader(rawReader, &LogSortKeyProvider{})
//
// DiskSortingReader - For larger datasets (moderate memory usage, 2x disk I/O):
//
//	reader := NewDiskSortingReader(rawReader, &LogSortKeyProvider{})
//
// MergesortReader - For merging multiple already-sorted sources (low memory, streaming):
//
//	keyProvider := NewTimeOrderedSortKeyProvider("timestamp")
//	reader := NewMergesortReader([]Reader{r1, r2, r3}, keyProvider)
//
// SequentialReader - Sequential processing (no sorting):
//
//	reader := NewSequentialReader([]Reader{r1, r2, r3})
//
// # Usage Patterns
//
// Time-ordered merge sort across multiple files:
//
//	readers := []Reader{
//	    NewParquetRawReader(file1, size1),
//	    NewParquetRawReader(file2, size2),
//	    NewJSONLinesReader(file3),
//	}
//
//	keyProvider := NewTimeOrderedSortKeyProvider("chq_timestamp")
//	ordered := NewMergesortReader(readers, keyProvider)
//	defer ordered.Close()
//
//	for {
//	    batch, err := ordered.Next(ctx)
//	    if err != nil {
//	        if errors.Is(err, io.EOF) {
//	            break
//	        }
//	        return err
//	    }
//	    if batch != nil {
//	        // rows arrive in timestamp order across all files
//	        for _, row := range batch.Rows {
//	            // process each row
//	        }
//	    }
//	}
//
// Composable reader trees:
//
//	// Process multiple file groups in timestamp order,
//	// then combine groups sequentially
//	keyProvider := NewTimeOrderedSortKeyProvider("timestamp")
//	group1 := NewMergesortReader(readers1, keyProvider)
//	group2 := NewMergesortReader(readers2, keyProvider)
//	final := NewSequentialReader([]Reader{group1, group2})
//
// # Memory Management & Batch Ownership
//
// The filereader package implements efficient memory management through batch ownership:
//
// **Batch Ownership**: Readers own the returned Batch and its Row maps. Callers must NOT
// retain references to batches beyond the next Next() call.
//
// **Memory Safety**: Use pipeline.CopyBatch() if you need to retain batch data:
//
//	for {
//	    batch, err := reader.Next(ctx)
//	    if err != nil {
//	        if errors.Is(err, io.EOF) {
//	            break
//	        }
//	        return err
//	    }
//	    if batch != nil {
//	        // Use data immediately or copy if retention needed
//	        safeBatch := pipeline.CopyBatch(batch)  // For retention
//	        // Process batch.Rows directly for immediate use
//	    }
//	}
//
// **Data Safety**: Readers maintain clean batch states and handle EOF correctly.
// Batches must not be accessed after the next Next() call.
//
// **Error Handling**: Next() returns nil batch on errors. Check error before accessing batch.
//
// # Resource Management
//
//   - All readers must be closed via Close()
//   - Parquet readers use random access (io.ReaderAt) - no buffering
//   - Streaming readers (JSON, Proto) process incrementally
//   - Composite readers automatically close child readers
package filereader
