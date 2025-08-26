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
//	type Reader interface {
//	    Read(rows []Row) (n int, err error)  // Returns row count and io.EOF when exhausted
//	    Close() error
//	}
//
//	type RowTranslator interface {
//	    TranslateRow(in Row) (Row, error)
//	}
//
// Use translators for data processing and TranslatingReader for composition.
//
// # Format Readers
//
// All format readers return raw, untransformed data from files:
//
//   - PreorderedParquetRawReader: Generic Parquet files using parquet-go/parquet-go (requires io.ReaderAt)
//   - JSONLinesReader: Streams JSON objects line-by-line from any io.ReadCloser
//   - ProtoLogsReader: Raw OTEL log records from protobuf
//   - IngestProtoMetricsReader: Raw OTEL metric data points from protobuf (ingestion only)
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
//	reader, err := NewJSONLinesReader(gzReader)
//	if err != nil {
//	    return err
//	}
//	defer reader.Close()
//
//	for {
//	    rows := make([]Row, 1)
//	    rows[0] = make(Row)
//	    n, err := reader.Read(rows)
//	    if errors.Is(err, io.EOF) {
//	        break
//	    }
//	    if err != nil {
//	        return err
//	    }
//	    if n > 0 {
//	        // process raw row data in rows[0]
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
//	reader := NewMemorySortingReader(rawReader, MetricNameTidTimestampSort())
//
// DiskSortingReader - For larger datasets (moderate memory usage, 2x disk I/O):
//
//	reader := NewDiskSortingReader(rawReader, TimestampSortKeyFunc(), TimestampSortFunc())
//
// PreorderedMultisourceReader - For merging multiple already-sorted sources (low memory, streaming):
//
//	selector := TimeOrderedSelector("timestamp")
//	reader := NewPreorderedMultisourceReader([]Reader{r1, r2, r3}, selector)
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
//	    NewPreorderedParquetRawReader(file1, size1),
//	    NewPreorderedParquetRawReader(file2, size2),
//	    NewJSONLinesReader(file3),
//	}
//
//	selector := TimeOrderedSelector("_cardinalhq.timestamp")
//	ordered := NewPreorderedMultisourceReader(readers, selector)
//	defer ordered.Close()
//
//	for {
//	    rows := make([]Row, 1)
//	    rows[0] = make(Row)
//	    n, err := ordered.Read(rows)
//	    if errors.Is(err, io.EOF) {
//	        break
//	    }
//	    if err != nil {
//	        return err
//	    }
//	    if n > 0 {
//	        // rows arrive in timestamp order across all files
//	    }
//	}
//
// Composable reader trees:
//
//	// Process multiple file groups in timestamp order,
//	// then combine groups sequentially
//	group1 := NewPreorderedMultisourceReader(readers1, TimeOrderedSelector("timestamp"))
//	group2 := NewPreorderedMultisourceReader(readers2, TimeOrderedSelector("timestamp"))
//	final := NewSequentialReader([]Reader{group1, group2})
//
// # Resource Management
//
//   - All readers must be closed via Close()
//   - Parquet readers use random access (io.ReaderAt) - no buffering
//   - Streaming readers (JSON, Proto) process incrementally
//   - Composite readers automatically close child readers
package filereader
