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

package filereader

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cardinalhq/lakerunner/internal/exemplars"
)

type multiReadCloser struct {
	io.Reader
	closers []io.Closer
}

func (m *multiReadCloser) Close() error {
	var firstErr error
	for _, c := range m.closers {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ReaderOptions provides options for creating readers.
type ReaderOptions struct {
	SignalType SignalType
	BatchSize  int // Batch size for readers (default: 1000)
	// Translation options for protobuf logs and metrics
	OrgID    string
	Bucket   string
	ObjectID string
	// Aggregation options for metrics
	EnableAggregation   bool  // Enable streaming aggregation
	AggregationPeriodMs int64 // Aggregation period in milliseconds (e.g., 10000 for 10s)
}

// ReaderForFile creates a Reader for the given file based on its extension and signal type.
// This is a convenience function that uses default options.
func ReaderForFile(filename string, signalType SignalType, orgId string, exemplarProcessor *exemplars.Processor) (Reader, error) {
	options := ReaderOptions{SignalType: signalType, BatchSize: 1000}
	options.OrgID = orgId
	return ReaderForFileWithOptions(filename, options)
}

// ReaderForMetricAggregation creates a Reader for metrics with aggregation enabled.
func ReaderForMetricAggregation(filename, orgId string, aggregationPeriodMs int64) (Reader, error) {
	opts := ReaderOptions{
		SignalType:          SignalTypeMetrics,
		BatchSize:           1000,
		EnableAggregation:   true,
		OrgID:               orgId,
		AggregationPeriodMs: aggregationPeriodMs,
	}
	return ReaderForFileWithOptions(filename, opts)
}

// WrapReaderForAggregation wraps a reader with aggregation if enabled.
func WrapReaderForAggregation(reader Reader, opts ReaderOptions) (Reader, error) {
	if opts.SignalType != SignalTypeMetrics {
		return reader, nil
	}

	wrappedReader := reader

	// Add aggregation if enabled
	if opts.EnableAggregation && opts.AggregationPeriodMs > 0 {
		var err error
		wrappedReader, err = NewAggregatingMetricsReader(wrappedReader, opts.AggregationPeriodMs, opts.BatchSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create aggregating metrics reader: %w", err)
		}
	}

	return wrappedReader, nil
}

// ReaderForFileWithOptions creates a Reader for the given file with the provided options.
// Supported file formats:
//   - .parquet: Creates a ParquetRawReader (works for all signal types)
//   - .json.gz: Creates a JSONLinesReader with gzip decompression (works for all signal types)
//   - .json: Creates a JSONLinesReader (works for all signal types)
//   - .csv: Creates a CSVReader (works for all signal types)
//   - .csv.gz: Creates a CSVReader with gzip decompression (works for all signal types)
//   - .binpb: Creates a signal-specific proto reader (NewIngestProtoLogsReader, NewIngestProtoMetricsReader, or NewProtoTracesReader)
//   - .binpb.gz: Creates a signal-specific proto reader with gzip decompression
func ReaderForFileWithOptions(filename string, opts ReaderOptions) (Reader, error) {
	// Determine file type from extension
	// Ensure default batch size
	if opts.BatchSize <= 0 {
		opts.BatchSize = 1000
	}

	switch {
	case strings.HasSuffix(filename, ".parquet"):
		return createParquetReader(filename, opts)
	case strings.HasSuffix(filename, ".json.gz"):
		return createJSONGzReader(filename, opts)
	case strings.HasSuffix(filename, ".json"):
		return createJSONReader(filename, opts)
	case strings.HasSuffix(filename, ".csv.gz"):
		return createCSVGzReader(filename, opts)
	case strings.HasSuffix(filename, ".csv"):
		return createCSVReader(filename, opts)
	case strings.HasSuffix(filename, ".binpb.gz"):
		return createProtoBinaryGzReader(filename, opts)
	case strings.HasSuffix(filename, ".binpb"):
		return createProtoBinaryReader(filename, opts)
	default:
		return nil, fmt.Errorf("unsupported file type: %s", filename)
	}
}

// createParquetReader creates a ParquetRawReader or ArrowRawReader for the given file.
// Uses Apache Arrow for log signal type to handle NULL-type columns gracefully.
// Uses parquet-go for other signal types to minimize blast radius.
func createParquetReader(filename string, opts ReaderOptions) (Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to stat parquet file: %w", err)
	}

	// Use Arrow reader only for log ingestion to handle NULL-type columns
	if opts.SignalType == SignalTypeLogs {
		ctx := context.Background()
		// os.File already implements parquet.ReaderAtSeeker (io.ReaderAt + io.Seeker)
		reader, err := NewArrowRawReader(ctx, file, opts.BatchSize)
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		return reader, nil
	}

	// Use standard parquet-go reader for metrics and traces to minimize risk
	reader, err := NewParquetRawReader(file, stat.Size(), opts.BatchSize)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return reader, nil
}

// createJSONGzReader creates a JSONLinesReader for a gzipped JSON file.
func createJSONGzReader(filename string, opts ReaderOptions) (Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open JSON.gz file: %w", err)
	}

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}

	rc := &multiReadCloser{
		Reader:  gzipReader,
		closers: []io.Closer{gzipReader, file},
	}

	reader, err := NewJSONLinesReader(rc, opts.BatchSize)
	if err != nil {
		_ = rc.Close()
		return nil, err
	}

	return reader, nil
}

// createJSONReader creates a JSONLinesReader for a plain JSON file.
func createJSONReader(filename string, opts ReaderOptions) (Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open JSON file: %w", err)
	}

	reader, err := NewJSONLinesReader(file, opts.BatchSize)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	return reader, nil
}

// createProtoBinaryGzReader creates a signal-specific proto reader for a gzipped protobuf file.
func createProtoBinaryGzReader(filename string, opts ReaderOptions) (Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open protobuf.gz file: %w", err)
	}

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}

	reader, err := createProtoReaderWithOptions(gzipReader, opts)
	if err != nil {
		_ = gzipReader.Close()
		_ = file.Close()
		return nil, err
	}

	// Close file handles after reader is constructed since NewIngestProtoMetricsReader
	// eagerly reads the entire stream into memory and doesn't need the handles
	_ = gzipReader.Close()
	_ = file.Close()

	return reader, nil
}

// createProtoBinaryReader creates a signal-specific proto reader for a protobuf file.
func createProtoBinaryReader(filename string, opts ReaderOptions) (Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open protobuf file: %w", err)
	}

	reader, err := createProtoReaderWithOptions(file, opts)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	// Close file handle after reader is constructed since NewIngestProtoMetricsReader
	// eagerly reads the entire stream into memory and doesn't need the handle
	_ = file.Close()

	return reader, nil
}

// createCSVReader creates a CSVReader with optional translation for a plain CSV file.
func createCSVReader(filename string, opts ReaderOptions) (Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}

	csvReader, err := NewCSVReader(file, opts.BatchSize)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	// Add translation for logs
	if opts.SignalType == SignalTypeLogs {
		translator := NewCSVLogTranslator(opts)
		translatingReader, err := NewTranslatingReader(csvReader, translator, opts.BatchSize)
		if err != nil {
			_ = csvReader.Close()
			return nil, err
		}
		return translatingReader, nil
	}

	return csvReader, nil
}

// createCSVGzReader creates a CSVReader with optional translation for a gzipped CSV file.
func createCSVGzReader(filename string, opts ReaderOptions) (Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV.gz file: %w", err)
	}

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}

	rc := &multiReadCloser{
		Reader:  gzipReader,
		closers: []io.Closer{gzipReader, file},
	}

	csvReader, err := NewCSVReader(rc, opts.BatchSize)
	if err != nil {
		_ = rc.Close()
		return nil, err
	}

	// Add translation for logs
	if opts.SignalType == SignalTypeLogs {
		translator := NewCSVLogTranslator(opts)
		translatingReader, err := NewTranslatingReader(csvReader, translator, opts.BatchSize)
		if err != nil {
			_ = csvReader.Close()
			return nil, err
		}
		return translatingReader, nil
	}

	return csvReader, nil
}

// createProtoReaderWithOptions creates the appropriate proto reader with optional translation
func createProtoReaderWithOptions(reader io.Reader, opts ReaderOptions) (Reader, error) {
	switch opts.SignalType {
	case SignalTypeLogs:
		protoReader, err := NewIngestProtoLogsReader(reader, opts)
		if err != nil {
			return nil, err
		}
		return protoReader, nil
	case SignalTypeMetrics:
		return NewIngestProtoMetricsReader(reader, opts)
	case SignalTypeTraces:
		return NewIngestProtoTracesReader(reader, opts)
	default:
		return nil, fmt.Errorf("unsupported signal type for protobuf: %s", opts.SignalType.String())
	}
}
