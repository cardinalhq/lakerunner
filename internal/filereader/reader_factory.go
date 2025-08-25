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
	"fmt"
	"os"
	"strings"

	"github.com/cardinalhq/oteltools/pkg/fingerprinter"
)

// ReaderOptions provides options for creating readers.
type ReaderOptions struct {
	SignalType SignalType
	// Translation options for protobuf logs
	OrgID              string
	Bucket             string
	ObjectID           string
	TrieClusterManager *fingerprinter.TrieClusterManager
	// Aggregation options for metrics
	EnableAggregation   bool  // Enable streaming aggregation
	AggregationPeriodMs int64 // Aggregation period in milliseconds (e.g., 10000 for 10s)
}

// ReaderForFile creates a Reader for the given file based on its extension and signal type.
// This is a convenience function that uses default options.
func ReaderForFile(filename string, signalType SignalType) (Reader, error) {
	return ReaderForFileWithOptions(filename, ReaderOptions{SignalType: signalType})
}

// ReaderForMetricAggregation creates a Reader for metrics with aggregation enabled.
func ReaderForMetricAggregation(filename string, aggregationPeriodMs int64) (Reader, error) {
	opts := ReaderOptions{
		SignalType:          SignalTypeMetrics,
		EnableAggregation:   true,
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
		wrappedReader, err = NewAggregatingReader(wrappedReader, opts.AggregationPeriodMs)
		if err != nil {
			return nil, fmt.Errorf("failed to create aggregating reader: %w", err)
		}
	}

	return wrappedReader, nil
}

// ReaderForFileWithOptions creates a Reader for the given file with the provided options.
// Supported file formats:
//   - .parquet: Creates a ParquetReader (works for all signal types)
//   - .json.gz: Creates a JSONLinesReader with gzip decompression (works for all signal types)
//   - .json: Creates a JSONLinesReader (works for all signal types)
//   - .binpb: Creates a signal-specific proto reader (NewProtoLogsReader, NewProtoMetricsReader, or NewProtoTracesReader)
//   - .binpb.gz: Creates a signal-specific proto reader with gzip decompression
func ReaderForFileWithOptions(filename string, opts ReaderOptions) (Reader, error) {
	// Determine file type from extension
	switch {
	case strings.HasSuffix(filename, ".parquet"):
		return createParquetReader(filename)
	case strings.HasSuffix(filename, ".json.gz"):
		return createJSONGzReader(filename)
	case strings.HasSuffix(filename, ".json"):
		return createJSONReader(filename)
	case strings.HasSuffix(filename, ".binpb.gz"):
		return createProtoBinaryGzReader(filename, opts)
	case strings.HasSuffix(filename, ".binpb"):
		return createProtoBinaryReader(filename, opts)
	default:
		return nil, fmt.Errorf("unsupported file type: %s", filename)
	}
}

// createParquetReader creates a ParquetReader for the given file.
func createParquetReader(filename string) (Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat parquet file: %w", err)
	}

	reader, err := NewParquetReader(file, stat.Size())
	if err != nil {
		file.Close()
		return nil, err
	}

	return reader, nil
}

// createJSONGzReader creates a JSONLinesReader for a gzipped JSON file.
func createJSONGzReader(filename string) (Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open JSON.gz file: %w", err)
	}

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}

	reader, err := NewJSONLinesReader(gzipReader)
	if err != nil {
		gzipReader.Close()
		file.Close()
		return nil, err
	}

	return reader, nil
}

// createJSONReader creates a JSONLinesReader for a plain JSON file.
func createJSONReader(filename string) (Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open JSON file: %w", err)
	}

	reader, err := NewJSONLinesReader(file)
	if err != nil {
		file.Close()
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
		file.Close()
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}

	reader, err := createProtoReaderWithOptions(gzipReader, opts)
	if err != nil {
		gzipReader.Close()
		file.Close()
		return nil, err
	}

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
		file.Close()
		return nil, err
	}

	return reader, nil
}

// createProtoReaderWithOptions creates the appropriate proto reader with optional translation
func createProtoReaderWithOptions(reader interface{ Read([]byte) (int, error) }, opts ReaderOptions) (Reader, error) {
	switch opts.SignalType {
	case SignalTypeLogs:
		protoReader, err := NewProtoLogsReader(reader)
		if err != nil {
			return nil, err
		}
		// Add translation for protobuf logs if options are provided
		if opts.TrieClusterManager != nil && opts.OrgID != "" {
			translator := NewProtoBinLogTranslator(opts.OrgID, opts.Bucket, opts.ObjectID, opts.TrieClusterManager)
			return NewTranslatingReader(protoReader, translator)
		}
		return protoReader, nil
	case SignalTypeMetrics:
		return NewProtoMetricsReader(reader)
	case SignalTypeTraces:
		return NewProtoTracesReader(reader)
	default:
		return nil, fmt.Errorf("unsupported signal type for protobuf: %s", opts.SignalType.String())
	}
}
