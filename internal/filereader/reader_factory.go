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
)

// ReaderOptions provides options for creating readers.
type ReaderOptions struct {
	SignalType SignalType
}

// ReaderForFile creates a Reader for the given file based on its extension and signal type.
// This is a convenience function that uses default options.
func ReaderForFile(filename string, signalType SignalType) (Reader, error) {
	return ReaderForFileWithOptions(filename, ReaderOptions{SignalType: signalType})
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
		return createProtoBinaryGzReader(filename, opts.SignalType)
	case strings.HasSuffix(filename, ".binpb"):
		return createProtoBinaryReader(filename, opts.SignalType)
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
func createProtoBinaryGzReader(filename string, signalType SignalType) (Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open protobuf.gz file: %w", err)
	}

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}

	reader, err := createProtoReader(gzipReader, signalType)
	if err != nil {
		gzipReader.Close()
		file.Close()
		return nil, err
	}

	return reader, nil
}

// createProtoBinaryReader creates a signal-specific proto reader for a protobuf file.
func createProtoBinaryReader(filename string, signalType SignalType) (Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open protobuf file: %w", err)
	}

	reader, err := createProtoReader(file, signalType)
	if err != nil {
		file.Close()
		return nil, err
	}

	return reader, nil
}

// createProtoReader creates the appropriate proto reader based on signal type.
func createProtoReader(reader interface{ Read([]byte) (int, error) }, signalType SignalType) (Reader, error) {
	switch signalType {
	case SignalTypeLogs:
		return NewProtoLogsReader(reader)
	case SignalTypeMetrics:
		return NewProtoMetricsReader(reader)
	case SignalTypeTraces:
		return NewProtoTracesReader(reader)
	default:
		return nil, fmt.Errorf("unsupported signal type for protobuf: %s", signalType.String())
	}
}
