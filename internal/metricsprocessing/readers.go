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

package metricsprocessing

import (
	"compress/gzip"
	"fmt"
	"os"
	"strings"

	"github.com/cardinalhq/lakerunner/internal/filereader"
)

// IsSupportedMetricsFile checks if the file is a supported metrics file type
func IsSupportedMetricsFile(objectID string) bool {
	if !strings.HasPrefix(objectID, "otel-raw/") {
		return false
	}

	return strings.HasSuffix(objectID, ".binpb") || strings.HasSuffix(objectID, ".binpb.gz")
}

// CreateMetricProtoReader creates a protocol buffer reader for metrics files
func CreateMetricProtoReader(filename string, opts filereader.ReaderOptions) (filereader.Reader, error) {
	switch {
	case strings.HasSuffix(filename, ".binpb.gz"):
		return createMetricProtoBinaryGzReader(filename, opts)
	case strings.HasSuffix(filename, ".binpb"):
		return createMetricProtoBinaryReader(filename, opts)
	default:
		return nil, fmt.Errorf("unsupported metrics file type: %s (only .binpb and .binpb.gz are supported)", filename)
	}
}

// createMetricProtoBinaryReader creates a metrics proto reader for a protobuf file
func createMetricProtoBinaryReader(filename string, opts filereader.ReaderOptions) (filereader.Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open protobuf file: %w", err)
	}

	reader, err := filereader.NewIngestProtoMetricsReader(file, opts)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create metrics proto reader: %w", err)
	}

	return reader, nil
}

// createMetricProtoBinaryGzReader creates a metrics proto reader for a gzipped protobuf file
func createMetricProtoBinaryGzReader(filename string, opts filereader.ReaderOptions) (filereader.Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open protobuf.gz file: %w", err)
	}

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}

	reader, err := filereader.NewIngestProtoMetricsReader(gzipReader, opts)
	if err != nil {
		gzipReader.Close()
		file.Close()
		return nil, fmt.Errorf("failed to create metrics proto reader: %w", err)
	}

	return reader, nil
}
