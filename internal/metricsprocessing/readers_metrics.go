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

// createSortingMetricProtoReader creates a sorting protocol buffer reader for metrics files.
// This combines proto parsing, translation, and in-memory sorting in one reader.
func createSortingMetricProtoReader(filename string, orgID, bucket, objectID string) (filereader.Reader, error) {
	switch {
	case strings.HasSuffix(filename, ".binpb.gz"):
		return createSortingMetricProtoBinaryGzReader(filename, orgID, bucket, objectID)
	case strings.HasSuffix(filename, ".binpb"):
		return createSortingMetricProtoBinaryReader(filename, orgID, bucket, objectID)
	default:
		return nil, fmt.Errorf("unsupported metrics file type: %s (only .binpb and .binpb.gz are supported)", filename)
	}
}

// createMetricProtoReaderNoSort creates a non-sorting protocol buffer reader for metrics files.
// Sorting is deferred to the Parquet writer (DuckDB backend) for better efficiency.
func createMetricProtoReaderNoSort(filename string, orgID, bucket, objectID string) (filereader.Reader, error) {
	switch {
	case strings.HasSuffix(filename, ".binpb.gz"):
		return createMetricProtoBinaryGzReaderNoSort(filename, orgID, bucket, objectID)
	case strings.HasSuffix(filename, ".binpb"):
		return createMetricProtoBinaryReaderNoSort(filename, orgID, bucket, objectID)
	default:
		return nil, fmt.Errorf("unsupported metrics file type: %s (only .binpb and .binpb.gz are supported)", filename)
	}
}

// createMetricProtoBinaryReaderNoSort creates a non-sorting metrics proto reader for a protobuf file
func createMetricProtoBinaryReaderNoSort(filename, orgID, bucket, objectID string) (filereader.Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open protobuf file: %w", err)
	}

	opts := filereader.IngestReaderOptions{
		OrgID:     orgID,
		Bucket:    bucket,
		ObjectID:  objectID,
		BatchSize: 1000,
	}

	reader, err := filereader.NewIngestProtoMetricsReader(file, opts)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to create metrics proto reader: %w", err)
	}

	return reader, nil
}

// createMetricProtoBinaryGzReaderNoSort creates a non-sorting metrics proto reader for a gzipped protobuf file
func createMetricProtoBinaryGzReaderNoSort(filename, orgID, bucket, objectID string) (filereader.Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open protobuf.gz file: %w", err)
	}

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}

	opts := filereader.IngestReaderOptions{
		OrgID:     orgID,
		Bucket:    bucket,
		ObjectID:  objectID,
		BatchSize: 1000,
	}

	reader, err := filereader.NewIngestProtoMetricsReader(gzipReader, opts)
	if err != nil {
		_ = gzipReader.Close()
		_ = file.Close()
		return nil, fmt.Errorf("failed to create metrics proto reader: %w", err)
	}

	return reader, nil
}

// createSortingMetricProtoBinaryReader creates a sorting metrics proto reader for a protobuf file
func createSortingMetricProtoBinaryReader(filename, orgID, bucket, objectID string) (filereader.Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open protobuf file: %w", err)
	}

	opts := filereader.SortingReaderOptions{
		OrgID:     orgID,
		Bucket:    bucket,
		ObjectID:  objectID,
		BatchSize: 1000,
	}

	reader, err := filereader.NewSortingIngestProtoMetricsReader(file, opts)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to create sorting metrics proto reader: %w", err)
	}

	return reader, nil
}

// createSortingMetricProtoBinaryGzReader creates a sorting metrics proto reader for a gzipped protobuf file
func createSortingMetricProtoBinaryGzReader(filename, orgID, bucket, objectID string) (filereader.Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open protobuf.gz file: %w", err)
	}

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}

	opts := filereader.SortingReaderOptions{
		OrgID:     orgID,
		Bucket:    bucket,
		ObjectID:  objectID,
		BatchSize: 1000,
	}

	reader, err := filereader.NewSortingIngestProtoMetricsReader(gzipReader, opts)
	if err != nil {
		_ = gzipReader.Close()
		_ = file.Close()
		return nil, fmt.Errorf("failed to create sorting metrics proto reader: %w", err)
	}

	return reader, nil
}

// createMetricProtoReader creates a protocol buffer reader for metrics files
func createMetricProtoReader(filename string, opts filereader.ReaderOptions) (filereader.Reader, error) {
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

	reader, err := filereader.NewSortingIngestProtoMetricsReader(file, filereader.SortingReaderOptions{
		OrgID:     opts.OrgID,
		Bucket:    opts.Bucket,
		ObjectID:  opts.ObjectID,
		BatchSize: opts.BatchSize,
	})
	if err != nil {
		_ = file.Close()
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
		_ = file.Close()
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}

	reader, err := filereader.NewSortingIngestProtoMetricsReader(gzipReader, filereader.SortingReaderOptions{
		OrgID:     opts.OrgID,
		Bucket:    opts.Bucket,
		ObjectID:  opts.ObjectID,
		BatchSize: opts.BatchSize,
	})
	if err != nil {
		_ = gzipReader.Close()
		_ = file.Close()
		return nil, fmt.Errorf("failed to create metrics proto reader: %w", err)
	}

	return reader, nil
}
