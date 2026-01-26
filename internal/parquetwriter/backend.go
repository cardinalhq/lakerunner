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

package parquetwriter

import (
	"context"
	"io"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/pipeline"
)

// ParquetBackend defines the interface for different Parquet writing implementations.
type ParquetBackend interface {
	// WriteBatch writes a batch of rows. Rows may have heterogeneous schemas.
	// The backend must handle schema evolution and null values.
	WriteBatch(ctx context.Context, batch *pipeline.Batch) error

	// Close finalizes the backend, writes the Parquet file to the writer,
	// and returns backend-specific metadata.
	Close(ctx context.Context, writer io.Writer) (*BackendMetadata, error)

	// Abort cleans up any resources without writing output.
	Abort()

	// Name returns the backend implementation name (e.g., "go-parquet", "arrow").
	Name() string
}

// BackendMetadata contains information about the written Parquet file.
type BackendMetadata struct {
	// RowCount is the total number of rows written
	RowCount int64

	// ColumnCount is the number of columns in the final schema
	ColumnCount int

	// Implementation-specific metadata
	Extra map[string]any
}

// BackendType identifies which backend implementation to use.
type BackendType string

const (
	// BackendGoParquet uses the segmentio/parquet-go library
	BackendGoParquet BackendType = "go-parquet"

	// BackendArrow uses Apache Arrow columnar format with streaming writes
	BackendArrow BackendType = "arrow"

	// BackendDuckDB uses DuckDB's Appender for efficient bulk loading and COPY TO for Parquet export
	BackendDuckDB BackendType = "duckdb"

	// DefaultBackend is the default backend type used when not specified
	DefaultBackend = BackendArrow
)

// BackendFactory creates a ParquetBackend based on configuration.
type BackendFactory interface {
	CreateBackend(config BackendConfig) (ParquetBackend, error)
}

// BackendConfig contains configuration for backend creation.
type BackendConfig struct {
	// Type specifies which backend to use
	Type BackendType

	// TmpDir for temporary files (go-parquet backend only)
	TmpDir string

	// ChunkSize for chunking (Arrow backend only)
	ChunkSize int64

	// StringConversionPrefixes for type coercion
	StringConversionPrefixes []string

	// Schema is the required upfront schema from the reader.
	// Must not be nil. All-null columns (HasNonNull=false) are filtered out automatically.
	Schema *filereader.ReaderSchema

	// SortColumns specifies column names to sort by when writing Parquet.
	// Order matters - first column is primary sort key, etc.
	// If empty, no sorting is applied. Only supported by DuckDB backend.
	SortColumns []string
}
