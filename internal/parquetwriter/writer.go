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

// Package parquetwriter provides a unified, modular Parquet writer architecture
// that supports different ordering requirements, file splitting strategies,
// and size estimation for logs, metrics, and traces.
package parquetwriter

import (
	"context"
	"errors"

	"github.com/cardinalhq/lakerunner/pipeline"
)

// ParquetWriter defines the common interface for writing Parquet files
// across all signal types (logs, metrics, traces).
type ParquetWriter interface {
	// WriteBatch adds multiple rows to the writer efficiently. This preserves
	// string interning from pipeline.Row and enables batch-level optimizations.
	WriteBatch(batch *pipeline.Batch) error

	// Close finalizes all processing, writes output files, and returns
	// metadata about the created files. After Close(), the writer cannot
	// be used for further writes.
	Close(ctx context.Context) ([]Result, error)

	// Abort stops processing and cleans up any temporary files.
	// Can be called multiple times safely.
	Abort()

	// Config returns the configuration used by this writer.
	Config() WriterConfig

	// GetCurrentStats returns current statistics about the writer's state.
	GetCurrentStats() WriterStats
}

// Result contains metadata about a single output Parquet file.
type Result struct {
	// FileName is the absolute path to the created Parquet file
	FileName string

	// RecordCount is the number of rows written to this file
	RecordCount int64

	// FileSize is the size of the Parquet file in bytes
	FileSize int64

	// Metadata contains signal-specific information about the file.
	// For metrics: TID count, TID range
	// For logs: timestamp range, fingerprint count
	// For traces: slot ID, span count
	Metadata any
}

// WriterStats contains current statistics about a writer's state.
type WriterStats struct {
	Closed         bool
	RecordsPerFile int64
}

// Common errors returned by writers
var (
	ErrWriterClosed = errors.New("parquetwriter: writer is already closed")
	ErrInvalidRow   = errors.New("parquetwriter: row contains invalid data")
)
