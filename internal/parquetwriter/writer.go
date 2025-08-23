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
)

// ParquetWriter defines the common interface for writing Parquet files
// across all signal types (logs, metrics, traces).
type ParquetWriter interface {
	// Write adds a single row to the writer. The row will be processed
	// according to the writer's ordering and grouping configuration.
	Write(row map[string]any) error

	// Close finalizes all processing, writes output files, and returns
	// metadata about the created files. After Close(), the writer cannot
	// be used for further writes.
	Close(ctx context.Context) ([]Result, error)

	// Abort stops processing and cleans up any temporary files.
	// Can be called multiple times safely.
	Abort()
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

// Common errors returned by writers
var (
	ErrWriterClosed    = errors.New("parquetwriter: writer is already closed")
	ErrInvalidRow      = errors.New("parquetwriter: row contains invalid data")
	ErrSchemaViolation = errors.New("parquetwriter: row violates schema")
	ErrWriteFailed     = errors.New("parquetwriter: failed to write data")
)
