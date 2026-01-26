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
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/pipeline"
)

const (
	// NoRecordLimitPerFile can be used as RecordsPerFile value to disable file splitting
	// and allow unlimited file size (all records go into a single file).
	NoRecordLimitPerFile = -1

	// DefaultChunkSize is the default number of rows to buffer before flushing to Parquet.
	// Larger chunks reduce memory usage and improve performance for Arrow backend.
	DefaultChunkSize = 50000
)

// DefaultStringConversionPrefixes are the default field name prefixes that will have
// their values converted to strings to avoid type conflicts.
var DefaultStringConversionPrefixes = []string{
	"resource_",
	"scope_",
	"attr_",
	"metric_",
}

// WriterConfig contains all configuration options for creating a ParquetWriter.
type WriterConfig struct {
	// TmpDir is the directory where temporary and output files are created
	TmpDir string

	// Schema is the required upfront schema from the reader.
	// Must not be nil. The writer will reject rows with columns not in the schema.
	// All-null columns (HasNonNull=false) are automatically filtered out.
	Schema *filereader.ReaderSchema

	// GroupKeyFunc extracts a grouping key from a row. Used to track which group
	// the current file belongs to. If nil, grouping is disabled.
	//
	// Example for metrics: func(row pipeline.Row) any {
	//     metricName := wkk.NewRowKey("metric_name")
	//     return [2]any{row[metricName], row[wkk.RowKeyCTID]}
	// }
	//
	// The returned key should be comparable (==) for group change detection.
	GroupKeyFunc func(pipeline.Row) any

	// NoSplitGroups, when true, prevents splitting a group across multiple files.
	//
	// IMPORTANT: This does NOT create new files on every group change. Instead:
	//
	// 1. Files split ONLY when RecordsPerFile limit is exceeded AND group changes
	//    between batches (not within a batch).
	//
	// 2. If a group exceeds RecordsPerFile, it stays together in one file.
	//    Example: RecordsPerFile=5, group A has 10 rows → all 10 rows in one file.
	//
	// 3. Splitting happens BETWEEN batches, never within a batch.
	//    Example: Batch 1 has groups [A,A,B,B,C]. All rows go into same file
	//    regardless of RecordsPerFile limit.
	//
	// 4. Split happens when a NEW batch arrives that would exceed RecordsPerFile
	//    AND has a different group key than the current file.
	//
	// Purpose: Keep similar rows (same group key) together for better compression
	// and query performance, while respecting file size limits at group boundaries.
	//
	// Requires: GroupKeyFunc must be set when NoSplitGroups is true.
	NoSplitGroups bool

	// RecordsPerFile is the estimated number of records that fit in a target file.
	// Set to NoRecordLimitPerFile (-1) to disable file splitting (unlimited file size).
	// Set to 0 or positive values to enable splitting at that record count.
	RecordsPerFile int64

	// Optional stats collection
	StatsProvider StatsProvider

	// StringConversionPrefixes is a list of field name prefixes that should have their
	// values converted to strings to avoid type conflicts during schema merging.
	// If nil or empty, uses default prefixes: resource_, scope_, attr_, metric_
	StringConversionPrefixes []string

	// ChunkSize controls the number of rows buffered before flushing to Parquet.
	// Used by both Arrow backend (RecordBatch size) and go-parquet backend (buffer flush).
	// If 0, uses backend-specific defaults.
	ChunkSize int64

	// BackendType specifies which Parquet writing backend to use.
	// If empty, defaults to BackendGoParquet.
	BackendType BackendType

	// SortColumns specifies column names to sort by when writing Parquet.
	// Order matters - first column is primary sort key, etc.
	// If empty, no sorting is applied. Only supported by DuckDB backend.
	SortColumns []string
}

// Validate checks that the configuration is valid and returns an error if not.
func (c *WriterConfig) Validate() error {
	if c.TmpDir == "" {
		return &ConfigError{Field: "TmpDir", Message: "cannot be empty"}
	}
	if c.Schema == nil {
		return &ConfigError{Field: "Schema", Message: "is required and cannot be nil"}
	}
	if c.NoSplitGroups && c.GroupKeyFunc == nil {
		return &ConfigError{Field: "GroupKeyFunc", Message: "required when NoSplitGroups is true"}
	}
	return nil
}

// GetStringConversionPrefixes returns the effective string conversion prefixes,
// using defaults if none were specified.
func (c *WriterConfig) GetStringConversionPrefixes() []string {
	if len(c.StringConversionPrefixes) > 0 {
		return c.StringConversionPrefixes
	}
	return DefaultStringConversionPrefixes
}

// GetChunkSize returns the effective chunk size, using the default if not specified.
func (c *WriterConfig) GetChunkSize() int64 {
	if c.ChunkSize > 0 {
		return c.ChunkSize
	}
	return DefaultChunkSize
}

// GetBackendType returns the effective backend type, using Arrow as default.
func (c *WriterConfig) GetBackendType() BackendType {
	if c.BackendType == "" {
		return BackendArrow
	}
	return c.BackendType
}

// ConfigError represents a configuration validation error.
type ConfigError struct {
	Field   string
	Message string
}

func (e *ConfigError) Error() string {
	return "parquetwriter config: " + e.Field + " " + e.Message
}
