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

package parquetwriter

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

	// Grouping configuration - controls how rows are grouped and whether
	// groups can be split across files
	GroupKeyFunc  func(map[string]any) any
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
	// Used by both Arrow backend (RecordBatch size) and go-parquet backend (CBOR buffer flush).
	// If 0, uses backend-specific defaults (typically 10000).
	ChunkSize int64
}

// Validate checks that the configuration is valid and returns an error if not.
func (c *WriterConfig) Validate() error {
	if c.TmpDir == "" {
		return &ConfigError{Field: "TmpDir", Message: "cannot be empty"}
	}
	if c.NoSplitGroups && c.GroupKeyFunc == nil {
		return &ConfigError{Field: "GroupKeyFunc", Message: "required when NoSplitGroups is true"}
	}
	// No schema validation needed - discovered dynamically
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

// ConfigError represents a configuration validation error.
type ConfigError struct {
	Field   string
	Message string
}

func (e *ConfigError) Error() string {
	return "parquetwriter config: " + e.Field + " " + e.Message
}
