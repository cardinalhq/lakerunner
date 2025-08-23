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

import (
	"github.com/parquet-go/parquet-go"
	
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/spillers"
)

// WriterConfig contains all configuration options for creating a ParquetWriter.
type WriterConfig struct {
	// BaseName is used as the base name for output files and schema metadata
	BaseName string

	// TmpDir is the directory where temporary and output files are created
	TmpDir string

	// SchemaNodes defines the Parquet schema for the output files
	SchemaNodes map[string]parquet.Node

	// TargetFileSize is the target size in bytes for each output file.
	// The writer will try to keep files close to this size, but may exceed
	// it to maintain data integrity (e.g., not splitting TID groups).
	TargetFileSize int64

	// Ordering configuration
	OrderBy      OrderingStrategy
	OrderKeyFunc func(map[string]any) any

	// Grouping configuration - controls how rows are grouped and whether
	// groups can be split across files
	GroupKeyFunc  func(map[string]any) any
	NoSplitGroups bool

	// Size estimation for intelligent file splitting
	SizeEstimator SizeEstimator

	// Optional stats collection
	StatsProvider StatsProvider

	// SpillBufferSize sets the maximum number of rows to keep in memory before spilling
	// Only used with OrderSpillable. Default: 50000
	SpillBufferSize int

	// Spiller provides the mechanism for writing/reading spill files
	// Only used with OrderSpillable. Default: GobSpiller
	Spiller spillers.Spiller
}

// OrderingStrategy defines how rows should be ordered in the output files.
type OrderingStrategy int

const (
	// OrderNone means no ordering is required - rows are written in the order received
	OrderNone OrderingStrategy = iota

	// OrderInMemory buffers rows in memory and sorts them before writing.
	// Best for smaller datasets that fit comfortably in memory.
	OrderInMemory

	// OrderMergeSort uses external merge sort for large datasets that don't fit in memory.
	// Writes sorted chunks to disk and merges them during finalization.
	OrderMergeSort

	// OrderSpillable automatically spills to disk when memory limits are exceeded.
	// Combines in-memory sorting with external merge sort as needed.
	OrderSpillable

	// OrderPresumed means the input is already in the correct order.
	// The writer will trust the order and write rows directly.
	OrderPresumed
)

// Validate checks that the configuration is valid and returns an error if not.
func (c *WriterConfig) Validate() error {
	if c.BaseName == "" {
		return &ConfigError{Field: "BaseName", Message: "cannot be empty"}
	}
	if c.TmpDir == "" {
		return &ConfigError{Field: "TmpDir", Message: "cannot be empty"}
	}
	if len(c.SchemaNodes) == 0 {
		return &ConfigError{Field: "SchemaNodes", Message: "cannot be empty"}
	}
	if c.TargetFileSize <= 0 {
		return &ConfigError{Field: "TargetFileSize", Message: "must be positive"}
	}
	if c.OrderBy != OrderNone && c.OrderBy != OrderPresumed && c.OrderKeyFunc == nil {
		return &ConfigError{Field: "OrderKeyFunc", Message: "required when OrderBy is not OrderNone or OrderPresumed"}
	}
	if c.NoSplitGroups && c.GroupKeyFunc == nil {
		return &ConfigError{Field: "GroupKeyFunc", Message: "required when NoSplitGroups is true"}
	}
	for name, node := range c.SchemaNodes {
		if name == "" {
			return &ConfigError{Field: "SchemaNodes", Message: "node name cannot be empty"}
		}
		if node == nil {
			return &ConfigError{Field: "SchemaNodes", Message: "node cannot be nil for field " + name}
		}
	}
	return nil
}

// ConfigError represents a configuration validation error.
type ConfigError struct {
	Field   string
	Message string
}

func (e *ConfigError) Error() string {
	return "parquetwriter config: " + e.Field + " " + e.Message
}
