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

// WriterConfig contains all configuration options for creating a ParquetWriter.
type WriterConfig struct {
	// TmpDir is the directory where temporary and output files are created
	TmpDir string

	// Grouping configuration - controls how rows are grouped and whether
	// groups can be split across files
	GroupKeyFunc  func(map[string]any) any
	NoSplitGroups bool

	// RecordsPerFile is the estimated number of records that fit in a target file
	RecordsPerFile int64

	// Optional stats collection
	StatsProvider StatsProvider
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

// ConfigError represents a configuration validation error.
type ConfigError struct {
	Field   string
	Message string
}

func (e *ConfigError) Error() string {
	return "parquetwriter config: " + e.Field + " " + e.Message
}
