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

package config

import (
	"fmt"
	"os"

	"github.com/cardinalhq/lakerunner/internal/helpers"
)

// DuckDBConfig holds DuckDB-specific configuration
type DuckDBConfig struct {
	// Memory and performance settings
	MemoryLimit          int64  `mapstructure:"memory_limit"`            // Memory limit in MB (0 = unlimited)
	TempDirectory        string `mapstructure:"temp_directory"`          // Directory for temporary files
	MaxTempDirectorySize string `mapstructure:"max_temp_directory_size"` // Max size for temp directory
	PoolSize             int    `mapstructure:"pool_size"`               // Connection pool size
	ConnTTLSeconds       int    `mapstructure:"conn_ttl_seconds"`        // Connection TTL in seconds
	ThreadsPerConn       int    `mapstructure:"threads_per_conn"`        // Threads per connection
}

// DefaultDuckDBConfig returns default DuckDB configuration
func DefaultDuckDBConfig() DuckDBConfig {
	return DuckDBConfig{
		MemoryLimit:          0,   // No limit by default
		TempDirectory:        "",  // Empty means use system default
		MaxTempDirectorySize: "",  // Empty means no limit
		PoolSize:             0,   // 0 means use default calculation in db.go
		ConnTTLSeconds:       240, // 4 minutes default
		ThreadsPerConn:       0,   // 0 means use default calculation
	}
}

// GetTempDirectory returns the configured temp directory
// Defaults to TMPDIR environment variable if not configured
func (c *DuckDBConfig) GetTempDirectory() string {
	if c.TempDirectory != "" {
		return c.TempDirectory
	}
	// Default to TMPDIR or /tmp
	if tmpdir := os.Getenv("TMPDIR"); tmpdir != "" {
		return tmpdir
	}
	return "/tmp"
}

// GetMaxTempDirectorySize returns the configured max temp directory size
// Defaults to 90% of the temp directory's volume size if not configured
func (c *DuckDBConfig) GetMaxTempDirectorySize() string {
	if c.MaxTempDirectorySize != "" {
		return c.MaxTempDirectorySize
	}
	// Default to 90% of temp directory's volume
	tempDir := c.GetTempDirectory()
	if usage, err := helpers.DiskUsage(tempDir); err == nil {
		// Calculate 90% of total volume size in GB (DuckDB DSN needs formatted value)
		maxSizeGB := uint64(float64(usage.TotalBytes) * 0.9 / (1024 * 1024 * 1024))
		if maxSizeGB > 0 {
			return fmt.Sprintf("%dGB", maxSizeGB)
		}
	}
	return "" // No limit if we can't determine volume size
}

// GetMemoryLimit returns the memory limit in MB (0 = unlimited)
func (c *DuckDBConfig) GetMemoryLimit() int64 {
	return c.MemoryLimit
}

// GetPoolSize returns the connection pool size (0 = use default)
func (c *DuckDBConfig) GetPoolSize() int {
	return c.PoolSize
}

// GetConnTTLSeconds returns the connection TTL in seconds
func (c *DuckDBConfig) GetConnTTLSeconds() int {
	if c.ConnTTLSeconds > 0 {
		return c.ConnTTLSeconds
	}
	return 240 // Default 4 minutes
}

// GetThreadsPerConn returns the threads per connection setting (0 = use default)
func (c *DuckDBConfig) GetThreadsPerConn() int {
	return c.ThreadsPerConn
}

// GetThreads returns the total threads setting (0 = use default)
func (c *DuckDBConfig) GetThreads() int {
	return c.ThreadsPerConn
}
