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
	"strconv"

	"github.com/cardinalhq/lakerunner/internal/helpers"
)

// DuckDBConfig holds DuckDB-specific configuration
type DuckDBConfig struct {
	// Extension paths for air-gapped mode
	ExtensionsPath  string `mapstructure:"extensions_path"`
	HTTPFSExtension string `mapstructure:"httpfs_extension"`
	AzureExtension  string `mapstructure:"azure_extension"`
	AWSExtension    string `mapstructure:"aws_extension"`

	// Memory and performance settings
	MemoryLimit          int64  `mapstructure:"memory_limit"`            // Memory limit in MB (0 = unlimited)
	TempDirectory        string `mapstructure:"temp_directory"`          // Directory for temporary files
	MaxTempDirectorySize string `mapstructure:"max_temp_directory_size"` // Max size for temp directory
	S3PoolSize           int    `mapstructure:"s3_pool_size"`            // Connection pool size for S3
	S3ConnTTLSeconds     int    `mapstructure:"s3_conn_ttl_seconds"`     // Connection TTL in seconds
	ThreadsPerConn       int    `mapstructure:"threads_per_conn"`        // Threads per connection
}

// DefaultDuckDBConfig returns default DuckDB configuration
func DefaultDuckDBConfig() DuckDBConfig {
	return DuckDBConfig{
		ExtensionsPath:       "",
		HTTPFSExtension:      "",
		AzureExtension:       "",
		AWSExtension:         "",
		MemoryLimit:          0,   // No limit by default
		TempDirectory:        "",  // Empty means use system default
		MaxTempDirectorySize: "",  // Empty means no limit
		S3PoolSize:           0,   // 0 means use default calculation in s3db.go
		S3ConnTTLSeconds:     240, // 4 minutes default
		ThreadsPerConn:       0,   // 0 means use default calculation
	}
}

// GetExtensionsPath returns the configured extensions path, checking both
// config and environment variables with appropriate fallbacks
func (c *DuckDBConfig) GetExtensionsPath() string {
	if c.ExtensionsPath != "" {
		return c.ExtensionsPath
	}
	// Check LAKERUNNER_EXTENSIONS_PATH for compatibility
	if path := os.Getenv("LAKERUNNER_EXTENSIONS_PATH"); path != "" {
		return path
	}
	return ""
}

// GetHTTPFSExtension returns the configured HTTPFS extension path
func (c *DuckDBConfig) GetHTTPFSExtension() string {
	if c.HTTPFSExtension != "" {
		return c.HTTPFSExtension
	}
	// Check LAKERUNNER_HTTPFS_EXTENSION for compatibility
	if path := os.Getenv("LAKERUNNER_HTTPFS_EXTENSION"); path != "" {
		return path
	}
	return ""
}

// GetAzureExtension returns the configured Azure extension path
func (c *DuckDBConfig) GetAzureExtension() string {
	if c.AzureExtension != "" {
		return c.AzureExtension
	}
	// Check LAKERUNNER_AZURE_EXTENSION for compatibility
	if path := os.Getenv("LAKERUNNER_AZURE_EXTENSION"); path != "" {
		return path
	}
	return ""
}

// GetAWSExtension returns the configured AWS extension path
func (c *DuckDBConfig) GetAWSExtension() string {
	if c.AWSExtension != "" {
		return c.AWSExtension
	}
	// Check LAKERUNNER_AWS_EXTENSION for compatibility
	if path := os.Getenv("LAKERUNNER_AWS_EXTENSION"); path != "" {
		return path
	}
	return ""
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
		// Calculate 90% of total volume size
		maxSize := uint64(float64(usage.TotalBytes) * 0.9)
		// DuckDB expects size in format like "100GB" or number of bytes
		// Return as bytes string
		return fmt.Sprintf("%d", maxSize)
	}
	return "" // No limit if we can't determine volume size
}

// GetMemoryLimit returns the memory limit in MB
func (c *DuckDBConfig) GetMemoryLimit() int64 {
	if c.MemoryLimit > 0 {
		return c.MemoryLimit
	}
	// Check environment variable for override
	if memStr := os.Getenv("DUCKDB_MEMORY_LIMIT"); memStr != "" {
		if mem, err := strconv.ParseInt(memStr, 10, 64); err == nil && mem > 0 {
			return mem
		}
	}
	return 0 // No limit
}

// GetS3PoolSize returns the S3 connection pool size
func (c *DuckDBConfig) GetS3PoolSize() int {
	if c.S3PoolSize > 0 {
		return c.S3PoolSize
	}
	// Check environment variable for override
	if poolStr := os.Getenv("DUCKDB_S3_POOL_SIZE"); poolStr != "" {
		if pool, err := strconv.Atoi(poolStr); err == nil && pool > 0 {
			return pool
		}
	}
	return 0 // Use default calculation
}

// GetS3ConnTTLSeconds returns the S3 connection TTL in seconds
func (c *DuckDBConfig) GetS3ConnTTLSeconds() int {
	if c.S3ConnTTLSeconds > 0 {
		return c.S3ConnTTLSeconds
	}
	// Check environment variable for override
	if ttlStr := os.Getenv("DUCKDB_S3_CONN_TTL_SECONDS"); ttlStr != "" {
		if ttl, err := strconv.Atoi(ttlStr); err == nil && ttl > 0 {
			return ttl
		}
	}
	return 240 // Default 4 minutes
}

// GetThreadsPerConn returns the threads per connection setting
func (c *DuckDBConfig) GetThreadsPerConn() int {
	if c.ThreadsPerConn > 0 {
		return c.ThreadsPerConn
	}
	// Check environment variable for override
	if threadsStr := os.Getenv("DUCKDB_THREADS_PER_CONN"); threadsStr != "" {
		if threads, err := strconv.Atoi(threadsStr); err == nil && threads > 0 {
			return threads
		}
	}
	return 0 // Use default calculation
}
