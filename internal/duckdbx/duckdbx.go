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

package duckdbx

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

type option func(*Config)

type Config struct {
	MemoryLimitMB int64
	Extensions    []ExtensionConfig
	Metrics       bool
	MetricsPeriod time.Duration
	SetupFx       SetupFunction

	pollerContext context.Context
}

type ExtensionConfig struct {
	Name string
}

type SetupFunction func(context.Context, *sql.Conn) error

// WithSetupFunction sets a function to be called on each connection
// returned by db.Conn().
func WithSetupFunction(f SetupFunction) option {
	return func(c *Config) {
		c.SetupFx = f
	}
}

// WithMemoryLimitMB sets a memory limit for DuckDB in megabytes.
func WithMemoryLimitMB(limit int64) option {
	return func(c *Config) {
		c.MemoryLimitMB = limit
	}
}

// WithExtension specifies a DuckDB extension to install and load on connection setup.
// In air-gapped mode (when LAKERUNNER_EXTENSIONS_PATH is set), extensions are loaded from
// pre-installed files. In development mode, extensions are downloaded from the network.
func WithExtension(ext string) option {
	return func(c *Config) {
		c.Extensions = append(c.Extensions, ExtensionConfig{
			Name: ext,
		})
	}
}

// WithoutExtension removes an extension from the list of extensions to load.
// This is useful for removing default extensions like httpfs if not needed.
// If the extension is not in the list, this is a no-op.
func WithoutExtension(ext string) option {
	return func(c *Config) {
		for i, existing := range c.Extensions {
			if existing.Name == ext {
				// Remove the extension by slicing around it
				c.Extensions = append(c.Extensions[:i], c.Extensions[i+1:]...)
				return
			}
		}
	}
}

// WithMetrics enables periodic polling of DuckDB memory metrics.
// The period argument specifies how often to poll the metrics.
// The context can be set with WithMetricsContext, which is recommended to allow
// for graceful shutdown of the polling goroutine.
func WithMetrics(period time.Duration) option {
	return func(c *Config) {
		c.Metrics = true
		c.MetricsPeriod = period
	}
}

// WithMetricsContext sets the context used for metrics polling.
// If the context is cancelled, metrics polling will stop.
func WithMetricsContext(ctx context.Context) option {
	return func(c *Config) {
		c.pollerContext = ctx
	}
}

type DB struct {
	db     *sql.DB
	config Config
}

// Open opens a DuckDB database with the given data source name and options.
// this is generally called once, and the returned DB is shared and used to
// create connections. By default, httpfs extension is loaded automatically.
func Open(dataSourceName string, opts ...option) (*DB, error) {
	db, err := sql.Open("duckdb", dataSourceName)
	if err != nil {
		return nil, err
	}

	config := Config{
		MetricsPeriod: 10 * time.Second,
		pollerContext: context.Background(),
		// Default extensions - httpfs is commonly needed for S3 access
		Extensions: []ExtensionConfig{
			{Name: "httpfs"},
		},
	}

	for _, opt := range opts {
		opt(&config)
	}

	d := &DB{db: db, config: config}

	if config.Metrics {
		go d.pollMemoryMetrics(config.pollerContext)
	}

	return d, nil
}

// Conn returns a new connection to the database, with any setup
// (such as setting memory limits and loading extensions) already performed.
func (d *DB) Conn(ctx context.Context) (*sql.Conn, error) {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	if err := d.setupConn(ctx, conn); err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

func (d *DB) setupConn(ctx context.Context, conn *sql.Conn) error {
	// Enable object cache to improve memory efficiency by reusing internal structures
	if _, err := conn.ExecContext(ctx, "PRAGMA enable_object_cache;"); err != nil {
		return fmt.Errorf("failed to enable object cache: %w", err)
	}

	if d.config.MemoryLimitMB > 0 {
		stmt := fmt.Sprintf("SET memory_limit='%dMB';", d.config.MemoryLimitMB)
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("failed to set memory limit: %w", err)
		}
	}
	for _, ext := range d.config.Extensions {
		if err := d.loadExtension(ctx, conn, ext.Name); err != nil {
			return fmt.Errorf("failed to load extension '%s': %w", ext.Name, err)
		}
	}
	return nil
}

func (d *DB) SetMaxOpenConns(n int) {
	d.db.SetMaxOpenConns(n)
}

func (d *DB) SetMaxIdleConns(n int) {
	d.db.SetMaxIdleConns(n)
}

func (d *DB) Close() error {
	return d.db.Close()
}

// loadExtension handles air-gapped extension loading with fallback to network
func (d *DB) loadExtension(ctx context.Context, conn *sql.Conn, extensionName string) error {
	// Check if we're in air-gapped mode (Docker) or development mode (local)
	extensionsBasePath := os.Getenv("LAKERUNNER_EXTENSIONS_PATH")

	if extensionsBasePath != "" {
		// Air-gapped mode: only load pre-installed extensions
		return d.loadAirGappedExtension(ctx, conn, extensionName, extensionsBasePath)
	}

	// Development mode: allow network downloads
	return d.loadNetworkExtension(ctx, conn, extensionName)
}

// loadAirGappedExtension loads extensions from pre-installed files only
func (d *DB) loadAirGappedExtension(ctx context.Context, conn *sql.Conn, extensionName, basePath string) error {
	// First check for specific environment variable for this extension
	specificEnvVar := fmt.Sprintf("LAKERUNNER_%s_EXTENSION", strings.ToUpper(extensionName))
	extensionPath := os.Getenv(specificEnvVar)

	if extensionPath == "" {
		// Fall back to naming convention: basePath/extensionName.duckdb_extension
		extensionPath = filepath.Join(basePath, extensionName+".duckdb_extension")
	}

	// Verify the file exists
	if _, err := os.Stat(extensionPath); os.IsNotExist(err) {
		return fmt.Errorf("extension '%s' not found at %s (air-gapped mode)", extensionName, extensionPath)
	}

	// Load the extension from the specific file path
	stmt := fmt.Sprintf("LOAD '%s';", extensionPath)
	if _, err := conn.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("failed to load extension from %s: %w", extensionPath, err)
	}

	return nil
}

// loadNetworkExtension loads extensions with network access (development mode)
func (d *DB) loadNetworkExtension(ctx context.Context, conn *sql.Conn, extensionName string) error {
	// Try to load the extension first (works for both static and installed extensions)
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("LOAD %s;", extensionName)); err != nil {
		// If load fails, try to install first (for non-static extensions)
		stmt := fmt.Sprintf("INSTALL %s", extensionName)
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("failed to install extension: %w", err)
		}
		// Then try to load again
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("LOAD %s;", extensionName)); err != nil {
			return fmt.Errorf("failed to load extension after install: %w", err)
		}
	}
	return nil
}

// Query executes a SQL query using a new DuckDB connection and returns the result set.
// The caller is responsible for calling rows.Close() after iteration.
func (d *DB) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	conn, err := d.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get duckdb connection: %w", err)
	}

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		closeErr := conn.Close()
		if closeErr != nil {
			return nil, fmt.Errorf("query failed, and closing connection also failed: %v; %v", err, closeErr)
		}
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	return rows, nil
}

func (d *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, *sql.Conn, error) {
	conn, err := d.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get duckdb connection: %w", err)
	}

	result, err := conn.ExecContext(ctx, query, args...)
	if err != nil {
		closeErr := conn.Close()
		if closeErr != nil {
			return nil, nil, fmt.Errorf("exec failed, and closing connection also failed: %v; %v", err, closeErr)
		}
		return nil, nil, fmt.Errorf("exec execution failed: %w", err)
	}

	return result, conn, nil
}

func (d *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, *sql.Conn, error) {
	conn, err := d.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get duckdb connection: %w", err)
	}

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		closeErr := conn.Close()
		if closeErr != nil {
			return nil, nil, fmt.Errorf("query failed, and closing connection also failed: %v; %v", err, closeErr)
		}
		return nil, nil, fmt.Errorf("query execution failed: %w", err)
	}

	return rows, conn, nil
}

func (d *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	conn, err := d.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get duckdb connection: %w", err)
	}

	tx, err := conn.BeginTx(ctx, opts)
	if err != nil {
		closeErr := conn.Close()
		if closeErr != nil {
			return nil, fmt.Errorf("begin transaction failed, and closing connection also failed: %v; %v", err, closeErr)
		}
		return nil, fmt.Errorf("begin transaction execution failed: %w", err)
	}

	return tx, nil
}
