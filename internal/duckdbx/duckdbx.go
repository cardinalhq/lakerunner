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
	Name     string
	LoadPath string
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
// The loadpath can be an empty string to use the default load path, which will
// load from the network if not already installed.
func WithExtension(ext string, loadpath string) option {
	return func(c *Config) {
		c.Extensions = append(c.Extensions, ExtensionConfig{
			Name:     ext,
			LoadPath: loadpath,
		})
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
// create connections.
func Open(dataSourceName string, opts ...option) (*DB, error) {
	db, err := sql.Open("duckdb", dataSourceName)
	if err != nil {
		return nil, err
	}

	config := Config{
		MetricsPeriod: 10 * time.Second,
		pollerContext: context.Background(),
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
	if d.config.MemoryLimitMB > 0 {
		stmt := fmt.Sprintf("SET memory_limit='%dMB';", d.config.MemoryLimitMB)
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("failed to set memory limit: %w", err)
		}
	}
	for _, ext := range d.config.Extensions {
		// Try to load the extension first (works for both static and installed extensions)
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("LOAD %s;", ext.Name)); err != nil {
			// If load fails, try to install first (for non-static extensions)
			stmt := fmt.Sprintf("INSTALL %s", ext.Name)
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("failed to install extension '%s': %w", ext.Name, err)
			}
			// Then try to load again
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("LOAD %s;", ext.Name)); err != nil {
				return fmt.Errorf("failed to load extension '%s': %w", ext.Name, err)
			}
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
