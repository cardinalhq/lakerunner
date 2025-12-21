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
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// DDSketchExtensionEnvVar is the environment variable for the DDSketch extension path.
const DDSketchExtensionEnvVar = "LAKERUNNER_DDSKETCH_EXTENSION"

// DB manages a pool of DuckDB connections to a single shared on-disk database.
// All connections open the same file and thus share the same in-process database instance.
type DB struct {
	dbPath         string // single on-disk database file for all connections
	cleanupOnClose bool   // whether to remove the database directory on Close

	// config (applied once to the shared database instance)
	memoryLimitMB int64
	tempDir       string
	maxTempSize   string
	poolSize      int
	threads       int // total threads for the shared database instance

	// one-time setup
	setupOnce sync.Once
	setupErr  error

	// Single global pool (per DB instance)
	pool *connectionPool

	// metrics
	metricsPeriod time.Duration
	metricsCtx    context.Context
	metricsCancel context.CancelFunc

	// connection lifecycle
	connMaxAge time.Duration // max lifetime for a pooled physical connection
}

type connectionPool struct {
	parent *DB
	size   int

	mu  sync.Mutex
	cur int

	ch chan *pooledConn
}

type pooledConn struct {
	db       *sql.DB
	conn     *sql.Conn
	bornAt   time.Time
	deadline time.Time
}

// dbConfig holds configuration options for DB
type dbConfig struct {
	dbPath        *string
	metricsPeriod time.Duration
	metricsCtx    context.Context
	connMaxAge    time.Duration
}

// DBOption is a functional option for configuring DB
type DBOption func(*dbConfig)

// WithDatabasePath sets the database path for DB.
// The path must not be empty.
func WithDatabasePath(path string) DBOption {
	return func(cfg *dbConfig) {
		if path == "" {
			panic("WithDatabasePath: path must not be empty")
		}
		cfg.dbPath = &path
	}
}

// WithMetrics enables periodic polling of DuckDB memory metrics.
// If period is 0, uses default of 30 seconds.
func WithMetrics(period time.Duration) DBOption {
	return func(cfg *dbConfig) {
		if period == 0 {
			period = 30 * time.Second
		}
		cfg.metricsPeriod = period
	}
}

// WithMetricsContext sets the context used for metrics polling.
// If not set, uses context.Background().
func WithMetricsContext(ctx context.Context) DBOption {
	return func(cfg *dbConfig) {
		cfg.metricsCtx = ctx
	}
}

// WithConnectionMaxAge sets the maximum lifetime for a pooled physical connection.
func WithConnectionMaxAge(d time.Duration) DBOption {
	return func(cfg *dbConfig) {
		if d < time.Minute {
			d = time.Minute
		}
		cfg.connMaxAge = d
	}
}

// NewDB creates a new DB instance with a shared database.
// Database location behavior:
//   - No options provided: creates a temporary file database (persistent across connections)
//   - WithDatabasePath("/path/to/db"): uses specified file database (persistent across connections)
func NewDB(opts ...DBOption) (*DB, error) {
	cfg := &dbConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	var dbPath string
	var cleanupOnClose bool
	if cfg.dbPath != nil {
		dbPath = *cfg.dbPath
		// User provided the path, don't clean it up
		cleanupOnClose = false
	} else {
		// No options provided - create a temp file that we'll clean up
		dbDir, err := os.MkdirTemp("", "")
		if err != nil {
			return nil, fmt.Errorf("create temp dir for DB: %w", err)
		}
		dbPath = filepath.Join(dbDir, "global.ddb")
		cleanupOnClose = true
	}

	memoryMB := envInt64("DUCKDB_MEMORY_LIMIT", 0)

	// Default pool: half the cores, capped at 8, min 2.
	poolDefault := min(8, max(2, runtime.GOMAXPROCS(0)/2))
	poolSize := envIntClamp("DUCKDB_POOL_SIZE", poolDefault, 1, 512)

	total := runtime.GOMAXPROCS(0)
	// All connections share the same database, so threads setting applies to the shared instance
	threads := envIntClamp("DUCKDB_THREADS", total, 1, 256)

	// connection TTL (default 25m; override by env or option)
	connMaxAge := envDuration("DUCKDB_POOL_CONN_MAX_AGE", 25*time.Minute)
	if cfg.connMaxAge > 0 {
		connMaxAge = cfg.connMaxAge
	}

	slog.Info("duckdbx: single shared database",
		"type", "file",
		"dbPath", dbPath,
		"memoryLimitMB", memoryMB,
		"tempDir", os.Getenv("DUCKDB_TEMP_DIRECTORY"),
		"maxTempSize", os.Getenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE"),
		"poolSize", poolSize,
		"threads", threads,
		"connMaxAge", connMaxAge,
	)

	d := &DB{
		dbPath:         dbPath,
		cleanupOnClose: cleanupOnClose,
		memoryLimitMB:  memoryMB,
		tempDir:        os.Getenv("DUCKDB_TEMP_DIRECTORY"),
		maxTempSize:    os.Getenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE"),
		poolSize:       poolSize,
		threads:        threads,
		metricsPeriod:  cfg.metricsPeriod,
		connMaxAge:     connMaxAge,
	}

	d.pool = &connectionPool{
		parent: d,
		size:   poolSize,
		ch:     make(chan *pooledConn, poolSize),
	}

	// Start metrics polling if enabled
	if cfg.metricsPeriod > 0 {
		ctx := cfg.metricsCtx
		if ctx == nil {
			ctx = context.Background()
		}
		d.metricsCtx, d.metricsCancel = context.WithCancel(ctx)
		go d.pollMemoryMetrics(d.metricsCtx)
	}

	return d, nil
}

func (d *DB) Close() error {
	// Cancel metrics polling if running
	if d.metricsCancel != nil {
		d.metricsCancel()
	}

	if d.pool != nil {
		d.pool.closeAll()
	}

	// Only clean up the directory if we created it (temp directories)
	// Never remove user-provided paths
	if d.cleanupOnClose && d.dbPath != "" {
		dbDir := filepath.Dir(d.dbPath)
		_ = os.RemoveAll(dbDir)
	}
	return nil
}

// GetDatabasePath returns the path to the database file.
func (d *DB) GetDatabasePath() string {
	return d.dbPath
}

// GetConnection returns a connection for local database queries.
func (d *DB) GetConnection(ctx context.Context) (*sql.Conn, func(), error) {
	return d.pool.acquire(ctx)
}

// ---------- connectionPool implementation ----------

func (p *connectionPool) acquire(ctx context.Context) (*sql.Conn, func(), error) {
	// Ensure database is set up before any connection is used
	if err := p.parent.ensureSetup(ctx); err != nil {
		return nil, nil, err
	}

	// Fast-path: try to take one without blocking
	select {
	case pc := <-p.ch:
		if p.isConnExpired(pc) {
			p.destroy(pc)
			// fall through to creation path below
		} else {
			return pc.conn, func() { p.release(pc) }, nil
		}
	default:
	}

	// Try to create a new one if capacity allows
	return p.createConn(ctx)
}

func (p *connectionPool) createConn(ctx context.Context) (*sql.Conn, func(), error) {
	p.mu.Lock()
	canCreate := p.cur < p.size
	if canCreate {
		p.cur++
	}
	p.mu.Unlock()

	if canCreate {
		pc, err := p.newConn(ctx)
		if err != nil {
			p.mu.Lock()
			p.cur--
			p.mu.Unlock()
			return nil, nil, err
		}
		return pc.conn, func() { p.release(pc) }, nil
	}

	// Otherwise, wait for an available connection
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case pc := <-p.ch:
		if p.isConnExpired(pc) {
			p.destroy(pc)
			// After destroying, capacity likely exists -> try create again
			return p.createConn(ctx)
		}
		return pc.conn, func() { p.release(pc) }, nil
	}
}

func (p *connectionPool) release(pc *pooledConn) {
	// Drop expired connections instead of pooling them
	if p.isConnExpired(pc) {
		p.destroy(pc)
		return
	}

	select {
	case p.ch <- pc:
		// returned to pool
	default:
		// pool is full (shouldn't happen), close this connection
		p.destroy(pc)
	}
}

func (p *connectionPool) destroy(pc *pooledConn) {
	_ = pc.conn.Close()
	_ = pc.db.Close()
	p.mu.Lock()
	p.cur--
	p.mu.Unlock()
}

func (p *connectionPool) isConnExpired(pc *pooledConn) bool {
	if p.parent.connMaxAge <= 0 {
		return false
	}
	return time.Now().After(pc.deadline)
}

// newConn creates a new pooled connection with standard configuration.
func (p *connectionPool) newConn(ctx context.Context) (*pooledConn, error) {
	// Ensure database-wide settings are applied (once)
	if err := p.parent.ensureSetup(ctx); err != nil {
		return nil, err
	}

	// Pass allow_unsigned_extensions in DSN to enable loading custom extensions like DDSketch
	dsn := p.parent.dbPath + "?allow_unsigned_extensions=true"

	connector, err := duckdb.NewConnector(dsn, nil)
	if err != nil {
		return nil, fmt.Errorf("create connector: %w", err)
	}

	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	now := time.Now()
	pc := &pooledConn{
		conn:     conn,
		db:       db,
		bornAt:   now,
		deadline: now.Add(p.parent.connMaxAge),
	}
	return pc, nil
}

func (p *connectionPool) closeAll() {
	for {
		select {
		case pc := <-p.ch:
			_ = pc.conn.Close()
			_ = pc.db.Close()
		default:
			return
		}
	}
}

// ---------- DB setup ----------

// ensureSetup runs once to configure the shared database instance
func (d *DB) ensureSetup(ctx context.Context) error {
	d.setupOnce.Do(func() {
		db, err := sql.Open("duckdb", d.dbPath)
		if err != nil {
			d.setupErr = fmt.Errorf("open db for setup: %w", err)
			return
		}
		defer func() { _ = db.Close() }()

		conn, err := db.Conn(ctx)
		if err != nil {
			d.setupErr = fmt.Errorf("get conn for setup: %w", err)
			return
		}
		defer func() { _ = conn.Close() }()

		// Set home_directory
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET home_directory='%s';", escapeSingle(filepath.Dir(d.dbPath)))); err != nil {
			slog.Warn("Failed to set home_directory", "error", err)
		}

		if d.memoryLimitMB > 0 {
			slog.Info("Setting memory limit for shared DuckDB instance", "memoryLimitMB", d.memoryLimitMB)
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET memory_limit='%dMB';", d.memoryLimitMB)); err != nil {
				d.setupErr = fmt.Errorf("set memory_limit: %w", err)
				return
			}
		}
		if d.tempDir != "" {
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET temp_directory = '%s';", escapeSingle(d.tempDir))); err != nil {
				d.setupErr = fmt.Errorf("set temp_directory: %w", err)
				return
			}
		}
		if d.maxTempSize != "" {
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET max_temp_directory_size = '%s';", escapeSingle(d.maxTempSize))); err != nil {
				d.setupErr = fmt.Errorf("set max_temp_directory_size: %w", err)
				return
			}
		}

		if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA threads=%d;", d.threads)); err != nil {
			d.setupErr = fmt.Errorf("set threads: %w", err)
			return
		}
		if _, err := conn.ExecContext(ctx, "PRAGMA enable_object_cache;"); err != nil {
			d.setupErr = fmt.Errorf("enable_object_cache: %w", err)
			return
		}
	})
	return d.setupErr
}

// ---------- helpers & metrics ----------

func escapeSingle(s string) string {
	var result []byte
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			result = append(result, '\'', '\'')
		} else {
			result = append(result, s[i])
		}
	}
	return string(result)
}

func envInt64(name string, def int64) int64 {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}
func envIntClamp(name string, def, minv, maxv int) int {
	if v := os.Getenv(name); v != "" {
		if iv, err := strconv.Atoi(v); err == nil {
			if iv < minv {
				return minv
			}
			if iv > maxv {
				return maxv
			}
			return iv
		}
	}
	return def
}
func envDuration(name string, def time.Duration) time.Duration {
	if v := os.Getenv(name); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// pollMemoryMetrics periodically polls DuckDB memory statistics and records them as OpenTelemetry metrics
func (d *DB) pollMemoryMetrics(ctx context.Context) {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/duckdbx")

	dbSizeGauge, err := meter.Int64Gauge("lakerunner.duckdb.memory.database_size",
		metric.WithDescription("DuckDB database size"),
		metric.WithUnit("By"),
	)
	if err != nil {
		slog.Error("failed to create database_size metric", "error", err)
		return
	}

	blockSizeGauge, err := meter.Int64Gauge("lakerunner.duckdb.memory.block_size",
		metric.WithDescription("DuckDB block size"),
		metric.WithUnit("By"),
	)
	if err != nil {
		slog.Error("failed to create block_size metric", "error", err)
		return
	}

	totalBlocksGauge, err := meter.Int64Gauge("lakerunner.duckdb.memory.total_blocks",
		metric.WithDescription("DuckDB total blocks"),
		metric.WithUnit("1"),
	)
	if err != nil {
		slog.Error("failed to create total_blocks metric", "error", err)
		return
	}

	usedBlocksGauge, err := meter.Int64Gauge("lakerunner.duckdb.memory.used_blocks",
		metric.WithDescription("DuckDB used blocks"),
		metric.WithUnit("1"),
	)
	if err != nil {
		slog.Error("failed to create used_blocks metric", "error", err)
		return
	}

	freeBlocksGauge, err := meter.Int64Gauge("lakerunner.duckdb.memory.free_blocks",
		metric.WithDescription("DuckDB free blocks"),
		metric.WithUnit("1"),
	)
	if err != nil {
		slog.Error("failed to create free_blocks metric", "error", err)
		return
	}

	walSizeGauge, err := meter.Int64Gauge("lakerunner.duckdb.memory.wal_size",
		metric.WithDescription("DuckDB WAL size"),
		metric.WithUnit("By"),
	)
	if err != nil {
		slog.Error("failed to create wal_size metric", "error", err)
		return
	}

	memoryUsageGauge, err := meter.Int64Gauge("lakerunner.duckdb.memory.memory_usage",
		metric.WithDescription("DuckDB memory usage"),
		metric.WithUnit("By"),
	)
	if err != nil {
		slog.Error("failed to create memory_usage metric", "error", err)
		return
	}

	memoryLimitGauge, err := meter.Int64Gauge("lakerunner.duckdb.memory.memory_limit",
		metric.WithDescription("DuckDB memory limit"),
		metric.WithUnit("By"),
	)
	if err != nil {
		slog.Error("failed to create memory_limit metric", "error", err)
		return
	}

	for {
		conn, release, err := d.GetConnection(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("failed to get connection for memory metrics", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(d.metricsPeriod):
				continue
			}
		}

		stats, err := GetDuckDBMemoryStats(conn)
		release()

		if err != nil {
			slog.Error("failed to get memory stats", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(d.metricsPeriod):
				continue
			}
		}

		for _, stat := range stats {
			attributes := []attribute.KeyValue{
				attribute.String("database_name", stat.DatabaseName),
				attribute.String("database_type", "duckdb"),
			}
			attr := metric.WithAttributeSet(attribute.NewSet(attributes...))
			dbSizeGauge.Record(ctx, stat.DatabaseSize, attr)
			blockSizeGauge.Record(ctx, stat.BlockSize, attr)
			totalBlocksGauge.Record(ctx, stat.TotalBlocks, attr)
			usedBlocksGauge.Record(ctx, stat.UsedBlocks, attr)
			freeBlocksGauge.Record(ctx, stat.FreeBlocks, attr)
			walSizeGauge.Record(ctx, stat.WALSize, attr)
			memoryUsageGauge.Record(ctx, stat.MemoryUsage, attr)
			memoryLimitGauge.Record(ctx, stat.MemoryLimit, attr)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(d.metricsPeriod):
		}
	}
}

// LoadDDSketchExtension loads the DDSketch extension on the given connection.
// The extension path is read from the LAKERUNNER_DDSKETCH_EXTENSION environment variable.
// Returns an error if the extension is not configured or cannot be loaded.
func LoadDDSketchExtension(ctx context.Context, conn *sql.Conn) error {
	extensionPath := os.Getenv(DDSketchExtensionEnvVar)
	if extensionPath == "" {
		return fmt.Errorf("DDSketch extension path not configured: set %s environment variable", DDSketchExtensionEnvVar)
	}
	return LoadDDSketchExtensionFromPath(ctx, conn, extensionPath)
}

// LoadDDSketchExtensionFromPath loads the DDSketch extension from a specific file path.
// Note: The connection must have allow_unsigned_extensions enabled (done automatically by duckdbx.DB).
func LoadDDSketchExtensionFromPath(ctx context.Context, conn *sql.Conn, extensionPath string) error {
	if _, err := os.Stat(extensionPath); os.IsNotExist(err) {
		return fmt.Errorf("DDSketch extension not found at %s", extensionPath)
	}

	loadQuery := fmt.Sprintf("LOAD '%s'", escapeSingle(extensionPath))
	if _, err := conn.ExecContext(ctx, loadQuery); err != nil {
		return fmt.Errorf("load ddsketch extension: %w", err)
	}

	return nil
}
