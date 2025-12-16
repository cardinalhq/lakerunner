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
	"database/sql/driver"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marcboeker/go-duckdb/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Global mutex to serialize extension loading across the process.
// DuckDB extension loading may crash when done concurrently in many engines.
var duckdbDDLMu sync.Mutex

// S3DB manages a pool of DuckDB connections to a single shared on-disk database.
// All connections open the same file and thus share the same in-process database instance.
type S3DB struct {
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

	// Single global pool (per S3DB instance)
	pool *connectionPool

	// metrics
	metricsPeriod time.Duration
	metricsCtx    context.Context
	metricsCancel context.CancelFunc

	// connection lifecycle
	connMaxAge time.Duration // max lifetime for a pooled physical connection
}

type connectionPool struct {
	parent *S3DB
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

// s3DBConfig holds configuration options for S3DB
type s3DBConfig struct {
	dbPath        *string
	metricsPeriod time.Duration
	metricsCtx    context.Context
	connMaxAge    time.Duration
}

// S3DBOption is a functional option for configuring S3DB
type S3DBOption func(*s3DBConfig)

// WithDatabasePath sets the database path for S3DB.
// The path must not be empty.
func WithDatabasePath(path string) S3DBOption {
	return func(cfg *s3DBConfig) {
		if path == "" {
			panic("WithDatabasePath: path must not be empty")
		}
		cfg.dbPath = &path
	}
}

// WithS3DBMetrics enables periodic polling of DuckDB memory metrics.
// If period is 0, uses default of 30 seconds.
func WithS3DBMetrics(period time.Duration) S3DBOption {
	return func(cfg *s3DBConfig) {
		if period == 0 {
			period = 30 * time.Second
		}
		cfg.metricsPeriod = period
	}
}

// WithS3DBMetricsContext sets the context used for metrics polling.
// If not set, uses context.Background().
func WithS3DBMetricsContext(ctx context.Context) S3DBOption {
	return func(cfg *s3DBConfig) {
		cfg.metricsCtx = ctx
	}
}

// WithConnectionMaxAge sets the maximum lifetime for a pooled physical connection.
func WithConnectionMaxAge(d time.Duration) S3DBOption {
	return func(cfg *s3DBConfig) {
		if d < time.Minute {
			d = time.Minute
		}
		cfg.connMaxAge = d
	}
}

// NewS3DB creates a new S3DB instance with a shared database.
// Database location behavior:
//   - No options provided: creates a temporary file database (persistent across connections)
//   - WithDatabasePath("/path/to/db"): uses specified file database (persistent across connections)
func NewS3DB(opts ...S3DBOption) (*S3DB, error) {
	cfg := &s3DBConfig{}
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
			return nil, fmt.Errorf("create temp dir for S3DB: %w", err)
		}
		dbPath = filepath.Join(dbDir, "global.ddb")
		cleanupOnClose = true
	}

	memoryMB := envInt64("DUCKDB_MEMORY_LIMIT", 0)

	// Default pool: half the cores, capped at 8, min 2.
	poolDefault := min(8, max(2, runtime.GOMAXPROCS(0)/2))
	poolSize := envIntClamp("DUCKDB_S3_POOL_SIZE", poolDefault, 1, 512)

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

	s3db := &S3DB{
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

	s3db.pool = &connectionPool{
		parent: s3db,
		size:   poolSize,
		ch:     make(chan *pooledConn, poolSize),
	}

	// Start metrics polling if enabled
	if cfg.metricsPeriod > 0 {
		ctx := cfg.metricsCtx
		if ctx == nil {
			ctx = context.Background()
		}
		s3db.metricsCtx, s3db.metricsCancel = context.WithCancel(ctx)
		go s3db.pollMemoryMetrics(s3db.metricsCtx)
	}

	return s3db, nil
}

func (s *S3DB) Close() error {
	// Cancel metrics polling if running
	if s.metricsCancel != nil {
		s.metricsCancel()
	}

	if s.pool != nil {
		s.pool.closeAll()
	}

	// Only clean up the directory if we created it (temp directories)
	// Never remove user-provided paths
	if s.cleanupOnClose && s.dbPath != "" {
		dbDir := filepath.Dir(s.dbPath)
		_ = os.RemoveAll(dbDir)
	}
	return nil
}

// GetDatabasePath returns the path to the database file.
func (s *S3DB) GetDatabasePath() string {
	return s.dbPath
}

// GetConnection returns a connection for local database queries.
func (s *S3DB) GetConnection(ctx context.Context) (*sql.Conn, func(), error) {
	return s.pool.acquire(ctx)
}

// ---------- connectionPool implementation ----------

func (p *connectionPool) acquire(ctx context.Context) (*sql.Conn, func(), error) {
	// Ensure extensions are loaded and database is set up before any connection is used
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
	// Ensure extensions are installed and database-wide settings are applied (once)
	if err := p.parent.ensureSetup(ctx); err != nil {
		return nil, err
	}

	connector, err := duckdb.NewConnector(p.parent.dbPath, func(execer driver.ExecerContext) error {
		// FIRST: Disable automatic extension loading/downloading before anything else
		if _, err := execer.ExecContext(ctx, "SET autoinstall_known_extensions = false;", nil); err != nil {
			slog.Warn("Failed to disable automatic extension installation", "error", err)
		}
		if _, err := execer.ExecContext(ctx, "SET autoload_known_extensions = false;", nil); err != nil {
			slog.Warn("Failed to disable automatic extension loading", "error", err)
		}

		// CRITICAL: Set memory limit on EVERY connection
		if p.parent.memoryLimitMB > 0 {
			if _, err := execer.ExecContext(ctx, fmt.Sprintf("SET memory_limit='%dMB';", p.parent.memoryLimitMB), nil); err != nil {
				slog.Warn("Failed to set memory_limit on connection", "error", err)
			}
		}

		// Load extensions for this connection
		if err := p.parent.loadExtensionsWithExecer(ctx, execer); err != nil {
			return fmt.Errorf("load extensions for connection: %w", err)
		}

		return nil
	})
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

// ---------- S3DB setup & extensions ----------

// ensureSetup runs once to configure the shared database instance and load extensions
func (s *S3DB) ensureSetup(ctx context.Context) error {
	s.setupOnce.Do(func() {
		db, err := sql.Open("duckdb", s.dbPath)
		if err != nil {
			s.setupErr = fmt.Errorf("open db for setup: %w", err)
			return
		}
		defer func() { _ = db.Close() }()

		conn, err := db.Conn(ctx)
		if err != nil {
			s.setupErr = fmt.Errorf("get conn for setup: %w", err)
			return
		}
		defer func() { _ = conn.Close() }()

		// Disable automatic extension loading/downloading
		if _, err := conn.ExecContext(ctx, "SET autoinstall_known_extensions = false;"); err != nil {
			slog.Warn("Failed to disable automatic extension installation", "error", err)
		}
		if _, err := conn.ExecContext(ctx, "SET autoload_known_extensions = false;"); err != nil {
			slog.Warn("Failed to disable automatic extension loading", "error", err)
		}

		// Set extension_directory
		extensionDir := os.Getenv("LAKERUNNER_EXTENSIONS_PATH")
		if extensionDir == "" {
			extensionDir = filepath.Join(filepath.Dir(s.dbPath), "extensions")
		}
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET extension_directory='%s';", escapeSingle(extensionDir))); err != nil {
			slog.Warn("Failed to set extension_directory", "error", err)
		}

		// Set home_directory
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET home_directory='%s';", escapeSingle(filepath.Dir(s.dbPath)))); err != nil {
			slog.Warn("Failed to set home_directory", "error", err)
		}

		if s.memoryLimitMB > 0 {
			slog.Info("Setting memory limit for shared DuckDB instance", "memoryLimitMB", s.memoryLimitMB)
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET memory_limit='%dMB';", s.memoryLimitMB)); err != nil {
				s.setupErr = fmt.Errorf("set memory_limit: %w", err)
				return
			}
		}
		if s.tempDir != "" {
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET temp_directory = '%s';", escapeSingle(s.tempDir))); err != nil {
				s.setupErr = fmt.Errorf("set temp_directory: %w", err)
				return
			}
		}
		if s.maxTempSize != "" {
			if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET max_temp_directory_size = '%s';", escapeSingle(s.maxTempSize))); err != nil {
				s.setupErr = fmt.Errorf("set max_temp_directory_size: %w", err)
				return
			}
		}

		if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA threads=%d;", s.threads)); err != nil {
			s.setupErr = fmt.Errorf("set threads: %w", err)
			return
		}
		if _, err := conn.ExecContext(ctx, "PRAGMA enable_object_cache;"); err != nil {
			s.setupErr = fmt.Errorf("enable_object_cache: %w", err)
			return
		}

		// LOAD extensions from local files (serialize LOAD across engines)
		duckdbDDLMu.Lock()
		err = s.loadExtensions(ctx, conn)
		duckdbDDLMu.Unlock()
		if err != nil {
			s.setupErr = err
			return
		}
	})
	return s.setupErr
}

func (s *S3DB) loadExtensions(ctx context.Context, conn *sql.Conn) error {
	execer := &connExecer{conn: conn, ctx: ctx}
	return s.loadExtensionsWithExecer(ctx, execer)
}

// loadExtensionsWithExecer loads extensions using a driver.ExecerContext interface.
func (s *S3DB) loadExtensionsWithExecer(ctx context.Context, execer driver.ExecerContext) error {
	base := os.Getenv("LAKERUNNER_EXTENSIONS_PATH")
	if base == "" {
		base = discoverExtensionsPath()
		if base == "" {
			return fmt.Errorf("extensions required but not found: LAKERUNNER_EXTENSIONS_PATH not set")
		}
	}

	extensions := []string{"httpfs", "aws", "azure"}

	for _, name := range extensions {
		path := filepath.Join(base, fmt.Sprintf("%s.duckdb_extension", name))
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("%s extension file not found at %s", name, path)
		}

		if _, err := execer.ExecContext(ctx, fmt.Sprintf("LOAD '%s';", escapeSingle(path)), nil); err != nil {
			if strings.Contains(err.Error(), "already registered") || strings.Contains(err.Error(), "already exists") {
				continue
			}
			return fmt.Errorf("load %s: %w", name, err)
		}
	}

	// Configure Azure transport after loading
	if _, err := execer.ExecContext(ctx, "SET azure_transport_option_type = 'curl';", nil); err != nil {
		slog.Debug("Failed to set azure transport option", "error", err)
	}

	return nil
}

// connExecer wraps a sql.Conn to implement driver.ExecerContext
type connExecer struct {
	conn *sql.Conn
	ctx  context.Context
}

func (c *connExecer) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := c.conn.ExecContext(ctx, query)
	return result, err
}

// ---------- helpers & metrics ----------

// discoverExtensionsPath attempts to find DuckDB extensions in the repository.
func discoverExtensionsPath() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}

	platform := getPlatformDir()
	if platform == "" {
		return ""
	}

	for {
		extensionsPath := filepath.Join(dir, "docker", "duckdb-extensions", platform)
		if info, err := os.Stat(extensionsPath); err == nil && info.IsDir() {
			httpfsPath := filepath.Join(extensionsPath, "httpfs.duckdb_extension")
			if _, err := os.Stat(httpfsPath); err == nil {
				return extensionsPath
			}
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent

		if strings.Count(dir, string(filepath.Separator)) < 2 {
			break
		}
	}

	return ""
}

// getPlatformDir returns the platform-specific directory name for DuckDB extensions
func getPlatformDir() string {
	goos := runtime.GOOS
	goarch := runtime.GOARCH

	switch {
	case goos == "darwin" && goarch == "arm64":
		return "osx_arm64"
	case goos == "darwin" && goarch == "amd64":
		return "osx_amd64"
	case goos == "linux" && goarch == "arm64":
		return "linux_arm64"
	case goos == "linux" && goarch == "amd64":
		return "linux_amd64"
	default:
		return ""
	}
}

func escapeSingle(s string) string { return strings.ReplaceAll(s, `'`, `''`) }

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
func (s *S3DB) pollMemoryMetrics(ctx context.Context) {
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
		conn, release, err := s.GetConnection(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("failed to get connection for memory metrics", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.metricsPeriod):
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
			case <-time.After(s.metricsPeriod):
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
		case <-time.After(s.metricsPeriod):
		}
	}
}
