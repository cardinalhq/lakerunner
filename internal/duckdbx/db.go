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
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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

	// config (used to build DSN)
	poolSize int
	dsn      string // full DSN with all settings

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

	// Single shared sql.DB from one connector
	db        *sql.DB
	dbOnce    sync.Once
	dbErr     error
	connector *duckdb.Connector
}

// DuckDBSettings holds DuckDB-specific settings for DSN construction.
// This mirrors config.DuckDBConfig to avoid circular imports.
type DuckDBSettings struct {
	MemoryLimitMB        int64  // Memory limit in MB (0 = unlimited)
	TempDirectory        string // Directory for temporary files
	MaxTempDirectorySize string // Max size for temp directory (e.g., "10GB" or bytes as string)
	PoolSize             int    // Connection pool size (0 = use default)
	Threads              int    // Total threads (0 = use default)
}

// dbConfig holds configuration options for DB
type dbConfig struct {
	dbPath        *string
	metricsPeriod time.Duration
	metricsCtx    context.Context
	connMaxAge    time.Duration
	duckdb        *DuckDBSettings
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

// WithDuckDBSettings sets DuckDB-specific configuration for DSN construction.
func WithDuckDBSettings(settings DuckDBSettings) DBOption {
	return func(cfg *dbConfig) {
		cfg.duckdb = &settings
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

	// Get settings from config or use defaults
	settings := cfg.duckdb
	if settings == nil {
		settings = &DuckDBSettings{}
	}

	// Pool size: from settings, or default (half cores, capped at 8, min 2)
	poolSize := settings.PoolSize
	if poolSize <= 0 {
		poolSize = min(8, max(2, runtime.GOMAXPROCS(0)/2))
	}

	// Threads: from settings, or default to GOMAXPROCS
	threads := settings.Threads
	if threads <= 0 {
		threads = runtime.GOMAXPROCS(0)
	}

	// connection TTL (default 25m; override by option)
	connMaxAge := 25 * time.Minute
	if cfg.connMaxAge > 0 {
		connMaxAge = cfg.connMaxAge
	}

	// Build DSN with all configuration parameters
	dsn := buildDSN(dbPath, settings, threads)

	slog.Info("duckdbx: single shared database",
		"type", "file",
		"dbPath", dbPath,
		"dsn", dsn,
		"poolSize", poolSize,
		"threads", threads,
		"connMaxAge", connMaxAge,
	)

	d := &DB{
		dbPath:         dbPath,
		cleanupOnClose: cleanupOnClose,
		poolSize:       poolSize,
		dsn:            dsn,
		metricsPeriod:  cfg.metricsPeriod,
		connMaxAge:     connMaxAge,
	}

	d.pool = &connectionPool{
		parent: d,
		size:   poolSize,
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

func (p *connectionPool) ensureDB(ctx context.Context) error {
	p.dbOnce.Do(func() {
		connector, err := duckdb.NewConnector(p.parent.dsn, nil)
		if err != nil {
			p.dbErr = fmt.Errorf("create connector: %w", err)
			return
		}
		p.connector = connector

		db := sql.OpenDB(connector)
		// Allow multiple connections from this single sql.DB
		db.SetMaxOpenConns(p.size)
		db.SetMaxIdleConns(p.size)
		p.db = db

		// Apply any settings that can't be set via DSN
		if err := p.parent.applyPostConnectSettings(ctx, db); err != nil {
			p.dbErr = err
			return
		}
	})
	return p.dbErr
}

func (p *connectionPool) acquire(ctx context.Context) (*sql.Conn, func(), error) {
	if err := p.ensureDB(ctx); err != nil {
		return nil, nil, err
	}

	conn, err := p.db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}

	return conn, func() { _ = conn.Close() }, nil
}

func (p *connectionPool) closeAll() {
	if p.db != nil {
		_ = p.db.Close()
	}
}

// ---------- DB setup ----------

// buildDSN constructs a DuckDB DSN with the provided settings.
// See https://duckdb.org/docs/api/go.html for supported parameters.
func buildDSN(dbPath string, settings *DuckDBSettings, threads int) string {
	// Start with required settings
	params := []string{"allow_unsigned_extensions=true"}

	// Memory limit (in MB)
	if settings.MemoryLimitMB > 0 {
		params = append(params, fmt.Sprintf("memory_limit=%dMB", settings.MemoryLimitMB))
	}

	// Thread count
	params = append(params, fmt.Sprintf("threads=%d", threads))

	// Temp directory
	if settings.TempDirectory != "" {
		params = append(params, "temp_directory="+settings.TempDirectory)
	}

	// Max temp directory size
	if settings.MaxTempDirectorySize != "" {
		params = append(params, "max_temp_directory_size="+settings.MaxTempDirectorySize)
	}

	return dbPath + "?" + strings.Join(params, "&")
}

// applyPostConnectSettings applies settings that cannot be set via DSN.
func (d *DB) applyPostConnectSettings(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("get conn for setup: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// Set home_directory (required for extension loading)
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET home_directory='%s';", escapeSingle(filepath.Dir(d.dbPath)))); err != nil {
		slog.Warn("Failed to set home_directory", "error", err)
	}

	// Enable object cache for better performance
	if _, err := conn.ExecContext(ctx, "PRAGMA enable_object_cache;"); err != nil {
		return fmt.Errorf("enable_object_cache: %w", err)
	}

	return nil
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
// If not set, it searches for the extension in standard locations.
func LoadDDSketchExtension(ctx context.Context, conn *sql.Conn) error {
	extensionPath := os.Getenv(DDSketchExtensionEnvVar)
	if extensionPath == "" {
		extensionPath = findDDSketchExtension()
	}
	if extensionPath == "" {
		return fmt.Errorf("DDSketch extension not found: set %s environment variable or place extension in docker/duckdb-extensions/", DDSketchExtensionEnvVar)
	}
	return LoadDDSketchExtensionFromPath(ctx, conn, extensionPath)
}

// findDDSketchExtension searches for the DDSketch extension in standard locations.
// If only a .gz version exists, it will be decompressed to a temp file.
func findDDSketchExtension() string {
	// Determine platform directory name (DuckDB uses "osx" not "darwin")
	goos := runtime.GOOS
	if goos == "darwin" {
		goos = "osx"
	}
	platform := goos + "_" + runtime.GOARCH
	extName := "ddsketch.duckdb_extension"

	// Search paths (relative to current directory and parent directories)
	basePaths := []string{
		filepath.Join("docker", "duckdb-extensions", platform),
		filepath.Join("..", "docker", "duckdb-extensions", platform),
		filepath.Join("..", "..", "docker", "duckdb-extensions", platform),
	}

	for _, basePath := range basePaths {
		// Try uncompressed first
		uncompressed := filepath.Join(basePath, extName)
		if _, err := os.Stat(uncompressed); err == nil {
			if absPath, err := filepath.Abs(uncompressed); err == nil {
				return absPath
			}
			return uncompressed
		}

		// Try gzipped and decompress
		gzipped := filepath.Join(basePath, extName+".gz")
		if _, err := os.Stat(gzipped); err == nil {
			if decompressed, err := decompressExtension(gzipped); err == nil {
				return decompressed
			}
		}
	}

	return ""
}

// decompressExtension decompresses a gzipped extension to a temp file.
// The extension filename must be exactly "ddsketch.duckdb_extension" for DuckDB to load it.
func decompressExtension(gzPath string) (string, error) {
	gzFile, err := os.Open(gzPath)
	if err != nil {
		return "", err
	}
	defer func() { _ = gzFile.Close() }()

	gzReader, err := gzip.NewReader(gzFile)
	if err != nil {
		return "", err
	}
	defer func() { _ = gzReader.Close() }()

	// Create temp directory and use exact extension name (DuckDB requires this)
	tmpDir, err := os.MkdirTemp("", "duckdb-ext-")
	if err != nil {
		return "", err
	}

	extPath := filepath.Join(tmpDir, "ddsketch.duckdb_extension")
	tmpFile, err := os.Create(extPath)
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return "", err
	}

	if _, err := io.Copy(tmpFile, gzReader); err != nil {
		_ = tmpFile.Close()
		_ = os.RemoveAll(tmpDir)
		return "", err
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.RemoveAll(tmpDir)
		return "", err
	}

	return extPath, nil
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
