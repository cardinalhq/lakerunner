// Copyright (C) 2025 CardinalHQ, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, version 3.

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
	"time"

	"github.com/marcboeker/go-duckdb/v2"
)

// LocalDB is a lightweight DuckDB wrapper for LOCAL-only queries (e.g. read_parquet on local files).
// It does NOT load httpfs/aws/azure extensions and does not seed any secrets.
type LocalDB struct {
	dbPath         string
	cleanupOnClose bool

	db *sql.DB

	// config
	memoryLimitMB int64
	tempDir       string
	maxTempSize   string
	poolSize      int
	threads       int
	connMaxAge    time.Duration
}

type localDBConfig struct {
	dbPath            *string
	poolSize          *int
	threads           *int
	connMaxAge        time.Duration
	enableObjectCache *bool
}

type LocalDBOption func(*localDBConfig)

func WithLocalDatabasePath(path string) LocalDBOption {
	return func(cfg *localDBConfig) {
		if path == "" {
			panic("WithLocalDatabasePath: path must not be empty")
		}
		cfg.dbPath = &path
	}
}

func WithLocalPoolSize(n int) LocalDBOption {
	return func(cfg *localDBConfig) {
		if n < 1 {
			n = 1
		}
		cfg.poolSize = &n
	}
}

func WithLocalThreads(n int) LocalDBOption {
	return func(cfg *localDBConfig) {
		if n < 1 {
			n = 1
		}
		cfg.threads = &n
	}
}

func WithLocalConnectionMaxAge(d time.Duration) LocalDBOption {
	return func(cfg *localDBConfig) {
		if d < time.Minute {
			d = time.Minute
		}
		cfg.connMaxAge = d
	}
}

func WithLocalEnableObjectCache(v bool) LocalDBOption {
	return func(cfg *localDBConfig) { cfg.enableObjectCache = &v }
}

func NewLocalDB(opts ...LocalDBOption) (*LocalDB, error) {
	cfg := &localDBConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	var dbPath string
	var cleanupOnClose bool
	if cfg.dbPath != nil {
		dbPath = *cfg.dbPath
		cleanupOnClose = false
	} else {
		dbDir, err := os.MkdirTemp("", "lakerunner-duckdb-local-*")
		if err != nil {
			return nil, fmt.Errorf("create temp dir for LocalDB: %w", err)
		}
		dbPath = filepath.Join(dbDir, "local.ddb")
		cleanupOnClose = true
	}

	memoryMB := envInt64("DUCKDB_MEMORY_LIMIT", 0)
	tempDir := os.Getenv("DUCKDB_TEMP_DIRECTORY")
	maxTemp := os.Getenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE")

	// Default pool: half cores, capped at 8, min 1.
	half := runtime.GOMAXPROCS(0) / 2
	if half < 1 {
		half = 1
	}
	if half > 8 {
		half = 8
	}
	poolDefault := half

	poolSize := envIntClamp("DUCKDB_LOCAL_POOL_SIZE", poolDefault, 1, 512)
	if cfg.poolSize != nil {
		poolSize = *cfg.poolSize
	}

	threads := envIntClamp("DUCKDB_THREADS", runtime.GOMAXPROCS(0), 1, 256)
	if cfg.threads != nil {
		threads = *cfg.threads
	}

	connMaxAge := envDuration("DUCKDB_POOL_CONN_MAX_AGE", 25*time.Minute)
	if cfg.connMaxAge > 0 {
		connMaxAge = cfg.connMaxAge
	}

	enableObjectCache := true
	if cfg.enableObjectCache != nil {
		enableObjectCache = *cfg.enableObjectCache
	}

	l := &LocalDB{
		dbPath:         dbPath,
		cleanupOnClose: cleanupOnClose,
		memoryLimitMB:  memoryMB,
		tempDir:        tempDir,
		maxTempSize:    maxTemp,
		poolSize:       poolSize,
		threads:        threads,
		connMaxAge:     connMaxAge,
	}

	slog.Info("duckdbx: LocalDB init",
		"dbPath", dbPath,
		"poolSize", poolSize,
		"threads", threads,
		"connMaxAge", connMaxAge,
		"memoryLimitMB", memoryMB,
		"enableObjectCache", enableObjectCache,
	)

	// One connector + one sql.DB for the instance.
	// Important: do not use request ctx inside the init hook.
	connector, err := duckdb.NewConnector(dbPath, func(execer driver.ExecerContext) error {
		ctx := context.Background()

		// Disable automatic extension loading/downloading.
		_, _ = execer.ExecContext(ctx, "SET autoinstall_known_extensions = false;", nil)
		_, _ = execer.ExecContext(ctx, "SET autoload_known_extensions = false;", nil)

		// Basic config.
		if l.memoryLimitMB > 0 {
			_, _ = execer.ExecContext(ctx, fmt.Sprintf("SET memory_limit='%dMB';", l.memoryLimitMB), nil)
		}
		if l.tempDir != "" {
			_, _ = execer.ExecContext(ctx, fmt.Sprintf("SET temp_directory='%s';", escapeSingle(l.tempDir)), nil)
		}
		if l.maxTempSize != "" {
			_, _ = execer.ExecContext(ctx, fmt.Sprintf("SET max_temp_directory_size='%s';", escapeSingle(l.maxTempSize)), nil)
		}
		_, _ = execer.ExecContext(ctx, fmt.Sprintf("PRAGMA threads=%d;", l.threads), nil)

		return nil
	})
	if err != nil {
		if cleanupOnClose {
			_ = os.RemoveAll(filepath.Dir(dbPath))
		}
		return nil, fmt.Errorf("create duckdb connector: %w", err)
	}

	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(poolSize)
	db.SetMaxIdleConns(poolSize)
	db.SetConnMaxLifetime(connMaxAge)

	l.db = db
	return l, nil
}

func (l *LocalDB) Close() error {
	if l.db != nil {
		_ = l.db.Close()
	}
	if l.cleanupOnClose && l.dbPath != "" {
		_ = os.RemoveAll(filepath.Dir(l.dbPath))
	}
	return nil
}

func (l *LocalDB) GetDatabasePath() string { return l.dbPath }

func (l *LocalDB) GetConnection(ctx context.Context) (*sql.Conn, func(), error) {
	c, err := l.db.Conn(ctx)
	if err != nil {
		return nil, nil, err
	}
	return c, func() { _ = c.Close() }, nil
}
