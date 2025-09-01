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
	"strings"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// Global mutex to serialize extension/secret DDL across the process.
// DuckDB extension loading & DDL may crash when done concurrently in many engines.
var duckdbDDLMu sync.Mutex

// S3DB manages per-bucket pools of DuckDB *instances* (one DB+Conn per item).
type S3DB struct {
	dsn string

	// config
	memoryLimitMB int64
	tempDir       string
	maxTempSize   string

	poolSize       int
	ttl            time.Duration
	totalCores     int
	threadsPerConn int

	installOnce sync.Once
	installErr  error

	poolsMu sync.Mutex
	pools   map[string]*bucketPool
}

type bucketPool struct {
	parent   *S3DB
	bucket   string
	region   string
	endpoint string
	size     int
	ttl      time.Duration

	mu  sync.Mutex
	cur int

	ch   chan *pooledConn
	init sync.Once
}

type pooledConn struct {
	db      *sql.DB
	conn    *sql.Conn
	expires time.Time
}

func NewS3DB(dataSourceName string) (*S3DB, error) {
	// Prefer in-memory DB unless the caller really wants a file;
	// treat legacy "s3" as a hint for in-memory.
	if dataSourceName == "s3" {
		dataSourceName = ""
	}

	memoryMB := envInt64("DUCKDB_MEMORY_LIMIT", 0)

	// Default pool: half the cores, capped at 8, min 2.
	// (Avoid oversubscription when each connection has multiple threads.)
	poolDefault := min(8, max(2, runtime.GOMAXPROCS(0)/2))
	poolSize := envIntClamp("DUCKDB_S3_POOL_SIZE", poolDefault, 1, 512)

	ttl := envDurationSeconds("DUCKDB_S3_CONN_TTL_SECONDS", 240)

	total := runtime.GOMAXPROCS(0)
	// Split cores across connections by default; allow explicit override.
	perConnDefault := max(1, total/max(1, poolSize))
	threadsPerConn := envIntClamp("DUCKDB_THREADS_PER_CONN", perConnDefault, 1, 256)
	slog.Info("duckdbx:",
		"dsn", dataSourceName,
		"memoryLimitMB", memoryMB,
		"tempDir", os.Getenv("DUCKDB_TEMP_DIRECTORY"),
		"maxTempSize", os.Getenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE"),
		"poolSize", poolSize,
		"ttl", ttl.String(),
		"totalCores", total,
		"threadsPerConn", threadsPerConn)

	return &S3DB{
		dsn:            dataSourceName,
		memoryLimitMB:  memoryMB,
		tempDir:        os.Getenv("DUCKDB_TEMP_DIRECTORY"),
		maxTempSize:    os.Getenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE"),
		poolSize:       poolSize,
		ttl:            ttl,
		totalCores:     total,
		threadsPerConn: threadsPerConn,
		pools:          make(map[string]*bucketPool, 32),
	}, nil
}

func (s *S3DB) Close() error {
	s.poolsMu.Lock()
	for _, p := range s.pools {
		p.closeAll()
	}
	s.pools = map[string]*bucketPool{}
	s.poolsMu.Unlock()
	return nil
}

func (s *S3DB) GetConnection(ctx context.Context, bucket, region string, endpoint string) (*sql.Conn, func(), error) {
	if bucket == "" {
		return nil, nil, fmt.Errorf("bucket is required")
	}
	s.poolsMu.Lock()
	p := s.pools[bucket]
	if p == nil {
		p = &bucketPool{
			parent:   s,
			bucket:   bucket,
			region:   region,
			endpoint: endpoint,
			size:     s.poolSize,
			ttl:      s.ttl,
			ch:       make(chan *pooledConn, s.poolSize),
		}
		s.pools[bucket] = p
	}
	s.poolsMu.Unlock()
	return p.acquire(ctx)
}

func (p *bucketPool) acquire(ctx context.Context) (*sql.Conn, func(), error) {
	p.init.Do(func() {})
	now := time.Now()

	// try pooled
	select {
	case pc := <-p.ch:
		if now.After(pc.expires) {
			_ = pc.conn.Close()
			_ = pc.db.Close()
			p.mu.Lock()
			p.cur--
			p.mu.Unlock()
		} else {
			return pc.conn, func() { p.release(pc) }, nil
		}
	default:
	}

	// create new if capacity
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

	// wait for one
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case pc := <-p.ch:
		if time.Now().After(pc.expires) {
			_ = pc.conn.Close()
			_ = pc.db.Close()
			p.mu.Lock()
			p.cur--
			p.mu.Unlock()
			return p.acquire(ctx)
		}
		return pc.conn, func() { p.release(pc) }, nil
	}
}

func (p *bucketPool) release(pc *pooledConn) {
	if time.Now().After(pc.expires) {
		_ = pc.conn.Close()
		_ = pc.db.Close()
		p.mu.Lock()
		p.cur--
		p.mu.Unlock()
		return
	}
	p.ch <- pc
}

func (p *bucketPool) newConn(ctx context.Context) (*pooledConn, error) {
	// best-effort global INSTALL for dev mode
	if err := p.parent.ensureInstall(ctx); err != nil {
		return nil, err
	}

	// brand-new DB instance for this pooled item
	db, err := sql.Open("duckdb", p.parent.dsn)
	if err != nil {
		return nil, err
	}
	// one physical connection per DB instance
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	// per-connection setup
	if err := p.parent.setupConn(ctx, conn); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, err
	}

	// create S3 secret for this bucket (serialize DDL)
	if err := seedS3SecretFromEnv(ctx, conn, p.bucket, p.region, p.endpoint); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, err
	}

	return &pooledConn{
		db:      db,
		conn:    conn,
		expires: time.Now().Add(p.ttl),
	}, nil
}

func (p *bucketPool) closeAll() {
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

func (s *S3DB) setupConn(ctx context.Context, conn *sql.Conn) error {
	if s.memoryLimitMB > 0 {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET memory_limit='%dMB';", s.memoryLimitMB)); err != nil {
			return fmt.Errorf("set memory_limit: %w", err)
		}
	}
	if s.tempDir != "" {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET temp_directory = '%s';", escapeSingle(s.tempDir))); err != nil {
			return fmt.Errorf("set temp_directory: %w", err)
		}
	}
	if s.maxTempSize != "" {
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET max_temp_directory_size = '%s';", escapeSingle(s.maxTempSize))); err != nil {
			return fmt.Errorf("set max_temp_directory_size: %w", err)
		}
	}

	if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA threads=%d;", s.threadsPerConn)); err != nil {
		return fmt.Errorf("set threads: %w", err)
	}
	// Keep DuckDB's object cache on to reduce repeated S3 GETs for metadata/manifest.
	if _, err := conn.ExecContext(ctx, "PRAGMA enable_object_cache;"); err != nil {
		return fmt.Errorf("enable_object_cache: %w", err)
	}

	// LOAD httpfs (serialize LOAD across engines)
	duckdbDDLMu.Lock()
	err := s.loadHTTPFS(ctx, conn)
	duckdbDDLMu.Unlock()
	return err
}

// Dev-mode best-effort INSTALL once. Air-gapped: only LOAD.
func (s *S3DB) ensureInstall(ctx context.Context) error {
	if os.Getenv("LAKERUNNER_EXTENSIONS_PATH") != "" {
		return nil
	}
	s.installOnce.Do(func() {
		db, err := sql.Open("duckdb", s.dsn)
		if err != nil {
			s.installErr = err
			return
		}
		defer db.Close()
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		conn, err := db.Conn(ctx)
		if err != nil {
			s.installErr = err
			return
		}
		defer conn.Close()
		if s.memoryLimitMB > 0 {
			_, _ = conn.ExecContext(ctx, fmt.Sprintf("SET memory_limit='%dMB';", s.memoryLimitMB))
		}
		duckdbDDLMu.Lock()
		_, _ = conn.ExecContext(ctx, "INSTALL httpfs;")
		duckdbDDLMu.Unlock()
	})
	return s.installErr
}

func (s *S3DB) loadHTTPFS(ctx context.Context, conn *sql.Conn) error {
	if base := os.Getenv("LAKERUNNER_EXTENSIONS_PATH"); base != "" {
		path := os.Getenv("LAKERUNNER_HTTPFS_EXTENSION")
		if path == "" {
			path = filepath.Join(base, "httpfs.duckdb_extension")
		}
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("httpfs extension not found at %s: %w", path, err)
		}
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("LOAD '%s';", escapeSingle(path))); err != nil {
			return fmt.Errorf("LOAD httpfs (air-gapped): %w", err)
		}
		return nil
	}
	if _, err := conn.ExecContext(ctx, "LOAD httpfs;"); err != nil {
		return fmt.Errorf("LOAD httpfs: %w", err)
	}
	return nil
}

// CREATE OR REPLACE SECRET for a bucket (serialized).
func seedS3SecretFromEnv(ctx context.Context, conn *sql.Conn, bucket, region string, endpoint string) error {
	keyID := os.Getenv("S3_ACCESS_KEY_ID")
	secret := os.Getenv("S3_SECRET_ACCESS_KEY")
	if keyID == "" || secret == "" {
		return fmt.Errorf("missing AWS creds in env: S3_ACCESS_KEY_ID/S3_SECRET_ACCESS_KEY")
	}
	session := os.Getenv("AWS_SESSION_TOKEN")

	if region == "" {
		if r := os.Getenv("AWS_REGION"); r != "" {
			region = r
		} else if r := os.Getenv("AWS_DEFAULT_REGION"); r != "" {
			region = r
		} else {
			region = "us-east-1"
		}
	}

	useSSL := "true"

	if endpoint == "" {
		endpoint = fmt.Sprintf("s3.%s.amazonaws.com", region)
	} else {
		if strings.HasPrefix(endpoint, "http://") {
			endpoint = strings.TrimPrefix(endpoint, "http://")
			useSSL = "false"
		} else if strings.HasPrefix(endpoint, "https://") {
			endpoint = strings.TrimPrefix(endpoint, "https://")
			useSSL = "true"
		}
	}

	urlStyle := os.Getenv("AWS_S3_URL_STYLE")
	if urlStyle == "" {
		urlStyle = "path"
	}

	secretName := "secret_" + strings.ReplaceAll(bucket, "-", "_")

	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "CREATE OR REPLACE SECRET %s (\n", quoteIdent(secretName))
	_, _ = fmt.Fprintf(&b, "  TYPE S3,\n")
	_, _ = fmt.Fprintf(&b, "  ENDPOINT '%s',\n", escapeSingle(endpoint))
	_, _ = fmt.Fprintf(&b, "  URL_STYLE '%s',\n", escapeSingle(urlStyle))
	_, _ = fmt.Fprintf(&b, "  USE_SSL '%s',\n", escapeSingle(useSSL))
	_, _ = fmt.Fprintf(&b, "  KEY_ID '%s',\n", escapeSingle(keyID))
	_, _ = fmt.Fprintf(&b, "  SECRET '%s',\n", escapeSingle(secret))
	if session != "" {
		_, _ = fmt.Fprintf(&b, "  SESSION_TOKEN '%s',\n", escapeSingle(session))
	}
	_, _ = fmt.Fprintf(&b, "  REGION '%s',\n", escapeSingle(region))
	_, _ = fmt.Fprintf(&b, "  SCOPE 's3://%s'\n", escapeSingle(bucket))
	_, _ = fmt.Fprintf(&b, ");")

	duckdbDDLMu.Lock()
	_, err := conn.ExecContext(ctx, b.String())
	duckdbDDLMu.Unlock()
	return err
}

func escapeSingle(s string) string { return strings.ReplaceAll(s, `'`, `''`) }
func quoteIdent(s string) string   { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

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
func envDurationSeconds(name string, defSec int) time.Duration {
	if v := os.Getenv(name); v != "" {
		if iv, err := strconv.Atoi(v); err == nil && iv >= 0 {
			return time.Duration(iv) * time.Second
		}
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return time.Duration(defSec) * time.Second
}

// small helpers (int)
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
