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

	_ "github.com/marcboeker/go-duckdb/v2"
)

// Global mutex to serialize extension/secret DDL across the process.
// DuckDB extension loading & DDL may crash when done concurrently in many engines.
var duckdbDDLMu sync.Mutex

// S3DB manages a pool of DuckDB connections to a single shared on-disk database.
// All connections open the same file and thus share the same in-process database instance.
// Credentials are set per-bucket on each connection acquisition to support multiple buckets.
type S3DB struct {
	dbPath string // single on-disk database file for all connections

	// config (applied once to the shared database instance)
	memoryLimitMB int64
	tempDir       string
	maxTempSize   string
	poolSize      int
	threads       int // total threads for the shared database instance

	// one-time setup
	setupOnce sync.Once
	setupErr  error

	// Single global pool
	pool *connectionPool
}

type connectionPool struct {
	parent *S3DB
	size   int

	mu  sync.Mutex
	cur int

	ch chan *pooledConn
}

type pooledConn struct {
	db   *sql.DB
	conn *sql.Conn
}

func NewS3DB() (*S3DB, error) {
	// Always use a single on-disk database to reduce memory footprint
	// Create a temp directory and single database file
	dbDir, err := os.MkdirTemp("", "duckdb-global-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir for S3DB: %w", err)
	}

	dbPath := filepath.Join(dbDir, "global.ddb")

	memoryMB := envInt64("DUCKDB_MEMORY_LIMIT", 0)

	// Default pool: half the cores, capped at 8, min 2.
	poolDefault := min(8, max(2, runtime.GOMAXPROCS(0)/2))
	poolSize := envIntClamp("DUCKDB_S3_POOL_SIZE", poolDefault, 1, 512)

	total := runtime.GOMAXPROCS(0)
	// All connections share the same database, so threads setting applies to the shared instance
	threads := envIntClamp("DUCKDB_THREADS", total, 1, 256)
	slog.Info("duckdbx: single shared database",
		"dbPath", dbPath,
		"memoryLimitMB", memoryMB,
		"tempDir", os.Getenv("DUCKDB_TEMP_DIRECTORY"),
		"maxTempSize", os.Getenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE"),
		"poolSize", poolSize,
		"threads", threads)

	s3db := &S3DB{
		dbPath:        dbPath,
		memoryLimitMB: memoryMB,
		tempDir:       os.Getenv("DUCKDB_TEMP_DIRECTORY"),
		maxTempSize:   os.Getenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE"),
		poolSize:      poolSize,
		threads:       threads,
	}

	// Initialize the global pool
	s3db.pool = &connectionPool{
		parent: s3db,
		size:   poolSize,
		ch:     make(chan *pooledConn, poolSize),
	}

	return s3db, nil
}

func (s *S3DB) Close() error {
	if s.pool != nil {
		s.pool.closeAll()
	}

	// Clean up the database file and its directory
	if s.dbPath != "" {
		dbDir := filepath.Dir(s.dbPath)
		_ = os.RemoveAll(dbDir)
	}
	return nil
}

func (s *S3DB) GetConnection(ctx context.Context, bucket, region string, endpoint string) (*sql.Conn, func(), error) {
	if bucket == "" {
		return nil, nil, fmt.Errorf("bucket is required")
	}
	return s.pool.acquire(ctx, bucket, region, endpoint)
}

func (p *connectionPool) acquire(ctx context.Context, bucket, region, endpoint string) (*sql.Conn, func(), error) {
	// Helper function to ensure credentials are set for the bucket
	ensureCredentials := func(conn *sql.Conn) error {
		// Skip for local file operations
		if bucket == "local" {
			return nil
		}
		// Create/replace the secret for this bucket
		// This is idempotent - CREATE OR REPLACE will update if needed
		return seedCloudSecretFromEnv(ctx, conn, bucket, region, endpoint)
	}

	// try to get a pooled connection
	select {
	case pc := <-p.ch:
		// Ensure credentials are configured for this specific bucket
		if err := ensureCredentials(pc.conn); err != nil {
			// If we can't set credentials, close this connection and try to create a new one
			_ = pc.conn.Close()
			_ = pc.db.Close()
			p.mu.Lock()
			p.cur--
			p.mu.Unlock()
			return nil, nil, err
		}
		return pc.conn, func() { p.release(pc) }, nil
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
		pc, err := p.newConn(ctx, bucket, region, endpoint)
		if err != nil {
			p.mu.Lock()
			p.cur--
			p.mu.Unlock()
			return nil, nil, err
		}
		return pc.conn, func() { p.release(pc) }, nil
	}

	// wait for one to become available
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case pc := <-p.ch:
		// Ensure credentials are configured for this specific bucket
		if err := ensureCredentials(pc.conn); err != nil {
			// If we can't set credentials, close this connection and try again
			_ = pc.conn.Close()
			_ = pc.db.Close()
			p.mu.Lock()
			p.cur--
			p.mu.Unlock()
			return nil, nil, err
		}
		return pc.conn, func() { p.release(pc) }, nil
	}
}

func (p *connectionPool) release(pc *pooledConn) {
	// Simply return to pool - no expiration checks needed
	select {
	case p.ch <- pc:
		// returned to pool
	default:
		// pool is full (shouldn't happen), close this connection
		_ = pc.conn.Close()
		_ = pc.db.Close()
		p.mu.Lock()
		p.cur--
		p.mu.Unlock()
	}
}

func (p *connectionPool) newConn(ctx context.Context, bucket, region, endpoint string) (*pooledConn, error) {
	// Ensure extensions are installed and database-wide settings are applied (once)
	if err := p.parent.ensureSetup(ctx); err != nil {
		return nil, err
	}

	// Open a connection to the shared database file
	// All connections opening the same file share the same in-process database instance
	db, err := sql.Open("duckdb", p.parent.dbPath)
	if err != nil {
		return nil, err
	}
	// one physical connection per DB handle
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	// create cloud storage secret for this bucket (serialize DDL)
	// Skip for local file operations
	if bucket != "local" {
		if err := seedCloudSecretFromEnv(ctx, conn, bucket, region, endpoint); err != nil {
			_ = conn.Close()
			_ = db.Close()
			return nil, err
		}
	}

	return &pooledConn{
		db:   db,
		conn: conn,
	}, nil
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

// ensureSetup runs once to configure the shared database instance and load extensions
func (s *S3DB) ensureSetup(ctx context.Context) error {
	s.setupOnce.Do(func() {
		// Open a temporary connection to configure the database
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

		// Configure database-wide settings (these affect the shared instance)
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
		// Enable object cache for S3 operations
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
	// Disable automatic extension loading/downloading
	if _, err := conn.ExecContext(ctx, "SET autoinstall_known_extensions = false;"); err != nil {
		slog.Warn("Failed to disable automatic extension installation", "error", err)
	}
	if _, err := conn.ExecContext(ctx, "SET autoload_known_extensions = false;"); err != nil {
		slog.Warn("Failed to disable automatic extension loading", "error", err)
	}

	// Load extensions from local files only
	if err := s.loadHTTPFS(ctx, conn); err != nil {
		return err
	}
	if err := s.loadAWS(ctx, conn); err != nil {
		return err
	}
	if err := s.loadAzure(ctx, conn); err != nil {
		return err
	}
	return nil
}

func (s *S3DB) loadHTTPFS(ctx context.Context, conn *sql.Conn) error {
	base := os.Getenv("LAKERUNNER_EXTENSIONS_PATH")
	if base == "" {
		slog.Warn("LAKERUNNER_EXTENSIONS_PATH not set, skipping httpfs extension")
		return nil
	}

	path := filepath.Join(base, "httpfs.duckdb_extension")
	if _, err := os.Stat(path); err != nil {
		slog.Warn("httpfs extension not found", "path", path, "error", err)
		return nil // Non-fatal, continue without extension
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("LOAD '%s';", escapeSingle(path))); err != nil {
		return fmt.Errorf("LOAD httpfs: %w", err)
	}
	slog.Info("Loaded httpfs extension", "path", path)
	return nil
}

func (s *S3DB) loadAWS(ctx context.Context, conn *sql.Conn) error {
	base := os.Getenv("LAKERUNNER_EXTENSIONS_PATH")
	if base == "" {
		return nil // No extensions path configured
	}

	path := filepath.Join(base, "aws.duckdb_extension")
	if _, err := os.Stat(path); err != nil {
		slog.Warn("aws extension not found", "path", path, "error", err)
		return nil // Non-fatal, continue without extension
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("LOAD '%s';", escapeSingle(path))); err != nil {
		return fmt.Errorf("LOAD aws: %w", err)
	}
	slog.Info("Loaded aws extension", "path", path)
	return nil
}

func (s *S3DB) loadAzure(ctx context.Context, conn *sql.Conn) error {
	base := os.Getenv("LAKERUNNER_EXTENSIONS_PATH")
	if base == "" {
		return nil // No extensions path configured
	}

	path := filepath.Join(base, "azure.duckdb_extension")
	if _, err := os.Stat(path); err != nil {
		slog.Warn("azure extension not found", "path", path, "error", err)
		return nil // Non-fatal, continue without extension
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("LOAD '%s';", escapeSingle(path))); err != nil {
		return fmt.Errorf("LOAD azure: %w", err)
	}
	// Configure Azure transport to use curl for better compatibility
	if _, err := conn.ExecContext(ctx, "SET azure_transport_option_type = 'curl';"); err != nil {
		slog.Warn("Failed to set azure transport option", "error", err)
	}
	slog.Info("Loaded azure extension", "path", path)
	return nil
}

// CREATE OR REPLACE SECRET for a bucket (serialized).
// Detects cloud provider based on environment variables and creates appropriate secret.
func seedCloudSecretFromEnv(ctx context.Context, conn *sql.Conn, bucket, region string, endpoint string) error {
	// Check for Azure credentials first
	if hasAzureCredentials() {
		return seedAzureSecretFromEnv(ctx, conn, bucket, region, endpoint)
	}

	// Fall back to S3/AWS
	return seedS3SecretFromEnv(ctx, conn, bucket, region, endpoint)
}

// Check if Azure credentials are available
func hasAzureCredentials() bool {
	authType := os.Getenv("AZURE_AUTH_TYPE")
	return authType != ""
}

// CREATE OR REPLACE SECRET for Azure Blob Storage (serialized).
func seedAzureSecretFromEnv(ctx context.Context, conn *sql.Conn, container, _ string, endpoint string) error {
	authType := os.Getenv("AZURE_AUTH_TYPE")
	if authType == "" {
		authType = "credential_chain" // Default to credential chain
	}

	// For Azure, storage account must be extracted from endpoint
	if endpoint == "" {
		return fmt.Errorf("Azure storage profiles require an endpoint to extract storage account name")
	}

	storageAccount := extractStorageAccountFromEndpoint(endpoint)
	secretName := "secret_" + strings.ReplaceAll(container, "-", "_")

	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "CREATE OR REPLACE SECRET %s (\n", quoteIdent(secretName))
	_, _ = fmt.Fprintf(&b, "  TYPE azure,\n")

	switch authType {
	case "service_principal":
		clientId := os.Getenv("AZURE_CLIENT_ID")
		clientSecret := os.Getenv("AZURE_CLIENT_SECRET")
		tenantId := os.Getenv("AZURE_TENANT_ID")

		if clientId == "" || clientSecret == "" || tenantId == "" {
			return fmt.Errorf("missing Azure service principal credentials: AZURE_CLIENT_ID/AZURE_CLIENT_SECRET/AZURE_TENANT_ID")
		}

		_, _ = fmt.Fprintf(&b, "  PROVIDER service_principal,\n")
		_, _ = fmt.Fprintf(&b, "  TENANT_ID '%s',\n", escapeSingle(tenantId))
		_, _ = fmt.Fprintf(&b, "  CLIENT_ID '%s',\n", escapeSingle(clientId))
		_, _ = fmt.Fprintf(&b, "  CLIENT_SECRET '%s',\n", escapeSingle(clientSecret))
		_, _ = fmt.Fprintf(&b, "  ACCOUNT_NAME '%s'\n", escapeSingle(storageAccount))

	case "connection_string":
		connectionString := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
		if connectionString == "" {
			return fmt.Errorf("missing Azure connection string: AZURE_STORAGE_CONNECTION_STRING")
		}

		_, _ = fmt.Fprintf(&b, "  PROVIDER connection_string,\n")
		_, _ = fmt.Fprintf(&b, "  CONNECTION_STRING '%s'\n", escapeSingle(connectionString))

	default:
		// For managed identity, workload identity, credential_chain, etc.
		_, _ = fmt.Fprintf(&b, "  PROVIDER credential_chain,\n")
		_, _ = fmt.Fprintf(&b, "  ACCOUNT_NAME '%s'\n", escapeSingle(storageAccount))
	}

	_, _ = fmt.Fprintf(&b, ");")

	duckdbDDLMu.Lock()
	_, err := conn.ExecContext(ctx, b.String())
	duckdbDDLMu.Unlock()
	return err
}

// Extract storage account name from Azure Blob endpoint
func extractStorageAccountFromEndpoint(endpoint string) string {
	// Remove protocol
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")

	// Extract storage account name (first part before the dot)
	if idx := strings.Index(endpoint, "."); idx > 0 {
		return endpoint[:idx]
	}

	return endpoint
}

// CREATE OR REPLACE SECRET for AWS S3 (serialized).
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
