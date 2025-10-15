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

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/marcboeker/go-duckdb/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Global mutex to serialize extension/secret DDL across the process.
// DuckDB extension loading & DDL may crash when done concurrently in many engines.
var duckdbDDLMu sync.Mutex

// S3DB manages a pool of DuckDB connections to a single shared on-disk database.
// All connections open the same file and thus share the same in-process database instance.
// Credentials are set per-bucket on each connection acquisition to support multiple buckets.
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

	// Single global pool
	pool *connectionPool

	// metrics
	metricsPeriod time.Duration
	metricsCtx    context.Context
	metricsCancel context.CancelFunc
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

// s3DBConfig holds configuration options for S3DB
type s3DBConfig struct {
	dbPath        *string
	metricsPeriod time.Duration
	metricsCtx    context.Context
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

	slog.Info("duckdbx: single shared database",
		"type", "file",
		"dbPath", dbPath,
		"memoryLimitMB", memoryMB,
		"tempDir", os.Getenv("DUCKDB_TEMP_DIRECTORY"),
		"maxTempSize", os.Getenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE"),
		"poolSize", poolSize,
		"threads", threads)

	s3db := &S3DB{
		dbPath:         dbPath,
		cleanupOnClose: cleanupOnClose,
		memoryLimitMB:  memoryMB,
		tempDir:        os.Getenv("DUCKDB_TEMP_DIRECTORY"),
		maxTempSize:    os.Getenv("DUCKDB_MAX_TEMP_DIRECTORY_SIZE"),
		poolSize:       poolSize,
		threads:        threads,
		metricsPeriod:  cfg.metricsPeriod,
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
// This can be useful for cleanup or debugging purposes.
func (s *S3DB) GetDatabasePath() string {
	return s.dbPath
}

// GetConnection returns a connection for local database queries (no S3 authentication).
func (s *S3DB) GetConnection(ctx context.Context) (*sql.Conn, func(), error) {
	return s.pool.acquireLocal(ctx)
}

// GetConnectionForBucket returns a connection configured with S3 credentials for the specified bucket.
func (s *S3DB) GetConnectionForBucket(ctx context.Context, bucket, region string, endpoint string, cloudProvider string) (*sql.Conn, func(), error) {
	if bucket == "" {
		return nil, nil, fmt.Errorf("bucket is required")
	}
	return s.pool.acquireForBucket(ctx, bucket, region, endpoint, cloudProvider)
}

// acquireLocal gets a connection for local database queries (no S3 authentication needed).
func (p *connectionPool) acquireLocal(ctx context.Context) (*sql.Conn, func(), error) {
	// Ensure extensions are loaded and database is set up before any connection is used
	if err := p.parent.ensureSetup(ctx); err != nil {
		return nil, nil, err
	}

	select {
	case pc := <-p.ch:
		return pc.conn, func() { p.release(pc) }, nil
	default:
	}

	p.mu.Lock()
	canCreate := p.cur < p.size
	if canCreate {
		p.cur++
	}
	p.mu.Unlock()

	if canCreate {
		pc, err := p.newConnLocal(ctx)
		if err != nil {
			p.mu.Lock()
			p.cur--
			p.mu.Unlock()
			return nil, nil, err
		}
		return pc.conn, func() { p.release(pc) }, nil
	}

	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case pc := <-p.ch:
		return pc.conn, func() { p.release(pc) }, nil
	}
}

// acquireForBucket gets a connection configured with S3 credentials for the specified bucket.
func (p *connectionPool) acquireForBucket(ctx context.Context, bucket, region, endpoint string, cloudProvider string) (*sql.Conn, func(), error) {
	// Ensure extensions are loaded and database is set up before any connection is used
	if err := p.parent.ensureSetup(ctx); err != nil {
		return nil, nil, err
	}

	// Helper function to ensure credentials are set for the bucket
	ensureCredentials := func(conn *sql.Conn) error {
		// Create/replace the secret for this bucket
		// This is idempotent - CREATE OR REPLACE will update if needed
		return seedCloudSecretFromEnv(ctx, conn, bucket, region, endpoint, cloudProvider)
	}

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

	p.mu.Lock()
	canCreate := p.cur < p.size
	if canCreate {
		p.cur++
	}
	p.mu.Unlock()

	if canCreate {
		pc, err := p.newConnForBucket(ctx, bucket, region, endpoint, cloudProvider)
		if err != nil {
			p.mu.Lock()
			p.cur--
			p.mu.Unlock()
			return nil, nil, err
		}
		return pc.conn, func() { p.release(pc) }, nil
	}

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
	// Return to pool
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

// newConnLocal creates a new connection for local database queries (no S3 authentication).
func (p *connectionPool) newConnLocal(ctx context.Context) (*pooledConn, error) {
	return p.newConn(ctx)
}

// newConnForBucket creates a new connection configured with S3 credentials for the specified bucket.
func (p *connectionPool) newConnForBucket(ctx context.Context, bucket, region, endpoint string, cloudProvider string) (*pooledConn, error) {
	pc, err := p.newConn(ctx)
	if err != nil {
		return nil, err
	}

	if err := seedCloudSecretFromEnv(ctx, pc.conn, bucket, region, endpoint, cloudProvider); err != nil {
		_ = pc.conn.Close()
		_ = pc.db.Close()
		return nil, err
	}

	return pc, nil
}

// newConn creates a new pooled connection with standard configuration.
// This is the common implementation used by both newConnLocal and newConnForBucket.
func (p *connectionPool) newConn(ctx context.Context) (*pooledConn, error) {
	// Ensure extensions are installed and database-wide settings are applied (once)
	if err := p.parent.ensureSetup(ctx); err != nil {
		return nil, err
	}

	// Create a connector with a boot function that runs on each new physical connection
	connector, err := duckdb.NewConnector(p.parent.dbPath, func(execer driver.ExecerContext) error {
		// FIRST: Disable automatic extension loading/downloading before anything else
		// This prevents Azure and other extensions from trying to auto-install
		if _, err := execer.ExecContext(ctx, "SET autoinstall_known_extensions = false;", nil); err != nil {
			slog.Warn("Failed to disable automatic extension installation", "error", err)
		}
		if _, err := execer.ExecContext(ctx, "SET autoload_known_extensions = false;", nil); err != nil {
			slog.Warn("Failed to disable automatic extension loading", "error", err)
		}

		// CRITICAL: Set memory limit on EVERY connection
		// DuckDB's memory_limit is global but new connections don't inherit it
		if p.parent.memoryLimitMB > 0 {
			if _, err := execer.ExecContext(ctx, fmt.Sprintf("SET memory_limit='%dMB';", p.parent.memoryLimitMB), nil); err != nil {
				slog.Warn("Failed to set memory_limit on connection", "error", err)
			}
		}

		// Load extensions for this connection - they must be loaded per-connection
		if err := p.parent.loadExtensionsWithExecer(ctx, execer); err != nil {
			return fmt.Errorf("load extensions for connection: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create connector: %w", err)
	}
	// Don't close the connector - it needs to stay open for the DB handle

	// Open a database handle using the connector
	db := sql.OpenDB(connector)
	// one physical connection per DB handle
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return &pooledConn{
		conn: conn,
		db:   db,
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

		// FIRST: Disable automatic extension loading/downloading
		// This must be done before any extension-related operations
		if _, err := conn.ExecContext(ctx, "SET autoinstall_known_extensions = false;"); err != nil {
			slog.Warn("Failed to disable automatic extension installation", "error", err)
		}
		if _, err := conn.ExecContext(ctx, "SET autoload_known_extensions = false;"); err != nil {
			slog.Warn("Failed to disable automatic extension loading", "error", err)
		}

		// Set extension_directory to prevent loading from ~/.duckdb/extensions
		// Use the same directory as our database file or the configured extensions path
		extensionDir := os.Getenv("LAKERUNNER_EXTENSIONS_PATH")
		if extensionDir == "" {
			// Use a subdirectory next to the database file
			extensionDir = filepath.Join(filepath.Dir(s.dbPath), "extensions")
		}
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("SET extension_directory='%s';", escapeSingle(extensionDir))); err != nil {
			slog.Warn("Failed to set extension_directory", "error", err)
		}

		// Set home_directory to prevent using ~/.duckdb
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
	// Wrap sql.Conn to implement driver.ExecerContext
	execer := &connExecer{conn: conn, ctx: ctx}
	return s.loadExtensionsWithExecer(ctx, execer)
}

// loadExtensionsWithExecer loads extensions using a driver.ExecerContext interface.
// This is used in the boot function when creating new connections.
// Extensions are loaded per-connection since DuckDB doesn't share loaded extensions across connections.
func (s *S3DB) loadExtensionsWithExecer(ctx context.Context, execer driver.ExecerContext) error {
	// Get extensions path
	base := os.Getenv("LAKERUNNER_EXTENSIONS_PATH")
	if base == "" {
		base = discoverExtensionsPath()
		if base == "" {
			return fmt.Errorf("extensions required but not found: LAKERUNNER_EXTENSIONS_PATH not set")
		}
	}

	// Define extensions to load
	extensions := []string{"httpfs", "aws", "azure"}

	// Load each extension
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
	// Convert to sql.Exec call
	result, err := c.conn.ExecContext(ctx, query)
	return result, err
}

// CREATE OR REPLACE SECRET for a bucket (serialized).
// Detects cloud provider and credentials source, then creates appropriate secret.
func seedCloudSecretFromEnv(ctx context.Context, conn *sql.Conn, bucket, region string, endpoint string, cloudProvider string) error {
	// Check for Azure credentials first
	if hasAzureCredentials() {
		return seedAzureSecretFromEnv(ctx, conn, bucket, region, endpoint)
	}

	// Determine if we should use AWS SDK credentials (real AWS) or env vars (S3-compatible)
	// Real AWS is identified by: cloudProvider == "aws" AND no custom endpoint
	useAWSSDK := cloudProvider == "aws" && endpoint == ""

	if useAWSSDK {
		// Use AWS SDK credential chain for real AWS (auto-refreshing credentials)
		return seedS3SecretFromAWSSDK(ctx, conn, bucket, region, endpoint)
	}

	// Fall back to environment variables for S3-compatible systems (MinIO, LocalStack, etc.)
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

// S3SecretConfig holds the configuration for creating an S3 secret in DuckDB.
type S3SecretConfig struct {
	Bucket       string
	Region       string
	Endpoint     string
	KeyID        string
	Secret       string
	SessionToken string
	URLStyle     string
	UseSSL       bool
}

// seedS3SecretFromAWSSDK fetches S3 credentials from AWS SDK credential chain and creates a DuckDB secret.
// This is used for real AWS (not S3-compatible systems) to get auto-refreshing credentials.
func seedS3SecretFromAWSSDK(ctx context.Context, conn *sql.Conn, bucket, region string, endpoint string) error {
	// Load AWS config (auto-refreshing credentials)
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	// Retrieve current credentials
	creds, err := cfg.Credentials.Retrieve(ctx)
	if err != nil {
		return fmt.Errorf("retrieve AWS credentials: %w", err)
	}

	// Determine region and endpoint
	if region == "" {
		region = cfg.Region
		if region == "" {
			region = "us-east-1"
		}
	}

	useSSL := true
	if endpoint == "" {
		endpoint = fmt.Sprintf("s3.%s.amazonaws.com", region)
	} else {
		if after, ok := strings.CutPrefix(endpoint, "http://"); ok {
			endpoint = after
			useSSL = false
		} else if after, ok = strings.CutPrefix(endpoint, "https://"); ok {
			endpoint = after
			useSSL = true
		}
	}

	// Use the credentials to create DuckDB secret
	secretConfig := S3SecretConfig{
		Bucket:       bucket,
		Region:       region,
		Endpoint:     endpoint,
		KeyID:        creds.AccessKeyID,
		Secret:       creds.SecretAccessKey,
		SessionToken: creds.SessionToken,
		URLStyle:     "path",
		UseSSL:       useSSL,
	}

	return createS3Secret(ctx, conn, secretConfig)
}

// seedS3SecretFromEnv fetches S3 credentials from environment and creates a DuckDB secret.
func seedS3SecretFromEnv(ctx context.Context, conn *sql.Conn, bucket, region string, endpoint string) error {
	// Check for S3_ prefixed credentials first, then fall back to AWS_ prefix
	keyID := os.Getenv("S3_ACCESS_KEY_ID")
	if keyID == "" {
		keyID = os.Getenv("AWS_ACCESS_KEY_ID")
	}

	secret := os.Getenv("S3_SECRET_ACCESS_KEY")
	if secret == "" {
		secret = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}

	if keyID == "" || secret == "" {
		return fmt.Errorf("missing S3/AWS credentials: S3_ACCESS_KEY_ID/AWS_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY/AWS_SECRET_ACCESS_KEY are required")
	}

	session := os.Getenv("S3_SESSION_TOKEN")
	if session == "" {
		session = os.Getenv("AWS_SESSION_TOKEN")
	}

	if region == "" {
		// Check S3_REGION first, then AWS_REGION, then AWS_DEFAULT_REGION
		if r := os.Getenv("S3_REGION"); r != "" {
			region = r
		} else if r := os.Getenv("AWS_REGION"); r != "" {
			region = r
		} else if r := os.Getenv("AWS_DEFAULT_REGION"); r != "" {
			region = r
		} else {
			region = "us-east-1"
		}
	}

	useSSL := true
	if endpoint == "" {
		endpoint = fmt.Sprintf("s3.%s.amazonaws.com", region)
	} else {
		if after, ok := strings.CutPrefix(endpoint, "http://"); ok {
			endpoint = after
			useSSL = false
		} else if after, ok = strings.CutPrefix(endpoint, "https://"); ok {
			endpoint = after
			useSSL = true
		}
	}

	// Check for S3_URL_STYLE first, then fall back to AWS_S3_URL_STYLE
	urlStyle := os.Getenv("S3_URL_STYLE")
	if urlStyle == "" {
		urlStyle = os.Getenv("AWS_S3_URL_STYLE")
	}
	if urlStyle == "" {
		urlStyle = "path"
	}

	config := S3SecretConfig{
		Bucket:       bucket,
		Region:       region,
		Endpoint:     endpoint,
		KeyID:        keyID,
		Secret:       secret,
		SessionToken: session,
		URLStyle:     urlStyle,
		UseSSL:       useSSL,
	}

	return createS3Secret(ctx, conn, config)
}

// createS3Secret creates or replaces an S3 secret in DuckDB with the given configuration.
// This function is testable as it doesn't depend on environment variables.
func createS3Secret(ctx context.Context, conn *sql.Conn, config S3SecretConfig) error {
	if config.KeyID == "" || config.Secret == "" {
		return fmt.Errorf("missing AWS credentials: KeyID and Secret are required")
	}
	if config.Bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	if config.Region == "" {
		config.Region = "us-east-1"
	}
	if config.URLStyle == "" {
		config.URLStyle = "path"
	}

	secretName := "secret_" + strings.ReplaceAll(config.Bucket, "-", "_")
	useSSLStr := "false"
	if config.UseSSL {
		useSSLStr = "true"
	}

	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "CREATE OR REPLACE SECRET %s (\n", quoteIdent(secretName))
	_, _ = fmt.Fprintf(&b, "  TYPE S3,\n")
	_, _ = fmt.Fprintf(&b, "  PROVIDER credential_chain,\n")
	_, _ = fmt.Fprintf(&b, "  REFRESH auto,\n")
	_, _ = fmt.Fprintf(&b, "  ENDPOINT '%s',\n", escapeSingle(config.Endpoint))
	_, _ = fmt.Fprintf(&b, "  URL_STYLE '%s',\n", escapeSingle(config.URLStyle))
	_, _ = fmt.Fprintf(&b, "  USE_SSL '%s',\n", escapeSingle(useSSLStr))
	_, _ = fmt.Fprintf(&b, "  REGION '%s',\n", escapeSingle(config.Region))
	_, _ = fmt.Fprintf(&b, "  SCOPE 's3://%s'\n", escapeSingle(config.Bucket))
	_, _ = fmt.Fprintf(&b, ");")

	duckdbDDLMu.Lock()
	_, err := conn.ExecContext(ctx, b.String())
	duckdbDDLMu.Unlock()

	return err
}

// discoverExtensionsPath attempts to find DuckDB extensions in the repository.
// This is primarily for testing purposes when LAKERUNNER_EXTENSIONS_PATH is not set.
// It walks up from the current directory looking for docker/duckdb-extensions.
func discoverExtensionsPath() string {
	// Start from current working directory
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}

	// Determine platform-specific subdirectory
	platform := getPlatformDir()
	if platform == "" {
		return ""
	}

	// Walk up the directory tree looking for the extensions
	for {
		// Check if docker/duckdb-extensions/platform exists
		extensionsPath := filepath.Join(dir, "docker", "duckdb-extensions", platform)
		if info, err := os.Stat(extensionsPath); err == nil && info.IsDir() {
			// Verify at least one extension exists
			httpfsPath := filepath.Join(extensionsPath, "httpfs.duckdb_extension")
			if _, err := os.Stat(httpfsPath); err == nil {
				return extensionsPath
			}
		}

		// Move up one directory
		parent := filepath.Dir(dir)
		if parent == dir {
			// Reached root
			break
		}
		dir = parent

		// Safety check: don't go too far up (max 10 levels)
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
		// Unsupported platform
		return ""
	}
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

// pollMemoryMetrics periodically polls DuckDB memory statistics and records them as OpenTelemetry metrics
func (s *S3DB) pollMemoryMetrics(ctx context.Context) {
	// Import metrics package constructs
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
		// Get a connection from the pool
		conn, release, err := s.GetConnection(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// Context was cancelled, exit gracefully
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

		// Get memory statistics
		stats, err := GetDuckDBMemoryStats(conn)
		release() // Release the connection back to the pool

		if err != nil {
			slog.Error("failed to get memory stats", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.metricsPeriod):
				continue
			}
		}

		// Record metrics for each database (usually just one)
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
