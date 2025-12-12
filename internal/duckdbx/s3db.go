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
	"hash/fnv"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/marcboeker/go-duckdb/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

// Global mutex to serialize extension loading across the process.
// DuckDB extension loading may crash when done concurrently in many engines.
// Note: Secret DDL uses per-S3DB, per-identity locks instead.
var duckdbDDLMu sync.Mutex

// credentialRefreshBuffer is subtracted from credential expiration to ensure
// we refresh before the actual expiry.
const credentialRefreshBuffer = 5 * time.Minute

// bucketSecretState tracks when credentials expire for a given identity key.
// Credentials are cached to avoid repeated AWS SDK calls, but the DuckDB secret
// must still be created on every connection since secrets are in-memory only.
type bucketSecretState struct {
	mu          sync.Mutex
	expiresAt   time.Time       // zero means not yet fetched, neverExpiresSentinel means env creds
	cachedCreds aws.Credentials // cached credentials to reuse when not expired
}

// seedResult holds the result of seeding credentials.
type seedResult struct {
	expiresAt time.Time
	creds     aws.Credentials
}

// S3DB manages a pool of DuckDB connections to a single shared on-disk database.
// All connections open the same file and thus share the same in-process database instance.
// Credentials are set per-bucket/profile on each connection acquisition to support multiple buckets.
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

	// Secret cache is PER S3DB instance to avoid cross-DBPath contamination.
	secretCache sync.Map // identityKey -> *bucketSecretState
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

// GetConnection returns a connection for local database queries (no S3 authentication).
func (s *S3DB) GetConnection(ctx context.Context) (*sql.Conn, func(), error) {
	return s.pool.acquire(ctx, nil)
}

// GetConnectionForBucket returns a connection configured with S3/Azure credentials
// for the specified storage profile.
func (s *S3DB) GetConnectionForBucket(ctx context.Context, profile storageprofile.StorageProfile) (*sql.Conn, func(), error) {
	if profile.Bucket == "" {
		return nil, nil, fmt.Errorf("bucket is required")
	}
	ensure := func(pc *pooledConn) error {
		return s.seedCloudSecretFromEnv(ctx, pc.conn, profile)
	}
	return s.pool.acquire(ctx, ensure)
}

// ---------- connectionPool implementation ----------

func (p *connectionPool) acquire(ctx context.Context, ensure func(*pooledConn) error) (*sql.Conn, func(), error) {
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
			if ensure != nil {
				if err := ensure(pc); err != nil {
					p.destroy(pc)
					// try to create a fresh one
					return p.createAndEnsure(ctx, ensure)
				}
			}
			return pc.conn, func() { p.release(pc) }, nil
		}
	default:
	}

	// Try to create a new one if capacity allows
	return p.createAndEnsure(ctx, ensure)
}

func (p *connectionPool) createAndEnsure(ctx context.Context, ensure func(*pooledConn) error) (*sql.Conn, func(), error) {
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
		if ensure != nil {
			if err := ensure(pc); err != nil {
				p.destroy(pc)
				return nil, nil, err
			}
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
			return p.createAndEnsure(ctx, ensure)
		}
		if ensure != nil {
			if err := ensure(pc); err != nil {
				p.destroy(pc)
				// After destroy, capacity exists -> try create again
				return p.createAndEnsure(ctx, ensure)
			}
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

// ---------- Secrets (AWS/Azure) ----------

// seedCloudSecretFromEnv is the public entry point that uses per-S3DB caching.
// Detects cloud provider and credentials source, then creates appropriate secret.
func (s *S3DB) seedCloudSecretFromEnv(ctx context.Context, conn *sql.Conn, profile storageprofile.StorageProfile) error {
	return s.ensureBucketSecret(ctx, conn, profile)
}

// ensureBucketSecret checks if the secret for a profile identity needs credential refresh,
// and creates/replaces it. DuckDB secrets must be created on every connection since they
// are in-memory only and not shared across different connector instances.
// Uses per-identity locking to serialize credential fetching (especially for AssumeRole).
func (s *S3DB) ensureBucketSecret(ctx context.Context, conn *sql.Conn, profile storageprofile.StorageProfile) error {
	if strings.TrimSpace(profile.Bucket) == "" {
		return fmt.Errorf("bucket is required")
	}

	key := secretIdentityKey(profile)

	stateAny, _ := s.secretCache.LoadOrStore(key, &bucketSecretState{})
	state := stateAny.(*bucketSecretState)

	state.mu.Lock()
	defer state.mu.Unlock()

	// Check if credentials need refresh.
	// Note: We still need to create the secret on THIS connection even if credentials are cached,
	// because DuckDB secrets are in-memory and may not be visible across different connectors.
	needsCredentialRefresh := true
	if state.expiresAt.Equal(neverExpiresSentinel) {
		needsCredentialRefresh = false
	} else if !state.expiresAt.IsZero() && time.Now().Before(state.expiresAt.Add(-credentialRefreshBuffer)) {
		needsCredentialRefresh = false
	}

	expiresAt, err := seedCloudSecretFromEnvLocked(ctx, conn, profile, needsCredentialRefresh, state.cachedCreds)
	if err != nil {
		return err
	}

	state.expiresAt = expiresAt.expiresAt
	state.cachedCreds = expiresAt.creds
	return nil
}

// neverExpiresSentinel is used to mark credentials that never expire.
var neverExpiresSentinel = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)

// seedCloudSecretFromEnvLocked does the actual secret creation.
// If needsCredentialRefresh is false and cachedCreds is valid, reuses cached credentials.
// Otherwise fetches fresh credentials.
// Always creates the DuckDB secret on the connection since secrets are in-memory only.
// Caller must hold the per-identity lock.
func seedCloudSecretFromEnvLocked(ctx context.Context, conn *sql.Conn, profile storageprofile.StorageProfile, needsCredentialRefresh bool, cachedCreds aws.Credentials) (seedResult, error) {
	if hasAzureCredentials() || profile.CloudProvider == "azure" {
		return seedAzureSecretFromEnvLocked(ctx, conn, profile)
	}

	// Real AWS is identified by: cloudProvider == "aws" AND no custom endpoint
	useAWSSDK := profile.CloudProvider == "aws" && strings.TrimSpace(profile.Endpoint) == ""

	if useAWSSDK {
		return seedS3SecretFromAWSSDKLocked(ctx, conn, profile, needsCredentialRefresh, cachedCreds)
	}

	return seedS3SecretFromEnvLocked(ctx, conn, profile)
}

// Check if Azure credentials are available
func hasAzureCredentials() bool {
	authType := os.Getenv("AZURE_AUTH_TYPE")
	return authType != ""
}

// seedAzureSecretFromEnvLocked creates an Azure secret.
// Azure credentials from env vars are treated as non-expiring.
// Caller must hold the per-identity lock.
func seedAzureSecretFromEnvLocked(ctx context.Context, conn *sql.Conn, profile storageprofile.StorageProfile) (seedResult, error) {
	authType := os.Getenv("AZURE_AUTH_TYPE")
	if authType == "" {
		authType = "credential_chain"
	}

	if strings.TrimSpace(profile.Endpoint) == "" {
		return seedResult{}, fmt.Errorf("azure storage profiles require an endpoint to extract storage account name")
	}

	storageAccount := extractStorageAccountFromEndpoint(profile.Endpoint)
	secretName := secretNameFromProfile(profile)

	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "CREATE OR REPLACE SECRET %s (\n", quoteIdent(secretName))
	_, _ = fmt.Fprintf(&b, "  TYPE azure,\n")

	switch authType {
	case "service_principal":
		clientId := os.Getenv("AZURE_CLIENT_ID")
		clientSecret := os.Getenv("AZURE_CLIENT_SECRET")
		tenantId := os.Getenv("AZURE_TENANT_ID")

		if clientId == "" || clientSecret == "" || tenantId == "" {
			return seedResult{}, fmt.Errorf("missing Azure service principal credentials: AZURE_CLIENT_ID/AZURE_CLIENT_SECRET/AZURE_TENANT_ID")
		}

		_, _ = fmt.Fprintf(&b, "  PROVIDER service_principal,\n")
		_, _ = fmt.Fprintf(&b, "  TENANT_ID '%s',\n", escapeSingle(tenantId))
		_, _ = fmt.Fprintf(&b, "  CLIENT_ID '%s',\n", escapeSingle(clientId))
		_, _ = fmt.Fprintf(&b, "  CLIENT_SECRET '%s',\n", escapeSingle(clientSecret))
		_, _ = fmt.Fprintf(&b, "  ACCOUNT_NAME '%s'\n", escapeSingle(storageAccount))

	case "connection_string":
		connectionString := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
		if connectionString == "" {
			return seedResult{}, fmt.Errorf("missing Azure connection string: AZURE_STORAGE_CONNECTION_STRING")
		}

		_, _ = fmt.Fprintf(&b, "  PROVIDER connection_string,\n")
		_, _ = fmt.Fprintf(&b, "  CONNECTION_STRING '%s'\n", escapeSingle(connectionString))

	default:
		_, _ = fmt.Fprintf(&b, "  PROVIDER credential_chain,\n")
		_, _ = fmt.Fprintf(&b, "  ACCOUNT_NAME '%s'\n", escapeSingle(storageAccount))
	}

	_, _ = fmt.Fprintf(&b, ");")

	_, err := conn.ExecContext(ctx, b.String())
	if err != nil {
		return seedResult{}, err
	}
	// Azure env creds don't have explicit expiration - treat as never expiring
	return seedResult{expiresAt: neverExpiresSentinel}, nil
}

// Extract storage account name from Azure Blob endpoint
func extractStorageAccountFromEndpoint(endpoint string) string {
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")

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

// seedS3SecretFromAWSSDKLocked uses AWS chain/AssumeRole and creates a DuckDB secret.
// If needsCredentialRefresh is false, reuses cachedCreds instead of fetching new ones.
// Always creates the DuckDB secret on the connection.
// Caller must hold the per-identity lock.
func seedS3SecretFromAWSSDKLocked(ctx context.Context, conn *sql.Conn, profile storageprofile.StorageProfile, needsCredentialRefresh bool, cachedCreds aws.Credentials) (seedResult, error) {
	var creds aws.Credentials
	var err error

	if needsCredentialRefresh || cachedCreds.AccessKeyID == "" {
		// Fetch fresh credentials from AWS SDK
		creds, err = getAWSCredsFromChainOrAssume(ctx, profile)
		if err != nil {
			return seedResult{}, err
		}
	} else {
		// Reuse cached credentials
		creds = cachedCreds
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return seedResult{}, fmt.Errorf("load AWS config: %w", err)
	}

	region := strings.TrimSpace(profile.Region)
	if region == "" {
		region = cfg.Region
		if region == "" {
			region = "us-east-1"
		}
	}

	useSSL := true
	endpoint := strings.TrimSpace(profile.Endpoint)
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

	secretConfig := S3SecretConfig{
		Bucket:       profile.Bucket,
		Region:       region,
		Endpoint:     endpoint,
		KeyID:        creds.AccessKeyID,
		Secret:       creds.SecretAccessKey,
		SessionToken: creds.SessionToken,
		URLStyle:     "path",
		UseSSL:       useSSL,
	}

	if err := createS3SecretWithCredsLocked(ctx, conn, secretConfig, creds, profile); err != nil {
		return seedResult{}, err
	}

	// Return the credentials and expiration time
	var expiresAt time.Time
	if creds.CanExpire {
		expiresAt = creds.Expires
	} else {
		expiresAt = neverExpiresSentinel
	}
	return seedResult{expiresAt: expiresAt, creds: creds}, nil
}

// seedS3SecretFromEnvLocked uses env creds for S3-compatible systems.
// Environment credentials are treated as non-expiring.
// Caller must hold the per-identity lock.
func seedS3SecretFromEnvLocked(ctx context.Context, conn *sql.Conn, profile storageprofile.StorageProfile) (seedResult, error) {
	keyID := os.Getenv("S3_ACCESS_KEY_ID")
	if keyID == "" {
		keyID = os.Getenv("AWS_ACCESS_KEY_ID")
	}

	secret := os.Getenv("S3_SECRET_ACCESS_KEY")
	if secret == "" {
		secret = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}

	if keyID == "" || secret == "" {
		return seedResult{}, fmt.Errorf("missing S3/AWS credentials: S3_ACCESS_KEY_ID/AWS_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY/AWS_SECRET_ACCESS_KEY are required")
	}

	session := os.Getenv("S3_SESSION_TOKEN")
	if session == "" {
		session = os.Getenv("AWS_SESSION_TOKEN")
	}

	region := strings.TrimSpace(profile.Region)
	if region == "" {
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
	endpoint := strings.TrimSpace(profile.Endpoint)
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

	urlStyle := os.Getenv("S3_URL_STYLE")
	if urlStyle == "" {
		urlStyle = os.Getenv("AWS_S3_URL_STYLE")
	}
	if urlStyle == "" {
		urlStyle = "path"
	}

	secretConfig := S3SecretConfig{
		Bucket:       profile.Bucket,
		Region:       region,
		Endpoint:     endpoint,
		KeyID:        keyID,
		Secret:       secret,
		SessionToken: session,
		URLStyle:     urlStyle,
		UseSSL:       useSSL,
	}

	creds := aws.Credentials{
		AccessKeyID:     keyID,
		SecretAccessKey: secret,
		SessionToken:    session,
		Source:          "env",
		CanExpire:       false,
	}

	if err := createS3SecretWithCredsLocked(ctx, conn, secretConfig, creds, profile); err != nil {
		return seedResult{}, err
	}
	// Environment credentials don't expire
	return seedResult{expiresAt: neverExpiresSentinel, creds: creds}, nil
}

// chooseSTSRegion picks a region for STS calls. Prefer profile.Region, otherwise env/defaults.
func chooseSTSRegion(ctx context.Context, profile storageprofile.StorageProfile) (string, error) {
	if r := strings.TrimSpace(profile.Region); r != "" {
		return r, nil
	}
	cfg, err := config.LoadDefaultConfig(ctx)
	if err == nil && cfg.Region != "" {
		return cfg.Region, nil
	}
	return "us-east-1", nil
}

// getAWSCredsFromChainOrAssume returns credentials using the default chain,
// or assumes the provided role with externalId if profile.Role is non-empty.
func getAWSCredsFromChainOrAssume(ctx context.Context, profile storageprofile.StorageProfile) (aws.Credentials, error) {
	region, _ := chooseSTSRegion(ctx, profile)
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("load AWS config: %w", err)
	}

	roleArn := strings.TrimSpace(profile.Role)
	if roleArn == "" {
		creds, err := cfg.Credentials.Retrieve(ctx)
		if err != nil {
			return aws.Credentials{}, fmt.Errorf("retrieve default chain credentials: %w", err)
		}
		return creds, nil
	}

	extID := strings.TrimSpace(profile.OrganizationID.String())
	stsClient := sts.NewFromConfig(cfg)
	out, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         &roleArn,
		RoleSessionName: aws.String(fmt.Sprintf("CardinalHQ-%d", time.Now().Unix())),
		ExternalId:      aws.String(extID),
		DurationSeconds: aws.Int32(3600),
	})
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("assume role %s: %w", roleArn, err)
	}
	c := out.Credentials
	return aws.Credentials{
		AccessKeyID:     aws.ToString(c.AccessKeyId),
		SecretAccessKey: aws.ToString(c.SecretAccessKey),
		SessionToken:    aws.ToString(c.SessionToken),
		Source:          "AssumeRole",
		CanExpire:       true,
		Expires:         aws.ToTime(c.Expiration),
	}, nil
}

// createS3SecretWithCredsLocked builds and executes DuckDB SECRET DDL
// using the provided explicit credentials.
// Caller must hold the per-identity lock.
func createS3SecretWithCredsLocked(
	ctx context.Context,
	conn *sql.Conn,
	config S3SecretConfig,
	creds aws.Credentials,
	profile storageprofile.StorageProfile,
) error {
	if strings.TrimSpace(config.Bucket) == "" {
		return fmt.Errorf("bucket is required")
	}
	if strings.TrimSpace(config.Region) == "" {
		config.Region = "us-east-1"
	}
	if strings.TrimSpace(config.URLStyle) == "" {
		config.URLStyle = "path"
	}
	if creds.AccessKeyID == "" || creds.SecretAccessKey == "" {
		return fmt.Errorf("missing access key/secret for secret creation")
	}

	endpointHost := config.Endpoint
	useSSL := config.UseSSL
	slog.Info("Creating S3 secret", "endpoint", endpointHost, "useSSL", useSSL, "bucket", config.Bucket)

	secretName := secretNameFromProfile(profile)

	useSSLStr := "false"
	if useSSL {
		useSSLStr = "true"
	}

	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "CREATE OR REPLACE SECRET %s (\n", quoteIdent(secretName))
	_, _ = fmt.Fprintf(&b, "  TYPE S3,\n")
	_, _ = fmt.Fprintf(&b, "  ENDPOINT '%s',\n", escapeSingle(endpointHost))
	_, _ = fmt.Fprintf(&b, "  URL_STYLE '%s',\n", escapeSingle(config.URLStyle))
	_, _ = fmt.Fprintf(&b, "  USE_SSL %s,\n", useSSLStr)
	_, _ = fmt.Fprintf(&b, "  KEY_ID '%s',\n", escapeSingle(creds.AccessKeyID))
	_, _ = fmt.Fprintf(&b, "  SECRET '%s',\n", escapeSingle(creds.SecretAccessKey))
	if creds.SessionToken != "" {
		_, _ = fmt.Fprintf(&b, "  SESSION_TOKEN '%s',\n", escapeSingle(creds.SessionToken))
	}
	_, _ = fmt.Fprintf(&b, "  REGION '%s',\n", escapeSingle(config.Region))
	_, _ = fmt.Fprintf(&b, "  SCOPE 's3://%s'\n", escapeSingle(config.Bucket))
	_, _ = fmt.Fprintf(&b, ");")

	_, err := conn.ExecContext(ctx, b.String())
	return err
}

// ---------- identity + secret naming ----------

func secretIdentityKey(p storageprofile.StorageProfile) string {
	parts := []string{
		strings.TrimSpace(p.CloudProvider),
		strings.TrimSpace(p.Bucket),
		strings.TrimSpace(p.Endpoint),
		strings.TrimSpace(p.Region),
		strings.TrimSpace(p.Role),
		p.OrganizationID.String(),
	}
	return strings.Join(parts, "|")
}

func shortHash(s string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum64())
}

// Secret name used within DuckDB.
// Include a short hash of the identity key to avoid collisions when
// multiple roles/endpoints/tenants target the same bucket in one process.
func secretNameFromProfile(p storageprofile.StorageProfile) string {
	b := strings.ReplaceAll(strings.TrimSpace(p.Bucket), "-", "_")
	if b == "" {
		b = "bucket"
	}
	key := secretIdentityKey(p)
	h := shortHash(key)
	if len(h) > 8 {
		h = h[:8]
	}
	return "secret_" + b + "_" + h
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
