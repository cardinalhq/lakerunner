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

package migrations

import (
	"context"
	"embed"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/pgx"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// GetMigrationFiles returns the embedded migration files for version checking
func GetMigrationFiles() embed.FS {
	return migrationFiles
}

// CheckExpectedVersion verifies that the lrdb database is at the expected migration version
func CheckExpectedVersion(ctx context.Context, pool *pgxpool.Pool) error {
	return CheckExpectedVersionWithOptions(ctx, pool, false)
}

// CheckExpectedVersionWithOptions verifies that the lrdb database is at the expected migration version
// If warnOnMismatch is true, mismatches will log warnings instead of failing
func CheckExpectedVersionWithOptions(ctx context.Context, pool *pgxpool.Pool, warnOnMismatch bool) error {
	// Get configuration from environment
	config := getMigrationCheckConfig()
	if !config.Enabled {
		slog.Debug("Migration version checking disabled for lrdb")
		return nil
	}

	return checkMigrationVersionWithOptions(ctx, pool, migrationFiles, "gomigrate_lrdb", "lrdb", config, warnOnMismatch)
}

// migrationCheckConfig holds configuration for migration version checking
type migrationCheckConfig struct {
	Enabled       bool
	Timeout       time.Duration
	RetryInterval time.Duration
	AllowDirty    bool
}

// getMigrationCheckConfig returns migration check configuration from environment variables
func getMigrationCheckConfig() migrationCheckConfig {
	enabled := true
	if val := os.Getenv("LRDB_MIGRATION_CHECK_ENABLED"); val != "" {
		enabled = strings.ToLower(val) == "true"
	}

	timeout := 60 * time.Second
	if val := os.Getenv("MIGRATION_CHECK_TIMEOUT"); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			timeout = d
		}
	}

	retryInterval := 5 * time.Second
	if val := os.Getenv("MIGRATION_CHECK_RETRY_INTERVAL"); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			retryInterval = d
		}
	}

	allowDirty := false
	if val := os.Getenv("MIGRATION_CHECK_ALLOW_DIRTY"); val != "" {
		allowDirty = strings.ToLower(val) == "true"
	}

	return migrationCheckConfig{
		Enabled:       enabled,
		Timeout:       timeout,
		RetryInterval: retryInterval,
		AllowDirty:    allowDirty,
	}
}

// extractLatestMigrationVersion extracts the highest migration version from embedded migration files
func extractLatestMigrationVersion(migrationFiles embed.FS) (uint, error) {
	entries, err := migrationFiles.ReadDir(".")
	if err != nil {
		return 0, fmt.Errorf("failed to read migration directory: %w", err)
	}

	var maxVersion uint
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}

		// Extract version from filename like "1751057788_initial.up.sql"
		parts := strings.SplitN(name, "_", 2)
		if len(parts) < 1 {
			continue
		}

		version, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			continue
		}

		if uint(version) > maxVersion {
			maxVersion = uint(version)
		}
	}

	if maxVersion == 0 {
		return 0, fmt.Errorf("no valid migration files found")
	}

	return maxVersion, nil
}

// checkMigrationVersionWithOptions verifies that the database is at the expected migration version
// If warnOnMismatch is true, version mismatches will log warnings instead of failing
func checkMigrationVersionWithOptions(ctx context.Context, pool *pgxpool.Pool, migrationFiles embed.FS, migrationTable string, dbName string, config migrationCheckConfig, warnOnMismatch bool) error {
	expectedVersion, err := extractLatestMigrationVersion(migrationFiles)
	if err != nil {
		return fmt.Errorf("failed to extract expected migration version for %s: %w", dbName, err)
	}

	// Check current version first
	currentVersion, dirty, err := getCurrentMigrationVersion(ctx, pool, migrationFiles, migrationTable)
	if err != nil {
		return fmt.Errorf("failed to get current migration version for %s: %w", dbName, err)
	}

	if dirty && !config.AllowDirty {
		if warnOnMismatch {
			slog.Warn("Database migration is in dirty state, but continuing anyway",
				slog.String("database", dbName))
		} else {
			return fmt.Errorf("database %s migration is in dirty state, please fix before proceeding", dbName)
		}
	}

	if dirty {
		slog.Warn("Database migration is dirty but allowed to continue", slog.String("database", dbName))
	}

	// If versions match, return silently (no logging)
	if currentVersion == expectedVersion {
		return nil
	}

	// Versions don't match - log info and handle accordingly
	slog.Info("Checking migration version",
		slog.String("database", dbName),
		slog.Uint64("current_version", uint64(currentVersion)),
		slog.Uint64("expected_version", uint64(expectedVersion)))

	if currentVersion > expectedVersion {
		if warnOnMismatch {
			slog.Warn("Database version is newer than expected, but continuing anyway",
				slog.String("database", dbName),
				slog.Uint64("current_version", uint64(currentVersion)),
				slog.Uint64("expected_version", uint64(expectedVersion)))
			return nil
		}
		return fmt.Errorf("database %s version %d is newer than expected version %d - you may need to update the application",
			dbName, currentVersion, expectedVersion)
	}

	// currentVersion < expectedVersion
	if warnOnMismatch {
		slog.Warn("Database version is older than expected, but continuing anyway",
			slog.String("database", dbName),
			slog.Uint64("current_version", uint64(currentVersion)),
			slog.Uint64("expected_version", uint64(expectedVersion)))
		return nil
	}

	// For non-admin commands, wait for migrations
	deadline := time.Now().Add(config.Timeout)
	ticker := time.NewTicker(config.RetryInterval)
	defer ticker.Stop()

	for {
		currentVersion, _, err = getCurrentMigrationVersion(ctx, pool, migrationFiles, migrationTable)
		if err != nil {
			return fmt.Errorf("failed to get current migration version for %s: %w", dbName, err)
		}

		if currentVersion == expectedVersion {
			slog.Info("Migration version check passed",
				slog.String("database", dbName),
				slog.Uint64("version", uint64(currentVersion)))
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for %s migration to complete: current version %d, expected %d",
				dbName, currentVersion, expectedVersion)
		}

		slog.Info("Waiting for migrations to complete",
			slog.String("database", dbName),
			slog.Uint64("current_version", uint64(currentVersion)),
			slog.Uint64("expected_version", uint64(expectedVersion)),
			slog.Duration("remaining_timeout", time.Until(deadline)))

		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while waiting for %s migrations", dbName)
		case <-ticker.C:
			// Continue checking
		}
	}
}

// getCurrentMigrationVersion gets the current migration version from the database
func getCurrentMigrationVersion(ctx context.Context, pool *pgxpool.Pool, migrationFiles embed.FS, migrationTable string) (uint, bool, error) {
	sourceDriver, err := iofs.New(migrationFiles, ".")
	if err != nil {
		return 0, false, fmt.Errorf("failed to create iofs driver: %w", err)
	}

	sqlDB := stdlib.OpenDBFromPool(pool)
	defer func() {
		_ = sqlDB.Close()
	}()

	dbDriver, err := pgx.WithInstance(sqlDB, &pgx.Config{
		MigrationsTable: migrationTable,
	})
	if err != nil {
		return 0, false, fmt.Errorf("failed to create pgx driver: %w", err)
	}
	defer func() {
		_ = dbDriver.Close()
	}()

	m, err := migrate.NewWithInstance("iofs", sourceDriver, "postgres", dbDriver)
	if err != nil {
		return 0, false, fmt.Errorf("failed to create migrate instance: %w", err)
	}

	version, dirty, err := m.Version()
	if err != nil {
		if err == migrate.ErrNilVersion {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("failed to get current version: %w", err)
	}

	return version, dirty, nil
}
