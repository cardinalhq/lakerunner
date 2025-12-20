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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/cmd/initialize"
	"github.com/cardinalhq/lakerunner/configdb"
	configdbmigrations "github.com/cardinalhq/lakerunner/configdb/migrations"
	"github.com/cardinalhq/lakerunner/internal/dbopen"
	"github.com/cardinalhq/lakerunner/lrdb"
	mdbmigrations "github.com/cardinalhq/lakerunner/lrdb/migrations"
)

var databases string
var configFile string
var apiKeysFile string

func init() {
	MigrateCmd.Flags().StringVar(&databases, "databases", "lrdb,configdb", "Comma-separated list of databases to migrate (lrdb,configdb)")
	MigrateCmd.Flags().StringVarP(&configFile, "config", "c", "", "Path to storage profile YAML configuration file (required if --initialize-if-needed is used)")
	MigrateCmd.Flags().StringVarP(&apiKeysFile, "api-keys", "k", "", "Path to API keys YAML configuration file (optional, used with --initialize-if-needed)")
	rootCmd.AddCommand(MigrateCmd)
}

var MigrateCmd = &cobra.Command{
	Use:   "migrate",
	Short: "Run database migrations",
	Long:  "Run database migrations on specified databases",
	RunE:  migrate,
}

func migrate(_ *cobra.Command, _ []string) error {
	dbList := strings.Split(databases, ",")

	var errors []error

	configWasMigrated := false

	for _, db := range dbList {
		db = strings.TrimSpace(db)
		switch db {
		case "lrdb":
			slog.Info("Running lrdb migrations")
			if err := migratelrdb(); err != nil {
				errors = append(errors, fmt.Errorf("failed to migrate lrdb: %w", err))
			} else {
				slog.Info("lrdb migrations completed successfully")
			}
		case "configdb":
			slog.Info("Running configdb migrations")
			if err := migrateconfigdb(); err != nil {
				errors = append(errors, fmt.Errorf("failed to migrate configdb: %w", err))
			} else {
				slog.Info("configdb migrations completed successfully")
			}
			configWasMigrated = true
		default:
			errors = append(errors, fmt.Errorf("unknown database: %s", db))
		}
	}

	if len(errors) > 0 {
		var errMsgs []string
		for _, err := range errors {
			errMsgs = append(errMsgs, err.Error())
		}
		return fmt.Errorf("migration errors: %s", strings.Join(errMsgs, "; "))
	}

	// Always try to run initialization if configdb is empty
	if configWasMigrated && os.Getenv("SYNC_LEGACY_TABLES") == "" {
		slog.Info("Checking if initialization is needed")
		if err := initializeIfNeededFunc(); err != nil {
			return fmt.Errorf("failed to initialize: %w", err)
		}
	}

	return nil
}

func migratelrdb() error {
	// No timeout for migrations - they may take a long time
	ctx := context.Background()
	pool, err := lrdb.ConnectTolrdb(ctx, dbopen.SkipMigrationCheck())
	if err != nil {
		return err
	}
	return mdbmigrations.RunMigrationsUp(context.Background(), pool)
}

func migrateconfigdb() error {
	// No timeout for migrations - they may take a long time
	ctx := context.Background()

	pool, err := configdb.ConnectToConfigDB(ctx, dbopen.SkipMigrationCheck())
	if err != nil {
		if errors.Is(err, dbopen.ErrDatabaseNotConfigured) {
			slog.Info("ConfigDB not configured, skipping migration")
			return nil
		}
		return err
	}
	return configdbmigrations.RunMigrationsUp(context.Background(), pool)
}

func initializeIfNeededFunc() error {
	ctx := context.Background()

	configDBPool, err := configdb.ConnectToConfigDB(ctx, dbopen.SkipMigrationCheck())
	if err != nil {
		if errors.Is(err, dbopen.ErrDatabaseNotConfigured) {
			slog.Info("ConfigDB not configured, skipping initialization")
			return nil
		}
		return fmt.Errorf("failed to connect to configdb: %w", err)
	}
	defer configDBPool.Close()

	qtxCheck := configdb.New(configDBPool)
	hasProfiles, err := qtxCheck.HasExistingStorageProfiles(ctx)
	if err != nil {
		return fmt.Errorf("failed to check existing storage profiles: %w", err)
	}
	if hasProfiles {
		slog.Info("Storage profiles already exist, skipping initialization")
		return nil
	}

	// Auto-detect storage profile file if none provided
	if configFile == "" {
		// Check STORAGE_PROFILE_FILE environment variable first
		if storageProfileFile := os.Getenv("STORAGE_PROFILE_FILE"); storageProfileFile != "" {
			configFile = storageProfileFile
			slog.Info("Using storage profile file from STORAGE_PROFILE_FILE", slog.String("file", configFile))
		} else {
			slog.Info("No config file provided, attempting to auto-detect storage profiles")

			// Look for storage profiles in the ConfigMap mount location
			configMapPath := "/app/config/storage_profiles.yaml"
			if _, err := os.Stat(configMapPath); err == nil {
				configFile = configMapPath
				slog.Info("Auto-detected storage profile file", slog.String("file", configFile))
			} else {
				return nil
			}
		}
	}

	// Auto-detect API keys file if none provided
	if apiKeysFile == "" {
		// Check API_KEYS_FILE environment variable first
		if apiKeysFileEnv := os.Getenv("API_KEYS_FILE"); apiKeysFileEnv != "" {
			apiKeysFile = apiKeysFileEnv
			slog.Info("Using API keys file from API_KEYS_FILE", slog.String("file", apiKeysFile))
		} else {
			// Look for API keys in the ConfigMap mount location
			apiKeysPath := "/app/config/apikeys.yaml"
			if _, err := os.Stat(apiKeysPath); err == nil {
				apiKeysFile = apiKeysPath
				slog.Info("Auto-detected API keys file", slog.String("file", apiKeysFile))
			}
			// Note: apiKeysFile can remain empty if not found - it's optional
		}
	}

	tx, err := configDBPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		// Use context.Background() for rollback to ensure it completes
		// even if the original context was cancelled. Rollback after
		// successful commit is a no-op in pgx.
		if err := tx.Rollback(context.Background()); err != nil {
			slog.Warn("Failed to rollback transaction", slog.Any("error", err))
		}
	}()

	qtx := configdb.New(tx)

	if err := initialize.InitializeConfig(ctx, configFile, apiKeysFile, qtx, false); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	slog.Info("Initialization completed successfully")
	return nil
}
