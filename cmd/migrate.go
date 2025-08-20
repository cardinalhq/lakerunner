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
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/cmd/initialize"
	"github.com/cardinalhq/lakerunner/configdb"
	configdbmigrations "github.com/cardinalhq/lakerunner/configdb/migrations"
	mdbmigrations "github.com/cardinalhq/lakerunner/lrdb/migrations"
)

var databases string
var initializeIfNeeded bool
var configFile string
var apiKeysFile string

func init() {
	MigrateCmd.Flags().StringVar(&databases, "databases", "lrdb,configdb", "Comma-separated list of databases to migrate (lrdb,configdb)")
	MigrateCmd.Flags().BoolVar(&initializeIfNeeded, "initialize-if-needed", false, "Run initialization after migration if configdb is empty")
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
	if initializeIfNeeded && configFile == "" {
		return fmt.Errorf("--config flag is required when using --initialize-if-needed")
	}

	dbList := strings.Split(databases, ",")

	var errors []error

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

	if initializeIfNeeded {
		slog.Info("Running initialization if needed")
		if err := initializeIfNeededFunc(); err != nil {
			return fmt.Errorf("failed to initialize: %w", err)
		}
	}

	return nil
}

func migratelrdb() error {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Minute))
	defer cancel()
	pool, err := dbopen.ConnectTolrdb(ctx)
	if err != nil {
		return err
	}
	return mdbmigrations.RunMigrationsUp(context.Background(), pool)
}

func migrateconfigdb() error {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Minute))
	defer cancel()

	pool, err := dbopen.ConnectToConfigDB(ctx)
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

	configDBPool, err := dbopen.ConnectToConfigDB(ctx)
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

	tx, err := configDBPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			slog.Warn("Failed to rollback transaction", slog.Any("error", err))
		}
	}()

	qtx := configdb.New(tx)

	if err := initialize.InitializeConfig(ctx, configFile, apiKeysFile, qtx, slog.Default(), false); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	slog.Info("Initialization completed successfully")
	return nil
}
