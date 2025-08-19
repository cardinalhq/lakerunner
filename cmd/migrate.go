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
	configdbmigrations "github.com/cardinalhq/lakerunner/configdb/migrations"
	mdbmigrations "github.com/cardinalhq/lakerunner/lrdb/migrations"
)

var databases string

func init() {
	MigrateCmd.Flags().StringVar(&databases, "databases", "lrdb,configdb", "Comma-separated list of databases to migrate (lrdb,configdb)")
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
