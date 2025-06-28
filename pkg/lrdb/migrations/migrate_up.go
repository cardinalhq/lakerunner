// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migrations

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/pgx"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// RunMigrationsUp applies all up migrations using embedded migration files.
func RunMigrationsUp(ctx context.Context, pool *pgxpool.Pool) error {
	sourceDriver, err := iofs.New(migrationFiles, ".")
	if err != nil {
		return fmt.Errorf("failed to create iofs driver: %w", err)
	}

	sqlDB := stdlib.OpenDBFromPool(pool)
	defer func() {
		slog.Info("closing sqlDB")
		_ = sqlDB.Close()
		slog.Info("closed sqlDB")
	}()

	dbDriver, err := pgx.WithInstance(sqlDB, &pgx.Config{
		MigrationsTable: "gomigrate_lrdb",
	})
	if err != nil {
		return fmt.Errorf("failed to create pgx driver: %w", err)
	}
	defer func() {
		_ = dbDriver.Close()
	}()

	m, err := migrate.NewWithInstance("iofs", sourceDriver, "postgres", dbDriver)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}

	_, dirty, err := m.Version()
	if err != nil && err != migrate.ErrNilVersion {
		return fmt.Errorf("failed to get current version: %w", err)
	}
	if dirty {
		return errors.New("migration is dirty, please fix it before proceeding")
	}

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("migration failed: %w", err)
	}

	return nil
}
