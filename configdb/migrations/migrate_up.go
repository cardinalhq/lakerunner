// Copyright (C) 2025-2026 CardinalHQ, Inc
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
		slog.Info("closing configdb sqlDB")
		_ = sqlDB.Close()
		slog.Info("closed configdb sqlDB")
	}()

	dbDriver, err := pgx.WithInstance(sqlDB, &pgx.Config{
		MigrationsTable: "gomigrate_lrconfigdb",
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
