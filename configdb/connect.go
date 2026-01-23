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

package configdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	configdbmigrations "github.com/cardinalhq/lakerunner/configdb/migrations"
	"github.com/cardinalhq/lakerunner/internal/dbopen"
	"github.com/cardinalhq/lakerunner/migrations"
)

func ConnectToConfigDB(ctx context.Context, opts ...dbopen.Options) (*pgxpool.Pool, error) {
	connectionString, err := dbopen.GetDatabaseURLFromEnv("CONFIGDB")
	if err != nil {
		return nil, errors.Join(dbopen.ErrDatabaseNotConfigured, fmt.Errorf("failed to get CONFIGDB connection string: %w", err))
	}

	pool, err := newConnectionPool(ctx, connectionString)
	if err != nil {
		return nil, err
	}

	// Apply migration check options
	var migrationCheckOptions []migrations.CheckOption
	if len(opts) > 0 && len(opts[0].MigrationCheckOptions) > 0 {
		migrationCheckOptions = opts[0].MigrationCheckOptions
	}

	if err := configdbmigrations.CheckVersion(ctx, pool, migrationCheckOptions...); err != nil {
		pool.Close()
		return nil, fmt.Errorf("CONFIGDB migration version check failed: %w", err)
	}

	return pool, nil
}

func ConfigDBStore(ctx context.Context) (QuerierFull, error) {
	pool, err := ConnectToConfigDB(ctx)
	if err != nil {
		return nil, err
	}
	configStore := NewStore(pool)
	return configStore, nil
}

// ConfigDBStoreForAdmin connects to ConfigDB with admin-friendly migration checking
// that warns and continues instead of failing on migration mismatches
func ConfigDBStoreForAdmin(ctx context.Context) (QuerierFull, error) {
	pool, err := ConnectToConfigDB(ctx, dbopen.WarnOnMigrationMismatch())
	if err != nil {
		return nil, err
	}
	configStore := NewStore(pool)
	return configStore, nil
}
