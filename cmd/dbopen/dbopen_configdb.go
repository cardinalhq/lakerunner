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

package dbopen

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cardinalhq/lakerunner/configdb"
	configdbmigrations "github.com/cardinalhq/lakerunner/configdb/migrations"
)

func ConnectToConfigDB(ctx context.Context, opts ...Options) (*pgxpool.Pool, error) {
	connectionString, err := getDatabaseURLFromEnv("CONFIGDB")
	if err != nil {
		return nil, errors.Join(ErrDatabaseNotConfigured, fmt.Errorf("failed to get CONFIGDB connection string: %w", err))
	}

	pool, err := configdb.NewConnectionPool(ctx, connectionString)
	if err != nil {
		return nil, err
	}

	// Check migration check options
	skipMigrationCheck := false
	warnOnMismatch := false
	if len(opts) > 0 {
		skipMigrationCheck = opts[0].SkipMigrationCheck
		warnOnMismatch = opts[0].WarnOnMigrationMismatch
	}

	if !skipMigrationCheck {
		if err := configdbmigrations.CheckExpectedVersionWithOptions(ctx, pool, warnOnMismatch); err != nil {
			pool.Close()
			return nil, fmt.Errorf("CONFIGDB migration version check failed: %w", err)
		}
	}

	return pool, nil
}

func ConfigDBStore(ctx context.Context) (configdb.QuerierFull, error) {
	pool, err := ConnectToConfigDB(ctx)
	if err != nil {
		return nil, err
	}
	configStore := configdb.NewStore(pool)
	return configStore, nil
}

// ConfigDBStoreForAdmin connects to ConfigDB with admin-friendly migration checking
// that warns and continues instead of failing on migration mismatches
func ConfigDBStoreForAdmin(ctx context.Context) (configdb.QuerierFull, error) {
	pool, err := ConnectToConfigDB(ctx, Options{WarnOnMigrationMismatch: true})
	if err != nil {
		return nil, err
	}
	configStore := configdb.NewStore(pool)
	return configStore, nil
}
