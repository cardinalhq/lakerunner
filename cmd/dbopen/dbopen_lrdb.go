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

	"github.com/cardinalhq/lakerunner/lrdb"
	lrdbmigrations "github.com/cardinalhq/lakerunner/lrdb/migrations"
	"github.com/cardinalhq/lakerunner/migrations"
)

func ConnectTolrdb(ctx context.Context, opts ...Options) (*pgxpool.Pool, error) {
	connectionString, err := getDatabaseURLFromEnv("LRDB")
	if err != nil {
		return nil, errors.Join(ErrDatabaseNotConfigured, fmt.Errorf("failed to get LRDB connection string: %w", err))
	}

	pool, err := lrdb.NewConnectionPool(ctx, connectionString)
	if err != nil {
		return nil, err
	}

	// Apply migration check options
	var migrationCheckOptions []migrations.CheckOption
	if len(opts) > 0 && len(opts[0].MigrationCheckOptions) > 0 {
		migrationCheckOptions = opts[0].MigrationCheckOptions
	}

	if err := lrdbmigrations.CheckVersion(ctx, pool, migrationCheckOptions...); err != nil {
		pool.Close()
		return nil, fmt.Errorf("LRDB migration version check failed: %w", err)
	}

	return pool, nil
}

func LRDBStore(ctx context.Context) (lrdb.StoreFull, error) {
	pool, err := ConnectTolrdb(ctx)
	if err != nil {
		return nil, err
	}
	lrStore := lrdb.NewStore(pool)
	return lrStore, nil
}

// LRDBStoreForAdmin connects to LRDB with admin-friendly migration checking
// that warns and continues instead of failing on migration mismatches
func LRDBStoreForAdmin(ctx context.Context) (lrdb.StoreFull, error) {
	pool, err := ConnectTolrdb(ctx, WarnOnMigrationMismatch())
	if err != nil {
		return nil, err
	}
	lrStore := lrdb.NewStore(pool)
	return lrStore, nil
}
