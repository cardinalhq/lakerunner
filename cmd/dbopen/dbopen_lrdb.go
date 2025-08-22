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

	// Check if migration check should be skipped
	skipMigrationCheck := false
	if len(opts) > 0 {
		skipMigrationCheck = opts[0].SkipMigrationCheck
	}

	if !skipMigrationCheck {
		if err := lrdbmigrations.CheckExpectedVersion(ctx, pool); err != nil {
			pool.Close()
			return nil, fmt.Errorf("LRDB migration version check failed: %w", err)
		}
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
