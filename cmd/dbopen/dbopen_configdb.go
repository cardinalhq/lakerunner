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

package dbopen

import (
	"context"
	"errors"
	"fmt"

	"github.com/cardinalhq/nestbox/dbase/configdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

func ConnectToConfigDB(ctx context.Context) (*pgxpool.Pool, error) {
	connectionString, err := getDatabaseURLFromEnv("CONFIGDB")
	if err != nil {
		return nil, errors.Join(ErrDatabaseNotConfigured, fmt.Errorf("failed to get CONFIGDB connection string: %w", err))
	}
	return configdb.NewConnectionPool(ctx, connectionString)
}

func ConfigDBStore(ctx context.Context) (configdb.QuerierFull, error) {
	pool, err := ConnectToConfigDB(ctx)
	if err != nil {
		return nil, err
	}
	configStore := configdb.NewStore(pool)
	return configStore, nil
}
