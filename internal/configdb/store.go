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
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jellydator/ttlcache/v3"
)

// Store provides all functions to execute db queries and transactions
type Store struct {
	*Queries
	connPool                           *pgxpool.Pool
	storageProfileCache                *ttlcache.Cache[GetStorageProfileParams, StorageProfileCacheValue]
	storageProfileByCollectorNameCache *ttlcache.Cache[GetStorageProfileByCollectorNameParams, StorageProfileByNameCacheValue]
	storageProfilesByBucketNameCache   *ttlcache.Cache[string, StorageProfilesByBucketNameCacheValue]
}

func NewEmptyStore() *Store {
	store := &Store{
		storageProfileCache: ttlcache.New(
			ttlcache.WithTTL[GetStorageProfileParams, StorageProfileCacheValue](5 * time.Minute),
		),
		storageProfileByCollectorNameCache: ttlcache.New(
			ttlcache.WithTTL[GetStorageProfileByCollectorNameParams, StorageProfileByNameCacheValue](5 * time.Minute),
		),
		storageProfilesByBucketNameCache: ttlcache.New(
			ttlcache.WithTTL[string, StorageProfilesByBucketNameCacheValue](5 * time.Minute),
		),
	}
	go store.storageProfileCache.Start()
	go store.storageProfileByCollectorNameCache.Start()
	return store
}

// NewStore creates a new Store
func NewStore(connPool *pgxpool.Pool) *Store {
	s := NewEmptyStore()
	s.connPool = connPool
	s.Queries = New(connPool)
	return s
}
