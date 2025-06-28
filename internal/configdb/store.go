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
}

func NewEmptyStore() *Store {
	store := &Store{
		storageProfileCache: ttlcache.New(
			ttlcache.WithTTL[GetStorageProfileParams, StorageProfileCacheValue](5 * time.Minute),
		),
		storageProfileByCollectorNameCache: ttlcache.New(
			ttlcache.WithTTL[GetStorageProfileByCollectorNameParams, StorageProfileByNameCacheValue](5 * time.Minute),
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
