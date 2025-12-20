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
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jellydator/ttlcache/v3"
)

// Store provides all functions to execute db queries and transactions
type Store struct {
	*Queries
	connPool                           *pgxpool.Pool
	storageProfileCache                *ttlcache.Cache[GetStorageProfileParams, StorageProfileCacheValue]
	storageProfileByCollectorNameCache *ttlcache.Cache[uuid.UUID, StorageProfileByNameCacheValue]
	storageProfilesByBucketNameCache   *ttlcache.Cache[string, StorageProfilesByBucketNameCacheValue]
}

func NewEmptyStore() *Store {
	store := &Store{
		storageProfileCache: ttlcache.New(
			ttlcache.WithTTL[GetStorageProfileParams, StorageProfileCacheValue](5 * time.Minute),
		),
		storageProfileByCollectorNameCache: ttlcache.New(
			ttlcache.WithTTL[uuid.UUID, StorageProfileByNameCacheValue](5 * time.Minute),
		),
		storageProfilesByBucketNameCache: ttlcache.New(
			ttlcache.WithTTL[string, StorageProfilesByBucketNameCacheValue](5 * time.Minute),
		),
	}
	go store.storageProfileCache.Start()
	go store.storageProfileByCollectorNameCache.Start()
	go store.storageProfilesByBucketNameCache.Start()
	return store
}

// NewStore creates a new Store
func NewStore(connPool *pgxpool.Pool) *Store {
	s := NewEmptyStore()
	s.connPool = connPool
	s.Queries = New(connPool)
	return s
}

// GetPool returns the underlying connection pool
func (s *Store) GetPool() *pgxpool.Pool {
	return s.connPool
}

// execTx executes a function within a database transaction (internal use only)
func (store *Store) execTx(ctx context.Context, fn func(*Store) error) (err error) {
	tx, err := store.connPool.Begin(ctx)
	if err != nil {
		return err
	}

	committed := false
	defer func() {
		if committed {
			return
		}
		// Use a timeout to prevent infinite hangs during cleanup.
		// Never use the caller ctx for cleanup as it may be cancelled.
		rbCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if rbErr := tx.Rollback(rbCtx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			if err != nil {
				err = errors.Join(err, fmt.Errorf("rollback failed: %w", rbErr))
			} else {
				err = fmt.Errorf("rollback failed: %w", rbErr)
			}
		}
	}()

	txStore := &Store{
		connPool:                           store.connPool,
		Queries:                            New(tx),
		storageProfileCache:                store.storageProfileCache,
		storageProfileByCollectorNameCache: store.storageProfileByCollectorNameCache,
		storageProfilesByBucketNameCache:   store.storageProfilesByBucketNameCache,
	}

	if err = fn(txStore); err != nil {
		return err
	}

	// Use a timeout for commit to prevent hanging if DB is unresponsive.
	commitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err = tx.Commit(commitCtx); err != nil {
		return err
	}
	committed = true
	return nil
}

// Close stops the cache background goroutines and closes the connection pool.
func (s *Store) Close() {
	s.storageProfileCache.Stop()
	s.storageProfileByCollectorNameCache.Stop()
	s.storageProfilesByBucketNameCache.Stop()
	if s.connPool != nil {
		s.connPool.Close()
	}
}
