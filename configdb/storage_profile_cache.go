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

package configdb

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jellydator/ttlcache/v3"
)

type storageProfileCacheValue struct {
	GetStorageProfileRow
	error
}

func (store *Store) GetStorageProfile(ctx context.Context, params GetStorageProfileParams) (GetStorageProfileRow, error) {
	loader := ttlcache.LoaderFunc[GetStorageProfileParams, storageProfileCacheValue](
		func(cache *ttlcache.Cache[GetStorageProfileParams, storageProfileCacheValue], key GetStorageProfileParams) *ttlcache.Item[GetStorageProfileParams, storageProfileCacheValue] {
			row, err := store.GetStorageProfileUncached(ctx, key)
			item := cache.Set(key, storageProfileCacheValue{
				GetStorageProfileRow: row,
				error:                err,
			}, ttlcache.DefaultTTL)
			return item
		},
	)
	v := store.storageProfileCache.Get(params, ttlcache.WithLoader(loader))
	if v != nil {
		return v.Value().GetStorageProfileRow, v.Value().error
	}
	return GetStorageProfileRow{}, errors.New("failed to get storage profile from cache")
}

type storageProfileByNameCacheValue struct {
	GetStorageProfileByCollectorNameRow
	error
}

func (store *Store) GetStorageProfileByCollectorName(ctx context.Context, organizationID uuid.UUID) (GetStorageProfileByCollectorNameRow, error) {
	loader := ttlcache.LoaderFunc[uuid.UUID, storageProfileByNameCacheValue](
		func(cache *ttlcache.Cache[uuid.UUID, storageProfileByNameCacheValue], key uuid.UUID) *ttlcache.Item[uuid.UUID, storageProfileByNameCacheValue] {
			pgUUID := pgtype.UUID{}
			if scanErr := pgUUID.Scan(key); scanErr != nil {
				item := cache.Set(key, storageProfileByNameCacheValue{
					error: scanErr,
				}, ttlcache.DefaultTTL)
				return item
			}
			row, err := store.GetStorageProfileByCollectorNameUncached(ctx, pgUUID)
			item := cache.Set(key, storageProfileByNameCacheValue{
				GetStorageProfileByCollectorNameRow: row,
				error:                               err,
			}, ttlcache.DefaultTTL)
			return item
		},
	)
	v := store.storageProfileByCollectorNameCache.Get(organizationID, ttlcache.WithLoader(loader))
	if v != nil {
		return v.Value().GetStorageProfileByCollectorNameRow, v.Value().error
	}
	return GetStorageProfileByCollectorNameRow{}, errors.New("failed to get storage profile by collector name from cache")
}

type storageProfilesByBucketNameCacheValue struct {
	rows []GetStorageProfilesByBucketNameRow
	err  error
}

func (store *Store) GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]GetStorageProfilesByBucketNameRow, error) {
	loader := ttlcache.LoaderFunc[string, storageProfilesByBucketNameCacheValue](
		func(cache *ttlcache.Cache[string, storageProfilesByBucketNameCacheValue], key string) *ttlcache.Item[string, storageProfilesByBucketNameCacheValue] {
			rows, err := store.GetStorageProfilesByBucketNameUncached(ctx, key)
			item := cache.Set(key, storageProfilesByBucketNameCacheValue{
				rows: rows,
				err:  err,
			}, ttlcache.DefaultTTL)
			return item
		},
	)
	v := store.storageProfilesByBucketNameCache.Get(bucketName, ttlcache.WithLoader(loader))
	if v != nil {
		return v.Value().rows, v.Value().err
	}
	return nil, errors.New("failed to get storage profiles by bucket name from cache")
}
