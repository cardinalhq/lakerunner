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
	"context"
	"errors"

	"github.com/jellydator/ttlcache/v3"
)

type StorageProfileCacheValue struct {
	GetStorageProfileRow
	error
}

func (store *Store) GetStorageProfile(ctx context.Context, params GetStorageProfileParams) (GetStorageProfileRow, error) {
	loader := ttlcache.LoaderFunc[GetStorageProfileParams, StorageProfileCacheValue](
		func(cache *ttlcache.Cache[GetStorageProfileParams, StorageProfileCacheValue], key GetStorageProfileParams) *ttlcache.Item[GetStorageProfileParams, StorageProfileCacheValue] {
			row, err := store.Queries.GetStorageProfileUncached(ctx, key)
			item := cache.Set(key, StorageProfileCacheValue{
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

type StorageProfileByNameCacheValue struct {
	GetStorageProfileByCollectorNameRow
	error
}

func (store *Store) GetStorageProfileByCollectorName(ctx context.Context, params GetStorageProfileByCollectorNameParams) (GetStorageProfileByCollectorNameRow, error) {
	loader := ttlcache.LoaderFunc[GetStorageProfileByCollectorNameParams, StorageProfileByNameCacheValue](
		func(cache *ttlcache.Cache[GetStorageProfileByCollectorNameParams, StorageProfileByNameCacheValue], key GetStorageProfileByCollectorNameParams) *ttlcache.Item[GetStorageProfileByCollectorNameParams, StorageProfileByNameCacheValue] {
			row, err := store.Queries.GetStorageProfileByCollectorNameUncached(ctx, key)
			item := cache.Set(key, StorageProfileByNameCacheValue{
				GetStorageProfileByCollectorNameRow: row,
				error:                               err,
			}, ttlcache.DefaultTTL)
			return item
		},
	)
	v := store.storageProfileByCollectorNameCache.Get(params, ttlcache.WithLoader(loader))
	if v != nil {
		return v.Value().GetStorageProfileByCollectorNameRow, v.Value().error
	}
	return GetStorageProfileByCollectorNameRow{}, errors.New("failed to get storage profile by collector name from cache")
}

type StorageProfilesByBucketNameCacheValue struct {
	rows []GetStorageProfilesByBucketNameRow
	err  error
}

func (store *Store) GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]GetStorageProfilesByBucketNameRow, error) {
	loader := ttlcache.LoaderFunc[string, StorageProfilesByBucketNameCacheValue](
		func(cache *ttlcache.Cache[string, StorageProfilesByBucketNameCacheValue], key string) *ttlcache.Item[string, StorageProfilesByBucketNameCacheValue] {
			rows, err := store.Queries.GetStorageProfilesByBucketNameUncached(ctx, key)
			item := cache.Set(key, StorageProfilesByBucketNameCacheValue{
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
