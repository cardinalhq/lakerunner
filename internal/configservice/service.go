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

// Package configservice provides cached access to organization configuration.
package configservice

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jellydator/ttlcache/v3"

	"github.com/cardinalhq/lakerunner/configdb"
)

// DefaultOrgID is the nil UUID used for system-wide default config values.
var DefaultOrgID = uuid.UUID{}

var (
	global     *Service
	globalOnce sync.Once
)

// NewGlobal initializes the global config service instance.
// Thread-safe: only the first call initializes the service; subsequent calls are no-ops.
func NewGlobal(querier OrgConfigQuerier, ttl time.Duration) {
	globalOnce.Do(func() {
		global = New(querier, ttl)
	})
}

// Global returns the global config service instance.
// Panics if NewGlobal has not been called.
func Global() *Service {
	if global == nil {
		panic("configservice: NewGlobal must be called before Global")
	}
	return global
}

// OrgConfigQuerier defines the minimal database interface required by the config service.
type OrgConfigQuerier interface {
	GetOrgConfig(ctx context.Context, arg configdb.GetOrgConfigParams) (json.RawMessage, error)
	UpsertOrgConfig(ctx context.Context, arg configdb.UpsertOrgConfigParams) error
	DeleteOrgConfig(ctx context.Context, arg configdb.DeleteOrgConfigParams) error
	ListOrgConfigs(ctx context.Context, organizationID uuid.UUID) ([]configdb.ListOrgConfigsRow, error)
}

// configCacheKey is the cache key for config lookups.
type configCacheKey struct {
	OrgID uuid.UUID
	Key   string
}

// configCacheValue holds a cached config value or error.
type configCacheValue struct {
	Value json.RawMessage
	Err   error
}

// Service provides cached access to organization configuration.
type Service struct {
	querier OrgConfigQuerier
	cache   *ttlcache.Cache[configCacheKey, configCacheValue]
}

// New creates a new ConfigService with the given querier and cache TTL.
func New(querier OrgConfigQuerier, ttl time.Duration) *Service {
	cache := ttlcache.New(
		ttlcache.WithTTL[configCacheKey, configCacheValue](ttl),
	)
	go cache.Start()
	return &Service{
		querier: querier,
		cache:   cache,
	}
}

// Close stops the cache background goroutine and releases resources.
func (s *Service) Close() {
	s.cache.Stop()
}

// getConfigCached fetches a config value with caching.
// The loader is created fresh for each call, capturing the current context.
// Since ttlcache uses lazy expiration (no background refresh), the loader
// is only invoked synchronously during the Get() call on cache miss.
func (s *Service) getConfigCached(ctx context.Context, orgID uuid.UUID, key string) (json.RawMessage, error) {
	cacheKey := configCacheKey{OrgID: orgID, Key: key}

	loader := ttlcache.LoaderFunc[configCacheKey, configCacheValue](
		func(cache *ttlcache.Cache[configCacheKey, configCacheValue], k configCacheKey) *ttlcache.Item[configCacheKey, configCacheValue] {
			val, err := s.querier.GetOrgConfig(ctx, configdb.GetOrgConfigParams{
				OrganizationID: k.OrgID,
				Key:            k.Key,
			})
			return cache.Set(k, configCacheValue{
				Value: val,
				Err:   err,
			}, ttlcache.DefaultTTL)
		},
	)

	cached := s.cache.Get(cacheKey, ttlcache.WithLoader(loader)).Value()
	return cached.Value, cached.Err
}

// setConfig sets a config value and invalidates the cache.
func (s *Service) setConfig(ctx context.Context, orgID uuid.UUID, key string, value json.RawMessage) error {
	err := s.querier.UpsertOrgConfig(ctx, configdb.UpsertOrgConfigParams{
		OrganizationID: orgID,
		Key:            key,
		Value:          value,
	})
	if err != nil {
		return err
	}
	s.cache.Delete(configCacheKey{OrgID: orgID, Key: key})
	return nil
}

// DeleteConfig deletes a specific config for an organization.
func (s *Service) DeleteConfig(ctx context.Context, orgID uuid.UUID, key string) error {
	err := s.querier.DeleteOrgConfig(ctx, configdb.DeleteOrgConfigParams{
		OrganizationID: orgID,
		Key:            key,
	})
	if err != nil {
		return err
	}
	s.cache.Delete(configCacheKey{OrgID: orgID, Key: key})
	return nil
}

// ListConfigs lists all config keys/values for an organization.
func (s *Service) ListConfigs(ctx context.Context, orgID uuid.UUID) ([]configdb.ListOrgConfigsRow, error) {
	return s.querier.ListOrgConfigs(ctx, orgID)
}
