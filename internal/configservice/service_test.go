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

package configservice

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/configdb"
)

// mockQuerier is a test mock for OrgConfigQuerier.
type mockQuerier struct {
	configs      map[string]json.RawMessage // key: "orgID:configKey"
	getCallCount atomic.Int32
	setCallCount atomic.Int32
	getErr       error
	setErr       error
	deleteErr    error
}

func newMockQuerier() *mockQuerier {
	return &mockQuerier{
		configs: make(map[string]json.RawMessage),
	}
}

func (m *mockQuerier) key(orgID uuid.UUID, configKey string) string {
	return orgID.String() + ":" + configKey
}

func (m *mockQuerier) GetOrgConfig(ctx context.Context, arg configdb.GetOrgConfigParams) (json.RawMessage, error) {
	m.getCallCount.Add(1)
	if m.getErr != nil {
		return nil, m.getErr
	}
	val, ok := m.configs[m.key(arg.OrganizationID, arg.Key)]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	return val, nil
}

func (m *mockQuerier) UpsertOrgConfig(ctx context.Context, arg configdb.UpsertOrgConfigParams) error {
	m.setCallCount.Add(1)
	if m.setErr != nil {
		return m.setErr
	}
	m.configs[m.key(arg.OrganizationID, arg.Key)] = arg.Value
	return nil
}

func (m *mockQuerier) DeleteOrgConfig(ctx context.Context, arg configdb.DeleteOrgConfigParams) error {
	if m.deleteErr != nil {
		return m.deleteErr
	}
	delete(m.configs, m.key(arg.OrganizationID, arg.Key))
	return nil
}

func (m *mockQuerier) ListOrgConfigs(ctx context.Context, organizationID uuid.UUID) ([]configdb.ListOrgConfigsRow, error) {
	var rows []configdb.ListOrgConfigsRow
	prefix := organizationID.String() + ":"
	for k, v := range m.configs {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			rows = append(rows, configdb.ListOrgConfigsRow{
				Key:   k[len(prefix):],
				Value: v,
			})
		}
	}
	return rows, nil
}

func TestDefaultOrgID(t *testing.T) {
	assert.Equal(t, uuid.UUID{}, DefaultOrgID)
	assert.Equal(t, "00000000-0000-0000-0000-000000000000", DefaultOrgID.String())
}

func TestService_CachingBehavior(t *testing.T) {
	ctx := context.Background()
	mock := newMockQuerier()
	svc := New(mock, 5*time.Minute)
	t.Cleanup(svc.Close)

	orgID := uuid.New()
	configKey := "test.key"
	configVal := json.RawMessage(`{"value": "test"}`)

	// Set a config value directly in mock
	mock.configs[mock.key(orgID, configKey)] = configVal

	t.Run("first call fetches from DB", func(t *testing.T) {
		initialCalls := mock.getCallCount.Load()

		val, err := svc.getConfigCached(ctx, orgID, configKey)
		require.NoError(t, err)
		assert.Equal(t, configVal, val)
		assert.Equal(t, initialCalls+1, mock.getCallCount.Load())
	})

	t.Run("second call uses cache", func(t *testing.T) {
		initialCalls := mock.getCallCount.Load()

		val, err := svc.getConfigCached(ctx, orgID, configKey)
		require.NoError(t, err)
		assert.Equal(t, configVal, val)
		// Should NOT have incremented - served from cache
		assert.Equal(t, initialCalls, mock.getCallCount.Load())
	})
}

func TestService_SetConfigInvalidatesCache(t *testing.T) {
	ctx := context.Background()
	mock := newMockQuerier()
	svc := New(mock, 5*time.Minute)
	t.Cleanup(svc.Close)

	orgID := uuid.New()
	configKey := "test.key"

	// Prime the cache
	mock.configs[mock.key(orgID, configKey)] = json.RawMessage(`{"value": "old"}`)
	_, err := svc.getConfigCached(ctx, orgID, configKey)
	require.NoError(t, err)

	initialGetCalls := mock.getCallCount.Load()

	// Update value via setConfig
	err = svc.setConfig(ctx, orgID, configKey, json.RawMessage(`{"value": "new"}`))
	require.NoError(t, err)

	// Next fetch should hit DB (cache was invalidated)
	val, err := svc.getConfigCached(ctx, orgID, configKey)
	require.NoError(t, err)
	assert.Equal(t, json.RawMessage(`{"value": "new"}`), val)
	assert.Equal(t, initialGetCalls+1, mock.getCallCount.Load())
}

func TestService_DeleteConfigInvalidatesCache(t *testing.T) {
	ctx := context.Background()
	mock := newMockQuerier()
	svc := New(mock, 5*time.Minute)
	t.Cleanup(svc.Close)

	orgID := uuid.New()
	configKey := "test.key"

	// Prime the cache
	mock.configs[mock.key(orgID, configKey)] = json.RawMessage(`{"value": "exists"}`)
	_, err := svc.getConfigCached(ctx, orgID, configKey)
	require.NoError(t, err)

	// Delete
	err = svc.DeleteConfig(ctx, orgID, configKey)
	require.NoError(t, err)

	// Next fetch should return ErrNoRows
	_, err = svc.getConfigCached(ctx, orgID, configKey)
	assert.ErrorIs(t, err, pgx.ErrNoRows)
}

func TestService_ListConfigs(t *testing.T) {
	ctx := context.Background()
	mock := newMockQuerier()
	svc := New(mock, 5*time.Minute)
	t.Cleanup(svc.Close)

	orgID := uuid.New()
	mock.configs[mock.key(orgID, "key1")] = json.RawMessage(`{"a": 1}`)
	mock.configs[mock.key(orgID, "key2")] = json.RawMessage(`{"b": 2}`)
	mock.configs[mock.key(uuid.New(), "other")] = json.RawMessage(`{"c": 3}`) // different org

	rows, err := svc.ListConfigs(ctx, orgID)
	require.NoError(t, err)
	assert.Len(t, rows, 2)

	keys := make(map[string]bool)
	for _, r := range rows {
		keys[r.Key] = true
	}
	assert.True(t, keys["key1"])
	assert.True(t, keys["key2"])
}

func TestService_ErrorHandling(t *testing.T) {
	ctx := context.Background()

	t.Run("get error propagates", func(t *testing.T) {
		mock := newMockQuerier()
		mock.getErr = errors.New("db connection failed")
		svc := New(mock, 5*time.Minute)
		t.Cleanup(svc.Close)

		_, err := svc.getConfigCached(ctx, uuid.New(), "key")
		assert.Error(t, err)
	})

	t.Run("set error propagates", func(t *testing.T) {
		mock := newMockQuerier()
		mock.setErr = errors.New("db write failed")
		svc := New(mock, 5*time.Minute)
		t.Cleanup(svc.Close)

		err := svc.setConfig(ctx, uuid.New(), "key", json.RawMessage(`{}`))
		assert.Error(t, err)
		assert.Equal(t, int32(1), mock.setCallCount.Load())
	})

	t.Run("set error does not invalidate cache", func(t *testing.T) {
		mock := newMockQuerier()
		svc := New(mock, 5*time.Minute)
		t.Cleanup(svc.Close)

		orgID := uuid.New()
		configKey := "test.key"

		// Prime cache
		mock.configs[mock.key(orgID, configKey)] = json.RawMessage(`{"value": "cached"}`)
		_, _ = svc.getConfigCached(ctx, orgID, configKey)

		// Make set fail
		mock.setErr = errors.New("db write failed")
		_ = svc.setConfig(ctx, orgID, configKey, json.RawMessage(`{"value": "new"}`))

		// Cache should still have old value (no invalidation on error)
		initialCalls := mock.getCallCount.Load()
		val, err := svc.getConfigCached(ctx, orgID, configKey)
		require.NoError(t, err)
		assert.Equal(t, json.RawMessage(`{"value": "cached"}`), val)
		assert.Equal(t, initialCalls, mock.getCallCount.Load()) // served from cache
	})
}

func TestService_ErrNoRowsCaching(t *testing.T) {
	ctx := context.Background()
	mock := newMockQuerier()
	svc := New(mock, 5*time.Minute)
	t.Cleanup(svc.Close)

	orgID := uuid.New()
	configKey := "nonexistent"

	// First call - returns ErrNoRows, should cache the "not found" result
	_, err := svc.getConfigCached(ctx, orgID, configKey)
	assert.ErrorIs(t, err, pgx.ErrNoRows)
	assert.Equal(t, int32(1), mock.getCallCount.Load())

	// Second call - should still return ErrNoRows from cache
	_, err = svc.getConfigCached(ctx, orgID, configKey)
	assert.ErrorIs(t, err, pgx.ErrNoRows)
	// Should NOT have called DB again
	assert.Equal(t, int32(1), mock.getCallCount.Load())
}
