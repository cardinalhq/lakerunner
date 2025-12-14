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

package configservice

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultLogStreamConfig(t *testing.T) {
	config := defaultLogStreamConfig()
	assert.Equal(t, "resource_service_name", config.FieldName)
}

func TestLogStreamConfig_JSONRoundTrip(t *testing.T) {
	original := LogStreamConfig{FieldName: "my_custom_field"}

	data, err := json.Marshal(original)
	require.NoError(t, err)
	assert.Equal(t, `{"field_name":"my_custom_field"}`, string(data))

	var parsed LogStreamConfig
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)
	assert.Equal(t, original, parsed)
}

func TestGetLogStreamConfig_FallbackChain(t *testing.T) {
	ctx := context.Background()

	t.Run("returns org-specific config when set", func(t *testing.T) {
		mock := newMockQuerier()
		svc := New(mock, 5*time.Minute)
		t.Cleanup(svc.Close)

		orgID := uuid.New()
		mock.configs[mock.key(orgID, configKeyLogStream)] = json.RawMessage(`{"field_name":"org_specific_field"}`)

		config := svc.GetLogStreamConfig(ctx, orgID)
		assert.Equal(t, "org_specific_field", config.FieldName)
	})

	t.Run("falls back to system default when org not set", func(t *testing.T) {
		mock := newMockQuerier()
		svc := New(mock, 5*time.Minute)
		t.Cleanup(svc.Close)

		orgID := uuid.New()
		// Set system default (DefaultOrgID)
		mock.configs[mock.key(DefaultOrgID, configKeyLogStream)] = json.RawMessage(`{"field_name":"system_default_field"}`)

		config := svc.GetLogStreamConfig(ctx, orgID)
		assert.Equal(t, "system_default_field", config.FieldName)
	})

	t.Run("falls back to hardcoded default when nothing set", func(t *testing.T) {
		mock := newMockQuerier()
		svc := New(mock, 5*time.Minute)
		t.Cleanup(svc.Close)

		orgID := uuid.New()
		// No configs set

		config := svc.GetLogStreamConfig(ctx, orgID)
		assert.Equal(t, "resource_service_name", config.FieldName)
	})

	t.Run("org config takes precedence over system default", func(t *testing.T) {
		mock := newMockQuerier()
		svc := New(mock, 5*time.Minute)
		t.Cleanup(svc.Close)

		orgID := uuid.New()
		mock.configs[mock.key(orgID, configKeyLogStream)] = json.RawMessage(`{"field_name":"org_wins"}`)
		mock.configs[mock.key(DefaultOrgID, configKeyLogStream)] = json.RawMessage(`{"field_name":"system_loses"}`)

		config := svc.GetLogStreamConfig(ctx, orgID)
		assert.Equal(t, "org_wins", config.FieldName)
	})

	t.Run("skips default lookup when querying DefaultOrgID", func(t *testing.T) {
		mock := newMockQuerier()
		svc := New(mock, 5*time.Minute)
		t.Cleanup(svc.Close)

		// Set system default
		mock.configs[mock.key(DefaultOrgID, configKeyLogStream)] = json.RawMessage(`{"field_name":"system_field"}`)

		config := svc.GetLogStreamConfig(ctx, DefaultOrgID)
		assert.Equal(t, "system_field", config.FieldName)
		// Should only make 1 call, not 2 (no fallback to itself)
		assert.Equal(t, int32(1), mock.getCallCount.Load())
	})
}

func TestGetLogStreamConfig_InvalidJSON(t *testing.T) {
	ctx := context.Background()

	t.Run("falls back when org config has invalid JSON", func(t *testing.T) {
		mock := newMockQuerier()
		svc := New(mock, 5*time.Minute)
		t.Cleanup(svc.Close)

		orgID := uuid.New()
		mock.configs[mock.key(orgID, configKeyLogStream)] = json.RawMessage(`not valid json`)
		mock.configs[mock.key(DefaultOrgID, configKeyLogStream)] = json.RawMessage(`{"field_name":"fallback"}`)

		config := svc.GetLogStreamConfig(ctx, orgID)
		assert.Equal(t, "fallback", config.FieldName)
	})

	t.Run("falls back when field_name is empty", func(t *testing.T) {
		mock := newMockQuerier()
		svc := New(mock, 5*time.Minute)
		t.Cleanup(svc.Close)

		orgID := uuid.New()
		mock.configs[mock.key(orgID, configKeyLogStream)] = json.RawMessage(`{"field_name":""}`)
		mock.configs[mock.key(DefaultOrgID, configKeyLogStream)] = json.RawMessage(`{"field_name":"fallback"}`)

		config := svc.GetLogStreamConfig(ctx, orgID)
		assert.Equal(t, "fallback", config.FieldName)
	})

	t.Run("returns hardcoded default when all configs are invalid", func(t *testing.T) {
		mock := newMockQuerier()
		svc := New(mock, 5*time.Minute)
		t.Cleanup(svc.Close)

		orgID := uuid.New()
		mock.configs[mock.key(orgID, configKeyLogStream)] = json.RawMessage(`invalid`)
		mock.configs[mock.key(DefaultOrgID, configKeyLogStream)] = json.RawMessage(`also invalid`)

		config := svc.GetLogStreamConfig(ctx, orgID)
		assert.Equal(t, "resource_service_name", config.FieldName)
	})
}

func TestSetLogStreamConfig(t *testing.T) {
	ctx := context.Background()

	t.Run("sets config with correct JSON structure", func(t *testing.T) {
		mock := newMockQuerier()
		svc := New(mock, 5*time.Minute)
		t.Cleanup(svc.Close)

		orgID := uuid.New()
		err := svc.SetLogStreamConfig(ctx, orgID, "my_custom_field")
		require.NoError(t, err)

		// Verify the stored value
		stored := mock.configs[mock.key(orgID, configKeyLogStream)]
		assert.Equal(t, `{"field_name":"my_custom_field"}`, string(stored))
	})

	t.Run("invalidates cache after set", func(t *testing.T) {
		mock := newMockQuerier()
		svc := New(mock, 5*time.Minute)
		t.Cleanup(svc.Close)

		orgID := uuid.New()

		// Prime cache with old value
		mock.configs[mock.key(orgID, configKeyLogStream)] = json.RawMessage(`{"field_name":"old"}`)
		_ = svc.GetLogStreamConfig(ctx, orgID)
		initialCalls := mock.getCallCount.Load()

		// Update
		err := svc.SetLogStreamConfig(ctx, orgID, "new")
		require.NoError(t, err)

		// Next get should fetch from DB
		config := svc.GetLogStreamConfig(ctx, orgID)
		assert.Equal(t, "new", config.FieldName)
		assert.Equal(t, initialCalls+1, mock.getCallCount.Load())
	})

	t.Run("propagates error on failure", func(t *testing.T) {
		mock := newMockQuerier()
		mock.setErr = assert.AnError
		svc := New(mock, 5*time.Minute)
		t.Cleanup(svc.Close)

		err := svc.SetLogStreamConfig(ctx, uuid.New(), "field")
		assert.Error(t, err)
	})
}

func TestGetLogStreamConfig_Caching(t *testing.T) {
	ctx := context.Background()

	t.Run("caches org config", func(t *testing.T) {
		mock := newMockQuerier()
		svc := New(mock, 5*time.Minute)
		t.Cleanup(svc.Close)

		orgID := uuid.New()
		mock.configs[mock.key(orgID, configKeyLogStream)] = json.RawMessage(`{"field_name":"cached"}`)

		// First call
		config1 := svc.GetLogStreamConfig(ctx, orgID)
		assert.Equal(t, "cached", config1.FieldName)
		calls1 := mock.getCallCount.Load()

		// Second call - should use cache
		config2 := svc.GetLogStreamConfig(ctx, orgID)
		assert.Equal(t, "cached", config2.FieldName)
		assert.Equal(t, calls1, mock.getCallCount.Load())
	})

	t.Run("caches system default lookup", func(t *testing.T) {
		mock := newMockQuerier()
		svc := New(mock, 5*time.Minute)
		t.Cleanup(svc.Close)

		orgID := uuid.New()
		mock.configs[mock.key(DefaultOrgID, configKeyLogStream)] = json.RawMessage(`{"field_name":"default"}`)

		// First call - checks org (miss), then default (hit)
		config1 := svc.GetLogStreamConfig(ctx, orgID)
		assert.Equal(t, "default", config1.FieldName)
		calls1 := mock.getCallCount.Load()

		// Second call - both org miss and default hit should be cached
		config2 := svc.GetLogStreamConfig(ctx, orgID)
		assert.Equal(t, "default", config2.FieldName)
		assert.Equal(t, calls1, mock.getCallCount.Load())
	})
}
