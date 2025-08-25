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

package adminconfig

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/configdb"
)

type mockAdminAPIKeyStore struct {
	getByHash func(ctx context.Context, keyHash string) (configdb.AdminApiKey, error)
	getAll    func(ctx context.Context) ([]configdb.AdminApiKey, error)
}

func (m *mockAdminAPIKeyStore) GetAdminAPIKeyByHash(ctx context.Context, keyHash string) (configdb.AdminApiKey, error) {
	return m.getByHash(ctx, keyHash)
}

func (m *mockAdminAPIKeyStore) GetAllAdminAPIKeys(ctx context.Context) ([]configdb.AdminApiKey, error) {
	return m.getAll(ctx)
}

func TestDBProviderValidateAPIKeyInitialAdmin(t *testing.T) {
	ctx := context.Background()
	store := &mockAdminAPIKeyStore{
		getByHash: func(ctx context.Context, keyHash string) (configdb.AdminApiKey, error) {
			return configdb.AdminApiKey{}, errors.New("not found")
		},
		getAll: func(ctx context.Context) ([]configdb.AdminApiKey, error) {
			return []configdb.AdminApiKey{}, nil
		},
	}

	os.Setenv("LAKERUNNER_INITITAL_ADMIN_API_KEY", "bootkey")
	defer os.Unsetenv("LAKERUNNER_INITITAL_ADMIN_API_KEY")

	provider := NewDBProvider(store)

	valid, err := provider.ValidateAPIKey(ctx, "bootkey")
	assert.NoError(t, err)
	assert.True(t, valid)

	valid, err = provider.ValidateAPIKey(ctx, "wrong")
	assert.NoError(t, err)
	assert.False(t, valid)
}

func TestDBProviderValidateAPIKeyNonEmptyTable(t *testing.T) {
	ctx := context.Background()
	store := &mockAdminAPIKeyStore{
		getByHash: func(ctx context.Context, keyHash string) (configdb.AdminApiKey, error) {
			return configdb.AdminApiKey{}, errors.New("not found")
		},
		getAll: func(ctx context.Context) ([]configdb.AdminApiKey, error) {
			return []configdb.AdminApiKey{{}}, nil
		},
	}

	os.Setenv("LAKERUNNER_INITITAL_ADMIN_API_KEY", "bootkey")
	defer os.Unsetenv("LAKERUNNER_INITITAL_ADMIN_API_KEY")

	provider := NewDBProvider(store)

	valid, err := provider.ValidateAPIKey(ctx, "bootkey")
	assert.NoError(t, err)
	assert.False(t, valid)
}

func TestDBProviderValidateAPIKeyFromDB(t *testing.T) {
	ctx := context.Background()
	store := &mockAdminAPIKeyStore{
		getByHash: func(ctx context.Context, keyHash string) (configdb.AdminApiKey, error) {
			return configdb.AdminApiKey{}, nil
		},
		getAll: func(ctx context.Context) ([]configdb.AdminApiKey, error) {
			return []configdb.AdminApiKey{{}}, nil
		},
	}

	provider := NewDBProvider(store)

	valid, err := provider.ValidateAPIKey(ctx, "fromdb")
	assert.NoError(t, err)
	assert.True(t, valid)
}
