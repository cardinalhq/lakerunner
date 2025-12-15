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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileProvider_ValidateAPIKey(t *testing.T) {
	tests := []struct {
		name        string
		config      string
		apiKey      string
		expectValid bool
		expectErr   bool
	}{
		{
			name: "valid API key",
			config: `apikeys:
  - name: "admin-key-1"
    key: "ak_test123456789"
    description: "Test admin key"`,
			apiKey:      "ak_test123456789",
			expectValid: true,
			expectErr:   false,
		},
		{
			name: "invalid API key",
			config: `apikeys:
  - name: "admin-key-1"
    key: "ak_test123456789"`,
			apiKey:      "ak_invalid",
			expectValid: false,
			expectErr:   false,
		},
		{
			name: "multiple keys - valid",
			config: `apikeys:
  - name: "admin-key-1"
    key: "ak_key1"
  - name: "admin-key-2"
    key: "ak_key2"`,
			apiKey:      "ak_key2",
			expectValid: true,
			expectErr:   false,
		},
		{
			name:        "empty config - rejects all keys",
			config:      "apikeys: []",
			apiKey:      "any-key",
			expectValid: false,
			expectErr:   false,
		},
		{
			name:        "no apikeys section - rejects all keys",
			config:      "some_other_config: value",
			apiKey:      "any-key",
			expectValid: false,
			expectErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			configFile := filepath.Join(tempDir, "admin.yaml")

			err := os.WriteFile(configFile, []byte(tt.config), 0644)
			require.NoError(t, err)

			provider, err := NewFileProvider(configFile)
			require.NoError(t, err)

			ctx := context.Background()
			valid, err := provider.ValidateAPIKey(ctx, tt.apiKey)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectValid, valid)
			}
		})
	}
}

func TestFileProvider_GetAPIKeyInfo(t *testing.T) {
	config := `apikeys:
  - name: "admin-key-1"
    key: "ak_test123456789"
    description: "Test admin key for development"`

	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "admin.yaml")
	err := os.WriteFile(configFile, []byte(config), 0644)
	require.NoError(t, err)

	provider, err := NewFileProvider(configFile)
	require.NoError(t, err)

	ctx := context.Background()

	// Test valid key
	info, err := provider.GetAPIKeyInfo(ctx, "ak_test123456789")
	require.NoError(t, err)
	assert.Equal(t, "admin-key-1", info.Name)
	assert.Equal(t, "Test admin key for development", info.Description)
	assert.Empty(t, info.Key) // Key should not be returned for security

	// Test invalid key
	info, err = provider.GetAPIKeyInfo(ctx, "invalid-key")
	assert.Error(t, err)
	assert.Nil(t, info)
}

func TestFileProvider_MissingFile(t *testing.T) {
	// Test that missing file returns error
	provider, err := NewFileProvider("/nonexistent/file.yaml")
	assert.Error(t, err)
	assert.Nil(t, provider)
	assert.Contains(t, err.Error(), "failed to read admin config")
}

func TestFileProvider_InvalidYAML(t *testing.T) {
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "invalid.yaml")

	err := os.WriteFile(configFile, []byte("invalid: [yaml: content"), 0644)
	require.NoError(t, err)

	provider, err := NewFileProvider(configFile)
	assert.Error(t, err)
	assert.Nil(t, provider)
	assert.Contains(t, err.Error(), "failed to unmarshal admin config")
}
