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
			name:        "empty config - backward compatibility",
			config:      "",
			apiKey:      "any-key",
			expectValid: true,
			expectErr:   false,
		},
		{
			name:        "no apikeys section - backward compatibility",
			config:      "some_other_config: value",
			apiKey:      "any-key",
			expectValid: true,
			expectErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			configFile := filepath.Join(tempDir, "admin.yaml")

			if tt.config != "" {
				err := os.WriteFile(configFile, []byte(tt.config), 0644)
				require.NoError(t, err)
			}

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

func TestSetupAdminConfig(t *testing.T) {
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "admin.yaml")

	config := `apikeys:
  - name: "test-key"
    key: "ak_test123"`

	err := os.WriteFile(configFile, []byte(config), 0644)
	require.NoError(t, err)

	tests := []struct {
		name    string
		setup   func()
		cleanup func()
		wantErr bool
	}{
		{
			name: "uses ADMIN_CONFIG_FILE environment variable",
			setup: func() {
				os.Setenv("ADMIN_CONFIG_FILE", configFile)
			},
			cleanup: func() {
				os.Unsetenv("ADMIN_CONFIG_FILE")
			},
			wantErr: false,
		},
		{
			name: "uses default path when env var not set",
			setup: func() {
				os.Unsetenv("ADMIN_CONFIG_FILE")
			},
			cleanup: func() {},
			// This will succeed because missing file creates empty provider
			wantErr: false,
		},
		{
			name: "file provider with env variable",
			setup: func() {
				os.Setenv("TEST_ADMIN_CONFIG_CONTENT", config)
				os.Setenv("ADMIN_CONFIG_FILE", "env:TEST_ADMIN_CONFIG_CONTENT")
			},
			cleanup: func() {
				os.Unsetenv("TEST_ADMIN_CONFIG_CONTENT")
				os.Unsetenv("ADMIN_CONFIG_FILE")
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup()
			}
			defer func() {
				if tt.cleanup != nil {
					tt.cleanup()
				}
			}()

			provider, err := SetupAdminConfig()

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, provider)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, provider)

			// Test that provider works
			ctx := context.Background()
			if tt.name == "uses ADMIN_CONFIG_FILE environment variable" || tt.name == "file provider with env variable" {
				// These tests use configs with actual keys, so test with the specific key
				valid, err := provider.ValidateAPIKey(ctx, "ak_test123")
				assert.NoError(t, err)
				assert.True(t, valid)
			} else {
				// This test uses default/missing config, so any key should work for backward compatibility
				valid, err := provider.ValidateAPIKey(ctx, "any-key")
				assert.NoError(t, err)
				assert.True(t, valid) // Should be true for backward compatibility
			}
		})
	}
}

func TestFileProvider_BackwardCompatibility(t *testing.T) {
	// Test that missing file creates empty provider
	provider, err := NewFileProvider("/nonexistent/file.yaml")
	require.NoError(t, err)

	ctx := context.Background()

	// Should allow any key for backward compatibility
	valid, err := provider.ValidateAPIKey(ctx, "any-key")
	assert.NoError(t, err)
	assert.True(t, valid)
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
