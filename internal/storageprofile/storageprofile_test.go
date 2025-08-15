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

package storageprofile

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetupStorageProfiles(t *testing.T) {
	// Create a temporary file for testing file provider
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "test_profiles.yaml")

	orgID := uuid.New()
	yamlContent := fmt.Sprintf(`
- organization_id: %s
  instance_num: 1
  collector_name: "test-collector"
  cloud_provider: "aws"
  region: "us-west-2"
  role: "test-role"
  bucket: "test-bucket"
`, orgID.String())

	err := os.WriteFile(tempFile, []byte(yamlContent), 0644)
	require.NoError(t, err)

	tests := []struct {
		name     string
		setup    func()
		cleanup  func()
		wantType string
		wantErr  bool
	}{
		{
			name: "falls back to file provider when database not configured",
			setup: func() {
				// Set the storage profile file path
				os.Setenv("STORAGE_PROFILE_FILE", tempFile)
			},
			cleanup: func() {
				os.Unsetenv("STORAGE_PROFILE_FILE")
			},
			wantType: "*storageprofile.fileProvider",
			wantErr:  false,
		},
		{
			name: "uses default file path when STORAGE_PROFILE_FILE not set",
			setup: func() {
				// Unset the env var to test default path
				os.Unsetenv("STORAGE_PROFILE_FILE")
			},
			cleanup: func() {},
			// This will fail since /app/config/storage_profiles.yaml doesn't exist
			wantErr: true,
		},
		{
			name: "file provider with env variable",
			setup: func() {
				// Test env: prefix
				os.Setenv("TEST_STORAGE_PROFILES_CONTENT", yamlContent)
				os.Setenv("STORAGE_PROFILE_FILE", "env:TEST_STORAGE_PROFILES_CONTENT")
			},
			cleanup: func() {
				os.Unsetenv("TEST_STORAGE_PROFILES_CONTENT")
				os.Unsetenv("STORAGE_PROFILE_FILE")
			},
			wantType: "*storageprofile.fileProvider",
			wantErr:  false,
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

			provider, err := SetupStorageProfiles()

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, provider)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, provider)

			// Check the provider type
			providerType := fmt.Sprintf("%T", provider)
			if tt.wantType != "" {
				assert.Equal(t, tt.wantType, providerType)
			}
		})
	}
}

func TestSetupStorageProfiles_FileErrors(t *testing.T) {
	tests := []struct {
		name    string
		setup   func()
		cleanup func()
		wantErr string
	}{
		{
			name: "file not found",
			setup: func() {
				os.Setenv("STORAGE_PROFILE_FILE", "/nonexistent/path/file.yaml")
			},
			cleanup: func() {
				os.Unsetenv("STORAGE_PROFILE_FILE")
			},
			wantErr: "failed to read storage profiles from file",
		},
		{
			name: "invalid yaml content",
			setup: func() {
				tempDir := t.TempDir()
				tempFile := filepath.Join(tempDir, "invalid.yaml")
				err := os.WriteFile(tempFile, []byte("invalid: [yaml: content"), 0644)
				require.NoError(t, err)
				os.Setenv("STORAGE_PROFILE_FILE", tempFile)
			},
			cleanup: func() {
				os.Unsetenv("STORAGE_PROFILE_FILE")
			},
			wantErr: "failed to unmarshal storage profiles from file",
		},
		{
			name: "env variable not set",
			setup: func() {
				os.Setenv("STORAGE_PROFILE_FILE", "env:NONEXISTENT_VAR")
			},
			cleanup: func() {
				os.Unsetenv("STORAGE_PROFILE_FILE")
			},
			wantErr: "environment variable NONEXISTENT_VAR is not set",
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

			provider, err := SetupStorageProfiles()
			assert.Error(t, err)
			assert.Nil(t, provider)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
