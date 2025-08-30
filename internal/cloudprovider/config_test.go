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

package cloudprovider

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfigFromEnv(t *testing.T) {
	tests := []struct {
		name     string
		envVars  map[string]string
		expected ProviderConfig
	}{
		{
			name: "AWS provider with basic config",
			envVars: map[string]string{
				"LAKERUNNER_PROVIDER_TYPE": "aws",
				"LAKERUNNER_AWS_REGION":    "us-west-2",
				"LAKERUNNER_BUCKET":        "test-bucket",
			},
			expected: ProviderConfig{
				Type: ProviderAWS,
				ObjectStore: ObjectStoreConfig{
					Bucket:           "test-bucket",
					Region:           "us-west-2",
					ProviderSettings: map[string]any{},
				},
				Settings: map[string]string{
					"region": "us-west-2",
				},
			},
		},
		{
			name: "AWS provider with custom endpoint (for S3-compatible like MinIO)",
			envVars: map[string]string{
				"LAKERUNNER_PROVIDER_TYPE":      "aws",
				"LAKERUNNER_AWS_ENDPOINT":       "https://minio.example.com",
				"LAKERUNNER_AWS_USE_PATH_STYLE": "true",
				"LAKERUNNER_AWS_INSECURE_TLS":   "true",
				"LAKERUNNER_BUCKET":             "minio-bucket",
			},
			expected: ProviderConfig{
				Type: ProviderAWS,
				ObjectStore: ObjectStoreConfig{
					Bucket:           "minio-bucket",
					Endpoint:         "https://minio.example.com",
					UsePathStyle:     true,
					InsecureTLS:      true,
					Region:           "us-east-1", // Default region for AWS
					ProviderSettings: map[string]any{},
				},
				Settings: map[string]string{
					"endpoint":       "https://minio.example.com",
					"use_path_style": "true",
					"insecure_tls":   "true",
				},
			},
		},
		{
			name: "Local provider with custom path",
			envVars: map[string]string{
				"LAKERUNNER_PROVIDER_TYPE":   "local",
				"LAKERUNNER_LOCAL_BASE_PATH": "/custom/path",
			},
			expected: ProviderConfig{
				Type: ProviderLocal,
				ObjectStore: ObjectStoreConfig{
					ProviderSettings: map[string]any{
						"base_path": "/custom/path",
					},
				},
				Settings: map[string]string{
					"base_path": "/custom/path",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear environment
			for key := range tt.envVars {
				os.Unsetenv(key)
			}

			// Set test environment variables
			for key, value := range tt.envVars {
				os.Setenv(key, value)
			}
			defer func() {
				for key := range tt.envVars {
					os.Unsetenv(key)
				}
			}()

			config, err := LoadConfigFromEnv()
			require.NoError(t, err)

			assert.Equal(t, tt.expected.Type, config.Type)
			assert.Equal(t, tt.expected.ObjectStore.Bucket, config.ObjectStore.Bucket)
			assert.Equal(t, tt.expected.ObjectStore.Region, config.ObjectStore.Region)
			assert.Equal(t, tt.expected.ObjectStore.Endpoint, config.ObjectStore.Endpoint)
			assert.Equal(t, tt.expected.ObjectStore.UsePathStyle, config.ObjectStore.UsePathStyle)

			// Check provider settings
			for key, value := range tt.expected.ObjectStore.ProviderSettings {
				assert.Equal(t, value, config.ObjectStore.ProviderSettings[key])
			}
		})
	}
}

func TestMigrateLegacyEnvVars(t *testing.T) {
	tests := []struct {
		name       string
		legacyVars map[string]string
		expected   map[string]string
	}{
		{
			name: "AWS legacy variables",
			legacyVars: map[string]string{
				"AWS_REGION":     "us-east-1",
				"S3_BUCKET":      "legacy-bucket",
				"AWS_ACCESS_KEY": "legacy-access",
				"AWS_SECRET_KEY": "legacy-secret",
				"S3_PROVIDER":    "aws",
			},
			expected: map[string]string{
				"LAKERUNNER_AWS_REGION":    "us-east-1",
				"LAKERUNNER_BUCKET":        "legacy-bucket",
				"LAKERUNNER_S3_ACCESS_KEY": "legacy-access",
				"LAKERUNNER_S3_SECRET_KEY": "legacy-secret",
				"LAKERUNNER_PROVIDER_TYPE": "aws",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear environment
			for key := range tt.legacyVars {
				os.Unsetenv(key)
			}
			for key := range tt.expected {
				os.Unsetenv(key)
			}

			// Set legacy environment variables
			for key, value := range tt.legacyVars {
				os.Setenv(key, value)
			}
			defer func() {
				for key := range tt.legacyVars {
					os.Unsetenv(key)
				}
				for key := range tt.expected {
					os.Unsetenv(key)
				}
			}()

			// Call migration
			migrateLegacyEnvVars()

			// Check that new variables are set
			for key, expectedValue := range tt.expected {
				actualValue := os.Getenv(key)
				assert.Equal(t, expectedValue, actualValue, "Expected %s=%s, got %s", key, expectedValue, actualValue)
			}
		})
	}
}

func TestParseBool(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"true", true},
		{"false", false},
		{"1", true},
		{"0", false},
		{"yes", false}, // Invalid, should return false
		{"", false},    // Empty, should return false
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseBool(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
