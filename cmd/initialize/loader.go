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

package initialize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/cardinalhq/lakerunner/configdb"
)

// InitializeConfig loads and imports storage profiles and API keys
func InitializeConfig(ctx context.Context, storageProfileFile, apiKeysFile string, qtx *configdb.Queries, logger *slog.Logger, replace bool) error {
	// First sync organizations from c_organizations table
	if err := qtx.SyncOrganizations(ctx); err != nil {
		return fmt.Errorf("failed to sync organizations: %w", err)
	}
	logger.Info("Synced organizations from c_organizations table")

	// Load and import storage profiles
	if err := loadAndImportStorageProfiles(ctx, storageProfileFile, qtx, logger, replace); err != nil {
		return fmt.Errorf("failed to import storage profiles: %w", err)
	}

	// Load and import API keys if provided
	if apiKeysFile != "" {
		if err := loadAndImportAPIKeys(ctx, apiKeysFile, qtx, logger, replace); err != nil {
			return fmt.Errorf("failed to import API keys: %w", err)
		}
	}

	return nil
}

func loadAndImportStorageProfiles(ctx context.Context, filename string, qtx *configdb.Queries, logger *slog.Logger, replace bool) error {
	contents, err := loadFileContents(filename)
	if err != nil {
		return err
	}

	return importStorageProfiles(ctx, contents, qtx, logger, replace)
}

func loadAndImportAPIKeys(ctx context.Context, filename string, qtx *configdb.Queries, logger *slog.Logger, replace bool) error {
	contents, err := loadFileContents(filename)
	if err != nil {
		return err
	}

	var apiKeysConfig APIKeysConfig
	dec := yaml.NewDecoder(bytes.NewReader(contents))
	dec.KnownFields(true)
	if err := dec.Decode(&apiKeysConfig); err != nil {
		return fmt.Errorf("failed to parse API keys YAML: %w", err)
	}

	logger.Info("Loaded API keys configuration", slog.Int("organizations", len(apiKeysConfig)))

	return importAPIKeys(ctx, apiKeysConfig, qtx, logger, replace)
}

func loadFileContents(filename string) ([]byte, error) {
	// Handle env: prefix for environment variables
	if after, ok := strings.CutPrefix(filename, "env:"); ok {
		envVar := after
		envContents := os.Getenv(envVar)
		if envContents == "" {
			return nil, fmt.Errorf("environment variable %s is not set", envVar)
		}
		return []byte(envContents), nil
	}

	contents, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filename, err)
	}
	return contents, nil
}

func importStorageProfiles(ctx context.Context, contents []byte, qtx *configdb.Queries, logger *slog.Logger, replace bool) error {
	var profiles []StorageProfile
	dec := yaml.NewDecoder(bytes.NewReader(contents))
	dec.KnownFields(true)
	if err := dec.Decode(&profiles); err != nil {
		return fmt.Errorf("failed to parse YAML configuration: %w", err)
	}

	logger.Info("Loaded storage profile configuration", slog.Int("profiles", len(profiles)))

	// In replace mode, clear existing data first (mirror sync like sweeper)
	if replace {
		if err := qtx.ClearBucketPrefixMappings(ctx); err != nil {
			return fmt.Errorf("failed to clear bucket prefix mappings: %w", err)
		}
		if err := qtx.ClearOrganizationBuckets(ctx); err != nil {
			return fmt.Errorf("failed to clear organization buckets: %w", err)
		}
		if err := qtx.ClearBucketConfigurations(ctx); err != nil {
			return fmt.Errorf("failed to clear bucket configurations: %w", err)
		}
		logger.Info("Cleared existing storage profile configuration for replace")
	}

	// Group profiles by bucket to create bucket configurations
	bucketProfiles := make(map[string][]StorageProfile)
	for _, profile := range profiles {
		bucketProfiles[profile.Bucket] = append(bucketProfiles[profile.Bucket], profile)
	}

	for bucketName, bucketProfileList := range bucketProfiles {
		// Use the first profile to create bucket configuration
		firstProfile := bucketProfileList[0]

		var endpoint *string
		if firstProfile.Endpoint != "" {
			endpoint = &firstProfile.Endpoint
		}
		var role *string
		if firstProfile.Role != "" {
			role = &firstProfile.Role
		}

		// Create bucket configuration
		bucketConfig, err := qtx.UpsertBucketConfiguration(ctx, configdb.UpsertBucketConfigurationParams{
			BucketName:    bucketName,
			CloudProvider: firstProfile.CloudProvider,
			Region:        firstProfile.Region,
			Endpoint:      endpoint,
			Role:          role,
		})
		if err != nil {
			return fmt.Errorf("failed to create bucket configuration for %s: %w", bucketName, err)
		}
		logger.Info("Created bucket configuration", slog.String("bucket", bucketName))

		// Create organization bucket mappings for each profile
		for _, profile := range bucketProfileList {
			if err := qtx.UpsertOrganizationBucket(ctx, configdb.UpsertOrganizationBucketParams{
				OrganizationID: profile.OrganizationID,
				BucketID:       bucketConfig.ID,
			}); err != nil {
				return fmt.Errorf("failed to create organization bucket mapping %s->%s: %w",
					profile.OrganizationID, bucketConfig.ID, err)
			}
			logger.Info("Created organization bucket mapping",
				slog.String("org_id", profile.OrganizationID.String()),
				slog.String("bucket", bucketName))
		}
	}

	return nil
}

func importAPIKeys(ctx context.Context, apiKeysConfig APIKeysConfig, qtx *configdb.Queries, logger *slog.Logger, replace bool) error {
	// Sync organizations again in case they weren't synced yet or have changed
	if err := qtx.SyncOrganizations(ctx); err != nil {
		return fmt.Errorf("failed to sync organizations before API key import: %w", err)
	}

	// In replace mode, clear existing API keys first (mirror sync like sweeper)
	if replace {
		if err := qtx.ClearOrganizationAPIKeyMappings(ctx); err != nil {
			return fmt.Errorf("failed to clear organization API key mappings: %w", err)
		}
		if err := qtx.ClearOrganizationAPIKeys(ctx); err != nil {
			return fmt.Errorf("failed to clear organization API keys: %w", err)
		}
		logger.Info("Cleared existing API keys for replace")
	}
	for _, orgKeys := range apiKeysConfig {
		for _, key := range orgKeys.Keys {
			keyHash := hashAPIKey(key)
			// Create organization API key
			_, err := qtx.UpsertOrganizationAPIKey(ctx, configdb.UpsertOrganizationAPIKeyParams{
				KeyHash:     keyHash,
				Name:        fmt.Sprintf("imported-key-%s", key[:8]), // Use first 8 chars as name
				Description: nil,
			})
			if err != nil {
				return fmt.Errorf("failed to create organization API key for %s: %w", orgKeys.OrganizationID, err)
			}

			// Get the API key ID to create mapping
			apiKeyRow, err := qtx.GetOrganizationAPIKeyByHash(ctx, keyHash)
			if err != nil {
				return fmt.Errorf("failed to retrieve created API key: %w", err)
			}

			// Create organization API key mapping
			if err := qtx.UpsertOrganizationAPIKeyMapping(ctx, configdb.UpsertOrganizationAPIKeyMappingParams{
				ApiKeyID:       apiKeyRow.ID,
				OrganizationID: orgKeys.OrganizationID,
			}); err != nil {
				return fmt.Errorf("failed to create API key mapping: %w", err)
			}

			logger.Info("Created organization API key",
				slog.String("org_id", orgKeys.OrganizationID.String()),
				slog.String("key_name", fmt.Sprintf("imported-key-%s", key[:8])))
		}
	}

	return nil
}

func hashAPIKey(apiKey string) string {
	h := sha256.Sum256([]byte(apiKey))
	return fmt.Sprintf("%x", h)
}
