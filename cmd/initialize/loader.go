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

package initialize

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"

	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/internal/logctx"
)

// FileReader interface for testable file operations
type FileReader interface {
	ReadFile(filename string) ([]byte, error)
	Getenv(key string) string
}

// OSFileReader implements FileReader using OS operations
type OSFileReader struct{}

func (r OSFileReader) ReadFile(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}

func (r OSFileReader) Getenv(key string) string {
	return os.Getenv(key)
}

// DatabaseQueries interface for testable database operations
type DatabaseQueries interface {
	HasExistingStorageProfiles(ctx context.Context) (bool, error)
	UpsertBucketConfiguration(ctx context.Context, arg configdb.UpsertBucketConfigurationParams) (configdb.BucketConfiguration, error)
	UpsertOrganizationBucket(ctx context.Context, arg configdb.UpsertOrganizationBucketParams) error
	UpsertOrganization(ctx context.Context, arg configdb.UpsertOrganizationParams) (configdb.Organization, error)
	ClearOrganizationAPIKeyMappings(ctx context.Context) error
	ClearOrganizationAPIKeys(ctx context.Context) error
	UpsertOrganizationAPIKey(ctx context.Context, arg configdb.UpsertOrganizationAPIKeyParams) (configdb.OrganizationApiKey, error)
	UpsertOrganizationAPIKeyMapping(ctx context.Context, arg configdb.UpsertOrganizationAPIKeyMappingParams) error
}

// InitializeConfig loads and imports storage profiles and API keys
func InitializeConfig(ctx context.Context, storageProfileFile, apiKeysFile string, qtx *configdb.Queries, replace bool) error {
	fileReader := OSFileReader{}
	return InitializeConfigWithDependencies(ctx, storageProfileFile, apiKeysFile, qtx, fileReader, replace)
}

// InitializeConfigWithDependencies loads and imports with injectable dependencies for testing
func InitializeConfigWithDependencies(ctx context.Context, storageProfileFile, apiKeysFile string, qtx DatabaseQueries, fileReader FileReader, replace bool) error {

	// Load and import storage profiles
	if err := loadAndImportStorageProfilesWithReader(ctx, storageProfileFile, qtx, fileReader, replace); err != nil {
		return fmt.Errorf("failed to import storage profiles: %w", err)
	}

	// Load and import API keys if provided
	if apiKeysFile != "" {
		if err := loadAndImportAPIKeysWithReader(ctx, apiKeysFile, qtx, fileReader, replace); err != nil {
			return fmt.Errorf("failed to import API keys: %w", err)
		}
	}

	return nil
}

func loadAndImportStorageProfilesWithReader(ctx context.Context, filename string, qtx DatabaseQueries, fileReader FileReader, replace bool) error {
	contents, err := loadFileContentsWithReader(filename, fileReader)
	if err != nil {
		return err
	}

	return importStorageProfiles(ctx, contents, qtx, replace)
}

func loadAndImportAPIKeysWithReader(ctx context.Context, filename string, qtx DatabaseQueries, fileReader FileReader, replace bool) error {
	ll := logctx.FromContext(ctx)

	contents, err := loadFileContentsWithReader(filename, fileReader)
	if err != nil {
		return err
	}

	var apiKeysConfig APIKeysConfig
	dec := yaml.NewDecoder(bytes.NewReader(contents))
	dec.KnownFields(true)
	if err := dec.Decode(&apiKeysConfig); err != nil {
		return fmt.Errorf("failed to parse API keys YAML: %w", err)
	}

	ll.Info("Loaded API keys configuration", slog.Int("organizations", len(apiKeysConfig)))

	return importAPIKeys(ctx, apiKeysConfig, qtx, replace)
}

func loadFileContentsWithReader(filename string, fileReader FileReader) ([]byte, error) {
	// Handle env: prefix for environment variables
	if after, ok := strings.CutPrefix(filename, "env:"); ok {
		envVar := after
		envContents := fileReader.Getenv(envVar)
		if envContents == "" {
			return nil, fmt.Errorf("environment variable %s is not set", envVar)
		}
		return []byte(envContents), nil
	}

	contents, err := fileReader.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filename, err)
	}
	return contents, nil
}

func importStorageProfiles(ctx context.Context, contents []byte, qtx DatabaseQueries, replace bool) error {
	ll := logctx.FromContext(ctx)

	var profiles []StorageProfile
	dec := yaml.NewDecoder(bytes.NewReader(contents))
	dec.KnownFields(true)
	if err := dec.Decode(&profiles); err != nil {
		return fmt.Errorf("failed to parse YAML configuration: %w", err)
	}

	ll.Info("Loaded storage profile configuration", slog.Int("profiles", len(profiles)))

	// Even in replace mode, we use non-destructive sync to preserve:
	// 1. Manual bucket_prefix_mappings entries (custom path routing)
	// 2. Foreign key relationships (bucket_prefix_mappings -> bucket_configurations)
	// The upsert operations will add new entries and update existing ones
	if replace {
		ll.Info("Using non-destructive sync (preserving bucket prefix mappings)")
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
			UsePathStyle:  firstProfile.UsePathStyle,
			InsecureTls:   firstProfile.InsecureTLS,
		})
		if err != nil {
			return fmt.Errorf("failed to create bucket configuration for %s: %w", bucketName, err)
		}
		ll.Info("Created bucket configuration", slog.String("bucket", bucketName))

		// Create organization bucket mappings for each profile
		for _, profile := range bucketProfileList {
			// Use values from YAML if provided, otherwise use defaults
			instanceNum := int16(profile.InstanceNum)
			if instanceNum == 0 {
				instanceNum = 1
			}
			collectorName := profile.CollectorName
			if collectorName == "" {
				collectorName = "default"
			}

			if err := qtx.UpsertOrganizationBucket(ctx, configdb.UpsertOrganizationBucketParams{
				OrganizationID: profile.OrganizationID,
				BucketID:       bucketConfig.ID,
				InstanceNum:    instanceNum,
				CollectorName:  collectorName,
			}); err != nil {
				return fmt.Errorf("failed to create organization bucket mapping %s->%s: %w",
					profile.OrganizationID, bucketConfig.ID, err)
			}
			ll.Info("Created organization bucket mapping",
				slog.String("org_id", profile.OrganizationID.String()),
				slog.String("bucket", bucketName))
		}
	}

	return nil
}

func importAPIKeys(ctx context.Context, apiKeysConfig APIKeysConfig, qtx DatabaseQueries, replace bool) error {
	ll := logctx.FromContext(ctx)

	// Note: We skip SyncOrganizations() during migration/initialization
	// as it depends on legacy c_organizations table. Organization IDs
	// in YAML are assumed to be valid.

	// Ensure organizations exist for the API keys we're importing
	if err := ensureOrganizationsFromAPIKeys(ctx, apiKeysConfig, qtx); err != nil {
		return fmt.Errorf("failed to ensure organizations exist: %w", err)
	}

	// In replace mode, clear existing API keys first (mirror sync like sweeper)
	if replace {
		if err := qtx.ClearOrganizationAPIKeyMappings(ctx); err != nil {
			return fmt.Errorf("failed to clear organization API key mappings: %w", err)
		}
		if err := qtx.ClearOrganizationAPIKeys(ctx); err != nil {
			return fmt.Errorf("failed to clear organization API keys: %w", err)
		}
		ll.Info("Cleared existing API keys for replace")
	}
	for _, orgKeys := range apiKeysConfig {
		for _, key := range orgKeys.Keys {
			keyHash := hashAPIKey(key)
			// Create organization API key
			apiKey, err := qtx.UpsertOrganizationAPIKey(ctx, configdb.UpsertOrganizationAPIKeyParams{
				KeyHash:     keyHash,
				Name:        fmt.Sprintf("imported-key-%s", key[:8]), // Use first 8 chars as name
				Description: nil,
			})
			if err != nil {
				return fmt.Errorf("failed to create organization API key for %s: %w", orgKeys.OrganizationID, err)
			}

			// Create organization API key mapping using the ID from the upsert result
			if err := qtx.UpsertOrganizationAPIKeyMapping(ctx, configdb.UpsertOrganizationAPIKeyMappingParams{
				ApiKeyID:       apiKey.ID,
				OrganizationID: orgKeys.OrganizationID,
			}); err != nil {
				return fmt.Errorf("failed to create API key mapping: %w", err)
			}

			ll.Info("Created organization API key",
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

func ensureOrganizationsFromAPIKeys(ctx context.Context, apiKeysConfig APIKeysConfig, qtx DatabaseQueries) error {
	ll := logctx.FromContext(ctx)

	// Create a set of unique organization IDs from the API keys config
	orgIDs := make(map[string]bool)
	for _, orgKeys := range apiKeysConfig {
		orgIDs[orgKeys.OrganizationID.String()] = true
	}

	// Ensure each organization exists
	for orgIDStr := range orgIDs {
		orgID, err := uuid.Parse(orgIDStr)
		if err != nil {
			return fmt.Errorf("invalid organization ID %s: %w", orgIDStr, err)
		}

		_, err = qtx.UpsertOrganization(ctx, configdb.UpsertOrganizationParams{
			ID:      orgID,
			Name:    fmt.Sprintf("imported-org-%s", orgIDStr[:8]), // Use first 8 chars as name
			Enabled: true,
		})
		if err != nil {
			return fmt.Errorf("failed to ensure organization %s exists: %w", orgIDStr, err)
		}

		ll.Info("Ensured organization exists", slog.String("org_id", orgIDStr))
	}

	return nil
}

// LoadFileContentsWithReader loads file contents using the provided FileReader
func LoadFileContentsWithReader(filename string, fileReader FileReader) ([]byte, error) {
	return loadFileContentsWithReader(filename, fileReader)
}

// UnmarshalYAML unmarshals YAML data into the provided interface
func UnmarshalYAML(data []byte, v interface{}) error {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	return dec.Decode(v)
}
