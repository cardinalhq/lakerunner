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

package cmd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/configdb"
)

func init() {
	var configFile string
	var apiKeysFile string

	initializeCmd := &cobra.Command{
		Use:   "initialize",
		Short: "Initialize storage profiles and API keys from YAML configuration",
		Long: `Initialize storage profiles and API keys by reading YAML configuration files and
populating the database tables.

This command replaces the environment variable-based file configuration approach
with a one-time database initialization from YAML.

Supports both v1 (simple list) and v2 (structured buckets) formats for storage profiles.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runInitialize(configFile, apiKeysFile)
		},
	}

	initializeCmd.Flags().StringVarP(&configFile, "config", "c", "", "Path to storage profile YAML configuration file (required)")
	initializeCmd.Flags().StringVarP(&apiKeysFile, "api-keys", "k", "", "Path to API keys YAML configuration file (optional)")
	if err := initializeCmd.MarkFlagRequired("config"); err != nil {
		panic(fmt.Sprintf("failed to mark flag as required: %v", err))
	}

	rootCmd.AddCommand(initializeCmd)
}

func runInitialize(configFile string, apiKeysFile string) error {
	ctx := context.Background()

	// Connect to configdb
	configDBPool, err := dbopen.ConnectToConfigDB(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to configdb: %w", err)
	}
	defer configDBPool.Close()

	// Start transaction for atomic import
	tx, err := configDBPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			slog.Warn("Failed to rollback transaction", slog.Any("error", err))
		}
	}()

	qtx := configdb.New(tx)

	// Load and import storage profiles
	if err := loadAndImportStorageProfiles(ctx, configFile, qtx, slog.Default()); err != nil {
		return fmt.Errorf("failed to import storage profiles: %w", err)
	}

	// Load and import API keys if provided
	if apiKeysFile != "" {
		if err := loadAndImportAPIKeys(ctx, apiKeysFile, qtx, slog.Default()); err != nil {
			return fmt.Errorf("failed to import API keys: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	slog.Info("Initialization completed successfully")
	return nil
}

// StorageProfileConfig represents the YAML structure for storage profile initialization (v2)
type StorageProfileConfig struct {
	Version int                    `yaml:"version"`
	Buckets []StorageProfileBucket `yaml:"buckets"`
}

type StorageProfileBucket struct {
	Name           string                        `yaml:"name"`
	CloudProvider  string                        `yaml:"cloud_provider"`
	Region         string                        `yaml:"region"`
	Endpoint       string                        `yaml:"endpoint,omitempty"`
	Role           string                        `yaml:"role,omitempty"`
	Organizations  []uuid.UUID                   `yaml:"organizations"`
	PrefixMappings []StorageProfilePrefixMapping `yaml:"prefix_mappings,omitempty"`
}

type StorageProfilePrefixMapping struct {
	OrganizationID uuid.UUID `yaml:"organization_id"`
	Prefix         string    `yaml:"prefix"`
	Signal         string    `yaml:"signal"`
}

// V1 Storage Profile format (simple list)
type StorageProfileV1 struct {
	OrganizationID uuid.UUID `yaml:"organization_id"`
	InstanceNum    int       `yaml:"instance_num,omitempty"`   // Ignored
	CollectorName  string    `yaml:"collector_name,omitempty"` // Ignored
	CloudProvider  string    `yaml:"cloud_provider"`
	Region         string    `yaml:"region"`
	Bucket         string    `yaml:"bucket"`
	Role           string    `yaml:"role,omitempty"`
	Endpoint       string    `yaml:"endpoint,omitempty"`
	UsePathStyle   bool      `yaml:"use_path_style,omitempty"`
	InsecureTLS    bool      `yaml:"insecure_tls,omitempty"`
	UseSSL         bool      `yaml:"use_ssl,omitempty"`
}

// API Keys configuration
type APIKeysConfig []APIKeyOrg

type APIKeyOrg struct {
	OrganizationID uuid.UUID `yaml:"organization_id"`
	Keys           []string  `yaml:"keys"`
}

func loadAndImportStorageProfiles(ctx context.Context, filename string, qtx *configdb.Queries, logger *slog.Logger) error {
	contents, err := loadFileContents(filename)
	if err != nil {
		return err
	}

	// Try to detect version by checking for version field
	var versionCheck struct {
		Version int `yaml:"version"`
	}
	dec := yaml.NewDecoder(bytes.NewReader(contents))
	if err := dec.Decode(&versionCheck); err == nil && versionCheck.Version == 2 {
		// V2 format
		return importStorageProfilesV2(ctx, contents, qtx, logger)
	}

	// V1 format (no version field)
	return importStorageProfilesV1(ctx, contents, qtx, logger)
}

func loadAndImportAPIKeys(ctx context.Context, filename string, qtx *configdb.Queries, logger *slog.Logger) error {
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

	return importAPIKeys(ctx, apiKeysConfig, qtx, logger)
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

func importStorageProfilesV2(ctx context.Context, contents []byte, qtx *configdb.Queries, logger *slog.Logger) error {
	var config StorageProfileConfig
	dec := yaml.NewDecoder(bytes.NewReader(contents))
	dec.KnownFields(true)
	if err := dec.Decode(&config); err != nil {
		return fmt.Errorf("failed to parse v2 YAML configuration: %w", err)
	}

	// Validate version
	if config.Version != 2 {
		return fmt.Errorf("unsupported configuration version %d, expected version 2", config.Version)
	}

	logger.Info("Loaded v2 storage profile configuration", slog.Int("buckets", len(config.Buckets)))

	return importStorageProfileConfigV2(ctx, &config, qtx, logger)
}

func importStorageProfileConfigV2(ctx context.Context, config *StorageProfileConfig, qtx *configdb.Queries, logger *slog.Logger) error {
	for _, bucket := range config.Buckets {
		// Create bucket configuration
		var endpoint *string
		if bucket.Endpoint != "" {
			endpoint = &bucket.Endpoint
		}
		var role *string
		if bucket.Role != "" {
			role = &bucket.Role
		}

		bucketConfig, err := qtx.UpsertBucketConfiguration(ctx, configdb.UpsertBucketConfigurationParams{
			BucketName:    bucket.Name,
			CloudProvider: bucket.CloudProvider,
			Region:        bucket.Region,
			Endpoint:      endpoint,
			Role:          role,
		})
		if err != nil {
			return fmt.Errorf("failed to create bucket configuration for %s: %w", bucket.Name, err)
		}
		logger.Info("Created bucket configuration", slog.String("bucket", bucket.Name))

		// Create organization bucket mappings
		for _, orgID := range bucket.Organizations {
			if err := qtx.UpsertOrganizationBucket(ctx, configdb.UpsertOrganizationBucketParams{
				OrganizationID: orgID,
				BucketID:       bucketConfig.ID,
			}); err != nil {
				return fmt.Errorf("failed to create organization bucket mapping %s->%s: %w",
					orgID, bucketConfig.ID, err)
			}
			logger.Info("Created organization bucket mapping",
				slog.String("org_id", orgID.String()),
				slog.String("bucket", bucket.Name))
		}

		// Create prefix mappings
		for _, mapping := range bucket.PrefixMappings {
			_, err := qtx.CreateBucketPrefixMapping(ctx, configdb.CreateBucketPrefixMappingParams{
				BucketID:       bucketConfig.ID,
				OrganizationID: mapping.OrganizationID,
				PathPrefix:     mapping.Prefix,
				Signal:         mapping.Signal,
			})
			if err != nil {
				return fmt.Errorf("failed to create prefix mapping %s: %w", mapping.Prefix, err)
			}
			logger.Info("Created prefix mapping",
				slog.String("bucket", bucket.Name),
				slog.String("prefix", mapping.Prefix),
				slog.String("signal", mapping.Signal))
		}
	}

	return nil
}

func importStorageProfilesV1(ctx context.Context, contents []byte, qtx *configdb.Queries, logger *slog.Logger) error {
	var profiles []StorageProfileV1
	dec := yaml.NewDecoder(bytes.NewReader(contents))
	dec.KnownFields(true)
	if err := dec.Decode(&profiles); err != nil {
		return fmt.Errorf("failed to parse v1 YAML configuration: %w", err)
	}

	logger.Info("Loaded v1 storage profile configuration", slog.Int("profiles", len(profiles)))

	// Group profiles by bucket to create bucket configurations
	bucketProfiles := make(map[string][]StorageProfileV1)
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

func importAPIKeys(ctx context.Context, apiKeysConfig APIKeysConfig, qtx *configdb.Queries, logger *slog.Logger) error {
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
