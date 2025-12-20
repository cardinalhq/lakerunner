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

package bootstrap

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/yaml.v3"

	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/internal/logctx"
)

const SupportedVersion = 2

// ImportFromYAML imports configuration from a YAML file into the database
func ImportFromYAML(ctx context.Context, filePath string, configDBPool *pgxpool.Pool) error {
	ll := logctx.FromContext(ctx)

	ll.Info("Starting bootstrap import from YAML", slog.String("file", filePath))

	// Read and parse YAML file
	config, err := loadConfig(filePath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Validate version
	if config.Version != SupportedVersion {
		return fmt.Errorf("unsupported config version %d, expected %d", config.Version, SupportedVersion)
	}

	ll.Info("Loaded bootstrap configuration",
		slog.Int("buckets", len(config.BucketConfigurations)),
		slog.Int("org_buckets", len(config.OrganizationBuckets)),
		slog.Int("prefix_mappings", len(config.BucketPrefixMappings)),
		slog.Int("admin_api_keys", len(config.AdminAPIKeys)),
		slog.Int("org_api_keys", len(config.OrganizationAPIKeys)),
		slog.Int("org_api_key_mappings", len(config.OrganizationAPIKeyMapping)))

	// Start transaction for atomic import
	tx, err := configDBPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		// Use a timeout to prevent infinite hangs during cleanup.
		// Rollback after successful commit is a no-op in pgx.
		rbCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := tx.Rollback(rbCtx); err != nil {
			ll.Warn("Failed to rollback transaction", slog.Any("error", err))
		}
	}()

	qtx := configdb.New(tx)

	// Import bucket configurations
	if err := importBucketConfigurations(ctx, config.BucketConfigurations, qtx); err != nil {
		return fmt.Errorf("failed to import bucket configurations: %w", err)
	}

	// Import organization bucket mappings
	if err := importOrganizationBuckets(ctx, config.OrganizationBuckets, qtx); err != nil {
		return fmt.Errorf("failed to import organization buckets: %w", err)
	}

	// Import bucket prefix mappings
	if err := importBucketPrefixMappings(ctx, config.BucketPrefixMappings, qtx); err != nil {
		return fmt.Errorf("failed to import bucket prefix mappings: %w", err)
	}

	// Import admin API keys
	if err := importAdminAPIKeys(ctx, config.AdminAPIKeys, qtx); err != nil {
		return fmt.Errorf("failed to import admin API keys: %w", err)
	}

	// Import organization API keys
	if err := importOrganizationAPIKeys(ctx, config.OrganizationAPIKeys, qtx); err != nil {
		return fmt.Errorf("failed to import organization API keys: %w", err)
	}

	// Import organization API key mappings
	if err := importOrganizationAPIKeyMappings(ctx, config.OrganizationAPIKeyMapping, qtx); err != nil {
		return fmt.Errorf("failed to import organization API key mappings: %w", err)
	}

	// Use a timeout for commit to prevent hanging if DB is unresponsive.
	commitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := tx.Commit(commitCtx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	ll.Info("Bootstrap import completed successfully")
	return nil
}

func loadConfig(filePath string) (*BootstrapConfig, error) {
	contents, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	var config BootstrapConfig
	dec := yaml.NewDecoder(bytes.NewReader(contents))
	dec.KnownFields(false) // Allow unknown fields for forward compatibility
	if err := dec.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	return &config, nil
}

func importBucketConfigurations(ctx context.Context, configs []BucketConfiguration, qtx *configdb.Queries) error {
	ll := logctx.FromContext(ctx)

	for _, config := range configs {
		_, err := qtx.UpsertBucketConfiguration(ctx, configdb.UpsertBucketConfigurationParams{
			BucketName:    config.BucketName,
			CloudProvider: config.CloudProvider,
			Region:        config.Region,
			Endpoint:      config.Endpoint,
			Role:          config.Role,
			UsePathStyle:  true,
			InsecureTls:   false,
		})
		if err != nil {
			return fmt.Errorf("failed to import bucket %s: %w", config.BucketName, err)
		}
		ll.Info("Imported bucket configuration", slog.String("bucket", config.BucketName))
	}
	return nil
}

func importOrganizationBuckets(ctx context.Context, mappings []OrganizationBucket, qtx *configdb.Queries) error {
	ll := logctx.FromContext(ctx)

	for _, mapping := range mappings {
		// Use values from config if provided, otherwise use defaults
		instanceNum := int16(1)
		if mapping.InstanceNum != nil {
			instanceNum = *mapping.InstanceNum
		}
		collectorName := "default"
		if mapping.CollectorName != nil {
			collectorName = *mapping.CollectorName
		}

		if err := qtx.UpsertOrganizationBucket(ctx, configdb.UpsertOrganizationBucketParams{
			OrganizationID: mapping.OrganizationID,
			BucketID:       mapping.BucketID,
			InstanceNum:    instanceNum,
			CollectorName:  collectorName,
		}); err != nil {
			return fmt.Errorf("failed to import organization bucket mapping %s->%s: %w",
				mapping.OrganizationID, mapping.BucketID, err)
		}
		ll.Info("Imported organization bucket mapping",
			slog.String("org_id", mapping.OrganizationID.String()),
			slog.String("bucket_id", mapping.BucketID.String()))
	}
	return nil
}

func importBucketPrefixMappings(ctx context.Context, mappings []BucketPrefixMapping, qtx *configdb.Queries) error {
	ll := logctx.FromContext(ctx)

	for _, mapping := range mappings {
		_, err := qtx.CreateBucketPrefixMapping(ctx, configdb.CreateBucketPrefixMappingParams{
			BucketID:       mapping.BucketID,
			OrganizationID: mapping.OrganizationID,
			PathPrefix:     mapping.PathPrefix,
			Signal:         mapping.Signal,
		})
		if err != nil {
			return fmt.Errorf("failed to import bucket prefix mapping %s: %w", mapping.PathPrefix, err)
		}
		ll.Info("Imported bucket prefix mapping",
			slog.String("bucket_id", mapping.BucketID.String()),
			slog.String("prefix", mapping.PathPrefix),
			slog.String("signal", mapping.Signal))
	}
	return nil
}

func importAdminAPIKeys(ctx context.Context, keys []AdminAPIKey, qtx *configdb.Queries) error {
	ll := logctx.FromContext(ctx)

	for _, key := range keys {
		keyHash := hashAPIKey(key.Key)
		_, err := qtx.UpsertAdminAPIKey(ctx, configdb.UpsertAdminAPIKeyParams{
			KeyHash:     keyHash,
			Name:        key.Name,
			Description: key.Description,
		})
		if err != nil {
			return fmt.Errorf("failed to import admin API key %s: %w", key.Name, err)
		}
		ll.Info("Imported admin API key", slog.String("name", key.Name))
	}
	return nil
}

func importOrganizationAPIKeys(ctx context.Context, keys []OrganizationAPIKey, qtx *configdb.Queries) error {
	ll := logctx.FromContext(ctx)

	for _, key := range keys {
		keyHash := hashAPIKey(key.Key)
		_, err := qtx.UpsertOrganizationAPIKey(ctx, configdb.UpsertOrganizationAPIKeyParams{
			KeyHash:     keyHash,
			Name:        key.Name,
			Description: key.Description,
		})
		if err != nil {
			return fmt.Errorf("failed to import organization API key %s: %w", key.Name, err)
		}
		ll.Info("Imported organization API key", slog.String("name", key.Name))
	}
	return nil
}

func importOrganizationAPIKeyMappings(ctx context.Context, mappings []OrganizationAPIKeyMapping, qtx *configdb.Queries) error {
	ll := logctx.FromContext(ctx)

	for _, mapping := range mappings {
		if err := qtx.UpsertOrganizationAPIKeyMapping(ctx, configdb.UpsertOrganizationAPIKeyMappingParams{
			ApiKeyID:       mapping.APIKeyID,
			OrganizationID: mapping.OrganizationID,
		}); err != nil {
			return fmt.Errorf("failed to import organization API key mapping %s->%s: %w",
				mapping.APIKeyID, mapping.OrganizationID, err)
		}
		ll.Info("Imported organization API key mapping",
			slog.String("api_key_id", mapping.APIKeyID.String()),
			slog.String("org_id", mapping.OrganizationID.String()))
	}
	return nil
}

func hashAPIKey(apiKey string) string {
	h := sha256.Sum256([]byte(apiKey))
	return fmt.Sprintf("%x", h)
}
