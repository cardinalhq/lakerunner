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

package sweeper

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cardinalhq/lakerunner/configdb"
)

func runLegacyTablesSync(ctx context.Context, ll *slog.Logger, cdb configdb.QuerierFull, cdbPool *pgxpool.Pool) (err error) {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		legacyTableSyncDuration.Record(ctx, duration)
		status := "success"
		if err != nil {
			status = "failure"
		}
		legacyTableSyncCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", status),
		))
	}()

	ll.Info("Starting legacy table sync")

	// Get all storage profiles from c_ tables
	profiles, err := cdb.GetAllCStorageProfilesForSync(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			ll.Info("No legacy storage profiles found")
			return nil
		}
		return err
	}

	if len(profiles) == 0 {
		ll.Info("No legacy storage profiles to sync")
		return nil
	}

	ll.Info("Found legacy storage profiles to sync", slog.Int("count", len(profiles)))

	// Start a transaction for atomic sync
	closed := false
	tx, err := cdbPool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if !closed {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				ll.Warn("Failed to rollback transaction", slog.Any("error", rbErr))
			}
		}
	}()

	qtx := configdb.New(tx)

	// Clear existing bucket management tables (mirror mode)
	if err = qtx.ClearBucketPrefixMappings(ctx); err != nil {
		return
	}
	if err = qtx.ClearOrganizationBuckets(ctx); err != nil {
		return
	}
	if err = qtx.ClearBucketConfigurations(ctx); err != nil {
		return
	}

	// Group profiles by bucket to ensure 1:1 mapping
	bucketToProfile := make(map[string]configdb.GetAllCStorageProfilesForSyncRow)
	bucketToOrgs := make(map[string][]uuid.UUID)

	for _, profile := range profiles {
		// Use first profile found for each bucket (1:1 mapping enforced)
		if _, exists := bucketToProfile[profile.BucketName]; !exists {
			bucketToProfile[profile.BucketName] = profile
		}

		bucketToOrgs[profile.BucketName] = append(bucketToOrgs[profile.BucketName], profile.OrganizationID)
	}

	// Create bucket configurations and organization mappings
	for bucketName, profile := range bucketToProfile {
		// Create/update bucket configuration
		_, err = qtx.UpsertBucketConfiguration(ctx, configdb.UpsertBucketConfigurationParams{
			BucketName:    bucketName,
			CloudProvider: profile.CloudProvider,
			Region:        profile.Region,
			Endpoint:      nil,
			Role:          profile.Role,
			UsePathStyle:  true,
			InsecureTls:   false,
		})
		if err != nil {
			return
		}

		ll.Info("Synced bucket configuration",
			slog.String("bucket", bucketName),
			slog.String("provider", profile.CloudProvider),
			slog.String("region", profile.Region))

	}

	// Sync organization buckets from c_collectors to our organization_buckets table
	ll.Info("Syncing organization buckets from c_collectors table")
	if err = qtx.SyncOrganizationBuckets(ctx); err != nil {
		err = fmt.Errorf("failed to sync organization buckets: %w", err)
		return
	}
	ll.Info("Successfully synced organization buckets")

	// Sync organizations from c_organizations to our organizations table
	ll.Info("Syncing organizations from c_organizations table")
	if err = qtx.SyncOrganizations(ctx); err != nil {
		err = fmt.Errorf("failed to sync organizations: %w", err)
		return
	}
	ll.Info("Successfully synced organizations")

	// Sync organization API keys from c_organization_api_keys to our organization tables
	if err = syncOrganizationAPIKeys(ctx, ll, qtx); err != nil {
		return
	}

	// Commit the transaction
	if err = tx.Commit(ctx); err != nil {
		return
	}
	closed = true

	ll.Info("Legacy table sync completed successfully",
		slog.Int("bucketsSync", len(bucketToProfile)),
		slog.Int("totalProfiles", len(profiles)))

	return nil
}

func syncOrganizationAPIKeys(ctx context.Context, ll *slog.Logger, qtx *configdb.Queries) error {
	ll.Info("Starting organization API keys sync")

	// Get all organization API keys from c_ table
	cAPIKeys, err := qtx.GetAllCOrganizationAPIKeysForSync(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			ll.Info("No legacy organization API keys found")
			return nil
		}
		return err
	}

	if len(cAPIKeys) == 0 {
		ll.Info("No legacy organization API keys to sync")
		return nil
	}

	ll.Info("Found legacy organization API keys to sync", slog.Int("count", len(cAPIKeys)))

	// Clear existing organization API key tables (mirror mode)
	if err := qtx.ClearOrganizationAPIKeyMappings(ctx); err != nil {
		return err
	}
	if err := qtx.ClearOrganizationAPIKeys(ctx); err != nil {
		return err
	}

	// Sync API keys
	for _, cAPIKey := range cAPIKeys {
		if !cAPIKey.OrganizationID.Valid || cAPIKey.ApiKey == nil {
			ll.Warn("Skipping API key with invalid data",
				slog.Bool("hasOrgID", cAPIKey.OrganizationID.Valid),
				slog.Bool("hasAPIKey", cAPIKey.ApiKey != nil))
			continue
		}

		orgID := cAPIKey.OrganizationID.Bytes
		orgUUID, err := uuid.FromBytes(orgID[:])
		if err != nil {
			ll.Warn("Skipping API key with invalid organization UUID", slog.Any("error", err))
			continue
		}

		apiKey := *cAPIKey.ApiKey
		keyHash := hashAPIKey(apiKey)

		// Create organization API key
		var name string
		if cAPIKey.Name != nil && *cAPIKey.Name != "" {
			name = *cAPIKey.Name
		} else {
			name = fmt.Sprintf("synced-key-%s", apiKey[:min(8, len(apiKey))])
		}

		apiKeyRow, err := qtx.UpsertOrganizationAPIKey(ctx, configdb.UpsertOrganizationAPIKeyParams{
			KeyHash:     keyHash,
			Name:        name,
			Description: nil,
		})
		if err != nil {
			return fmt.Errorf("failed to sync organization API key for %s: %w", orgUUID, err)
		}

		// Create organization API key mapping
		if err := qtx.UpsertOrganizationAPIKeyMapping(ctx, configdb.UpsertOrganizationAPIKeyMappingParams{
			ApiKeyID:       apiKeyRow.ID,
			OrganizationID: orgUUID,
		}); err != nil {
			return fmt.Errorf("failed to create API key mapping: %w", err)
		}

		ll.Info("Synced organization API key",
			slog.String("org_id", orgUUID.String()),
			slog.String("key_name", name))
	}

	ll.Info("Organization API keys sync completed successfully", slog.Int("keysSync", len(cAPIKeys)))
	return nil
}

func hashAPIKey(apiKey string) string {
	h := sha256.Sum256([]byte(apiKey))
	return fmt.Sprintf("%x", h)
}
