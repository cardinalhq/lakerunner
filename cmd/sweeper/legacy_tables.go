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
	"github.com/cardinalhq/lakerunner/internal/logctx"
)

// SyncQuerier interface for sync operations (testable)
type SyncQuerier interface {
	GetAllCOrganizations(ctx context.Context) ([]configdb.GetAllCOrganizationsRow, error)
	GetAllOrganizations(ctx context.Context) ([]configdb.GetAllOrganizationsRow, error)
	UpsertOrganizationSync(ctx context.Context, arg configdb.UpsertOrganizationSyncParams) error
	DeleteOrganizationsNotInList(ctx context.Context, idsToDelete []uuid.UUID) error
	GetAllCBucketData(ctx context.Context) ([]configdb.GetAllCBucketDataRow, error)
	GetAllBucketConfigurations(ctx context.Context) ([]configdb.GetAllBucketConfigurationsRow, error)
	GetAllOrganizationBucketMappings(ctx context.Context) ([]configdb.GetAllOrganizationBucketMappingsRow, error)
	UpsertBucketConfiguration(ctx context.Context, arg configdb.UpsertBucketConfigurationParams) (configdb.BucketConfiguration, error)
	UpsertOrganizationBucket(ctx context.Context, arg configdb.UpsertOrganizationBucketParams) error
	DeleteOrganizationBucketMappings(ctx context.Context, arg configdb.DeleteOrganizationBucketMappingsParams) error
	GetAllCOrganizationAPIKeys(ctx context.Context) ([]configdb.GetAllCOrganizationAPIKeysRow, error)
	GetAllOrganizationAPIKeyMappings(ctx context.Context) ([]configdb.GetAllOrganizationAPIKeyMappingsRow, error)
	UpsertOrganizationAPIKey(ctx context.Context, arg configdb.UpsertOrganizationAPIKeyParams) (configdb.OrganizationApiKey, error)
	UpsertOrganizationAPIKeyMapping(ctx context.Context, arg configdb.UpsertOrganizationAPIKeyMappingParams) error
	DeleteOrganizationAPIKeyMappingByHash(ctx context.Context, arg configdb.DeleteOrganizationAPIKeyMappingByHashParams) error
}

func runLegacyTablesSync(ctx context.Context, cdbPool *pgxpool.Pool) (err error) {
	ll := logctx.FromContext(ctx)

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

	ll.Debug("Starting legacy table sync")

	// Start a transaction for atomic sync
	closed := false
	tx, err := cdbPool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if !closed {
			// Use context.Background() for rollback to ensure it completes
			// even if the original context was cancelled
			if rbErr := tx.Rollback(context.Background()); rbErr != nil {
				ll.Warn("Failed to rollback transaction", slog.Any("error", rbErr))
			}
		}
	}()

	qtx := configdb.New(tx)

	// Sync organizations
	if err = syncOrganizations(ctx, SyncQuerier(qtx)); err != nil {
		return fmt.Errorf("failed to sync organizations: %w", err)
	}

	// Sync bucket configurations and organization bucket mappings
	if err = syncBucketData(ctx, SyncQuerier(qtx)); err != nil {
		return fmt.Errorf("failed to sync bucket data: %w", err)
	}

	// Sync API keys
	if err = syncOrganizationAPIKeys(ctx, SyncQuerier(qtx)); err != nil {
		return fmt.Errorf("failed to sync API keys: %w", err)
	}

	// Commit the transaction
	if err = tx.Commit(ctx); err != nil {
		return
	}
	closed = true

	ll.Debug("Legacy table sync completed successfully")
	return nil
}

func syncOrganizations(ctx context.Context, qtx SyncQuerier) error {
	ll := logctx.FromContext(ctx)

	// Step 1: Fetch all organizations from c_ tables
	cOrgs, err := qtx.GetAllCOrganizations(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			ll.Debug("No organizations in c_ tables")
			cOrgs = []configdb.GetAllCOrganizationsRow{}
		} else {
			return err
		}
	}

	// Step 2: Fetch all our organizations
	ourOrgs, err := qtx.GetAllOrganizations(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			ourOrgs = []configdb.GetAllOrganizationsRow{}
		} else {
			return err
		}
	}

	// Create lookup maps
	cOrgMap := make(map[uuid.UUID]configdb.GetAllCOrganizationsRow)
	for _, org := range cOrgs {
		cOrgMap[org.ID] = org
	}

	ourOrgMap := make(map[uuid.UUID]configdb.GetAllOrganizationsRow)
	for _, org := range ourOrgs {
		ourOrgMap[org.ID] = org
	}

	// Step 3: Add/Update organizations that exist in c_ tables
	for id, cOrg := range cOrgMap {
		name := ""
		if cOrg.Name != nil {
			name = *cOrg.Name
		}
		enabled := true
		if cOrg.Enabled.Valid {
			enabled = cOrg.Enabled.Bool
		}

		ourOrg, exists := ourOrgMap[id]
		if !exists || ourOrg.Name != name || ourOrg.Enabled != enabled {
			if err := qtx.UpsertOrganizationSync(ctx, configdb.UpsertOrganizationSyncParams{
				ID:      id,
				Name:    name,
				Enabled: enabled,
			}); err != nil {
				return err
			}
			if !exists {
				ll.Debug("Added organization", slog.String("id", id.String()), slog.String("name", name))
			} else {
				ll.Debug("Updated organization", slog.String("id", id.String()), slog.String("name", name))
			}
		}
	}

	// Step 4: Delete organizations that don't exist in c_ tables
	var toDelete []uuid.UUID
	for id := range ourOrgMap {
		if _, exists := cOrgMap[id]; !exists {
			toDelete = append(toDelete, id)
		}
	}

	if len(toDelete) > 0 {
		if err := qtx.DeleteOrganizationsNotInList(ctx, toDelete); err != nil {
			return err
		}
		ll.Debug("Deleted organizations", slog.Int("count", len(toDelete)))
	}

	return nil
}

func syncBucketData(ctx context.Context, qtx SyncQuerier) error {
	ll := logctx.FromContext(ctx)

	// Step 1: Fetch all bucket data from c_ tables
	cBucketData, err := qtx.GetAllCBucketData(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			ll.Debug("No bucket data in c_ tables")
			cBucketData = []configdb.GetAllCBucketDataRow{}
		} else {
			return err
		}
	}

	// Step 2: Fetch our bucket configurations
	ourBuckets, err := qtx.GetAllBucketConfigurations(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			ourBuckets = []configdb.GetAllBucketConfigurationsRow{}
		} else {
			return err
		}
	}

	// Step 3: Fetch our organization bucket mappings
	ourMappings, err := qtx.GetAllOrganizationBucketMappings(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			ourMappings = []configdb.GetAllOrganizationBucketMappingsRow{}
		} else {
			return err
		}
	}

	// Create bucket lookup map
	bucketMap := make(map[string]uuid.UUID)
	for _, bucket := range ourBuckets {
		bucketMap[bucket.BucketName] = bucket.ID
	}

	// Process bucket configurations from c_ tables
	// Use the configuration with a role if available, otherwise use the first one
	seenBuckets := make(map[string]configdb.GetAllCBucketDataRow)
	for _, data := range cBucketData {
		if existing, exists := seenBuckets[data.BucketName]; !exists {
			seenBuckets[data.BucketName] = data
		} else {
			// If we find a configuration with a role and current doesn't have one, use it
			if existing.Role == nil && data.Role != nil {
				seenBuckets[data.BucketName] = data
				ll.Debug("Updated bucket configuration to use entry with role",
					slog.String("bucket", data.BucketName),
					slog.Any("role", data.Role))
			} else if existing.CloudProvider != data.CloudProvider || existing.Region != data.Region {
				// Only warn if provider or region are actually different
				ll.Warn("Inconsistent bucket configuration",
					slog.String("bucket", data.BucketName),
					slog.String("existing_provider", existing.CloudProvider),
					slog.String("new_provider", data.CloudProvider),
					slog.String("existing_region", existing.Region),
					slog.String("new_region", data.Region))
			}
		}
	}

	// Add/Update bucket configurations
	for bucketName, data := range seenBuckets {
		bucketConfig, err := qtx.UpsertBucketConfiguration(ctx, configdb.UpsertBucketConfigurationParams{
			BucketName:    bucketName,
			CloudProvider: data.CloudProvider,
			Region:        data.Region,
			Role:          data.Role,
			Endpoint:      nil,
			UsePathStyle:  true,
			InsecureTls:   false,
		})
		if err != nil {
			return err
		}
		bucketMap[bucketName] = bucketConfig.ID
		ll.Debug("Synced bucket configuration", slog.String("bucket", bucketName))
	}

	// Process organization bucket mappings
	type mappingKey struct {
		OrgID         uuid.UUID
		BucketName    string
		InstanceNum   int16
		CollectorName string
	}

	cMappings := make(map[mappingKey]bool)
	for _, data := range cBucketData {
		if data.OrganizationID.Valid && data.InstanceNum.Valid && data.CollectorName != nil {
			orgUUID, err := uuid.FromBytes(data.OrganizationID.Bytes[:])
			if err != nil {
				ll.Warn("Invalid organization UUID", slog.Any("error", err))
				continue
			}

			key := mappingKey{
				OrgID:         orgUUID,
				BucketName:    data.BucketName,
				InstanceNum:   data.InstanceNum.Int16,
				CollectorName: *data.CollectorName,
			}
			cMappings[key] = true

			// Add/Update mapping
			if bucketID, exists := bucketMap[data.BucketName]; exists {
				if err := qtx.UpsertOrganizationBucket(ctx, configdb.UpsertOrganizationBucketParams{
					OrganizationID: orgUUID,
					BucketID:       bucketID,
					InstanceNum:    data.InstanceNum.Int16,
					CollectorName:  *data.CollectorName,
				}); err != nil {
					return err
				}
			}
		}
	}

	// Delete mappings that don't exist in c_ tables
	var deleteOrgIDs []uuid.UUID
	var deleteInstanceNums []int16
	var deleteCollectorNames []string

	for _, mapping := range ourMappings {
		key := mappingKey{
			OrgID:         mapping.OrganizationID,
			BucketName:    mapping.BucketName,
			InstanceNum:   mapping.InstanceNum,
			CollectorName: mapping.CollectorName,
		}
		if !cMappings[key] {
			deleteOrgIDs = append(deleteOrgIDs, mapping.OrganizationID)
			deleteInstanceNums = append(deleteInstanceNums, mapping.InstanceNum)
			deleteCollectorNames = append(deleteCollectorNames, mapping.CollectorName)
		}
	}

	if len(deleteOrgIDs) > 0 {
		if err := qtx.DeleteOrganizationBucketMappings(ctx, configdb.DeleteOrganizationBucketMappingsParams{
			OrgIds:         deleteOrgIDs,
			InstanceNums:   deleteInstanceNums,
			CollectorNames: deleteCollectorNames,
		}); err != nil {
			return err
		}
		ll.Debug("Deleted organization bucket mappings", slog.Int("count", len(deleteOrgIDs)))
	}

	return nil
}

func syncOrganizationAPIKeys(ctx context.Context, qtx SyncQuerier) error {
	ll := logctx.FromContext(ctx)

	ll.Debug("Starting organization API keys sync")

	// Step 1: Get all API keys from c_ table
	cAPIKeys, err := qtx.GetAllCOrganizationAPIKeys(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			ll.Debug("No API keys in c_ tables")
			cAPIKeys = []configdb.GetAllCOrganizationAPIKeysRow{}
		} else {
			return err
		}
	}

	// Step 2: Get all our API key mappings
	ourMappings, err := qtx.GetAllOrganizationAPIKeyMappings(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			ourMappings = []configdb.GetAllOrganizationAPIKeyMappingsRow{}
		} else {
			return err
		}
	}

	// Create lookup maps
	type apiKeyMapping struct {
		OrgID   uuid.UUID
		KeyHash string
	}

	cAPIKeyMap := make(map[apiKeyMapping]bool)
	for _, key := range cAPIKeys {
		if key.OrganizationID.Valid && key.ApiKey != nil {
			orgUUID, err := uuid.FromBytes(key.OrganizationID.Bytes[:])
			if err != nil {
				ll.Warn("Invalid organization UUID in API key", slog.Any("error", err))
				continue
			}

			keyHash := hashAPIKey(*key.ApiKey)
			mapping := apiKeyMapping{
				OrgID:   orgUUID,
				KeyHash: keyHash,
			}
			cAPIKeyMap[mapping] = true

			// Add/Update API key
			var name string
			if key.Name != nil && *key.Name != "" {
				name = *key.Name
			} else {
				name = fmt.Sprintf("synced-key-%s", (*key.ApiKey)[:min(8, len(*key.ApiKey))])
			}

			apiKeyRow, err := qtx.UpsertOrganizationAPIKey(ctx, configdb.UpsertOrganizationAPIKeyParams{
				KeyHash:     keyHash,
				Name:        name,
				Description: nil,
			})
			if err != nil {
				return fmt.Errorf("failed to upsert API key: %w", err)
			}

			if err := qtx.UpsertOrganizationAPIKeyMapping(ctx, configdb.UpsertOrganizationAPIKeyMappingParams{
				ApiKeyID:       apiKeyRow.ID,
				OrganizationID: orgUUID,
			}); err != nil {
				return fmt.Errorf("failed to upsert API key mapping: %w", err)
			}
		}
	}

	// Delete mappings that don't exist in c_ tables
	for _, mapping := range ourMappings {
		key := apiKeyMapping{
			OrgID:   mapping.OrganizationID,
			KeyHash: mapping.KeyHash,
		}
		if !cAPIKeyMap[key] {
			if err := qtx.DeleteOrganizationAPIKeyMappingByHash(ctx, configdb.DeleteOrganizationAPIKeyMappingByHashParams{
				OrganizationID: mapping.OrganizationID,
				KeyHash:        mapping.KeyHash,
			}); err != nil {
				return err
			}
			ll.Debug("Deleted API key mapping",
				slog.String("org_id", mapping.OrganizationID.String()),
				slog.String("key_hash", mapping.KeyHash[:8]))
		}
	}

	ll.Debug("Organization API keys sync completed successfully")
	return nil
}

func hashAPIKey(apiKey string) string {
	h := sha256.Sum256([]byte(apiKey))
	return fmt.Sprintf("%x", h)
}
