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
	"context"
	"fmt"
	"strings"

	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

// GetObjectStoreClientForProfile creates an object store client for the given storage profile.
// This is the main replacement for awsmanager.GetS3ForProfile.
func GetObjectStoreClientForProfile(ctx context.Context, profile storageprofile.StorageProfile) (ObjectStoreClient, error) {
	// Convert StorageProfile to ObjectStoreConfig
	config := ObjectStoreConfig{
		Bucket:           profile.Bucket,
		Region:           profile.Region,
		Endpoint:         profile.Endpoint,
		Role:             profile.Role,
		UsePathStyle:     profile.UsePathStyle,
		InsecureTLS:      profile.InsecureTLS,
		ProviderSettings: make(map[string]any),
	}

	// Copy provider-specific settings
	if profile.ProviderConfig != nil {
		for k, v := range profile.ProviderConfig {
			config.ProviderSettings[k] = v
		}
	}

	// Determine provider type
	providerType := ProviderType(profile.ProviderType)
	if providerType == "" {
		providerType = ParseProviderTypeFromProfile(profile)
	}

	// Create provider configuration
	providerConfig := ProviderConfig{
		Type:        providerType,
		ObjectStore: config,
		Settings:    make(map[string]string),
	}

	// Create provider manager
	manager, err := NewManager(providerConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create provider manager: %w", err)
	}

	// Get object store client
	client, err := manager.GetObjectStoreClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get object store client: %w", err)
	}

	return client, nil
}

// ParseProviderTypeFromProfile determines the provider type from a storage profile.
// This handles backward compatibility where profiles might not have ProviderType set.
func ParseProviderTypeFromProfile(profile storageprofile.StorageProfile) ProviderType {
	if profile.ProviderType != "" {
		return ProviderType(profile.ProviderType)
	}

	// Try to infer from endpoint or other settings
	if profile.Endpoint != "" {
		endpoint := strings.ToLower(profile.Endpoint)
		if strings.Contains(endpoint, "storage.googleapis.com") || strings.Contains(endpoint, "storage.cloud.google.com") {
			return ProviderGCP
		}
		if strings.Contains(endpoint, "blob.core.windows.net") {
			return ProviderAzure
		}
		if strings.Contains(endpoint, "amazonaws.com") {
			return ProviderAWS
		}
		// If custom endpoint and not AWS/GCP/Azure, assume S3-compatible through AWS provider
		return ProviderAWS
	}

	// Default to AWS if no specific indicators
	return ProviderAWS
}
