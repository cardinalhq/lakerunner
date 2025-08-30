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

	"github.com/cardinalhq/lakerunner/internal/cloudprovider/credential"
	"github.com/cardinalhq/lakerunner/internal/cloudprovider/objstore"
	"github.com/cardinalhq/lakerunner/internal/cloudprovider/pubsub"
)

// ProviderType represents the type of cloud provider
type ProviderType string

const (
	ProviderAWS   ProviderType = "aws"
	ProviderGCP   ProviderType = "gcp"
	ProviderAzure ProviderType = "azure"
	ProviderLocal ProviderType = "local"
)

// ObjectStoreClient is an alias for objstore.S3Manager for backward compatibility
// New code should use objstore.S3Manager directly
type ObjectStoreClient = objstore.S3Manager

// CloudProvider represents a cloud provider that can provide object storage and pubsub services
type CloudProvider interface {
	// Name returns the human-readable name of the provider
	Name() string

	// Type returns the provider type
	Type() ProviderType

	// CreateObjectStoreClient creates an object store client for the given configuration
	CreateObjectStoreClient(ctx context.Context, config ObjectStoreConfig) (ObjectStoreClient, error)

	// CreatePubSubBackend creates a pubsub backend, returns nil if not supported by this provider
	CreatePubSubBackend(ctx context.Context, config PubSubConfig) (pubsub.Backend, error)

	// GetCredentialProvider returns the credential provider for this cloud provider
	GetCredentialProvider() credential.Provider

	// SupportsFeature returns true if the provider supports the given feature
	SupportsFeature(feature string) bool

	// Validate validates the provider configuration
	Validate(config ProviderConfig) error
}

// ObjectStoreConfig represents configuration for object storage
type ObjectStoreConfig struct {
	// Common fields
	Bucket       string `json:"bucket"`
	Region       string `json:"region,omitempty"`
	Endpoint     string `json:"endpoint,omitempty"`
	UsePathStyle bool   `json:"use_path_style,omitempty"`
	InsecureTLS  bool   `json:"insecure_tls,omitempty"`

	// Authentication
	AccessKey string `json:"access_key,omitempty"`
	SecretKey string `json:"secret_key,omitempty"`
	Role      string `json:"role,omitempty"`

	// Provider-specific settings (stored as map for flexibility)
	ProviderSettings map[string]any `json:"provider_settings,omitempty"`
}

// PubSubConfig represents configuration for pubsub services
type PubSubConfig struct {
	Type     string `json:"type"` // sqs, gcp_pubsub, azure_servicebus, http, local
	Endpoint string `json:"endpoint,omitempty"`

	// Provider-specific settings
	ProviderSettings map[string]any `json:"provider_settings,omitempty"`
}

// ProviderConfig represents the complete configuration for a cloud provider
type ProviderConfig struct {
	Type        ProviderType      `json:"type"`
	ObjectStore ObjectStoreConfig `json:"object_store"`
	PubSub      PubSubConfig      `json:"pubsub"`

	// Raw settings from environment variables
	Settings map[string]string `json:"settings,omitempty"`
}

// Features that providers can support
const (
	FeatureMultiRegion         = "multi_region"
	FeatureCrossAccount        = "cross_account"
	FeatureKubernetesAuth      = "kubernetes_auth"
	FeatureBucketNotifications = "bucket_notifications"
)
