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

	"github.com/cardinalhq/lakerunner/internal/cloudprovider/pubsub"
)

// Manager manages cloud provider instances and configurations
type Manager struct {
	providers map[ProviderType]CloudProvider
	config    ProviderConfig
}

// NewManager creates a new provider manager with the given configuration
func NewManager(config ProviderConfig) (*Manager, error) {
	if err := validateProviderConfig(config); err != nil {
		return nil, fmt.Errorf("invalid provider config: %w", err)
	}

	mgr := &Manager{
		providers: make(map[ProviderType]CloudProvider),
		config:    config,
	}

	return mgr, nil
}

// GetProvider returns the cloud provider for the configured type
func (m *Manager) GetProvider() (CloudProvider, error) {
	provider, exists := m.providers[m.config.Type]
	if !exists {
		var err error
		provider, err = createProvider(m.config.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to create provider %s: %w", m.config.Type, err)
		}
		m.providers[m.config.Type] = provider
	}

	return provider, nil
}

// GetObjectStoreClient returns an object store client using the configured provider
func (m *Manager) GetObjectStoreClient(ctx context.Context) (ObjectStoreClient, error) {
	provider, err := m.GetProvider()
	if err != nil {
		return nil, err
	}

	return provider.CreateObjectStoreClient(ctx, m.config.ObjectStore)
}

// GetPubSubBackend returns a pubsub backend, either from the provider or override
func (m *Manager) GetPubSubBackend(ctx context.Context) (pubsub.Backend, error) {
	// Check if there's a pubsub type override
	if m.config.PubSub.Type != "" {
		return createPubSubBackend(ctx, m.config.PubSub)
	}

	// Use provider's default pubsub
	provider, err := m.GetProvider()
	if err != nil {
		return nil, err
	}

	backend, err := provider.CreatePubSubBackend(ctx, m.config.PubSub)
	if err != nil {
		return nil, fmt.Errorf("provider %s does not support pubsub: %w", provider.Type(), err)
	}

	return backend, nil
}

// createProvider creates a provider instance for the given type
func createProvider(providerType ProviderType) (CloudProvider, error) {
	switch providerType {
	case ProviderAWS:
		return NewAWSProvider(), nil
	case ProviderGCP:
		return NewGCPProvider(), nil
	case ProviderAzure:
		return NewAzureProvider(), nil
	case ProviderLocal:
		return NewLocalProvider(), nil
	default:
		return nil, fmt.Errorf("unsupported provider type: %s", providerType)
	}
}

// validateProviderConfig validates the provider configuration
func validateProviderConfig(config ProviderConfig) error {
	if config.Type == "" {
		return fmt.Errorf("provider type is required")
	}

	if config.ObjectStore.Bucket == "" && config.Type != ProviderLocal {
		return fmt.Errorf("bucket is required for provider %s", config.Type)
	}

	return nil
}

// createPubSubBackend creates a pubsub backend for the given configuration
func createPubSubBackend(ctx context.Context, config PubSubConfig) (pubsub.Backend, error) {
	backendType, err := mapPubSubType(config.Type)
	if err != nil {
		return nil, err
	}

	return pubsub.NewBackend(ctx, backendType)
}

// mapPubSubType maps string pubsub types to backend types
func mapPubSubType(pubsubType string) (pubsub.BackendType, error) {
	switch pubsubType {
	case "sqs":
		return pubsub.BackendTypeSQS, nil
	case "gcp_pubsub":
		return pubsub.BackendTypeGCPPubSub, nil
	case "http":
		return pubsub.BackendTypeHTTP, nil
	case "local":
		return pubsub.BackendTypeLocal, nil
	default:
		return "", fmt.Errorf("unsupported pubsub type: %s", pubsubType)
	}
}

// Placeholder implementations - these will be implemented in separate files
func NewAWSProvider() CloudProvider {
	provider, err := newAWSProviderImpl()
	if err != nil {
		panic(fmt.Sprintf("failed to create AWS provider: %v", err))
	}
	return provider
}

func NewGCPProvider() CloudProvider   { panic("not implemented") }
func NewAzureProvider() CloudProvider { panic("not implemented") }
func NewLocalProvider() CloudProvider {
	provider, err := newLocalProviderImpl()
	if err != nil {
		panic(fmt.Sprintf("failed to create local provider: %v", err))
	}
	return provider
}

// These functions will be implemented in separate provider files
var newAWSProviderImpl = func() (CloudProvider, error) { return nil, fmt.Errorf("AWS provider not implemented") }
var newLocalProviderImpl = func() (CloudProvider, error) { return nil, fmt.Errorf("local provider not implemented") }

// RegisterAWSProvider registers the AWS provider implementation
func RegisterAWSProvider(impl func() (CloudProvider, error)) {
	newAWSProviderImpl = impl
}

// RegisterLocalProvider registers the local provider implementation
func RegisterLocalProvider(impl func() (CloudProvider, error)) {
	newLocalProviderImpl = impl
}
