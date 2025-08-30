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

package local

import (
	"context"
	"fmt"
	"os"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/cloudprovider"
	"github.com/cardinalhq/lakerunner/internal/pubsub"
)

// LocalProvider implements the CloudProvider interface for local filesystem development
type LocalProvider struct{}

// NewLocalProvider creates a new local provider
func NewLocalProvider() (cloudprovider.CloudProvider, error) {
	return &LocalProvider{}, nil
}

func (p *LocalProvider) Name() string {
	return "Local Development"
}

func (p *LocalProvider) Type() cloudprovider.ProviderType {
	return cloudprovider.ProviderLocal
}

func (p *LocalProvider) CreateObjectStoreClient(ctx context.Context, config cloudprovider.ObjectStoreConfig) (cloudprovider.ObjectStoreClient, error) {
	// Get base path from config
	basePath := "/tmp/lakerunner-dev"
	if bp, ok := config.ProviderSettings["base_path"].(string); ok && bp != "" {
		basePath = bp
	}

	// Create base directory if it doesn't exist
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create local storage directory %s: %w", basePath, err)
	}

	// For local provider, we'll create a mock S3Client that uses local filesystem
	// This is a simplified implementation - in a full version we'd implement a complete local storage layer
	localS3Client, err := createLocalS3Client(basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create local S3 client: %w", err)
	}

	return &LocalObjectStoreClient{
		basePath: basePath,
		s3Client: localS3Client,
	}, nil
}

func (p *LocalProvider) CreatePubSubBackend(ctx context.Context, config cloudprovider.PubSubConfig) (pubsub.Backend, error) {
	// Local provider supports file-based pubsub or HTTP fallback
	if config.Type == "" || config.Type == "local" {
		return createLocalPubSubBackend(ctx, config)
	}

	// If user specified a different pubsub type (like http), delegate to it
	// This allows local storage with HTTP webhooks for notifications
	switch config.Type {
	case "http":
		// This would be handled by the main pubsub factory
		return nil, fmt.Errorf("HTTP pubsub should be handled by main factory")
	default:
		return nil, fmt.Errorf("local provider does not support pubsub type: %s", config.Type)
	}
}

func (p *LocalProvider) SupportsFeature(feature string) bool {
	switch feature {
	case cloudprovider.FeatureMultiRegion:
		return false // Local is single "region"
	case cloudprovider.FeatureCrossAccount:
		return false // Local has no accounts
	case cloudprovider.FeatureKubernetesAuth:
		return false // No auth needed
	case cloudprovider.FeatureBucketNotifications:
		return true // File system watching
	default:
		return false
	}
}

func (p *LocalProvider) Validate(config cloudprovider.ProviderConfig) error {
	// Local provider is very permissive - just need a base path
	basePath := "/tmp/lakerunner-dev"
	if bp, ok := config.ObjectStore.ProviderSettings["base_path"].(string); ok && bp != "" {
		basePath = bp
	}

	// Check if we can create the directory
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return fmt.Errorf("cannot create local storage directory %s: %w", basePath, err)
	}

	return nil
}

// LocalObjectStoreClient wraps a local filesystem implementation
type LocalObjectStoreClient struct {
	basePath string
	s3Client *awsclient.S3Client // This would be a mock/local implementation
}

func (c *LocalObjectStoreClient) GetS3Client() *awsclient.S3Client {
	return c.s3Client
}

// createLocalS3Client creates a local filesystem-based S3 client
func createLocalS3Client(basePath string) (*awsclient.S3Client, error) {
	// For now, this is a placeholder - in a full implementation we would:
	// 1. Create a mock S3 service that uses local filesystem
	// 2. Use something like MinIO in filesystem mode
	// 3. Implement a local adapter that translates S3 operations to file operations

	// For the initial implementation, we'll return an error to indicate this needs more work
	return nil, fmt.Errorf("local S3 client implementation not yet complete - please use MinIO in local mode or configure a real S3 endpoint")
}

// createLocalPubSubBackend creates a local file-based pubsub backend
func createLocalPubSubBackend(ctx context.Context, config cloudprovider.PubSubConfig) (pubsub.Backend, error) {
	// For now, return nil to indicate local pubsub should fall back to HTTP or file watching
	// In a full implementation, we could:
	// 1. Use filesystem watching (fsnotify) to detect new files
	// 2. Create a simple file-based queue system
	// 3. Use local HTTP server for webhook simulation

	return nil, fmt.Errorf("local pubsub not yet implemented - use HTTP pubsub with local webhook server")
}
