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

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/internal/cloudprovider"
	"github.com/cardinalhq/lakerunner/internal/cloudprovider/credential"
	"github.com/cardinalhq/lakerunner/internal/cloudprovider/pubsub"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// LocalProvider implements the CloudProvider interface for local filesystem development
type LocalProvider struct{}

func (p *LocalProvider) GetCredentialProvider() credential.Provider {
	return nil // Local provider doesn't need credentials
}

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
	// Local provider object store not implemented yet - use AWS provider for real usage
	return nil, fmt.Errorf("LocalProvider object store client not implemented - use AWS provider instead")
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
	s3Client *s3.Client // This would be a mock/local implementation
}

// DownloadObject downloads an object from local storage to a destination path
func (c *LocalObjectStoreClient) DownloadObject(ctx context.Context, bucketID, objectID, destPath string) error {
	// TODO: Implement local filesystem download
	return fmt.Errorf("local provider download not yet implemented")
}

// UploadObject uploads a file from source path to local storage
func (c *LocalObjectStoreClient) UploadObject(ctx context.Context, bucketID, objectID, sourcePath string) error {
	// TODO: Implement local filesystem upload
	return fmt.Errorf("local provider upload not yet implemented")
}

// DeleteObject deletes an object from local storage
func (c *LocalObjectStoreClient) DeleteObject(ctx context.Context, bucketID, objectID string) error {
	// TODO: Implement local filesystem delete
	return fmt.Errorf("local provider delete not yet implemented")
}

func (c *LocalObjectStoreClient) GetS3Client() *s3.Client {
	return c.s3Client
}

// GetTracer returns a no-op tracer for local provider
func (c *LocalObjectStoreClient) GetTracer() trace.Tracer {
	// Return a no-op tracer for local development
	return trace.NewNoopTracerProvider().Tracer("local")
}

// ScheduleDelete schedules an object for deletion (no-op for local)
func (c *LocalObjectStoreClient) ScheduleDelete(ctx context.Context, mdb lrdb.StoreFull, orgID uuid.UUID, instanceNum int16, bucketID, objectID string) error {
	// For local provider, just delete immediately
	return c.DeleteObject(ctx, bucketID, objectID)
}

// IsNotFoundError checks if an error indicates object not found
func (c *LocalObjectStoreClient) IsNotFoundError(err error) bool {
	// TODO: Implement proper error checking for local filesystem
	return false
}

// createLocalS3Client creates a local filesystem-based S3 client
func createLocalS3Client(basePath string) (*s3.Client, error) {
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
