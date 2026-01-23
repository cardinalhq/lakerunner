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

package cloudstorage

import (
	"context"
	"fmt"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/azureclient"
	"github.com/cardinalhq/lakerunner/internal/gcpclient"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

// CloudManagers holds all cloud provider managers for unified access. It
// implements ClientProvider to allow callers to create storage clients without
// depending on the concrete struct, enabling easier testing.
type CloudManagers struct {
	AWS   *awsclient.Manager
	Azure *azureclient.Manager
	GCP   *gcpclient.Manager
}

// Ensure CloudManagers implements ClientProvider
var _ ClientProvider = (*CloudManagers)(nil)

// NewCloudManagers creates managers for all supported cloud providers
func NewCloudManagers(ctx context.Context) (ClientProvider, error) {
	// Create AWS manager - required for S3-compatible storage
	awsManager, err := awsclient.NewManager(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS manager: %w", err)
	}

	// Create Azure manager
	azureManager, err := azureclient.NewManager(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure manager: %w", err)
	}

	// Create GCP manager
	gcpManager, err := gcpclient.NewManager(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCP manager: %w", err)
	}

	return &CloudManagers{
		AWS:   awsManager,
		Azure: azureManager,
		GCP:   gcpManager,
	}, nil
}

// NewClient creates a storage Client for the given profile.
func (m *CloudManagers) NewClient(ctx context.Context, profile storageprofile.StorageProfile) (Client, error) {
	switch profile.CloudProvider {
	case "aws", "":
		awsS3Client, err := m.AWS.GetS3ForProfile(ctx, profile)
		if err != nil {
			return nil, fmt.Errorf("failed to create S3 client: %w", err)
		}
		return &s3Client{awsS3Client: awsS3Client}, nil
	case "gcp":
		gcpStorageClient, err := m.GCP.GetStorageForProfile(ctx, profile)
		if err != nil {
			return nil, fmt.Errorf("failed to create GCS client: %w", err)
		}
		return &gcsClient{storageClient: gcpStorageClient}, nil
	case "azure":
		azureBlobClient, err := m.Azure.GetBlobForProfile(ctx, profile)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure blob client: %w", err)
		}
		return newAzureClientFromManager(azureBlobClient), nil
	default:
		return nil, fmt.Errorf("unsupported cloud provider: %s", profile.CloudProvider)
	}
}
