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
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

// Client provides a unified interface for cloud storage operations across different providers
type Client interface {
	// DownloadObject downloads an object from cloud storage to a local file
	// Returns the temp filename, size, whether object was not found, and error
	DownloadObject(ctx context.Context, tmpdir, bucket, key string) (filename string, size int64, notFound bool, err error)

	// UploadObject uploads a local file to cloud storage
	UploadObject(ctx context.Context, bucket, key, sourceFilename string) error

	// DeleteObject deletes an object from cloud storage
	DeleteObject(ctx context.Context, bucket, key string) error
}

// NewClient creates a new cloud storage client using unified cloud managers
func NewClient(ctx context.Context, managers *CloudManagers, profile storageprofile.StorageProfile) (Client, error) {
	switch profile.CloudProvider {
	case "aws", "gcp", "": // Empty defaults to AWS for backward compatibility
		// Both AWS and GCP use the S3 client - reuse existing GetS3ForProfile
		awsS3Client, err := managers.AWS.GetS3ForProfile(ctx, profile)
		if err != nil {
			return nil, fmt.Errorf("failed to create S3 client: %w", err)
		}
		return &s3Client{awsS3Client: awsS3Client}, nil
	case "azure":
		// Use Azure Blob Storage client via manager
		azureBlobClient, err := managers.Azure.GetBlobForProfile(ctx, profile)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure blob client: %w", err)
		}
		return newAzureClientFromManager(azureBlobClient), nil
	default:
		return nil, fmt.Errorf("unsupported cloud provider: %s", profile.CloudProvider)
	}
}
