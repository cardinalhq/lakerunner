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
	"io"
	"os"
	"path/filepath"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/cardinalhq/lakerunner/internal/azureclient"
)

// azureClient implements the Client interface for Azure Blob Storage
type azureClient struct {
	client *azblob.Client
}

// newAzureClientFromManager creates an Azure client using the managed BlobClient
func newAzureClientFromManager(blobClient *azureclient.BlobClient) Client {
	return &azureClient{client: blobClient.Client}
}

// DownloadObject downloads a blob from Azure Blob Storage
func (c *azureClient) DownloadObject(ctx context.Context, tmpdir, bucket, key string) (string, int64, bool, error) {
	containerName := bucket
	blobName := key
	// Preserve the original file extension for proper file type detection
	ext := filepath.Ext(key)
	if ext == "" {
		ext = ".data" // fallback for files without extension
	}

	// Create temp file
	f, err := os.CreateTemp(tmpdir, "azure-*"+ext)
	if err != nil {
		return "", 0, false, fmt.Errorf("create temp file: %w", err)
	}
	defer f.Close()

	// Get the download response
	resp, err := c.client.DownloadStream(ctx, containerName, blobName, nil)
	if err != nil {
		_ = os.Remove(f.Name())

		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return "", 0, true, nil
		}
		return "", 0, false, fmt.Errorf("download blob %s/%s: %w", containerName, blobName, err)
	}
	defer resp.Body.Close()

	// Copy the blob content to the temp file
	size, err := io.Copy(f, resp.Body)
	if err != nil {
		_ = os.Remove(f.Name())
		return "", 0, false, fmt.Errorf("copy blob content: %w", err)
	}

	return f.Name(), size, false, nil
}

// UploadObject uploads a file to Azure Blob Storage
func (c *azureClient) UploadObject(ctx context.Context, bucket, key, sourceFilename string) error {
	containerName := bucket
	blobName := key
	// Open the source file
	file, err := os.Open(sourceFilename)
	if err != nil {
		return fmt.Errorf("failed to open source file %s: %w", sourceFilename, err)
	}
	defer file.Close()

	// Upload the blob with Parquet content type and metadata
	_, err = c.client.UploadStream(ctx, containerName, blobName, file, &azblob.UploadStreamOptions{
		Metadata: map[string]*string{
			"writer": to.Ptr("lakerunner-go"),
		},
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType: to.Ptr("application/vnd.apache.parquet"),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to upload blob %s/%s: %w", containerName, blobName, err)
	}

	return nil
}

// DeleteObject deletes a blob from Azure Blob Storage
func (c *azureClient) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := c.client.DeleteBlob(ctx, bucket, key, nil)
	if err != nil {
		return fmt.Errorf("failed to delete blob %s/%s: %w", bucket, key, err)
	}
	return nil
}
