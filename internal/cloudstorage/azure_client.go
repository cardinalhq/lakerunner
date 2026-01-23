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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/internal/azureclient"
)

// azureClient implements the Client interface for Azure Blob Storage
type azureClient struct {
	blobClient *azureclient.BlobClient
}

// newAzureClientFromManager creates an Azure client using the managed BlobClient
func newAzureClientFromManager(blobClient *azureclient.BlobClient) Client {
	return &azureClient{blobClient: blobClient}
}

// DownloadObject downloads a blob from Azure Blob Storage
func (c *azureClient) DownloadObject(ctx context.Context, tmpdir, bucket, key string) (string, int64, bool, error) {
	ctx, span := c.blobClient.Tracer.Start(ctx, "cloudstorage.azureDownloadObject",
		trace.WithAttributes(
			attribute.String("bucket", bucket),
			attribute.String("key", key),
		),
	)
	defer span.End()

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
	defer func() { _ = f.Close() }()

	// Get the download response
	resp, err := c.blobClient.Client.DownloadStream(ctx, containerName, blobName, nil)
	if err != nil {
		_ = os.Remove(f.Name())

		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			downloadErrors.Add(ctx, 1, metric.WithAttributes(
				attribute.String("bucket", bucket),
				attribute.String("reason", "not_found"),
			))
			return "", 0, true, nil
		}
		downloadErrors.Add(ctx, 1, metric.WithAttributes(
			attribute.String("bucket", bucket),
			attribute.String("reason", "unknown"),
		))
		return "", 0, false, fmt.Errorf("download blob %s/%s: %w", containerName, blobName, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Copy the blob content to the temp file
	size, err := io.Copy(f, resp.Body)
	if err != nil {
		_ = os.Remove(f.Name())
		downloadErrors.Add(ctx, 1, metric.WithAttributes(
			attribute.String("bucket", bucket),
			attribute.String("reason", "copy_failed"),
		))
		return "", 0, false, fmt.Errorf("copy blob content: %w", err)
	}

	downloadCount.Add(ctx, 1, metric.WithAttributes(
		attribute.String("bucket", bucket),
	))
	downloadBytes.Add(ctx, size, metric.WithAttributes(
		attribute.String("bucket", bucket),
	))

	return f.Name(), size, false, nil
}

// UploadObject uploads a file to Azure Blob Storage
func (c *azureClient) UploadObject(ctx context.Context, bucket, key, sourceFilename string) error {
	ctx, span := c.blobClient.Tracer.Start(ctx, "cloudstorage.azureUploadObject",
		trace.WithAttributes(
			attribute.String("bucket", bucket),
			attribute.String("key", key),
		),
	)
	defer span.End()

	containerName := bucket
	blobName := key
	// Open the source file
	file, err := os.Open(sourceFilename)
	if err != nil {
		return fmt.Errorf("failed to open source file %s: %w", sourceFilename, err)
	}
	defer func() { _ = file.Close() }()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat source file: %w", err)
	}

	// Upload the blob with Parquet content type and metadata
	_, err = c.blobClient.Client.UploadStream(ctx, containerName, blobName, file, &azblob.UploadStreamOptions{
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

	uploadCount.Add(ctx, 1, metric.WithAttributes(
		attribute.String("bucket", bucket),
	))
	uploadBytes.Add(ctx, stat.Size(), metric.WithAttributes(
		attribute.String("bucket", bucket),
	))

	return nil
}

// DeleteObject deletes a blob from Azure Blob Storage
func (c *azureClient) DeleteObject(ctx context.Context, bucket, key string) error {
	ctx, span := c.blobClient.Tracer.Start(ctx, "cloudstorage.azureDeleteObject",
		trace.WithAttributes(
			attribute.String("bucket", bucket),
			attribute.String("key", key),
		),
	)
	defer span.End()

	_, err := c.blobClient.Client.DeleteBlob(ctx, bucket, key, nil)
	if err != nil {
		return fmt.Errorf("failed to delete blob %s/%s: %w", bucket, key, err)
	}
	return nil
}

// DeleteObjects deletes multiple blobs from Azure Blob Storage
// Azure doesn't have native batch delete, so we delete sequentially.
func (c *azureClient) DeleteObjects(ctx context.Context, bucket string, keys []string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	ctx, span := c.blobClient.Tracer.Start(ctx, "cloudstorage.azureDeleteObjects",
		trace.WithAttributes(
			attribute.String("bucket", bucket),
			attribute.Int("object_count", len(keys)),
		),
	)
	defer span.End()

	var failed []string
	for _, key := range keys {
		_, err := c.blobClient.Client.DeleteBlob(ctx, bucket, key, nil)
		if err != nil {
			failed = append(failed, key)
			span.RecordError(fmt.Errorf("failed to delete blob %s/%s: %w", bucket, key, err))
		}
	}

	if len(failed) > 0 {
		span.SetAttributes(attribute.Int("failed_object_count", len(failed)))
	}

	return failed, nil
}
