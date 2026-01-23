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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/internal/gcpclient"
)

// gcsClient implements the Client interface for Google Cloud Storage.
type gcsClient struct {
	storageClient *gcpclient.StorageClient
}

// DownloadObject downloads an object from GCS to a temporary file.
func (c *gcsClient) DownloadObject(ctx context.Context, tmpdir, bucket, key string) (string, int64, bool, error) {
	ctx, span := c.storageClient.Tracer.Start(ctx, "cloudstorage.gcsDownloadObject",
		trace.WithAttributes(
			attribute.String("bucket", bucket),
			attribute.String("key", key),
		),
	)
	defer span.End()

	// Use the filename with random prefix for proper file type detection
	filename := filepath.Base(key)
	f, err := os.CreateTemp(tmpdir, "*-"+filename)
	if err != nil {
		return "", 0, false, fmt.Errorf("create temp file: %w", err)
	}

	obj := c.storageClient.Client.Bucket(bucket).Object(key)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		if errors.Is(err, storage.ErrObjectNotExist) {
			return "", 0, true, nil
		}
		return "", 0, false, fmt.Errorf("download %s/%s: %w", bucket, key, err)
	}
	defer func() { _ = reader.Close() }()

	size, err := io.Copy(f, reader)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", 0, false, fmt.Errorf("copy object content: %w", err)
	}

	_ = f.Close()
	return f.Name(), size, false, nil
}

// UploadObject uploads a file to GCS.
func (c *gcsClient) UploadObject(ctx context.Context, bucket, key, sourceFilename string) error {
	ctx, span := c.storageClient.Tracer.Start(ctx, "cloudstorage.gcsUploadObject",
		trace.WithAttributes(
			attribute.String("bucket", bucket),
			attribute.String("key", key),
		),
	)
	defer span.End()

	file, err := os.Open(sourceFilename)
	if err != nil {
		return fmt.Errorf("failed to open source file %s: %w", sourceFilename, err)
	}
	defer func() { _ = file.Close() }()

	obj := c.storageClient.Client.Bucket(bucket).Object(key)
	writer := obj.NewWriter(ctx)
	writer.ContentType = "application/vnd.apache.parquet"
	writer.Metadata = map[string]string{
		"writer": "lakerunner-go",
	}

	if _, err := io.Copy(writer, file); err != nil {
		_ = writer.Close()
		return fmt.Errorf("failed to upload object %s/%s: %w", bucket, key, err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer for %s/%s: %w", bucket, key, err)
	}

	return nil
}

// DeleteObject deletes an object from GCS.
func (c *gcsClient) DeleteObject(ctx context.Context, bucket, key string) error {
	ctx, span := c.storageClient.Tracer.Start(ctx, "cloudstorage.gcsDeleteObject",
		trace.WithAttributes(
			attribute.String("bucket", bucket),
			attribute.String("key", key),
		),
	)
	defer span.End()

	obj := c.storageClient.Client.Bucket(bucket).Object(key)
	if err := obj.Delete(ctx); err != nil {
		return fmt.Errorf("failed to delete object %s/%s: %w", bucket, key, err)
	}

	return nil
}

// DeleteObjects deletes multiple objects from GCS.
// GCS SDK does not support batch delete, so we delete sequentially.
func (c *gcsClient) DeleteObjects(ctx context.Context, bucket string, keys []string) ([]string, error) {
	ctx, span := c.storageClient.Tracer.Start(ctx, "cloudstorage.gcsDeleteObjects",
		trace.WithAttributes(
			attribute.String("bucket", bucket),
			attribute.Int("object_count", len(keys)),
		),
	)
	defer span.End()

	var failed []string
	for _, key := range keys {
		obj := c.storageClient.Client.Bucket(bucket).Object(key)
		if err := obj.Delete(ctx); err != nil {
			failed = append(failed, key)
		}
	}

	return failed, nil
}
