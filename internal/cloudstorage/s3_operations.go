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
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
)

var (
	downloadErrors metric.Int64Counter
	downloadCount  metric.Int64Counter
	downloadBytes  metric.Int64Counter
	uploadCount    metric.Int64Counter
	uploadBytes    metric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/internal/cloudstorage")

	var err error
	downloadErrors, err = meter.Int64Counter(
		"lakerunner.s3.download.errors",
		metric.WithDescription("Number of S3 download errors"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create download.errors counter: %w", err))
	}

	downloadCount, err = meter.Int64Counter(
		"lakerunner.s3.download.count",
		metric.WithDescription("Number of S3 downloads"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create download.count counter: %w", err))
	}

	downloadBytes, err = meter.Int64Counter(
		"lakerunner.s3.download.bytes",
		metric.WithDescription("Bytes downloaded from S3"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create download.bytes counter: %w", err))
	}

	uploadCount, err = meter.Int64Counter(
		"lakerunner.s3.upload.count",
		metric.WithDescription("Number of S3 uploads"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create upload.count counter: %w", err))
	}

	uploadBytes, err = meter.Int64Counter(
		"lakerunner.s3.upload.bytes",
		metric.WithDescription("Bytes uploaded to S3"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create upload.bytes counter: %w", err))
	}
}

func s3ErrorIs404(err error) bool {
	var noKeyErr *types.NoSuchKey
	return errors.As(err, &noKeyErr)
}

func downloadS3Object(
	ctx context.Context,
	dir string,
	s3client *awsclient.S3Client,
	bucketID, objectID string,
) (tmpfile string, size int64, notFound bool, err error) {
	downloader := manager.NewDownloader(s3client.Client)

	// Use the full filename with random prefix for proper file type detection
	filename := filepath.Base(objectID)
	f, err := os.CreateTemp(dir, "*-"+filename)
	if err != nil {
		return "", 0, false, fmt.Errorf("create temp file: %w", err)
	}

	ctx, span := s3client.Tracer.Start(ctx, "cloudstorage.downloadS3Object",
		trace.WithAttributes(
			attribute.String("bucketID", bucketID),
			attribute.String("objectID", objectID),
		),
	)
	defer span.End()

	size, err = downloader.Download(ctx, f, &s3.GetObjectInput{
		Bucket: aws.String(bucketID),
		Key:    aws.String(objectID),
	})
	if err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		if s3ErrorIs404(err) {
			downloadErrors.Add(ctx, 1, metric.WithAttributes(
				attribute.String("bucket", bucketID),
				attribute.String("reason", "not_found"),
			))
			return "", 0, true, nil
		}
		downloadErrors.Add(ctx, 1, metric.WithAttributes(
			attribute.String("bucket", bucketID),
			attribute.String("reason", "unknown"),
		))
		return "", 0, false, fmt.Errorf("download %s/%s: %w", bucketID, objectID, err)
	}

	downloadCount.Add(ctx, 1, metric.WithAttributes(
		attribute.String("bucket", bucketID),
	))
	downloadBytes.Add(ctx, size, metric.WithAttributes(
		attribute.String("bucket", bucketID),
	))

	// close on success; ignore close error because the bytes are already flushed by the SDK
	_ = f.Close()
	return f.Name(), size, false, nil
}

func uploadS3Object(ctx context.Context, s3client *awsclient.S3Client, bucketID, objectID string, sourceFilename string) error {
	uploader := manager.NewUploader(s3client.Client)
	file, err := os.Open(sourceFilename)
	if err != nil {
		return fmt.Errorf("failed to open temporarily file %s: %w", sourceFilename, err)
	}
	defer func() { _ = file.Close() }()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat source file: %w", err)
	}

	var span trace.Span
	ctx, span = s3client.Tracer.Start(ctx, "cloudstorage.uploadS3Object",
		trace.WithAttributes(
			attribute.String("bucketID", bucketID),
		),
	)
	defer span.End()

	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucketID),
		Key:         aws.String(objectID),
		Body:        file,
		ContentType: aws.String("application/vnd.apache.parquet"),
		Metadata: map[string]string{
			"writer": "lakerunner-go",
		},
	})
	if err != nil {
		return fmt.Errorf("failed to upload S3 object: %w", err)
	}

	uploadCount.Add(ctx, 1, metric.WithAttributes(
		attribute.String("bucket", bucketID),
	))
	uploadBytes.Add(ctx, stat.Size(), metric.WithAttributes(
		attribute.String("bucket", bucketID),
	))

	return nil
}

func deleteS3Object(ctx context.Context, s3client *awsclient.S3Client, bucketID, objectID string) error {
	var span trace.Span
	ctx, span = s3client.Tracer.Start(ctx, "cloudstorage.deleteS3Object",
		trace.WithAttributes(
			attribute.String("bucketID", bucketID),
		),
	)
	defer span.End()

	_, err := s3client.Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucketID),
		Key:    aws.String(objectID),
	})
	if err != nil {
		return fmt.Errorf("failed to delete S3 object: %w", err)
	}
	return nil
}

func deleteS3Objects(ctx context.Context, s3client *awsclient.S3Client, bucketID string, objectKeys []string) ([]string, error) {
	if len(objectKeys) == 0 {
		return nil, nil
	}

	var span trace.Span
	ctx, span = s3client.Tracer.Start(ctx, "cloudstorage.deleteS3Objects",
		trace.WithAttributes(
			attribute.String("bucketID", bucketID),
			attribute.Int("object_count", len(objectKeys)),
		),
	)
	defer span.End()

	// S3 batch delete supports up to 1000 objects per request
	const maxBatchSize = 1000
	var allFailed []string

	for i := 0; i < len(objectKeys); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(objectKeys) {
			end = len(objectKeys)
		}
		batch := objectKeys[i:end]

		// Build the delete request
		objects := make([]types.ObjectIdentifier, len(batch))
		for j, key := range batch {
			objects[j] = types.ObjectIdentifier{
				Key: aws.String(key),
			}
		}

		deleteInput := &s3.DeleteObjectsInput{
			Bucket: aws.String(bucketID),
			Delete: &types.Delete{
				Objects: objects,
				Quiet:   aws.Bool(false), // Return info about deleted and failed objects
			},
		}

		result, err := s3client.Client.DeleteObjects(ctx, deleteInput)
		if err != nil {
			// If the entire batch fails, consider all keys failed
			allFailed = append(allFailed, batch...)
			continue
		}

		// Collect keys that failed to delete
		for _, failed := range result.Errors {
			if failed.Key != nil {
				allFailed = append(allFailed, *failed.Key)
			}
		}
	}

	return allFailed, nil
}
