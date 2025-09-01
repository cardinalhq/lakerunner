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

package s3helper

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
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/lrdb"
)

var (
	downloadErrors   metric.Int64Counter
	downloadNotFound metric.Int64Counter
	downloadCount    metric.Int64Counter
	downloadBytes    metric.Int64Counter
	uploadCount      metric.Int64Counter
	uploadBytes      metric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/internal/awsclient/s3helper")

	var err error
	downloadErrors, err = meter.Int64Counter(
		"lakerunner.s3.download_errors",
		metric.WithDescription("Number of S3 download errors"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create download_errors counter: %w", err))
	}

	downloadNotFound, err = meter.Int64Counter(
		"lakerunner.s3.download_not_found",
		metric.WithDescription("Number of missing S3 objects during download"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create download_not_found counter: %w", err))
	}

	downloadCount, err = meter.Int64Counter(
		"lakerunner.s3.download_count",
		metric.WithDescription("Number of S3 downloads"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create download_count counter: %w", err))
	}

	downloadBytes, err = meter.Int64Counter(
		"lakerunner.s3.download_bytes",
		metric.WithDescription("Bytes downloaded from S3"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create download_bytes counter: %w", err))
	}

	uploadCount, err = meter.Int64Counter(
		"lakerunner.s3.upload_count",
		metric.WithDescription("Number of S3 uploads"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create upload_count counter: %w", err))
	}

	uploadBytes, err = meter.Int64Counter(
		"lakerunner.s3.upload_bytes",
		metric.WithDescription("Bytes uploaded to S3"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create upload_bytes counter: %w", err))
	}
}

func S3ErrorIs404(err error) bool {
	var noKeyErr *types.NoSuchKey
	return errors.As(err, &noKeyErr)
}

func DownloadS3Object(
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

	ctx, span := s3client.Tracer.Start(ctx, "s3helper.DownloadS3Object",
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
		if S3ErrorIs404(err) {
			downloadNotFound.Add(ctx, 1, metric.WithAttributes(
				attribute.String("bucket", bucketID),
			))
			return "", 0, true, nil
		}
		downloadErrors.Add(ctx, 1, metric.WithAttributes(
			attribute.String("bucket", bucketID),
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

func UploadS3Object(ctx context.Context, s3client *awsclient.S3Client, bucketID, objectID string, sourceFilename string) error {
	uploader := manager.NewUploader(s3client.Client)
	file, err := os.Open(sourceFilename)
	if err != nil {
		return fmt.Errorf("failed to open temporarily file %s: %w", sourceFilename, err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat source file: %w", err)
	}

	var span trace.Span
	ctx, span = s3client.Tracer.Start(ctx, "s3helper.UploadS3Object",
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

func DeleteS3Object(ctx context.Context, s3client *awsclient.S3Client, bucketID, objectID string) error {
	var span trace.Span
	ctx, span = s3client.Tracer.Start(ctx, "s3helper.DeleteS3Object",
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

// ObjectCleanupStore provides the minimal interface needed for scheduling S3 object deletions
type ObjectCleanupStore interface {
	ObjectCleanupAdd(ctx context.Context, arg lrdb.ObjectCleanupAddParams) error
}

func ScheduleS3Delete(ctx context.Context, mdb ObjectCleanupStore, org_id uuid.UUID, instanceNum int16, bucketID, objectID string) error {
	return mdb.ObjectCleanupAdd(ctx, lrdb.ObjectCleanupAddParams{
		OrganizationID: org_id,
		BucketID:       bucketID,
		ObjectID:       objectID,
		InstanceNum:    instanceNum,
	})
}
