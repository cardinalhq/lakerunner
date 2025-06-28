// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3helper

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/pkg/lrdb"
)

func S3ErrorIs404(err error) bool {
	var noKeyErr *types.NoSuchKey
	return errors.As(err, &noKeyErr)
}

func DownloadS3Object(ctx context.Context, dir string, s3client *awsclient.S3Client, bucketID, objectID string) (string, int64, error) {
	downloader := manager.NewDownloader(s3client.Client)
	tmpfile, err := os.CreateTemp(dir, "s3-*.parquet")
	if err != nil {
		return "", 0, fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer tmpfile.Close()

	var span trace.Span
	ctx, span = s3client.Tracer.Start(ctx, "s3helper.DownloadS3Object",
		trace.WithAttributes(
			attribute.String("bucketID", bucketID),
		),
	)
	defer span.End()

	fileSize, err := downloader.Download(ctx, tmpfile, &s3.GetObjectInput{
		Bucket: aws.String(bucketID),
		Key:    aws.String(objectID),
	})
	if err != nil {
		_ = os.Remove(tmpfile.Name())
		return "", 0, fmt.Errorf("failed to download S3 object: %w", err)
	}
	return tmpfile.Name(), fileSize, nil
}

func UploadS3Object(ctx context.Context, s3client *awsclient.S3Client, bucketID, objectID string, sourceFilename string) error {
	uploader := manager.NewUploader(s3client.Client)
	file, err := os.Open(sourceFilename)
	if err != nil {
		return fmt.Errorf("failed to open temporarily file %s: %w", sourceFilename, err)
	}
	defer file.Close()

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

// func deleteS3Objects(ctx context.Context, s3client *s3.Client, bucketID string, objectIDs []string) error {
// 	oids := make([]types.ObjectIdentifier, len(objectIDs))
// 	for i, id := range objectIDs {
// 		oids[i] = types.ObjectIdentifier{
// 			Key: aws.String(id),
// 		}
// 	}
// 	result, err := s3client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
// 		Bucket: aws.String(bucketID),
// 		Delete: &types.Delete{
// 			Objects: oids,
// 			Quiet:   aws.Bool(true), // only return errors
// 		},
// 	})
// 	if err != nil {
// 		slog.Error("Failed to bunk delete objects", slog.Any("error", err))
// 		return err
// 	}
// 	for _, err := range result.Errors {
// 		slog.Error("Failed to delete S3 object", slog.String("objectID", *err.Key), slog.Any("error", err.Message))
// 	}
// 	return nil
// }

func ScheduleS3Delete(ctx context.Context, mdb lrdb.StoreFull, org_id uuid.UUID, instance_num int16, bucketID, objectID string) error {
	return mdb.ObjectCleanupAdd(ctx, lrdb.ObjectCleanupAddParams{
		OrganizationID: org_id,
		InstanceNum:    instance_num,
		BucketID:       bucketID,
		ObjectID:       objectID,
	})
}
