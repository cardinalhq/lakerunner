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

package cloudprovider

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sony/sonyflake"
)

var (
	flake *sonyflake.Sonyflake
	once  sync.Once
)

// GenerateID generates a unique ID using Sonyflake
func GenerateID() int64 {
	once.Do(func() {
		machineID := uint16(os.Getpid() % 65536)
		flake = sonyflake.NewSonyflake(sonyflake.Settings{
			MachineID: func() (uint16, error) {
				return machineID, nil
			},
		})
		if flake == nil {
			panic("failed to initialize flake generator")
		}
	})

	if flake == nil {
		panic("flake generator is not initialized")
	}
	id, err := flake.NextID()
	if err != nil {
		panic(fmt.Sprintf("failed to generate ID: %v", err))
	}
	return int64(id)
}

// HourFromMillis converts milliseconds since epoch to hour of day (0-23)
func HourFromMillis(ms int64) int16 {
	t := time.UnixMilli(ms).UTC()
	return int16(t.Hour())
}

// DownloadToTempFile downloads an object to a temporary file and returns the filename.
// Returns: tempFileName, fileSize, isNotFound, error
func DownloadToTempFile(ctx context.Context, tmpDir string, client ObjectStoreClient, bucketID, objectID string) (string, int64, bool, error) {
	// Create temporary file
	tempFileName := fmt.Sprintf("%s/%s", tmpDir, objectID)

	// Download the object
	err := client.DownloadObject(ctx, bucketID, objectID, tempFileName)
	if err != nil {
		if client.IsNotFoundError(err) {
			return "", 0, true, nil
		}
		return "", 0, false, err
	}

	// Get file size
	info, err := os.Stat(tempFileName)
	if err != nil {
		return "", 0, false, fmt.Errorf("failed to stat downloaded file: %w", err)
	}

	return tempFileName, info.Size(), false, nil
}

// UploadS3Object uploads a file to S3 using AWS SDK v2
func UploadS3Object(ctx context.Context, s3client *s3.Client, bucketID, objectID, sourceFilename string) error {
	uploader := manager.NewUploader(s3client)
	file, err := os.Open(sourceFilename)
	if err != nil {
		return fmt.Errorf("opening file %s: %w", sourceFilename, err)
	}
	defer file.Close()

	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketID),
		Key:    aws.String(objectID),
		Body:   file,
	})
	if err != nil {
		return fmt.Errorf("uploading file to S3: %w", err)
	}

	return nil
}

// DeleteS3Object deletes an object from S3 using AWS SDK v2
func DeleteS3Object(ctx context.Context, s3client *s3.Client, bucketID, objectID string) error {
	_, err := s3client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucketID),
		Key:    aws.String(objectID),
	})
	if err != nil {
		return fmt.Errorf("deleting S3 object: %w", err)
	}

	return nil
}
