// Copyright (C) 2025-2026 CardinalHQ, Inc
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

	"github.com/cardinalhq/lakerunner/internal/awsclient"
)

// s3Client wraps the existing S3 implementation
type s3Client struct {
	awsS3Client *awsclient.S3Client
}

// DownloadObject downloads an object from S3 to a temporary file
func (c *s3Client) DownloadObject(ctx context.Context, tmpdir, bucket, key string) (string, int64, bool, error) {
	return downloadS3Object(ctx, tmpdir, c.awsS3Client, bucket, key)
}

// UploadObject uploads a file to S3
func (c *s3Client) UploadObject(ctx context.Context, bucket, key, sourceFilename string) error {
	return uploadS3Object(ctx, c.awsS3Client, bucket, key, sourceFilename)
}

// DeleteObject deletes an object from S3
func (c *s3Client) DeleteObject(ctx context.Context, bucket, key string) error {
	return deleteS3Object(ctx, c.awsS3Client, bucket, key)
}

// DeleteObjects deletes multiple objects from S3 in a batch operation
func (c *s3Client) DeleteObjects(ctx context.Context, bucket string, keys []string) ([]string, error) {
	return deleteS3Objects(ctx, c.awsS3Client, bucket, keys)
}
