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

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
)

// s3Client wraps the existing S3 implementation
type s3Client struct {
	awsS3Client *awsclient.S3Client
}

// DownloadObject reuses existing s3helper.DownloadS3Object
func (c *s3Client) DownloadObject(ctx context.Context, tmpdir, bucket, key string) (string, int64, bool, error) {
	return s3helper.DownloadS3Object(ctx, tmpdir, c.awsS3Client, bucket, key)
}

// UploadObject reuses existing s3helper.UploadS3Object
func (c *s3Client) UploadObject(ctx context.Context, bucket, key, sourceFilename string) error {
	return s3helper.UploadS3Object(ctx, c.awsS3Client, bucket, key, sourceFilename)
}

// DeleteObject reuses existing s3helper.DeleteS3Object
func (c *s3Client) DeleteObject(ctx context.Context, bucket, key string) error {
	return s3helper.DeleteS3Object(ctx, c.awsS3Client, bucket, key)
}