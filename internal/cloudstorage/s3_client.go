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
	"os"
	"strings"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/helpers"
)

// s3Client wraps the existing S3 implementation
type s3Client struct {
	awsS3Client *awsclient.S3Client
	isGCP       bool // true if this client is for GCP Cloud Storage
}

// DownloadObject downloads an object from S3 to a temporary file.
// For GCP Cloud Storage, it detects if the file was transparently decompressed
// and renames the file to remove the .gz extension if so.
func (c *s3Client) DownloadObject(ctx context.Context, tmpdir, bucket, key string) (string, int64, bool, error) {
	filename, size, notFound, err := downloadS3Object(ctx, tmpdir, c.awsS3Client, bucket, key)
	if err != nil || notFound {
		return filename, size, notFound, err
	}

	// For GCP, check if .gz files were transparently decompressed
	if c.isGCP && strings.HasSuffix(filename, ".gz") {
		isGzip, checkErr := helpers.IsGzipFile(filename)
		if checkErr == nil && !isGzip {
			// File was transparently decompressed by GCP, rename to remove .gz
			newFilename := strings.TrimSuffix(filename, ".gz")
			if renameErr := os.Rename(filename, newFilename); renameErr != nil {
				// Must rename or readers will fail trying to decompress non-gzip data
				return "", 0, false, renameErr
			}
			filename = newFilename
		}
	}

	return filename, size, notFound, nil
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
