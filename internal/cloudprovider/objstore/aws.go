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

package objstore

import (
	"context"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// AWSS3Store implements ObjectStore using AWS S3
type AWSS3Store struct {
	client *s3.Client
}

// NewAWSS3Store creates a new AWS S3 object store
func NewAWSS3Store(client *s3.Client) ObjectStore {
	return &AWSS3Store{client: client}
}

// Get retrieves an object from S3
func (s *AWSS3Store) Get(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Put stores an object in S3
func (s *AWSS3Store) Put(ctx context.Context, bucket, key string, reader io.Reader) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   reader,
	})
	return err
}

// Delete removes an object from S3
func (s *AWSS3Store) Delete(ctx context.Context, bucket, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return err
}

// Exists checks if an object exists in S3
func (s *AWSS3Store) Exists(ctx context.Context, bucket, key string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if s.IsNotFoundError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// IsNotFoundError checks if an error indicates the object was not found
func (s *AWSS3Store) IsNotFoundError(err error) bool {
	var notFound *types.NoSuchKey
	if errors.As(err, &notFound) {
		return true
	}

	var notFoundError *types.NotFound
	return errors.As(err, &notFoundError)
}

// GetS3Client returns the underlying S3 client for advanced operations
func (s *AWSS3Store) GetS3Client() *s3.Client {
	return s.client
}
