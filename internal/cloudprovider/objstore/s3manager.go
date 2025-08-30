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
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cardinalhq/lakerunner/internal/cloudprovider/credential"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
)

// S3Manager provides higher-level operations on top of ObjectStore
type S3Manager interface {
	ObjectStore

	// DownloadToTempFile downloads an object to a temporary file
	DownloadToTempFile(ctx context.Context, tmpDir, bucket, key string) (string, int64, bool, error)

	// UploadFromFile uploads a file to the object store
	UploadFromFile(ctx context.Context, bucket, key, filePath string) error

	// GetS3Client returns the underlying S3 client for advanced operations
	GetS3Client() *s3.Client

	// GetTracer returns the OpenTelemetry tracer for this client
	GetTracer() trace.Tracer

	// ScheduleDelete schedules an object for deletion by adding it to the cleanup queue
	ScheduleDelete(ctx context.Context, mdb lrdb.StoreFull, orgID uuid.UUID, instanceNum int16, bucketID, objectID string) error

	// Backward compatibility aliases
	DownloadObject(ctx context.Context, bucketID, objectID, destPath string) error
	UploadObject(ctx context.Context, bucketID, objectID, sourcePath string) error
	DeleteObject(ctx context.Context, bucketID, objectID string) error
}

// AWSS3Manager implements S3Manager using AWS S3
type AWSS3Manager struct {
	*AWSS3Store
	tracer trace.Tracer
}

// NewAWSS3Manager creates a new AWS S3 manager
func NewAWSS3Manager(client *s3.Client) S3Manager {
	return &AWSS3Manager{
		AWSS3Store: &AWSS3Store{client: client},
		tracer:     trace.NewNoopTracerProvider().Tracer("s3manager"), // Default no-op tracer
	}
}

// NewAWSS3ManagerWithTracer creates a new AWS S3 manager with custom tracer
func NewAWSS3ManagerWithTracer(client *s3.Client, tracer trace.Tracer) S3Manager {
	return &AWSS3Manager{
		AWSS3Store: &AWSS3Store{client: client},
		tracer:     tracer,
	}
}

// DownloadToTempFile downloads an object to a temporary file and returns the file path, size, and whether it existed
func (m *AWSS3Manager) DownloadToTempFile(ctx context.Context, tmpDir, bucket, key string) (string, int64, bool, error) {
	// Check if object exists first
	exists, err := m.Exists(ctx, bucket, key)
	if err != nil {
		return "", 0, false, fmt.Errorf("checking if object exists: %w", err)
	}
	if !exists {
		return "", 0, false, nil
	}

	// Get the object
	reader, err := m.Get(ctx, bucket, key)
	if err != nil {
		return "", 0, false, fmt.Errorf("getting object: %w", err)
	}
	defer reader.Close()

	// Create temporary file
	tempFile, err := os.CreateTemp(tmpDir, "download-*")
	if err != nil {
		return "", 0, false, fmt.Errorf("creating temporary file: %w", err)
	}
	defer tempFile.Close()

	// Copy data to temporary file
	size, err := io.Copy(tempFile, reader)
	if err != nil {
		os.Remove(tempFile.Name())
		return "", 0, false, fmt.Errorf("copying data to temporary file: %w", err)
	}

	return tempFile.Name(), size, true, nil
}

// UploadFromFile uploads a file to the object store
func (m *AWSS3Manager) UploadFromFile(ctx context.Context, bucket, key, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("opening file %s: %w", filePath, err)
	}
	defer file.Close()

	return m.Put(ctx, bucket, key, file)
}

// GetS3Client returns the underlying S3 client for advanced operations
func (m *AWSS3Manager) GetS3Client() *s3.Client {
	return m.AWSS3Store.GetS3Client()
}

// GetTracer returns the OpenTelemetry tracer for this client
func (m *AWSS3Manager) GetTracer() trace.Tracer {
	return m.tracer
}

// ScheduleDelete schedules an object for deletion by adding it to the cleanup queue
func (m *AWSS3Manager) ScheduleDelete(ctx context.Context, mdb lrdb.StoreFull, orgID uuid.UUID, instanceNum int16, bucketID, objectID string) error {
	return mdb.ObjectCleanupAdd(ctx, lrdb.ObjectCleanupAddParams{
		OrganizationID: orgID,
		BucketID:       bucketID,
		ObjectID:       objectID,
		InstanceNum:    instanceNum,
	})
}

// DownloadObject downloads an object to a specific destination path (backward compatibility)
func (m *AWSS3Manager) DownloadObject(ctx context.Context, bucketID, objectID, destPath string) error {
	reader, err := m.Get(ctx, bucketID, objectID)
	if err != nil {
		return fmt.Errorf("getting object: %w", err)
	}
	defer reader.Close()

	file, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("creating destination file: %w", err)
	}
	defer file.Close()

	_, err = io.Copy(file, reader)
	if err != nil {
		return fmt.Errorf("copying data to file: %w", err)
	}

	return nil
}

// UploadObject uploads a file to the object store (backward compatibility)
func (m *AWSS3Manager) UploadObject(ctx context.Context, bucketID, objectID, sourcePath string) error {
	return m.UploadFromFile(ctx, bucketID, objectID, sourcePath)
}

// DeleteObject deletes an object from the object store (backward compatibility)
func (m *AWSS3Manager) DeleteObject(ctx context.Context, bucketID, objectID string) error {
	return m.Delete(ctx, bucketID, objectID)
}

// S3ManagerFactory creates S3Manager instances for different profiles
type S3ManagerFactory struct {
	credProvider credential.Provider
}

// NewS3ManagerFactory creates a new S3Manager factory
func NewS3ManagerFactory(credProvider credential.Provider) *S3ManagerFactory {
	return &S3ManagerFactory{
		credProvider: credProvider,
	}
}

// GetS3ManagerForProfile creates an S3Manager for the specified profile
func (f *S3ManagerFactory) GetS3ManagerForProfile(ctx context.Context, profile, region string) (S3Manager, error) {
	config := credential.CredentialConfig{
		Provider: "aws",
		Region:   region,
		Profile:  profile,
	}

	creds, err := f.credProvider.GetCredentials(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("getting credentials for profile %s: %w", profile, err)
	}

	// Extract AWS config from credentials
	awsCreds, ok := creds.(*credential.AWSCredentials)
	if !ok {
		return nil, fmt.Errorf("expected AWS credentials, got %T", creds)
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(awsCreds.GetAWSConfig())

	return NewAWSS3Manager(s3Client), nil
}
