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

package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

// mockStorageProfileProvider implements StorageProfileProvider for testing
type mockStorageProfileProvider struct {
	mock.Mock
}

func (m *mockStorageProfileProvider) GetStorageProfileForBucket(ctx context.Context, organizationID uuid.UUID, bucketName string) (storageprofile.StorageProfile, error) {
	args := m.Called(ctx, organizationID, bucketName)
	return args.Get(0).(storageprofile.StorageProfile), args.Error(1)
}

func (m *mockStorageProfileProvider) GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]storageprofile.StorageProfile, error) {
	args := m.Called(ctx, bucketName)
	return args.Get(0).([]storageprofile.StorageProfile), args.Error(1)
}

func (m *mockStorageProfileProvider) GetStorageProfileForOrganization(ctx context.Context, organizationID uuid.UUID) (storageprofile.StorageProfile, error) {
	args := m.Called(ctx, organizationID)
	return args.Get(0).(storageprofile.StorageProfile), args.Error(1)
}

func (m *mockStorageProfileProvider) GetStorageProfileForOrganizationAndInstance(ctx context.Context, organizationID uuid.UUID, instanceNum int16) (storageprofile.StorageProfile, error) {
	args := m.Called(ctx, organizationID, instanceNum)
	return args.Get(0).(storageprofile.StorageProfile), args.Error(1)
}

func (m *mockStorageProfileProvider) GetStorageProfileForOrganizationAndCollector(ctx context.Context, organizationID uuid.UUID, collectorName string) (storageprofile.StorageProfile, error) {
	args := m.Called(ctx, organizationID, collectorName)
	return args.Get(0).(storageprofile.StorageProfile), args.Error(1)
}

func (m *mockStorageProfileProvider) GetLowestInstanceStorageProfile(ctx context.Context, organizationID uuid.UUID, bucketName string) (storageprofile.StorageProfile, error) {
	args := m.Called(ctx, organizationID, bucketName)
	return args.Get(0).(storageprofile.StorageProfile), args.Error(1)
}

func (m *mockStorageProfileProvider) ResolveOrganization(ctx context.Context, bucketName, objectPath string) (storageprofile.OrganizationResolution, error) {
	args := m.Called(ctx, bucketName, objectPath)
	return args.Get(0).(storageprofile.OrganizationResolution), args.Error(1)
}

// mockDeduplicator implements Deduplicator interface for testing
type mockDeduplicator struct {
	mock.Mock
}

func (m *mockDeduplicator) CheckAndRecord(ctx context.Context, bucket, objectID, source string) (bool, error) {
	args := m.Called(ctx, bucket, objectID, source)
	return args.Bool(0), args.Error(1)
}

func (m *mockDeduplicator) CheckAndRecordBatch(ctx context.Context, items []DedupItem, source string) ([]DedupItem, error) {
	args := m.Called(ctx, items, source)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]DedupItem), args.Error(1)
}

func TestConvertItemsToKafkaMessages_OtelRawPath(t *testing.T) {
	ctx := context.Background()

	orgID := uuid.New()

	// Mock storage profile provider
	mockSP := &mockStorageProfileProvider{}
	mockSP.On("GetLowestInstanceStorageProfile", ctx, orgID, "test-bucket").Return(
		storageprofile.StorageProfile{
			OrganizationID: orgID,
			InstanceNum:    1,
			CollectorName:  "test-collector",
			Bucket:         "test-bucket",
		}, nil)

	// Mock deduplicator
	mockDedup := &mockDeduplicator{}
	mockDedup.On("CheckAndRecord", ctx, mock.AnythingOfType("*IngestItem")).Return(true, nil)

	// Test input
	items := []IngestItem{
		{
			OrganizationID: orgID,
			Bucket:         "test-bucket",
			ObjectID:       "otel-raw/logs/test-file.json",
			Signal:         "logs",
			FileSize:       1024,
			QueuedAt:       time.Now(),
		},
	}

	// Call the function
	result, err := convertItemsToKafkaMessages(ctx, items, mockSP)

	// Assertions
	assert.NoError(t, err)
	assert.Equal(t, 1, result.ItemsProcessed)
	assert.Equal(t, 0, result.ItemsSkipped)
	assert.Len(t, result.NotificationsBySignal, 1)
	assert.Contains(t, result.NotificationsBySignal, "logs")
	assert.Len(t, result.NotificationsBySignal["logs"], 1)

	notification := result.NotificationsBySignal["logs"][0]
	assert.Equal(t, orgID, notification.OrganizationID)
	assert.Equal(t, int16(1), notification.InstanceNum)
	assert.Equal(t, "test-bucket", notification.Bucket)
	assert.Equal(t, "otel-raw/logs/test-file.json", notification.ObjectID)
	assert.Equal(t, int64(1024), notification.FileSize)

	// Verify mocks were called as expected
	mockSP.AssertExpectations(t)
}

func TestConvertItemsToKafkaMessages_DatabaseFileSkipped(t *testing.T) {
	ctx := context.Background()

	mockSP := &mockStorageProfileProvider{}
	mockDedup := &mockDeduplicator{}

	// Test input with database file
	items := []IngestItem{
		{
			OrganizationID: uuid.New(),
			Bucket:         "test-bucket",
			ObjectID:       "db/some-database-file.sql",
			Signal:         "logs",
			FileSize:       1024,
			QueuedAt:       time.Now(),
		},
	}

	// Call the function
	result, err := convertItemsToKafkaMessages(ctx, items, mockSP)

	// Assertions
	assert.NoError(t, err)
	assert.Equal(t, 0, result.ItemsProcessed)
	assert.Equal(t, 1, result.ItemsSkipped)
	assert.Equal(t, 1, result.SkipReasons["database_file"])
	assert.Len(t, result.NotificationsBySignal, 0)

	// Verify no calls were made to mocks
	mockSP.AssertExpectations(t)
	mockDedup.AssertExpectations(t)
}

func TestConvertItemsToKafkaMessages_NonOtelRawPath(t *testing.T) {
	ctx := context.Background()

	orgID := uuid.New()

	// Mock storage profile provider
	mockSP := &mockStorageProfileProvider{}
	mockSP.On("ResolveOrganization", ctx, "test-bucket", "custom/path/file.json").Return(
		storageprofile.OrganizationResolution{
			OrganizationID: orgID,
			Signal:         "metrics",
		}, nil)
	mockSP.On("GetLowestInstanceStorageProfile", ctx, orgID, "test-bucket").Return(
		storageprofile.StorageProfile{
			OrganizationID: orgID,
			InstanceNum:    2,
			CollectorName:  "custom-collector",
			Bucket:         "test-bucket",
		}, nil)

	// Mock deduplicator
	mockDedup := &mockDeduplicator{}
	mockDedup.On("CheckAndRecord", ctx, mock.AnythingOfType("*IngestItem")).Return(true, nil)

	// Test input with non-otel-raw path
	items := []IngestItem{
		{
			OrganizationID: uuid.Nil, // Will be resolved
			Bucket:         "test-bucket",
			ObjectID:       "custom/path/file.json",
			Signal:         "", // Will be resolved
			FileSize:       2048,
			QueuedAt:       time.Now(),
		},
	}

	// Call the function
	result, err := convertItemsToKafkaMessages(ctx, items, mockSP)

	// Assertions
	assert.NoError(t, err)
	assert.Equal(t, 1, result.ItemsProcessed)
	assert.Equal(t, 0, result.ItemsSkipped)
	assert.Len(t, result.NotificationsBySignal, 1)
	assert.Contains(t, result.NotificationsBySignal, "metrics")
	assert.Len(t, result.NotificationsBySignal["metrics"], 1)

	notification := result.NotificationsBySignal["metrics"][0]
	assert.Equal(t, orgID, notification.OrganizationID)
	assert.Equal(t, int16(2), notification.InstanceNum)
	assert.Equal(t, "test-bucket", notification.Bucket)
	assert.Equal(t, "custom/path/file.json", notification.ObjectID)
	assert.Equal(t, int64(2048), notification.FileSize)

	// Verify mocks were called as expected
	mockSP.AssertExpectations(t)
}

func TestGetFileExtensionCategory(t *testing.T) {
	tests := []struct {
		name     string
		objectID string
		want     string
	}{
		{"json file", "logs/file.json", "json"},
		{"JSON uppercase", "logs/FILE.JSON", "json"},
		{"json.gz file", "logs/file.json.gz", "json.gz"},
		{"JSON.GZ uppercase", "logs/FILE.JSON.GZ", "json.gz"},
		{"binpb file", "metrics/data.binpb", "binpb"},
		{"binpb.gz file", "metrics/data.binpb.gz", "binpb.gz"},
		{"parquet file", "traces/data.parquet", "parquet"},
		{"PARQUET uppercase", "traces/DATA.PARQUET", "parquet"},
		{"unknown extension", "data.txt", "other"},
		{"no extension", "datafile", "other"},
		{"csv file", "data.csv", "other"},
		{"tar.gz file", "archive.tar.gz", "other"},
		{"complex path json", "org1/2024/01/15/data.json", "json"},
		{"complex path json.gz", "org1/2024/01/15/data.json.gz", "json.gz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getFileExtensionCategory(tt.objectID)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestConvertItemsToKafkaMessages_FileTypeCounting(t *testing.T) {
	ctx := context.Background()
	orgID := uuid.New()

	// Create test items with various file types
	// Using otel-raw prefix to skip ResolveOrganization call
	items := []IngestItem{
		{
			Bucket:         "test-bucket",
			ObjectID:       "otel-raw/logs/file1.json",
			FileSize:       1024,
			Signal:         "logs",
			OrganizationID: orgID,
			InstanceNum:    1,
		},
		{
			Bucket:         "test-bucket",
			ObjectID:       "otel-raw/logs/file2.json.gz",
			FileSize:       2048,
			Signal:         "logs",
			OrganizationID: orgID,
			InstanceNum:    1,
		},
		{
			Bucket:         "test-bucket",
			ObjectID:       "otel-raw/metrics/data1.binpb",
			FileSize:       3072,
			Signal:         "metrics",
			OrganizationID: orgID,
			InstanceNum:    1,
		},
		{
			Bucket:         "test-bucket",
			ObjectID:       "otel-raw/metrics/data2.binpb.gz",
			FileSize:       4096,
			Signal:         "metrics",
			OrganizationID: orgID,
			InstanceNum:    1,
		},
		{
			Bucket:         "test-bucket",
			ObjectID:       "otel-raw/traces/span1.parquet",
			FileSize:       5120,
			Signal:         "traces",
			OrganizationID: orgID,
			InstanceNum:    1,
		},
		{
			Bucket:         "test-bucket",
			ObjectID:       "otel-raw/traces/span2.parquet",
			FileSize:       6144,
			Signal:         "traces",
			OrganizationID: orgID,
			InstanceNum:    1,
		},
		{
			Bucket:         "test-bucket",
			ObjectID:       "otel-raw/logs/unknown.txt",
			FileSize:       7168,
			Signal:         "logs",
			OrganizationID: orgID,
			InstanceNum:    1,
		},
	}

	// Set up mock storage profile provider
	mockSP := new(mockStorageProfileProvider)
	mockProfile := storageprofile.StorageProfile{
		OrganizationID: orgID,
		InstanceNum:    1,
		CollectorName:  "test-collector",
		Bucket:         "test-bucket",
	}
	mockSP.On("GetLowestInstanceStorageProfile", ctx, orgID, "test-bucket").Return(mockProfile, nil)

	// Call the function
	result, err := convertItemsToKafkaMessages(ctx, items, mockSP)

	// Assertions
	require.NoError(t, err)
	assert.Equal(t, 7, result.ItemsProcessed)
	assert.Equal(t, 0, result.ItemsSkipped)

	// Check file type counts for logs
	assert.Equal(t, 1, result.FileTypeCounts["logs"]["json"])
	assert.Equal(t, 1, result.FileTypeCounts["logs"]["json.gz"])
	assert.Equal(t, 1, result.FileTypeCounts["logs"]["other"])
	assert.Equal(t, 0, result.FileTypeCounts["logs"]["binpb"])
	assert.Equal(t, 0, result.FileTypeCounts["logs"]["binpb.gz"])
	assert.Equal(t, 0, result.FileTypeCounts["logs"]["parquet"])

	// Check file type counts for metrics
	assert.Equal(t, 1, result.FileTypeCounts["metrics"]["binpb"])
	assert.Equal(t, 1, result.FileTypeCounts["metrics"]["binpb.gz"])
	assert.Equal(t, 0, result.FileTypeCounts["metrics"]["json"])
	assert.Equal(t, 0, result.FileTypeCounts["metrics"]["json.gz"])
	assert.Equal(t, 0, result.FileTypeCounts["metrics"]["parquet"])
	assert.Equal(t, 0, result.FileTypeCounts["metrics"]["other"])

	// Check file type counts for traces
	assert.Equal(t, 2, result.FileTypeCounts["traces"]["parquet"])
	assert.Equal(t, 0, result.FileTypeCounts["traces"]["json"])
	assert.Equal(t, 0, result.FileTypeCounts["traces"]["json.gz"])
	assert.Equal(t, 0, result.FileTypeCounts["traces"]["binpb"])
	assert.Equal(t, 0, result.FileTypeCounts["traces"]["binpb.gz"])
	assert.Equal(t, 0, result.FileTypeCounts["traces"]["other"])

	// Verify notifications were created
	assert.Len(t, result.NotificationsBySignal["logs"], 3)
	assert.Len(t, result.NotificationsBySignal["metrics"], 2)
	assert.Len(t, result.NotificationsBySignal["traces"], 2)

	mockSP.AssertExpectations(t)
}
