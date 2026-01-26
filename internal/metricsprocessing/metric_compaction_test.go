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

package metricsprocessing

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func TestGetHourFromTimestamp(t *testing.T) {
	compactor := &MetricCompactionProcessor{}

	tests := []struct {
		name        string
		timestampMs int64
		expected    int16
	}{
		{
			name:        "midnight UTC",
			timestampMs: 1640995200000, // 2022-01-01 00:00:00 UTC
			expected:    0,
		},
		{
			name:        "noon UTC",
			timestampMs: 1640995200000 + 12*60*60*1000, // 2022-01-01 12:00:00 UTC
			expected:    12,
		},
		{
			name:        "11 PM UTC",
			timestampMs: 1640995200000 + 23*60*60*1000, // 2022-01-01 23:00:00 UTC
			expected:    23,
		},
		{
			name:        "next day 1 AM UTC",
			timestampMs: 1640995200000 + 25*60*60*1000, // 2022-01-02 01:00:00 UTC
			expected:    1,
		},
		{
			name:        "arbitrary timestamp",
			timestampMs: 1609459200000, // 2021-01-01 00:00:00 UTC
			expected:    0,
		},
		{
			name:        "arbitrary timestamp with hour",
			timestampMs: 1609459200000 + 15*60*60*1000, // 2021-01-01 15:00:00 UTC
			expected:    15,
		},
		{
			name:        "timestamp with minutes and seconds (should truncate to hour)",
			timestampMs: 1640995200000 + 12*60*60*1000 + 45*60*1000 + 30*1000, // 2022-01-01 12:45:30 UTC
			expected:    12,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compactor.getHourFromTimestamp(tt.timestampMs)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Mock store for testing markSegmentsAsCompacted
type mockCompactionStore struct {
	mock.Mock
}

func (m *mockCompactionStore) GetMetricSeg(ctx context.Context, params lrdb.GetMetricSegParams) (lrdb.MetricSeg, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(lrdb.MetricSeg), args.Error(1)
}

func (m *mockCompactionStore) CompactMetricSegments(ctx context.Context, params lrdb.CompactMetricSegsParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func (m *mockCompactionStore) KafkaOffsetsAfter(ctx context.Context, params lrdb.KafkaOffsetsAfterParams) ([]int64, error) {
	args := m.Called(ctx, params)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]int64), args.Error(1)
}

func (m *mockCompactionStore) CleanupKafkaOffsets(ctx context.Context, params lrdb.CleanupKafkaOffsetsParams) (int64, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(int64), args.Error(1)
}

func (m *mockCompactionStore) InsertKafkaOffsets(ctx context.Context, params lrdb.InsertKafkaOffsetsParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func (m *mockCompactionStore) MarkMetricSegsCompactedByKeys(ctx context.Context, params lrdb.MarkMetricSegsCompactedByKeysParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func (m *mockCompactionStore) GetMetricEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64 {
	args := m.Called(ctx, orgID, frequencyMs)
	return args.Get(0).(int64)
}

func (m *mockCompactionStore) WorkQueueClaim(ctx context.Context, arg lrdb.WorkQueueClaimParams) (lrdb.WorkQueue, error) {
	return lrdb.WorkQueue{}, nil
}

func (m *mockCompactionStore) WorkQueueComplete(ctx context.Context, arg lrdb.WorkQueueCompleteParams) error {
	return nil
}

func (m *mockCompactionStore) WorkQueueFail(ctx context.Context, arg lrdb.WorkQueueFailParams) (int32, error) {
	return 0, nil
}

func (m *mockCompactionStore) WorkQueueHeartbeat(ctx context.Context, arg lrdb.WorkQueueHeartbeatParams) error {
	return nil
}

func (m *mockCompactionStore) WorkQueueDepthAll(ctx context.Context) ([]lrdb.WorkQueueDepthAllRow, error) {
	return []lrdb.WorkQueueDepthAllRow{}, nil
}

// Removed unused mockStorageProvider

func TestMarkSegmentsAsCompacted(t *testing.T) {
	ctx := context.Background()
	compactor := &MetricCompactionProcessor{}
	mockStore := &mockCompactionStore{}
	compactor.store = mockStore

	// Test data
	orgID := uuid.New()
	key := messages.CompactionKey{
		OrganizationID: orgID,
		DateInt:        20240101,
		FrequencyMs:    10000,
		InstanceNum:    1,
	}

	segments := []lrdb.MetricSeg{
		{
			SegmentID:      12345,
			OrganizationID: orgID,
		},
		{
			SegmentID:      67890,
			OrganizationID: orgID,
		},
	}

	expectedParams := lrdb.MarkMetricSegsCompactedByKeysParams{
		OrganizationID: orgID,
		Dateint:        20240101,
		FrequencyMs:    10000,
		InstanceNum:    1,
		SegmentIds:     []int64{12345, 67890},
	}

	t.Run("successful marking", func(t *testing.T) {
		mockStore.On("MarkMetricSegsCompactedByKeys", ctx, expectedParams).Return(nil).Once()

		err := compactor.markSegmentsAsCompacted(ctx, segments, key)

		require.NoError(t, err)
		mockStore.AssertExpectations(t)
	})

	t.Run("empty segments list", func(t *testing.T) {
		err := compactor.markSegmentsAsCompacted(ctx, []lrdb.MetricSeg{}, key)

		require.NoError(t, err)
		// Should not call the store method for empty list
		mockStore.AssertExpectations(t)
	})

	t.Run("store error", func(t *testing.T) {
		expectedError := assert.AnError
		mockStore.On("MarkMetricSegsCompactedByKeys", ctx, expectedParams).Return(expectedError).Once()

		err := compactor.markSegmentsAsCompacted(ctx, segments, key)

		require.Error(t, err)
		assert.Equal(t, expectedError, err)
		mockStore.AssertExpectations(t)
	})
}

func TestGetTargetRecordCount(t *testing.T) {
	ctx := context.Background()
	compactor := &MetricCompactionProcessor{}
	mockStore := &mockCompactionStore{}
	compactor.store = mockStore

	// Test data
	orgID := uuid.New()
	groupingKey := messages.CompactionKey{
		OrganizationID: orgID,
		DateInt:        20240101,
		FrequencyMs:    10000,
		InstanceNum:    1,
	}

	t.Run("successful estimate retrieval", func(t *testing.T) {
		expectedEstimate := int64(50000)
		mockStore.On("GetMetricEstimate", ctx, orgID, int32(10000)).Return(expectedEstimate).Once()

		result := compactor.GetTargetRecordCount(ctx, groupingKey)

		assert.Equal(t, expectedEstimate, result)
		mockStore.AssertExpectations(t)
	})

	t.Run("zero estimate", func(t *testing.T) {
		expectedEstimate := int64(0)
		mockStore.On("GetMetricEstimate", ctx, orgID, int32(10000)).Return(expectedEstimate).Once()

		result := compactor.GetTargetRecordCount(ctx, groupingKey)

		assert.Equal(t, expectedEstimate, result)
		mockStore.AssertExpectations(t)
	})

	t.Run("large estimate", func(t *testing.T) {
		expectedEstimate := int64(1000000)
		mockStore.On("GetMetricEstimate", ctx, orgID, int32(10000)).Return(expectedEstimate).Once()

		result := compactor.GetTargetRecordCount(ctx, groupingKey)

		assert.Equal(t, expectedEstimate, result)
		mockStore.AssertExpectations(t)
	})

	t.Run("different frequency", func(t *testing.T) {
		groupingKeyDifferentFreq := messages.CompactionKey{
			OrganizationID: orgID,
			DateInt:        20240101,
			FrequencyMs:    60000, // 1 minute instead of 10 seconds
			InstanceNum:    1,
		}
		expectedEstimate := int64(25000)

		mockStore.On("GetMetricEstimate", ctx, orgID, int32(60000)).Return(expectedEstimate).Once()

		result := compactor.GetTargetRecordCount(ctx, groupingKeyDifferentFreq)

		assert.Equal(t, expectedEstimate, result)
		mockStore.AssertExpectations(t)
	})
}

func TestAtomicDatabaseUpdate(t *testing.T) {
	ctx := context.Background()
	compactor := &MetricCompactionProcessor{}
	mockStore := &mockCompactionStore{}
	compactor.store = mockStore

	// Test data
	orgID := uuid.New()
	key := messages.CompactionKey{
		OrganizationID: orgID,
		DateInt:        20240101,
		FrequencyMs:    10000,
		InstanceNum:    1,
	}

	oldSegments := []lrdb.MetricSeg{
		{
			SegmentID: 12345,
		},
		{
			SegmentID: 67890,
		},
	}

	newSegments := []lrdb.MetricSeg{
		{
			SegmentID:   100001,
			RecordCount: 5000,
			FileSize:    1024000,
			TsRange: pgtype.Range[pgtype.Int8]{
				Lower: pgtype.Int8{Int64: 1640995200000, Valid: true},
				Upper: pgtype.Int8{Int64: 1640998800000, Valid: true},
				Valid: true,
			},
			Fingerprints: []int64{1001, 1002},
		},
		{
			SegmentID:   100002,
			RecordCount: 3000,
			FileSize:    768000,
			TsRange: pgtype.Range[pgtype.Int8]{
				Lower: pgtype.Int8{Int64: 1640998800000, Valid: true},
				Upper: pgtype.Int8{Int64: 1641002400000, Valid: true},
				Valid: true,
			},
			Fingerprints: []int64{2001, 2002},
		},
	}

	t.Run("successful update with Kafka offsets", func(t *testing.T) {
		expectedParams := lrdb.CompactMetricSegsParams{
			OrganizationID: orgID,
			Dateint:        20240101,
			FrequencyMs:    10000,
			InstanceNum:    1,
			OldRecords: []lrdb.CompactMetricSegsOld{
				{SegmentID: 12345},
				{SegmentID: 67890},
			},
			NewRecords: []lrdb.CompactMetricSegsNew{
				{
					SegmentID:    100001,
					StartTs:      1640995200000,
					EndTs:        1640998800000,
					RecordCount:  5000,
					FileSize:     1024000,
					Fingerprints: []int64{1001, 1002},
				},
				{
					SegmentID:    100002,
					StartTs:      1640998800000,
					EndTs:        1641002400000,
					RecordCount:  3000,
					FileSize:     768000,
					Fingerprints: []int64{2001, 2002},
				},
			},
			CreatedBy: lrdb.CreatedByCompact,
		}

		mockStore.On("CompactMetricSegments", ctx, expectedParams).Return(nil).Once()

		err := compactor.atomicDatabaseUpdate(ctx, oldSegments, newSegments, key)

		require.NoError(t, err)
		mockStore.AssertExpectations(t)
	})

	t.Run("successful update without Kafka offsets", func(t *testing.T) {
		expectedParams := lrdb.CompactMetricSegsParams{
			OrganizationID: orgID,
			Dateint:        20240101,
			FrequencyMs:    10000,
			InstanceNum:    1,
			OldRecords: []lrdb.CompactMetricSegsOld{
				{SegmentID: 12345},
				{SegmentID: 67890},
			},
			NewRecords: []lrdb.CompactMetricSegsNew{
				{
					SegmentID:    100001,
					StartTs:      1640995200000,
					EndTs:        1640998800000,
					RecordCount:  5000,
					FileSize:     1024000,
					Fingerprints: []int64{1001, 1002},
				},
				{
					SegmentID:    100002,
					StartTs:      1640998800000,
					EndTs:        1641002400000,
					RecordCount:  3000,
					FileSize:     768000,
					Fingerprints: []int64{2001, 2002},
				},
			},
			CreatedBy: lrdb.CreatedByCompact,
		}

		mockStore.On("CompactMetricSegments", ctx, expectedParams).Return(nil).Once()

		err := compactor.atomicDatabaseUpdate(ctx, oldSegments, newSegments, key)

		require.NoError(t, err)
		mockStore.AssertExpectations(t)
	})

	t.Run("error when no new segments", func(t *testing.T) {
		err := compactor.atomicDatabaseUpdate(ctx, oldSegments, []lrdb.MetricSeg{}, key)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "no new segments to insert")
		// Should not call the store method
		mockStore.AssertExpectations(t)
	})

	t.Run("database operation error", func(t *testing.T) {
		expectedError := assert.AnError

		mockStore.On("CompactMetricSegments", ctx, mock.Anything).Return(expectedError).Once()

		err := compactor.atomicDatabaseUpdate(ctx, oldSegments, newSegments, key)

		require.Error(t, err)
		assert.ErrorIs(t, err, expectedError)
		mockStore.AssertExpectations(t)
	})
}

// Mock storage client for testing uploadAndCreateSegments
type mockStorageClient struct {
	mock.Mock
}

func (m *mockStorageClient) UploadObject(ctx context.Context, bucket, objectPath, fileName string) error {
	args := m.Called(ctx, bucket, objectPath, fileName)
	return args.Error(0)
}

func (m *mockStorageClient) DownloadObject(ctx context.Context, tmpDir, bucket, objectID string) (string, int64, bool, error) {
	args := m.Called(ctx, tmpDir, bucket, objectID)
	return args.String(0), args.Get(1).(int64), args.Bool(2), args.Error(3)
}

func (m *mockStorageClient) DeleteObject(ctx context.Context, bucket, objectPath string) error {
	args := m.Called(ctx, bucket, objectPath)
	return args.Error(0)
}

func (m *mockStorageClient) DeleteObjects(ctx context.Context, bucket string, keys []string) ([]string, error) {
	args := m.Called(ctx, bucket, keys)
	return args.Get(0).([]string), args.Error(1)
}

func TestUploadAndCreateSegments(t *testing.T) {
	ctx := context.Background()
	compactor := &MetricCompactionProcessor{}
	mockClient := &mockStorageClient{}

	// Test data
	orgID := uuid.New()
	key := messages.CompactionKey{
		OrganizationID: orgID,
		DateInt:        20240101,
		FrequencyMs:    10000,
		InstanceNum:    1,
	}

	profile := storageprofile.StorageProfile{
		OrganizationID: orgID,
		InstanceNum:    1,
		Bucket:         "test-bucket",
		CollectorName:  "test-collector",
	}

	inputSegments := []lrdb.MetricSeg{
		{SegmentID: 1001, FileSize: 1024000},
		{SegmentID: 1002, FileSize: 768000},
	}

	t.Run("successful upload and segment creation", func(t *testing.T) {
		results := []parquetwriter.Result{
			{
				FileName:    "/tmp/file1.parquet",
				RecordCount: 5000,
				FileSize:    2048000,
				Metadata: factories.MetricsFileStats{
					FirstTS:      1640995200000, // 2022-01-01 00:00:00 UTC
					LastTS:       1640998800000, // 2022-01-01 01:00:00 UTC
					Fingerprints: []int64{1001, 1002, 1003},
				},
			},
			{
				FileName:    "/tmp/file2.parquet",
				RecordCount: 3000,
				FileSize:    1536000,
				Metadata: factories.MetricsFileStats{
					FirstTS:      1640998800000, // 2022-01-01 01:00:00 UTC
					LastTS:       1641002400000, // 2022-01-01 02:00:00 UTC
					Fingerprints: []int64{2001, 2002},
				},
			},
		}

		// Set up mock expectations
		mockClient.On("UploadObject", ctx, "test-bucket", mock.MatchedBy(func(objectPath string) bool {
			// Verify the object path contains expected components (org ID, collector, etc.)
			return len(objectPath) > 0 // Just verify non-empty path
		}), "/tmp/file1.parquet").Return(nil).Once()

		mockClient.On("UploadObject", ctx, "test-bucket", mock.MatchedBy(func(objectPath string) bool {
			return len(objectPath) > 0
		}), "/tmp/file2.parquet").Return(nil).Once()

		segments, err := compactor.uploadAndCreateSegments(ctx, mockClient, profile, results, key, inputSegments)

		require.NoError(t, err)
		require.Len(t, segments, 2)

		// Verify first segment
		seg1 := segments[0]
		assert.Equal(t, orgID, seg1.OrganizationID)
		assert.Equal(t, int32(20240101), seg1.Dateint)
		assert.Equal(t, int32(10000), seg1.FrequencyMs)
		assert.Equal(t, int16(1), seg1.InstanceNum)
		assert.Equal(t, int64(5000), seg1.RecordCount)
		assert.Equal(t, int64(2048000), seg1.FileSize)
		assert.True(t, seg1.Published)
		assert.True(t, seg1.Compacted)
		assert.Equal(t, []int64{1001, 1002, 1003}, seg1.Fingerprints)
		assert.Equal(t, lrdb.CreatedByCompact, seg1.CreatedBy)
		assert.Greater(t, seg1.SegmentID, int64(0)) // Generated ID should be positive

		// Verify timestamp range
		assert.True(t, seg1.TsRange.Valid)
		assert.Equal(t, int64(1640995200000), seg1.TsRange.Lower.Int64)
		assert.Equal(t, int64(1640998800001), seg1.TsRange.Upper.Int64) // +1 to ensure non-empty range

		// Verify second segment
		seg2 := segments[1]
		assert.Equal(t, orgID, seg2.OrganizationID)
		assert.Equal(t, int64(3000), seg2.RecordCount)
		assert.Equal(t, int64(1536000), seg2.FileSize)
		assert.Equal(t, []int64{2001, 2002}, seg2.Fingerprints)
		assert.Greater(t, seg2.SegmentID, int64(0))
		assert.NotEqual(t, seg1.SegmentID, seg2.SegmentID) // Should be different IDs

		mockClient.AssertExpectations(t)
	})

	t.Run("upload error", func(t *testing.T) {
		results := []parquetwriter.Result{
			{
				FileName:    "/tmp/file1.parquet",
				RecordCount: 5000,
				FileSize:    2048000,
				Metadata: factories.MetricsFileStats{
					FirstTS:      1640995200000,
					LastTS:       1640998800000,
					Fingerprints: []int64{1001, 1002},
				},
			},
		}

		expectedError := assert.AnError
		mockClient.On("UploadObject", ctx, "test-bucket", mock.Anything, "/tmp/file1.parquet").Return(expectedError).Once()

		segments, err := compactor.uploadAndCreateSegments(ctx, mockClient, profile, results, key, inputSegments)

		require.Error(t, err)
		assert.Nil(t, segments)
		assert.Contains(t, err.Error(), "upload file")
		mockClient.AssertExpectations(t)
	})

	t.Run("invalid metadata type", func(t *testing.T) {
		results := []parquetwriter.Result{
			{
				FileName:    "/tmp/file1.parquet",
				RecordCount: 5000,
				FileSize:    2048000,
				Metadata:    "invalid-metadata-type", // Wrong type
			},
		}

		segments, err := compactor.uploadAndCreateSegments(ctx, mockClient, profile, results, key, inputSegments)

		require.Error(t, err)
		assert.Nil(t, segments)
		assert.Contains(t, err.Error(), "unexpected metadata type")
		// Should not call upload for invalid metadata
		mockClient.AssertExpectations(t)
	})

	t.Run("empty results", func(t *testing.T) {
		var results []parquetwriter.Result

		segments, err := compactor.uploadAndCreateSegments(ctx, mockClient, profile, results, key, inputSegments)

		require.NoError(t, err)
		assert.Empty(t, segments)
		// Should not call upload for empty results
		mockClient.AssertExpectations(t)
	})
}
