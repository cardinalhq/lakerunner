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

package accumulation

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
	compactor := &MetricCompactorProcessor{}

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

func (m *mockCompactionStore) CompactMetricSegsWithKafkaOffsetsWithOrg(ctx context.Context, params lrdb.CompactMetricSegsParams, kafkaOffsets []lrdb.KafkaOffsetUpdateWithOrg) error {
	args := m.Called(ctx, params, kafkaOffsets)
	return args.Error(0)
}

func (m *mockCompactionStore) KafkaJournalGetLastProcessedWithOrgInstance(ctx context.Context, params lrdb.KafkaJournalGetLastProcessedWithOrgInstanceParams) (int64, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(int64), args.Error(1)
}

func (m *mockCompactionStore) MarkMetricSegsCompactedByKeys(ctx context.Context, params lrdb.MarkMetricSegsCompactedByKeysParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func (m *mockCompactionStore) GetMetricEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64 {
	args := m.Called(ctx, orgID, frequencyMs)
	return args.Get(0).(int64)
}

// Removed unused mockStorageProvider

func TestMarkSegmentsAsCompacted(t *testing.T) {
	ctx := context.Background()
	compactor := &MetricCompactorProcessor{}
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
	compactor := &MetricCompactorProcessor{}
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
	compactor := &MetricCompactorProcessor{}
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
			SlotID:    1,
		},
		{
			SegmentID: 67890,
			SlotID:    2,
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
		kafkaCommitData := &KafkaCommitData{
			Topic:         "test-topic",
			ConsumerGroup: "test-group",
			Offsets: map[int32]int64{
				0: 100,
				1: 200,
				2: 150,
			},
		}

		expectedParams := lrdb.CompactMetricSegsParams{
			OrganizationID: orgID,
			Dateint:        20240101,
			FrequencyMs:    10000,
			InstanceNum:    1,
			SlotID:         0,
			SlotCount:      1,
			IngestDateint:  20240101,
			OldRecords: []lrdb.CompactMetricSegsOld{
				{SegmentID: 12345, SlotID: 1},
				{SegmentID: 67890, SlotID: 2},
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

		mockStore.On("CompactMetricSegsWithKafkaOffsetsWithOrg", ctx, expectedParams, mock.MatchedBy(func(offsets []lrdb.KafkaOffsetUpdateWithOrg) bool {
			// Check that offsets contain expected data (don't care about order since sorting is tested separately)
			if len(offsets) != 3 {
				return false
			}

			// Check all expected offsets are present with correct org/instance info
			offsetMap := make(map[int32]int64)
			for _, offset := range offsets {
				if offset.Topic != "test-topic" || offset.ConsumerGroup != "test-group" ||
					offset.OrganizationID != orgID || offset.InstanceNum != 1 {
					return false
				}
				offsetMap[offset.Partition] = offset.Offset
			}

			return offsetMap[0] == 100 && offsetMap[1] == 200 && offsetMap[2] == 150
		})).Return(nil).Once()

		err := compactor.atomicDatabaseUpdate(ctx, oldSegments, newSegments, kafkaCommitData, key)

		require.NoError(t, err)
		mockStore.AssertExpectations(t)
	})

	t.Run("successful update without Kafka offsets", func(t *testing.T) {
		expectedParams := lrdb.CompactMetricSegsParams{
			OrganizationID: orgID,
			Dateint:        20240101,
			FrequencyMs:    10000,
			InstanceNum:    1,
			SlotID:         0,
			SlotCount:      1,
			IngestDateint:  20240101,
			OldRecords: []lrdb.CompactMetricSegsOld{
				{SegmentID: 12345, SlotID: 1},
				{SegmentID: 67890, SlotID: 2},
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

		mockStore.On("CompactMetricSegsWithKafkaOffsetsWithOrg", ctx, expectedParams, []lrdb.KafkaOffsetUpdateWithOrg(nil)).Return(nil).Once()

		err := compactor.atomicDatabaseUpdate(ctx, oldSegments, newSegments, nil, key)

		require.NoError(t, err)
		mockStore.AssertExpectations(t)
	})

	t.Run("error when no new segments", func(t *testing.T) {
		err := compactor.atomicDatabaseUpdate(ctx, oldSegments, []lrdb.MetricSeg{}, nil, key)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "no new segments to insert")
		// Should not call the store method
		mockStore.AssertExpectations(t)
	})

	t.Run("database operation error", func(t *testing.T) {
		expectedError := assert.AnError

		mockStore.On("CompactMetricSegsWithKafkaOffsetsWithOrg", ctx, mock.Anything, []lrdb.KafkaOffsetUpdateWithOrg(nil)).Return(expectedError).Once()

		err := compactor.atomicDatabaseUpdate(ctx, oldSegments, newSegments, nil, key)

		require.Error(t, err)
		assert.Equal(t, expectedError, err)
		mockStore.AssertExpectations(t)
	})
}

func TestSortKafkaOffsets(t *testing.T) {
	orgID := uuid.New()

	t.Run("empty slice", func(t *testing.T) {
		var offsets []lrdb.KafkaOffsetUpdateWithOrg
		sortKafkaOffsets(offsets)
		assert.Empty(t, offsets)
	})

	t.Run("single offset", func(t *testing.T) {
		offsets := []lrdb.KafkaOffsetUpdateWithOrg{
			{
				Topic:          "topic1",
				Partition:      0,
				ConsumerGroup:  "group1",
				OrganizationID: orgID,
				InstanceNum:    1,
				Offset:         100,
			},
		}
		expected := make([]lrdb.KafkaOffsetUpdateWithOrg, len(offsets))
		copy(expected, offsets)

		sortKafkaOffsets(offsets)
		assert.Equal(t, expected, offsets)
	})

	t.Run("sort by topic", func(t *testing.T) {
		offsets := []lrdb.KafkaOffsetUpdateWithOrg{
			{
				Topic:          "ztopic",
				Partition:      0,
				ConsumerGroup:  "group1",
				OrganizationID: orgID,
				InstanceNum:    1,
				Offset:         100,
			},
			{
				Topic:          "atopic",
				Partition:      0,
				ConsumerGroup:  "group1",
				OrganizationID: orgID,
				InstanceNum:    1,
				Offset:         200,
			},
			{
				Topic:          "mtopic",
				Partition:      0,
				ConsumerGroup:  "group1",
				OrganizationID: orgID,
				InstanceNum:    1,
				Offset:         300,
			},
		}

		sortKafkaOffsets(offsets)

		assert.Equal(t, "atopic", offsets[0].Topic)
		assert.Equal(t, "mtopic", offsets[1].Topic)
		assert.Equal(t, "ztopic", offsets[2].Topic)
	})

	t.Run("sort by partition when topics are same", func(t *testing.T) {
		offsets := []lrdb.KafkaOffsetUpdateWithOrg{
			{
				Topic:          "topic1",
				Partition:      5,
				ConsumerGroup:  "group1",
				OrganizationID: orgID,
				InstanceNum:    1,
				Offset:         100,
			},
			{
				Topic:          "topic1",
				Partition:      1,
				ConsumerGroup:  "group1",
				OrganizationID: orgID,
				InstanceNum:    1,
				Offset:         200,
			},
			{
				Topic:          "topic1",
				Partition:      3,
				ConsumerGroup:  "group1",
				OrganizationID: orgID,
				InstanceNum:    1,
				Offset:         300,
			},
		}

		sortKafkaOffsets(offsets)

		assert.Equal(t, int32(1), offsets[0].Partition)
		assert.Equal(t, int32(3), offsets[1].Partition)
		assert.Equal(t, int32(5), offsets[2].Partition)
	})

	t.Run("sort by consumer group when topic and partition are same", func(t *testing.T) {
		offsets := []lrdb.KafkaOffsetUpdateWithOrg{
			{
				Topic:          "topic1",
				Partition:      0,
				ConsumerGroup:  "zgroup",
				OrganizationID: orgID,
				InstanceNum:    1,
				Offset:         100,
			},
			{
				Topic:          "topic1",
				Partition:      0,
				ConsumerGroup:  "agroup",
				OrganizationID: orgID,
				InstanceNum:    1,
				Offset:         200,
			},
			{
				Topic:          "topic1",
				Partition:      0,
				ConsumerGroup:  "mgroup",
				OrganizationID: orgID,
				InstanceNum:    1,
				Offset:         300,
			},
		}

		sortKafkaOffsets(offsets)

		assert.Equal(t, "agroup", offsets[0].ConsumerGroup)
		assert.Equal(t, "mgroup", offsets[1].ConsumerGroup)
		assert.Equal(t, "zgroup", offsets[2].ConsumerGroup)
	})

	t.Run("complex mixed sort", func(t *testing.T) {
		offsets := []lrdb.KafkaOffsetUpdateWithOrg{
			// Should be last: ztopic, partition 0, group1
			{
				Topic:          "ztopic",
				Partition:      0,
				ConsumerGroup:  "group1",
				OrganizationID: orgID,
				InstanceNum:    1,
				Offset:         500,
			},
			// Should be 4th: btopic, partition 2, group1
			{
				Topic:          "btopic",
				Partition:      2,
				ConsumerGroup:  "group1",
				OrganizationID: orgID,
				InstanceNum:    1,
				Offset:         400,
			},
			// Should be 2nd: btopic, partition 0, groupz
			{
				Topic:          "btopic",
				Partition:      0,
				ConsumerGroup:  "groupz",
				OrganizationID: orgID,
				InstanceNum:    1,
				Offset:         200,
			},
			// Should be first: btopic, partition 0, groupa
			{
				Topic:          "btopic",
				Partition:      0,
				ConsumerGroup:  "groupa",
				OrganizationID: orgID,
				InstanceNum:    1,
				Offset:         100,
			},
			// Should be 3rd: btopic, partition 1, group1
			{
				Topic:          "btopic",
				Partition:      1,
				ConsumerGroup:  "group1",
				OrganizationID: orgID,
				InstanceNum:    1,
				Offset:         300,
			},
		}

		sortKafkaOffsets(offsets)

		// Verify the sorted order
		expected := []struct {
			topic         string
			partition     int32
			consumerGroup string
			offset        int64
		}{
			{"btopic", 0, "groupa", 100},
			{"btopic", 0, "groupz", 200},
			{"btopic", 1, "group1", 300},
			{"btopic", 2, "group1", 400},
			{"ztopic", 0, "group1", 500},
		}

		require.Len(t, offsets, 5)
		for i, exp := range expected {
			assert.Equal(t, exp.topic, offsets[i].Topic, "mismatch at index %d", i)
			assert.Equal(t, exp.partition, offsets[i].Partition, "mismatch at index %d", i)
			assert.Equal(t, exp.consumerGroup, offsets[i].ConsumerGroup, "mismatch at index %d", i)
			assert.Equal(t, exp.offset, offsets[i].Offset, "mismatch at index %d", i)
		}
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

func TestUploadAndCreateSegments(t *testing.T) {
	ctx := context.Background()
	compactor := &MetricCompactorProcessor{}
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
		assert.Equal(t, int32(0), seg1.SlotID) // Compacted segments use 0
		assert.Equal(t, int32(1), seg1.SlotCount)
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
		assert.Equal(t, int64(1640998800000), seg1.TsRange.Upper.Int64)

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
