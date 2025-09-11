//go:build integration

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

package queries

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/testhelpers"
)

func TestMarkMetricSegsCompactedByKeys_SingleSegment(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create a single metric segment
	segmentID := int64(12345)
	err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		SegmentID:      segmentID,
		InstanceNum:    1,
		StartTs:        now.UnixMilli(),
		EndTs:          now.Add(time.Hour).UnixMilli(),
		RecordCount:    1000,
		FileSize:       50000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{123, 456},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		Compacted:      false,
	})
	require.NoError(t, err)

	// Mark the segment as compacted
	err = db.MarkMetricSegsCompactedByKeys(ctx, lrdb.MarkMetricSegsCompactedByKeysParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     []int64{segmentID},
	})
	require.NoError(t, err)

	// Verify the segment is now marked as compacted and unpublished
	segments, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     []int64{segmentID},
	})
	require.NoError(t, err)
	require.Len(t, segments, 1)

	segment := segments[0]
	assert.Equal(t, segmentID, segment.SegmentID)
	assert.True(t, segment.Compacted, "Segment should be marked as compacted")
	assert.False(t, segment.Published, "Segment should be marked as unpublished")
}

func TestMarkMetricSegsCompactedByKeys_MultipleSegments(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create multiple metric segments
	segmentIDs := []int64{12345, 12346, 12347}
	for _, segmentID := range segmentIDs {
		err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
			OrganizationID: orgID,
			Dateint:        20250830,
			FrequencyMs:    5000,
			SegmentID:      segmentID,
			InstanceNum:    1,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(time.Hour).UnixMilli(),
			RecordCount:    1000,
			FileSize:       50000,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Fingerprints:   []int64{123, 456},
			SortVersion:    lrdb.CurrentMetricSortVersion,
			Compacted:      false,
		})
		require.NoError(t, err)
	}

	// Mark all segments as compacted
	err := db.MarkMetricSegsCompactedByKeys(ctx, lrdb.MarkMetricSegsCompactedByKeysParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     segmentIDs,
	})
	require.NoError(t, err)

	// Verify all segments are now marked as compacted and unpublished
	segments, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     segmentIDs,
	})
	require.NoError(t, err)
	require.Len(t, segments, 3)

	// Create a map for easier lookup
	segmentMap := make(map[int64]lrdb.MetricSeg)
	for _, segment := range segments {
		segmentMap[segment.SegmentID] = segment
	}

	// Verify each segment is compacted and unpublished
	for _, segmentID := range segmentIDs {
		segment, exists := segmentMap[segmentID]
		require.True(t, exists, "Segment should exist")
		assert.Equal(t, segmentID, segment.SegmentID)
		assert.True(t, segment.Compacted, "Segment should be marked as compacted")
		assert.False(t, segment.Published, "Segment should be marked as unpublished")
	}
}

func TestMarkMetricSegsCompactedByKeys_OnlyMatchingKeys(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create segments with different frequency_ms values
	segmentID1 := int64(12345) // 5000ms frequency
	segmentID2 := int64(12346) // 10000ms frequency

	err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		SegmentID:      segmentID1,
		InstanceNum:    1,
		StartTs:        now.UnixMilli(),
		EndTs:          now.Add(time.Hour).UnixMilli(),
		RecordCount:    1000,
		FileSize:       50000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{123, 456},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		Compacted:      false,
	})
	require.NoError(t, err)

	err = db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    10000, // Different frequency
		SegmentID:      segmentID2,
		InstanceNum:    1,
		StartTs:        now.UnixMilli(),
		EndTs:          now.Add(time.Hour).UnixMilli(),
		RecordCount:    1000,
		FileSize:       50000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{789, 101},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		Compacted:      false,
	})
	require.NoError(t, err)

	// Mark only the 5000ms frequency segment as compacted
	err = db.MarkMetricSegsCompactedByKeys(ctx, lrdb.MarkMetricSegsCompactedByKeysParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000, // Only matches segmentID1
		InstanceNum:    1,
		SegmentIds:     []int64{segmentID1, segmentID2}, // Both IDs, but only one will match
	})
	require.NoError(t, err)

	// Verify only the matching segment is compacted
	segments5k, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     []int64{segmentID1},
	})
	require.NoError(t, err)
	require.Len(t, segments5k, 1)
	assert.True(t, segments5k[0].Compacted, "5000ms segment should be compacted")
	assert.False(t, segments5k[0].Published, "5000ms segment should be unpublished")

	segments10k, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    10000,
		InstanceNum:    1,
		SegmentIds:     []int64{segmentID2},
	})
	require.NoError(t, err)
	require.Len(t, segments10k, 1)
	assert.False(t, segments10k[0].Compacted, "10000ms segment should not be compacted")
	assert.True(t, segments10k[0].Published, "10000ms segment should still be published")
}

func TestMarkMetricSegsCompactedByKeys_AlreadyCompacted(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create a segment that's already compacted
	segmentID := int64(12345)
	err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		SegmentID:      segmentID,
		InstanceNum:    1,
		StartTs:        now.UnixMilli(),
		EndTs:          now.Add(time.Hour).UnixMilli(),
		RecordCount:    1000,
		FileSize:       50000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      false, // Already unpublished
		Fingerprints:   []int64{123, 456},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		Compacted:      true, // Already compacted
	})
	require.NoError(t, err)

	// Try to mark the already compacted segment as compacted (should be no-op)
	err = db.MarkMetricSegsCompactedByKeys(ctx, lrdb.MarkMetricSegsCompactedByKeysParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     []int64{segmentID},
	})
	require.NoError(t, err) // Should succeed without error

	// Verify segment state remains unchanged
	segments, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     []int64{segmentID},
	})
	require.NoError(t, err)
	require.Len(t, segments, 1)

	segment := segments[0]
	assert.Equal(t, segmentID, segment.SegmentID)
	assert.True(t, segment.Compacted, "Segment should remain compacted")
	assert.False(t, segment.Published, "Segment should remain unpublished")
}

func TestMarkMetricSegsCompactedByKeys_NonExistentSegment(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()

	// Try to mark non-existent segment as compacted (should be no-op)
	err := db.MarkMetricSegsCompactedByKeys(ctx, lrdb.MarkMetricSegsCompactedByKeysParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     []int64{99999}, // Non-existent segment
	})
	require.NoError(t, err) // Should succeed without error (no rows affected)
}

func TestMarkMetricSegsCompactedByKeys_EmptySegmentList(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()

	// Try to mark empty list of segments (should be no-op)
	err := db.MarkMetricSegsCompactedByKeys(ctx, lrdb.MarkMetricSegsCompactedByKeysParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     []int64{}, // Empty list
	})
	require.NoError(t, err) // Should succeed without error
}
