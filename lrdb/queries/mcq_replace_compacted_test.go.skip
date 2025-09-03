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

func TestSetMetricSegCompacted_SingleFile(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create a single metric segment that should only be marked compacted (not unpublished)
	segmentID := int64(12345)
	err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		IngestDateint:  20250830,
		FrequencyMs:    5000,
		SegmentID:      segmentID,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli(),
		EndTs:          now.Add(time.Hour).UnixMilli(),
		RecordCount:    1000,
		FileSize:       50000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true, // Should remain true after compaction
		Fingerprints:   []int64{123, 456},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		SlotCount:      1,
		Compacted:      false, // Should become true
	})
	require.NoError(t, err)

	// Mark the segment as compacted using SetMetricSegCompacted (single-file case)
	err = db.SetMetricSegCompacted(ctx, lrdb.SetMetricSegCompactedParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     []int64{segmentID},
	})
	require.NoError(t, err)

	// Verify the segment is marked as compacted but still published
	segments, err := db.GetMetricSegsForCompaction(ctx, lrdb.GetMetricSegsForCompactionParams{
		OrganizationID:  orgID,
		Dateint:         20250830,
		FrequencyMs:     5000,
		InstanceNum:     1,
		SlotID:          0,
		StartTs:         now.UnixMilli() - 1000,
		EndTs:           now.Add(2 * time.Hour).UnixMilli(),
		MaxFileSize:     1000000,
		CursorCreatedAt: time.Time{},
		CursorSegmentID: 0,
		Maxrows:         100,
	})
	require.NoError(t, err)
	require.Len(t, segments, 1)

	segment := segments[0]
	assert.Equal(t, segmentID, segment.SegmentID)
	assert.True(t, segment.Compacted, "Single-file segment should be marked as compacted")
	assert.True(t, segment.Published, "Single-file segment should remain published (available for queries)")
	assert.Equal(t, int64(1000), segment.RecordCount)
	assert.Equal(t, int64(50000), segment.FileSize)
}

func TestSetMetricSegCompacted_MultipleSegments(t *testing.T) {
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
			IngestDateint:  20250830,
			FrequencyMs:    5000,
			SegmentID:      segmentID,
			InstanceNum:    1,
			SlotID:         0,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(time.Hour).UnixMilli(),
			RecordCount:    1000,
			FileSize:       50000,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Fingerprints:   []int64{123, 456},
			SortVersion:    lrdb.CurrentMetricSortVersion,
			SlotCount:      1,
			Compacted:      false,
		})
		require.NoError(t, err)
	}

	// Mark all segments as compacted using SetMetricSegCompacted
	err := db.SetMetricSegCompacted(ctx, lrdb.SetMetricSegCompactedParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     segmentIDs,
	})
	require.NoError(t, err)

	// Verify all segments are marked as compacted but still published
	segments, err := db.GetMetricSegsForCompaction(ctx, lrdb.GetMetricSegsForCompactionParams{
		OrganizationID:  orgID,
		Dateint:         20250830,
		FrequencyMs:     5000,
		InstanceNum:     1,
		SlotID:          0,
		StartTs:         now.UnixMilli() - 1000,
		EndTs:           now.Add(2 * time.Hour).UnixMilli(),
		MaxFileSize:     1000000,
		CursorCreatedAt: time.Time{},
		CursorSegmentID: 0,
		Maxrows:         100,
	})
	require.NoError(t, err)
	require.Len(t, segments, 3)

	// Create a map for easier lookup
	segmentMap := make(map[int64]lrdb.MetricSeg)
	for _, segment := range segments {
		segmentMap[segment.SegmentID] = segment
	}

	// Verify each segment is compacted but still published
	for _, segmentID := range segmentIDs {
		segment, exists := segmentMap[segmentID]
		require.True(t, exists, "Segment should exist")
		assert.Equal(t, segmentID, segment.SegmentID)
		assert.True(t, segment.Compacted, "Segment should be marked as compacted")
		assert.True(t, segment.Published, "Segment should remain published (SetMetricSegCompacted doesn't change published status)")
	}
}

func TestSetMetricSegCompacted_AlreadyCompacted(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create a segment that's already compacted
	segmentID := int64(12345)
	err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		IngestDateint:  20250830,
		FrequencyMs:    5000,
		SegmentID:      segmentID,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli(),
		EndTs:          now.Add(time.Hour).UnixMilli(),
		RecordCount:    1000,
		FileSize:       50000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{123, 456},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		SlotCount:      1,
		Compacted:      true, // Already compacted
	})
	require.NoError(t, err)

	// Try to mark the already compacted segment as compacted (should be no-op due to WHERE compacted = false)
	err = db.SetMetricSegCompacted(ctx, lrdb.SetMetricSegCompactedParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     []int64{segmentID},
	})
	require.NoError(t, err) // Should succeed without error

	// Verify segment state remains unchanged
	segments, err := db.GetMetricSegsForCompaction(ctx, lrdb.GetMetricSegsForCompactionParams{
		OrganizationID:  orgID,
		Dateint:         20250830,
		FrequencyMs:     5000,
		InstanceNum:     1,
		SlotID:          0,
		StartTs:         now.UnixMilli() - 1000,
		EndTs:           now.Add(2 * time.Hour).UnixMilli(),
		MaxFileSize:     1000000,
		CursorCreatedAt: time.Time{},
		CursorSegmentID: 0,
		Maxrows:         100,
	})
	require.NoError(t, err)
	require.Len(t, segments, 1)

	segment := segments[0]
	assert.Equal(t, segmentID, segment.SegmentID)
	assert.True(t, segment.Compacted, "Segment should remain compacted")
	assert.True(t, segment.Published, "Segment should remain published")
}

func TestSetMetricSegCompacted_CompareWithMarkCompacted(t *testing.T) {
	// This test demonstrates the difference between SetMetricSegCompacted and MarkMetricSegsCompactedByKeys
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create two identical segments
	segmentID1 := int64(12345)
	segmentID2 := int64(12346)

	for _, segmentID := range []int64{segmentID1, segmentID2} {
		err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
			OrganizationID: orgID,
			Dateint:        20250830,
			IngestDateint:  20250830,
			FrequencyMs:    5000,
			SegmentID:      segmentID,
			InstanceNum:    1,
			SlotID:         0,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(time.Hour).UnixMilli(),
			RecordCount:    1000,
			FileSize:       50000,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Fingerprints:   []int64{123, 456},
			SortVersion:    lrdb.CurrentMetricSortVersion,
			SlotCount:      1,
			Compacted:      false,
		})
		require.NoError(t, err)
	}

	// Use SetMetricSegCompacted on first segment (single-file case)
	err := db.SetMetricSegCompacted(ctx, lrdb.SetMetricSegCompactedParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     []int64{segmentID1},
	})
	require.NoError(t, err)

	// Use MarkMetricSegsCompactedByKeys on second segment (multi-file merge case)
	err = db.MarkMetricSegsCompactedByKeys(ctx, lrdb.MarkMetricSegsCompactedByKeysParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     []int64{segmentID2},
	})
	require.NoError(t, err)

	// Verify the different behaviors
	segments, err := db.GetMetricSegsForCompaction(ctx, lrdb.GetMetricSegsForCompactionParams{
		OrganizationID:  orgID,
		Dateint:         20250830,
		FrequencyMs:     5000,
		InstanceNum:     1,
		SlotID:          0,
		StartTs:         now.UnixMilli() - 1000,
		EndTs:           now.Add(2 * time.Hour).UnixMilli(),
		MaxFileSize:     1000000,
		CursorCreatedAt: time.Time{},
		CursorSegmentID: 0,
		Maxrows:         100,
	})
	require.NoError(t, err)
	require.Len(t, segments, 2)

	segmentMap := make(map[int64]lrdb.MetricSeg)
	for _, segment := range segments {
		segmentMap[segment.SegmentID] = segment
	}

	// Segment 1: SetMetricSegCompacted keeps published=true
	segment1 := segmentMap[segmentID1]
	assert.True(t, segment1.Compacted, "Segment1 should be compacted")
	assert.True(t, segment1.Published, "Segment1 should remain published (single-file compaction)")

	// Segment 2: MarkMetricSegsCompactedByKeys sets published=false
	segment2 := segmentMap[segmentID2]
	assert.True(t, segment2.Compacted, "Segment2 should be compacted")
	assert.False(t, segment2.Published, "Segment2 should be unpublished (multi-file merge compaction)")
}
