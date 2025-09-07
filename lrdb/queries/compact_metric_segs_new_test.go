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

// TestCompactMetricSegs_BasicReplacement tests the core compaction operation:
// replacing multiple old segments with a single new compacted segment
func TestCompactMetricSegs_BasicReplacement(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Step 1: Create old segments that will be compacted
	oldSegmentIDs := []int64{12345, 12346}
	for i, segmentID := range oldSegmentIDs {
		err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
			OrganizationID: orgID,
			Dateint:        20250830,
			IngestDateint:  20250830,
			FrequencyMs:    5000,
			SegmentID:      segmentID,
			InstanceNum:    1,
			SlotID:         0,
			StartTs:        now.Add(time.Duration(i) * time.Minute).UnixMilli(),
			EndTs:          now.Add(time.Duration(i+1) * time.Minute).UnixMilli(),
			RecordCount:    1000,
			FileSize:       50000,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{100 + int64(i), 200 + int64(i)},
			SortVersion:    lrdb.CurrentMetricSortVersion,
			SlotCount:      1,
			Compacted:      false,
		})
		require.NoError(t, err)
	}

	// Step 2: Perform compaction - replace old segments with new one
	newSegmentID := int64(99999)
	err := db.CompactMetricSegs(ctx, lrdb.CompactMetricSegsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      1,
		IngestDateint:  20250830,
		FrequencyMs:    5000,
		OldRecords: []lrdb.CompactMetricSegsOld{
			{SegmentID: oldSegmentIDs[0], SlotID: 0},
			{SegmentID: oldSegmentIDs[1], SlotID: 0},
		},
		NewRecords: []lrdb.CompactMetricSegsNew{
			{
				SegmentID:    newSegmentID,
				StartTs:      now.UnixMilli(),
				EndTs:        now.Add(2 * time.Minute).UnixMilli(),
				RecordCount:  1900, // Slightly less due to deduplication
				FileSize:     45000,
				Fingerprints: []int64{100, 101, 200, 201}, // Combined fingerprints
			},
		},
		CreatedBy: lrdb.CreatedByCompact,
	})
	require.NoError(t, err)

	// Step 3: Verify the state using GetMetricSegsByIds
	// Check old segments are marked as compacted and unpublished
	oldSegs, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     oldSegmentIDs,
	})
	require.NoError(t, err)
	require.Len(t, oldSegs, 2)

	for _, seg := range oldSegs {
		assert.True(t, seg.Compacted, "Old segment %d should be marked as compacted", seg.SegmentID)
		assert.False(t, seg.Published, "Old segment %d should be unpublished", seg.SegmentID)
		assert.Equal(t, lrdb.CreatedByIngest, seg.CreatedBy, "Old segment should retain original creator")
	}

	// Check new segment exists and is properly configured
	newSegs, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     []int64{newSegmentID},
	})
	require.NoError(t, err)
	require.Len(t, newSegs, 1)

	newSeg := newSegs[0]
	assert.Equal(t, newSegmentID, newSeg.SegmentID)
	assert.True(t, newSeg.Published, "New segment should be published")
	assert.False(t, newSeg.Compacted, "New segment should not be marked as compacted")
	assert.False(t, newSeg.Rolledup, "New segment should not be rolled up")
	assert.Equal(t, lrdb.CreatedByCompact, newSeg.CreatedBy)
	assert.Equal(t, int64(1900), newSeg.RecordCount)
	assert.Equal(t, int64(45000), newSeg.FileSize)
	assert.Equal(t, []int64{100, 101, 200, 201}, newSeg.Fingerprints)
}

// TestCompactMetricSegs_InitialInsert tests inserting new segments
// without any old segments to replace (empty old records)
func TestCompactMetricSegs_InitialInsert(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Insert a new segment without replacing any old ones
	newSegmentID := int64(88888)
	err := db.CompactMetricSegs(ctx, lrdb.CompactMetricSegsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      1,
		IngestDateint:  20250830,
		FrequencyMs:    5000,
		OldRecords:     []lrdb.CompactMetricSegsOld{}, // Empty - no segments to replace
		NewRecords: []lrdb.CompactMetricSegsNew{
			{
				SegmentID:    newSegmentID,
				StartTs:      now.UnixMilli(),
				EndTs:        now.Add(time.Hour).UnixMilli(),
				RecordCount:  5000,
				FileSize:     100000,
				Fingerprints: []int64{1, 2, 3},
			},
		},
		CreatedBy: lrdb.CreatedByIngest,
	})
	require.NoError(t, err)

	// Verify the new segment was created
	segs, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     []int64{newSegmentID},
	})
	require.NoError(t, err)
	require.Len(t, segs, 1)

	seg := segs[0]
	assert.Equal(t, newSegmentID, seg.SegmentID)
	assert.True(t, seg.Published)
	assert.False(t, seg.Compacted)
	assert.Equal(t, int64(5000), seg.RecordCount)
}

// TestCompactMetricSegs_DeletionOnly tests removing segments
// without adding new ones (empty new records)
func TestCompactMetricSegs_DeletionOnly(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create segments to delete
	segmentIDs := []int64{77777, 77778}
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
			Fingerprints:   []int64{},
			SortVersion:    lrdb.CurrentMetricSortVersion,
			SlotCount:      1,
			Compacted:      false,
		})
		require.NoError(t, err)
	}

	// Mark segments as compacted/unpublished without adding new ones
	err := db.CompactMetricSegs(ctx, lrdb.CompactMetricSegsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      1,
		IngestDateint:  20250830,
		FrequencyMs:    5000,
		OldRecords: []lrdb.CompactMetricSegsOld{
			{SegmentID: segmentIDs[0], SlotID: 0},
			{SegmentID: segmentIDs[1], SlotID: 0},
		},
		NewRecords:  []lrdb.CompactMetricSegsNew{}, // Empty - no new segments
		CreatedBy: lrdb.CreatedByIngest,
	})
	require.NoError(t, err)

	// Verify segments are marked as compacted and unpublished
	segs, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     segmentIDs,
	})
	require.NoError(t, err)
	require.Len(t, segs, 2)

	for _, seg := range segs {
		assert.True(t, seg.Compacted, "Segment %d should be compacted", seg.SegmentID)
		assert.False(t, seg.Published, "Segment %d should be unpublished", seg.SegmentID)
	}
}

// TestCompactMetricSegs_MultipleNewSegments tests replacing old segments
// with multiple new segments (split scenario)
func TestCompactMetricSegs_MultipleNewSegments(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create large segments to be split
	oldSegmentIDs := []int64{66666, 66667, 66668}
	for i, segmentID := range oldSegmentIDs {
		err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
			OrganizationID: orgID,
			Dateint:        20250830,
			IngestDateint:  20250830,
			FrequencyMs:    5000,
			SegmentID:      segmentID,
			InstanceNum:    1,
			SlotID:         0,
			StartTs:        now.Add(time.Duration(i) * time.Hour).UnixMilli(),
			EndTs:          now.Add(time.Duration(i+1) * time.Hour).UnixMilli(),
			RecordCount:    10000,
			FileSize:       500000,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{int64(i * 100), int64(i*100 + 1)},
			SortVersion:    lrdb.CurrentMetricSortVersion,
			SlotCount:      1,
			Compacted:      false,
		})
		require.NoError(t, err)
	}

	// Replace with multiple smaller segments (split for size optimization)
	newSegmentIDs := []int64{55555, 55556}
	err := db.CompactMetricSegs(ctx, lrdb.CompactMetricSegsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      1,
		IngestDateint:  20250830,
		FrequencyMs:    5000,
		OldRecords: []lrdb.CompactMetricSegsOld{
			{SegmentID: oldSegmentIDs[0], SlotID: 0},
			{SegmentID: oldSegmentIDs[1], SlotID: 0},
			{SegmentID: oldSegmentIDs[2], SlotID: 0},
		},
		NewRecords: []lrdb.CompactMetricSegsNew{
			{
				SegmentID:    newSegmentIDs[0],
				StartTs:      now.UnixMilli(),
				EndTs:        now.Add(90 * time.Minute).UnixMilli(),
				RecordCount:  15000,
				FileSize:     400000,
				Fingerprints: []int64{0, 1, 100, 101},
			},
			{
				SegmentID:    newSegmentIDs[1],
				StartTs:      now.Add(90 * time.Minute).UnixMilli(),
				EndTs:        now.Add(3 * time.Hour).UnixMilli(),
				RecordCount:  14000,
				FileSize:     380000,
				Fingerprints: []int64{200, 201},
			},
		},
		CreatedBy: lrdb.CreatedByCompact,
	})
	require.NoError(t, err)

	// Verify old segments are compacted and unpublished
	oldSegs, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     oldSegmentIDs,
	})
	require.NoError(t, err)
	require.Len(t, oldSegs, 3)

	for _, seg := range oldSegs {
		assert.True(t, seg.Compacted)
		assert.False(t, seg.Published)
	}

	// Verify new segments exist and are properly configured
	newSegs, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     newSegmentIDs,
	})
	require.NoError(t, err)
	require.Len(t, newSegs, 2)

	totalRecords := int64(0)
	for _, seg := range newSegs {
		assert.True(t, seg.Published)
		assert.False(t, seg.Compacted)
		assert.Equal(t, lrdb.CreatedByCompact, seg.CreatedBy)
		totalRecords += seg.RecordCount
	}
	assert.Equal(t, int64(29000), totalRecords, "Total records should be preserved (minus deduplication)")
}

// TestCompactMetricSegs_IdempotentOperation tests that compaction operations
// can be safely replayed without causing issues
func TestCompactMetricSegs_IdempotentOperation(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create initial segments
	oldSegmentIDs := []int64{44444, 44445}
	for i, segmentID := range oldSegmentIDs {
		err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
			OrganizationID: orgID,
			Dateint:        20250830,
			IngestDateint:  20250830,
			FrequencyMs:    5000,
			SegmentID:      segmentID,
			InstanceNum:    1,
			SlotID:         0,
			StartTs:        now.Add(time.Duration(i) * time.Minute).UnixMilli(),
			EndTs:          now.Add(time.Duration(i+1) * time.Minute).UnixMilli(),
			RecordCount:    1000,
			FileSize:       50000,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{},
			SortVersion:    lrdb.CurrentMetricSortVersion,
			SlotCount:      1,
			Compacted:      false,
		})
		require.NoError(t, err)
	}

	newSegmentID := int64(33333)
	compactionParams := lrdb.CompactMetricSegsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      1,
		IngestDateint:  20250830,
		FrequencyMs:    5000,
		OldRecords: []lrdb.CompactMetricSegsOld{
			{SegmentID: oldSegmentIDs[0], SlotID: 0},
			{SegmentID: oldSegmentIDs[1], SlotID: 0},
		},
		NewRecords: []lrdb.CompactMetricSegsNew{
			{
				SegmentID:    newSegmentID,
				StartTs:      now.UnixMilli(),
				EndTs:        now.Add(2 * time.Minute).UnixMilli(),
				RecordCount:  1900,
				FileSize:     45000,
				Fingerprints: []int64{},
			},
		},
		CreatedBy: lrdb.CreatedByCompact,
	}

	// First compaction
	err := db.CompactMetricSegs(ctx, compactionParams)
	require.NoError(t, err)

	// Verify state after first compaction
	allSegmentIDs := append(oldSegmentIDs, newSegmentID)
	segs1, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     allSegmentIDs,
	})
	require.NoError(t, err)
	require.Len(t, segs1, 3)

	// Create a map of the first state
	firstState := make(map[int64]lrdb.MetricSeg)
	for _, seg := range segs1 {
		firstState[seg.SegmentID] = seg
	}

	// Replay the same compaction (idempotent operation)
	err = db.CompactMetricSegs(ctx, compactionParams)
	require.NoError(t, err)

	// Verify state remains the same after replay
	segs2, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     allSegmentIDs,
	})
	require.NoError(t, err)
	require.Len(t, segs2, 3)

	// Verify each segment's state is identical
	for _, seg := range segs2 {
		original := firstState[seg.SegmentID]
		assert.Equal(t, original.Compacted, seg.Compacted, "Segment %d compacted state should be unchanged", seg.SegmentID)
		assert.Equal(t, original.Published, seg.Published, "Segment %d published state should be unchanged", seg.SegmentID)
		assert.Equal(t, original.RecordCount, seg.RecordCount, "Segment %d record count should be unchanged", seg.SegmentID)
		assert.Equal(t, original.FileSize, seg.FileSize, "Segment %d file size should be unchanged", seg.SegmentID)
	}
}

// TestCompactMetricSegs_CrossFrequencyIsolation tests that compaction
// only affects segments with matching frequency
func TestCompactMetricSegs_CrossFrequencyIsolation(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create segments at different frequencies
	seg5000 := int64(22222)
	seg10000 := int64(22223)

	err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		IngestDateint:  20250830,
		FrequencyMs:    5000, // 5 second frequency
		SegmentID:      seg5000,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli(),
		EndTs:          now.Add(time.Minute).UnixMilli(),
		RecordCount:    1000,
		FileSize:       50000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		SlotCount:      1,
		Compacted:      false,
	})
	require.NoError(t, err)

	err = db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		IngestDateint:  20250830,
		FrequencyMs:    10000, // 10 second frequency
		SegmentID:      seg10000,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli(),
		EndTs:          now.Add(time.Minute).UnixMilli(),
		RecordCount:    2000,
		FileSize:       100000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		SlotCount:      1,
		Compacted:      false,
	})
	require.NoError(t, err)

	// Compact only the 5000ms segment
	newSegmentID := int64(11111)
	err = db.CompactMetricSegs(ctx, lrdb.CompactMetricSegsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      1,
		IngestDateint:  20250830,
		FrequencyMs:    5000, // Only affects 5000ms segments
		OldRecords: []lrdb.CompactMetricSegsOld{
			{SegmentID: seg5000, SlotID: 0},
		},
		NewRecords: []lrdb.CompactMetricSegsNew{
			{
				SegmentID:    newSegmentID,
				StartTs:      now.UnixMilli(),
				EndTs:        now.Add(time.Minute).UnixMilli(),
				RecordCount:  950,
				FileSize:     45000,
				Fingerprints: []int64{},
			},
		},
		CreatedBy: lrdb.CreatedByCompact,
	})
	require.NoError(t, err)

	// Verify 5000ms segment was compacted
	seg5000Data, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     []int64{seg5000},
	})
	require.NoError(t, err)
	require.Len(t, seg5000Data, 1)
	assert.True(t, seg5000Data[0].Compacted)
	assert.False(t, seg5000Data[0].Published)

	// Verify 10000ms segment was NOT affected
	seg10000Data, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    10000,
		InstanceNum:    1,
		SegmentIds:     []int64{seg10000},
	})
	require.NoError(t, err)
	require.Len(t, seg10000Data, 1)
	assert.False(t, seg10000Data[0].Compacted, "10000ms segment should not be compacted")
	assert.True(t, seg10000Data[0].Published, "10000ms segment should remain published")
	assert.Equal(t, int64(2000), seg10000Data[0].RecordCount, "10000ms segment should be unchanged")
}
