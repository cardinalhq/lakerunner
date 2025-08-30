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

func TestCompactMetricSegs_BasicCompaction(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create some existing metric segments to compact
	oldSegmentIDs := []int64{12345, 12346}
	for _, segmentID := range oldSegmentIDs {
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

	// Test CompactMetricSegs function
	err := db.CompactMetricSegs(ctx, lrdb.ReplaceMetricSegsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      1,
		IngestDateint:  20250830,
		FrequencyMs:    5000,
		Published:      true,
		Rolledup:       false,
		OldRecords: []lrdb.ReplaceMetricSegsOld{
			{SegmentID: oldSegmentIDs[0], SlotID: 0},
			{SegmentID: oldSegmentIDs[1], SlotID: 0},
		},
		NewRecords: []lrdb.ReplaceMetricSegsNew{
			{
				SegmentID:   99999,
				StartTs:     now.UnixMilli(),
				EndTs:       now.Add(time.Hour).UnixMilli(),
				RecordCount: 1500,
				FileSize:    40000,
			},
		},
		CreatedBy:    lrdb.CreatedByIngest,
		Fingerprints: []int64{789, 101112},
		SortVersion:  lrdb.CurrentMetricSortVersion,
	})
	require.NoError(t, err)

	// Verify old segments were marked as compacted and unpublished
	allSegs, err := db.GetMetricSegsForCompaction(ctx, lrdb.GetMetricSegsForCompactionParams{
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

	// Check that we have the right segments
	segmentMap := make(map[int64]lrdb.MetricSeg)
	for _, seg := range allSegs {
		segmentMap[seg.SegmentID] = seg
	}

	// Verify old segments were marked as compacted and unpublished
	for _, segmentID := range oldSegmentIDs {
		seg, exists := segmentMap[segmentID]
		require.True(t, exists, "Old segment should still exist")
		assert.True(t, seg.Compacted, "Old segment should be marked as compacted")
		assert.False(t, seg.Published, "Old segment should be unpublished")
	}

	// Verify new segment was created
	newSeg, exists := segmentMap[99999]
	require.True(t, exists, "New segment should exist")
	assert.False(t, newSeg.Compacted, "New segment should not be compacted")
	assert.True(t, newSeg.Published, "New segment should be published")
	assert.Equal(t, int64(1500), newSeg.RecordCount)
	assert.Equal(t, int64(40000), newSeg.FileSize)
	assert.Equal(t, []int64{789, 101112}, newSeg.Fingerprints)
}

func TestCompactMetricSegs_EmptyOldRecords(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Test with no old records to compact - just inserting new segments
	err := db.CompactMetricSegs(ctx, lrdb.ReplaceMetricSegsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      1,
		IngestDateint:  20250830,
		FrequencyMs:    5000,
		Published:      true,
		Rolledup:       false,
		OldRecords:     []lrdb.ReplaceMetricSegsOld{}, // Empty
		NewRecords: []lrdb.ReplaceMetricSegsNew{
			{
				SegmentID:   88888,
				StartTs:     now.UnixMilli(),
				EndTs:       now.Add(time.Hour).UnixMilli(),
				RecordCount: 500,
				FileSize:    25000,
			},
		},
		CreatedBy:    lrdb.CreatedByIngest,
		Fingerprints: []int64{111, 222},
		SortVersion:  lrdb.CurrentMetricSortVersion,
	})
	require.NoError(t, err)

	// Verify new segment was created
	allSegs, err := db.GetMetricSegsForCompaction(ctx, lrdb.GetMetricSegsForCompactionParams{
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
	require.Len(t, allSegs, 1)

	newSeg := allSegs[0]
	assert.Equal(t, int64(88888), newSeg.SegmentID)
	assert.False(t, newSeg.Compacted, "New segment should not be compacted")
	assert.True(t, newSeg.Published, "New segment should be published")
	assert.Equal(t, int64(500), newSeg.RecordCount)
	assert.Equal(t, int64(25000), newSeg.FileSize)
	assert.Equal(t, []int64{111, 222}, newSeg.Fingerprints)
}

func TestCompactMetricSegs_EmptyNewRecords(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create existing metric segments
	oldSegmentIDs := []int64{54321, 54322}
	for _, segmentID := range oldSegmentIDs {
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
			RecordCount:    800,
			FileSize:       35000,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Fingerprints:   []int64{333, 444},
			SortVersion:    lrdb.CurrentMetricSortVersion,
			SlotCount:      1,
			Compacted:      false,
		})
		require.NoError(t, err)
	}

	// Test with no new records - just marking old as compacted
	err := db.CompactMetricSegs(ctx, lrdb.ReplaceMetricSegsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      1,
		IngestDateint:  20250830,
		FrequencyMs:    5000,
		Published:      true,
		Rolledup:       false,
		OldRecords: []lrdb.ReplaceMetricSegsOld{
			{SegmentID: oldSegmentIDs[0], SlotID: 0},
			{SegmentID: oldSegmentIDs[1], SlotID: 0},
		},
		NewRecords:   []lrdb.ReplaceMetricSegsNew{}, // Empty
		CreatedBy:    lrdb.CreatedByIngest,
		Fingerprints: []int64{},
		SortVersion:  lrdb.CurrentMetricSortVersion,
	})
	require.NoError(t, err)

	// Verify old segments were marked as compacted and unpublished
	allSegs, err := db.GetMetricSegsForCompaction(ctx, lrdb.GetMetricSegsForCompactionParams{
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

	// Check that old segments are marked compacted
	segmentMap := make(map[int64]lrdb.MetricSeg)
	for _, seg := range allSegs {
		segmentMap[seg.SegmentID] = seg
	}

	for _, segmentID := range oldSegmentIDs {
		seg, exists := segmentMap[segmentID]
		require.True(t, exists, "Old segment should still exist")
		assert.True(t, seg.Compacted, "Old segment should be marked as compacted")
		assert.False(t, seg.Published, "Old segment should be unpublished")
	}
}

func TestCompactMetricSegs_MultipleNewSegments(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create some existing metric segments to compact
	oldSegmentIDs := []int64{11111, 22222, 33333}
	for _, segmentID := range oldSegmentIDs {
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
			RecordCount:    500,
			FileSize:       25000,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Fingerprints:   []int64{100, 200},
			SortVersion:    lrdb.CurrentMetricSortVersion,
			SlotCount:      1,
			Compacted:      false,
		})
		require.NoError(t, err)
	}

	// Test CompactMetricSegs with multiple new segments
	err := db.CompactMetricSegs(ctx, lrdb.ReplaceMetricSegsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      1,
		IngestDateint:  20250830,
		FrequencyMs:    5000,
		Published:      true,
		Rolledup:       false,
		OldRecords: []lrdb.ReplaceMetricSegsOld{
			{SegmentID: oldSegmentIDs[0], SlotID: 0},
			{SegmentID: oldSegmentIDs[1], SlotID: 0},
			{SegmentID: oldSegmentIDs[2], SlotID: 0},
		},
		NewRecords: []lrdb.ReplaceMetricSegsNew{
			{
				SegmentID:   77777,
				StartTs:     now.UnixMilli(),
				EndTs:       now.Add(30 * time.Minute).UnixMilli(),
				RecordCount: 800,
				FileSize:    40000,
			},
			{
				SegmentID:   88888,
				StartTs:     now.Add(30 * time.Minute).UnixMilli(),
				EndTs:       now.Add(time.Hour).UnixMilli(),
				RecordCount: 700,
				FileSize:    35000,
			},
		},
		CreatedBy:    lrdb.CreatedByIngest,
		Fingerprints: []int64{777, 888},
		SortVersion:  lrdb.CurrentMetricSortVersion,
	})
	require.NoError(t, err)

	// Verify results
	allSegs, err := db.GetMetricSegsForCompaction(ctx, lrdb.GetMetricSegsForCompactionParams{
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

	segmentMap := make(map[int64]lrdb.MetricSeg)
	for _, seg := range allSegs {
		segmentMap[seg.SegmentID] = seg
	}

	// Verify old segments were compacted
	for _, segmentID := range oldSegmentIDs {
		seg, exists := segmentMap[segmentID]
		require.True(t, exists)
		assert.True(t, seg.Compacted, "Old segment should be compacted")
		assert.False(t, seg.Published, "Old segment should be unpublished")
	}

	// Verify new segments were created
	newSegmentIDs := []int64{77777, 88888}
	for _, segmentID := range newSegmentIDs {
		seg, exists := segmentMap[segmentID]
		require.True(t, exists, "New segment should exist")
		assert.False(t, seg.Compacted, "New segment should not be compacted")
		assert.True(t, seg.Published, "New segment should be published")
		assert.Equal(t, []int64{777, 888}, seg.Fingerprints)
	}

	// Verify record counts
	assert.Equal(t, int64(800), segmentMap[77777].RecordCount)
	assert.Equal(t, int64(700), segmentMap[88888].RecordCount)
}

func TestCompactMetricSegs_IdempotentReplays(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Test that duplicate inserts are handled gracefully (ON CONFLICT DO NOTHING)
	params := lrdb.ReplaceMetricSegsParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      1,
		IngestDateint:  20250830,
		FrequencyMs:    5000,
		Published:      true,
		Rolledup:       false,
		OldRecords:     []lrdb.ReplaceMetricSegsOld{},
		NewRecords: []lrdb.ReplaceMetricSegsNew{
			{
				SegmentID:   66666,
				StartTs:     now.UnixMilli(),
				EndTs:       now.Add(time.Hour).UnixMilli(),
				RecordCount: 300,
				FileSize:    15000,
			},
		},
		CreatedBy:    lrdb.CreatedByIngest,
		Fingerprints: []int64{666, 999},
		SortVersion:  lrdb.CurrentMetricSortVersion,
	}

	// First call should succeed
	err := db.CompactMetricSegs(ctx, params)
	require.NoError(t, err)

	// Second call with same data should also succeed (idempotent)
	err = db.CompactMetricSegs(ctx, params)
	require.NoError(t, err)

	// Verify only one segment exists
	allSegs, err := db.GetMetricSegsForCompaction(ctx, lrdb.GetMetricSegsForCompactionParams{
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
	require.Len(t, allSegs, 1)

	seg := allSegs[0]
	assert.Equal(t, int64(66666), seg.SegmentID)
	assert.Equal(t, int64(300), seg.RecordCount)
	assert.Equal(t, []int64{666, 999}, seg.Fingerprints)
}
