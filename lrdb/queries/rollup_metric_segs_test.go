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

func TestRollupMetricSegs_BasicRollup(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create source segments at 10s frequency that will be rolled up
	sourceSegmentIDs := []int64{10001, 10002}
	for _, segmentID := range sourceSegmentIDs {
		err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
			OrganizationID: orgID,
			Dateint:        20250830,
			IngestDateint:  20250830,
			FrequencyMs:    10000, // 10 seconds - source frequency
			SegmentID:      segmentID,
			InstanceNum:    1,
			SlotID:         0,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(10 * time.Second).UnixMilli(),
			RecordCount:    600,
			FileSize:       30000,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Fingerprints:   []int64{100 + segmentID, 200 + segmentID},
			SortVersion:    lrdb.CurrentMetricSortVersion,
			SlotCount:      1,
			Compacted:      false,
		})
		require.NoError(t, err)
	}

	// Create existing target segment at 1min frequency to replace
	existingTargetSegmentID := int64(20001)
	err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		IngestDateint:  20250830,
		FrequencyMs:    60000, // 1 minute - target frequency
		SegmentID:      existingTargetSegmentID,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli(),
		EndTs:          now.Add(time.Minute).UnixMilli(),
		RecordCount:    100,
		FileSize:       5000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{777},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		SlotCount:      1,
		Compacted:      false,
	})
	require.NoError(t, err)

	// Perform rollup operation using RollupMetricSegs
	err = db.RollupMetricSegs(ctx, lrdb.RollupMetricSegsParams{
		SourceSegments: struct {
			OrganizationID uuid.UUID
			Dateint        int32
			FrequencyMs    int32
			InstanceNum    int16
			SlotID         int32
			SegmentIDs     []int64
		}{
			OrganizationID: orgID,
			Dateint:        20250830,
			FrequencyMs:    10000, // Source frequency
			InstanceNum:    1,
			SlotID:         0,
			SegmentIDs:     sourceSegmentIDs,
		},
		TargetReplacement: lrdb.ReplaceMetricSegsParams{
			OrganizationID: orgID,
			Dateint:        20250830,
			InstanceNum:    1,
			SlotID:         0,
			SlotCount:      1,
			IngestDateint:  20250830,
			FrequencyMs:    60000, // Target frequency
			Published:      true,
			Rolledup:       false, // New rolled-up segments are not themselves rolled up
			CreatedBy:      lrdb.CreateByRollup,
			SortVersion:    lrdb.CurrentMetricSortVersion,
			OldRecords: []lrdb.ReplaceMetricSegsOld{
				{SegmentID: existingTargetSegmentID, SlotID: 0},
			},
			NewRecords: []lrdb.ReplaceMetricSegsNew{
				{
					SegmentID:    30001,
					StartTs:      now.UnixMilli(),
					EndTs:        now.Add(time.Minute).UnixMilli(),
					RecordCount:  120, // Rolled up from 600 + 600 = 1200 to 120
					FileSize:     12000,
					Fingerprints: []int64{9001, 9002},
				},
			},
		},
	})
	require.NoError(t, err)

	// Verify source segments were marked as rolled up (but still exist)
	sourceSegs, err := db.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    10000,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli() - 1000,
		EndTs:          now.Add(2 * time.Minute).UnixMilli(),
	})
	require.NoError(t, err)

	sourceSegMap := make(map[int64]lrdb.MetricSeg)
	for _, seg := range sourceSegs {
		sourceSegMap[seg.SegmentID] = seg
	}

	for _, segmentID := range sourceSegmentIDs {
		seg, exists := sourceSegMap[segmentID]
		require.True(t, exists, "Source segment should still exist")
		assert.True(t, seg.Rolledup, "Source segment should be marked as rolled up")
		assert.True(t, seg.Published, "Source segment should remain published") // Unlike compaction
		assert.False(t, seg.Compacted, "Source segment should not be compacted")
	}

	// Verify target segments were replaced
	targetSegs, err := db.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    60000,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli() - 1000,
		EndTs:          now.Add(2 * time.Minute).UnixMilli(),
	})
	require.NoError(t, err)

	targetSegMap := make(map[int64]lrdb.MetricSeg)
	for _, seg := range targetSegs {
		targetSegMap[seg.SegmentID] = seg
	}

	// Old target segment should be deleted (not just marked)
	_, oldExists := targetSegMap[existingTargetSegmentID]
	assert.False(t, oldExists, "Old target segment should be deleted")

	// New target segment should exist
	newSeg, newExists := targetSegMap[30001]
	require.True(t, newExists, "New target segment should exist")
	assert.False(t, newSeg.Rolledup, "New target segment should not be rolled up")
	assert.True(t, newSeg.Published, "New target segment should be published")
	assert.False(t, newSeg.Compacted, "New target segment should not be compacted")
	assert.Equal(t, int64(120), newSeg.RecordCount)
	assert.Equal(t, int64(12000), newSeg.FileSize)
	assert.Equal(t, []int64{9001, 9002}, newSeg.Fingerprints)
	assert.Equal(t, lrdb.CreateByRollup, newSeg.CreatedBy)
}

func TestRollupMetricSegs_EmptySourceSegments(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create existing target segment to replace
	existingTargetID := int64(40001)
	err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		IngestDateint:  20250830,
		FrequencyMs:    60000,
		SegmentID:      existingTargetID,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli(),
		EndTs:          now.Add(time.Minute).UnixMilli(),
		RecordCount:    100,
		FileSize:       5000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{888},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		SlotCount:      1,
	})
	require.NoError(t, err)

	// Test rollup with no source segments to mark (just replace target)
	err = db.RollupMetricSegs(ctx, lrdb.RollupMetricSegsParams{
		SourceSegments: struct {
			OrganizationID uuid.UUID
			Dateint        int32
			FrequencyMs    int32
			InstanceNum    int16
			SlotID         int32
			SegmentIDs     []int64
		}{
			OrganizationID: orgID,
			Dateint:        20250830,
			FrequencyMs:    10000,
			InstanceNum:    1,
			SlotID:         0,
			SegmentIDs:     []int64{}, // Empty source segments
		},
		TargetReplacement: lrdb.ReplaceMetricSegsParams{
			OrganizationID: orgID,
			Dateint:        20250830,
			InstanceNum:    1,
			SlotID:         0,
			SlotCount:      1,
			IngestDateint:  20250830,
			FrequencyMs:    60000,
			Published:      true,
			CreatedBy:      lrdb.CreateByRollup,
			SortVersion:    lrdb.CurrentMetricSortVersion,
			OldRecords: []lrdb.ReplaceMetricSegsOld{
				{SegmentID: existingTargetID, SlotID: 0},
			},
			NewRecords: []lrdb.ReplaceMetricSegsNew{
				{
					SegmentID:    50001,
					StartTs:      now.UnixMilli(),
					EndTs:        now.Add(time.Minute).UnixMilli(),
					RecordCount:  200,
					FileSize:     10000,
					Fingerprints: []int64{5001, 5002},
				},
			},
		},
	})
	require.NoError(t, err)

	// Verify target was replaced
	segs, err := db.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    60000,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli() - 1000,
		EndTs:          now.Add(2 * time.Minute).UnixMilli(),
	})
	require.NoError(t, err)
	require.Len(t, segs, 1)

	newSeg := segs[0]
	assert.Equal(t, int64(50001), newSeg.SegmentID)
	assert.Equal(t, int64(200), newSeg.RecordCount)
	assert.Equal(t, []int64{5001, 5002}, newSeg.Fingerprints)
}

func TestRollupMetricSegs_EmptyTargetReplacement(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create source segments to mark as rolled up
	sourceSegmentIDs := []int64{60001, 60002}
	for _, segmentID := range sourceSegmentIDs {
		err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
			OrganizationID: orgID,
			Dateint:        20250830,
			IngestDateint:  20250830,
			FrequencyMs:    10000,
			SegmentID:      segmentID,
			InstanceNum:    1,
			SlotID:         0,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(10 * time.Second).UnixMilli(),
			RecordCount:    300,
			FileSize:       15000,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Fingerprints:   []int64{600 + segmentID},
			SortVersion:    lrdb.CurrentMetricSortVersion,
			SlotCount:      1,
		})
		require.NoError(t, err)
	}

	// Test rollup with just marking source segments (no target replacement)
	err := db.RollupMetricSegs(ctx, lrdb.RollupMetricSegsParams{
		SourceSegments: struct {
			OrganizationID uuid.UUID
			Dateint        int32
			FrequencyMs    int32
			InstanceNum    int16
			SlotID         int32
			SegmentIDs     []int64
		}{
			OrganizationID: orgID,
			Dateint:        20250830,
			FrequencyMs:    10000,
			InstanceNum:    1,
			SlotID:         0,
			SegmentIDs:     sourceSegmentIDs,
		},
		TargetReplacement: lrdb.ReplaceMetricSegsParams{
			OrganizationID: orgID,
			Dateint:        20250830,
			InstanceNum:    1,
			SlotID:         0,
			SlotCount:      1,
			IngestDateint:  20250830,
			FrequencyMs:    60000,
			Published:      true,
			CreatedBy:      lrdb.CreateByRollup,
			SortVersion:    lrdb.CurrentMetricSortVersion,
			OldRecords:     []lrdb.ReplaceMetricSegsOld{}, // Empty
			NewRecords:     []lrdb.ReplaceMetricSegsNew{}, // Empty
		},
	})
	require.NoError(t, err)

	// Verify source segments were marked as rolled up
	segs, err := db.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    10000,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli() - 1000,
		EndTs:          now.Add(2 * time.Minute).UnixMilli(),
	})
	require.NoError(t, err)

	segMap := make(map[int64]lrdb.MetricSeg)
	for _, seg := range segs {
		segMap[seg.SegmentID] = seg
	}

	for _, segmentID := range sourceSegmentIDs {
		seg, exists := segMap[segmentID]
		require.True(t, exists, "Source segment should exist")
		assert.True(t, seg.Rolledup, "Source segment should be marked as rolled up")
		assert.True(t, seg.Published, "Source segment should remain published")
	}
}

func TestRollupMetricSegs_MultipleTargetSegments(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create source segments
	sourceSegmentIDs := []int64{70001, 70002, 70003}
	for _, segmentID := range sourceSegmentIDs {
		err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
			OrganizationID: orgID,
			Dateint:        20250830,
			IngestDateint:  20250830,
			FrequencyMs:    10000,
			SegmentID:      segmentID,
			InstanceNum:    1,
			SlotID:         0,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(10 * time.Second).UnixMilli(),
			RecordCount:    400,
			FileSize:       20000,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Fingerprints:   []int64{700 + segmentID},
			SortVersion:    lrdb.CurrentMetricSortVersion,
			SlotCount:      1,
		})
		require.NoError(t, err)
	}

	// Create multiple existing target segments to replace
	existingTargetIDs := []int64{80001, 80002}
	for _, segmentID := range existingTargetIDs {
		err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
			OrganizationID: orgID,
			Dateint:        20250830,
			IngestDateint:  20250830,
			FrequencyMs:    60000,
			SegmentID:      segmentID,
			InstanceNum:    1,
			SlotID:         0,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(time.Minute).UnixMilli(),
			RecordCount:    150,
			FileSize:       7500,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Fingerprints:   []int64{800 + segmentID},
			SortVersion:    lrdb.CurrentMetricSortVersion,
			SlotCount:      1,
		})
		require.NoError(t, err)
	}

	// Test rollup with multiple target segments
	err := db.RollupMetricSegs(ctx, lrdb.RollupMetricSegsParams{
		SourceSegments: struct {
			OrganizationID uuid.UUID
			Dateint        int32
			FrequencyMs    int32
			InstanceNum    int16
			SlotID         int32
			SegmentIDs     []int64
		}{
			OrganizationID: orgID,
			Dateint:        20250830,
			FrequencyMs:    10000,
			InstanceNum:    1,
			SlotID:         0,
			SegmentIDs:     sourceSegmentIDs,
		},
		TargetReplacement: lrdb.ReplaceMetricSegsParams{
			OrganizationID: orgID,
			Dateint:        20250830,
			InstanceNum:    1,
			SlotID:         0,
			SlotCount:      1,
			IngestDateint:  20250830,
			FrequencyMs:    60000,
			Published:      true,
			CreatedBy:      lrdb.CreateByRollup,
			SortVersion:    lrdb.CurrentMetricSortVersion,
			OldRecords: []lrdb.ReplaceMetricSegsOld{
				{SegmentID: existingTargetIDs[0], SlotID: 0},
				{SegmentID: existingTargetIDs[1], SlotID: 0},
			},
			NewRecords: []lrdb.ReplaceMetricSegsNew{
				{
					SegmentID:    90001,
					StartTs:      now.UnixMilli(),
					EndTs:        now.Add(30 * time.Second).UnixMilli(),
					RecordCount:  200,
					FileSize:     20000,
					Fingerprints: []int64{9001, 9101},
				},
				{
					SegmentID:    90002,
					StartTs:      now.Add(30 * time.Second).UnixMilli(),
					EndTs:        now.Add(time.Minute).UnixMilli(),
					RecordCount:  180,
					FileSize:     18000,
					Fingerprints: []int64{9002, 9102},
				},
			},
		},
	})
	require.NoError(t, err)

	// Verify source segments marked as rolled up
	sourceSegs, err := db.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    10000,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli() - 1000,
		EndTs:          now.Add(2 * time.Minute).UnixMilli(),
	})
	require.NoError(t, err)

	sourceSegMap := make(map[int64]lrdb.MetricSeg)
	for _, seg := range sourceSegs {
		sourceSegMap[seg.SegmentID] = seg
	}

	for _, segmentID := range sourceSegmentIDs {
		seg, exists := sourceSegMap[segmentID]
		require.True(t, exists)
		assert.True(t, seg.Rolledup, "Source segment should be rolled up")
	}

	// Verify target segments were replaced
	targetSegs, err := db.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    60000,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli() - 1000,
		EndTs:          now.Add(2 * time.Minute).UnixMilli(),
	})
	require.NoError(t, err)

	targetSegMap := make(map[int64]lrdb.MetricSeg)
	for _, seg := range targetSegs {
		targetSegMap[seg.SegmentID] = seg
	}

	// Old target segments should be deleted
	for _, oldID := range existingTargetIDs {
		_, exists := targetSegMap[oldID]
		assert.False(t, exists, "Old target segment should be deleted")
	}

	// New target segments should exist
	newSeg1, exists1 := targetSegMap[90001]
	require.True(t, exists1)
	assert.Equal(t, int64(200), newSeg1.RecordCount)
	assert.Equal(t, []int64{9001, 9101}, newSeg1.Fingerprints)

	newSeg2, exists2 := targetSegMap[90002]
	require.True(t, exists2)
	assert.Equal(t, int64(180), newSeg2.RecordCount)
	assert.Equal(t, []int64{9002, 9102}, newSeg2.Fingerprints)
}

func TestRollupMetricSegs_AtomicTransactionFailure(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create source segment
	sourceSegmentID := int64(99001)
	err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		IngestDateint:  20250830,
		FrequencyMs:    10000,
		SegmentID:      sourceSegmentID,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli(),
		EndTs:          now.Add(10 * time.Second).UnixMilli(),
		RecordCount:    500,
		FileSize:       25000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{990},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		SlotCount:      1,
	})
	require.NoError(t, err)

	// Test transaction rollback by trying to insert a segment with duplicate primary key
	// First, create a segment that will conflict
	conflictingSegmentID := int64(99999)
	err = db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		IngestDateint:  20250830,
		FrequencyMs:    60000,
		SegmentID:      conflictingSegmentID,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli(),
		EndTs:          now.Add(time.Minute).UnixMilli(),
		RecordCount:    100,
		FileSize:       5000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{999},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		SlotCount:      1,
	})
	require.NoError(t, err)

	// Try to rollup with a new record that has the same primary key (should fail)
	err = db.RollupMetricSegs(ctx, lrdb.RollupMetricSegsParams{
		SourceSegments: struct {
			OrganizationID uuid.UUID
			Dateint        int32
			FrequencyMs    int32
			InstanceNum    int16
			SlotID         int32
			SegmentIDs     []int64
		}{
			OrganizationID: orgID,
			Dateint:        20250830,
			FrequencyMs:    10000,
			InstanceNum:    1,
			SlotID:         0,
			SegmentIDs:     []int64{sourceSegmentID},
		},
		TargetReplacement: lrdb.ReplaceMetricSegsParams{
			OrganizationID: orgID,
			Dateint:        20250830,
			InstanceNum:    1,
			SlotID:         0,
			SlotCount:      1,
			IngestDateint:  20250830,
			FrequencyMs:    60000,
			Published:      true,
			CreatedBy:      lrdb.CreateByRollup,
			SortVersion:    lrdb.CurrentMetricSortVersion,
			OldRecords:     []lrdb.ReplaceMetricSegsOld{}, // Don't delete the existing one
			NewRecords: []lrdb.ReplaceMetricSegsNew{
				{
					SegmentID:    conflictingSegmentID, // This will cause a primary key violation
					StartTs:      now.UnixMilli(),
					EndTs:        now.Add(time.Minute).UnixMilli(),
					RecordCount:  200,
					FileSize:     10000,
					Fingerprints: []int64{9999},
				},
			},
		},
	})
	// Should fail due to primary key violation
	require.Error(t, err)

	// Verify that source segment was NOT marked as rolled up due to transaction rollback
	sourceSegs, err := db.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    10000,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli() - 1000,
		EndTs:          now.Add(2 * time.Minute).UnixMilli(),
	})
	require.NoError(t, err)
	require.Len(t, sourceSegs, 1)

	sourceSeg := sourceSegs[0]
	assert.Equal(t, sourceSegmentID, sourceSeg.SegmentID)
	assert.False(t, sourceSeg.Rolledup, "Source segment should NOT be marked as rolled up due to transaction failure")
	assert.True(t, sourceSeg.Published, "Source segment should remain published")

	// Verify original conflicting segment still exists unchanged
	targetSegs, err := db.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    60000,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli() - 1000,
		EndTs:          now.Add(2 * time.Minute).UnixMilli(),
	})
	require.NoError(t, err)
	require.Len(t, targetSegs, 1)

	targetSeg := targetSegs[0]
	assert.Equal(t, conflictingSegmentID, targetSeg.SegmentID)
	assert.Equal(t, int64(100), targetSeg.RecordCount)    // Original record count
	assert.Equal(t, []int64{999}, targetSeg.Fingerprints) // Original fingerprints
}

func TestRollupMetricSegs_DifferentOrganizations(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID1 := uuid.New()
	orgID2 := uuid.New()
	now := time.Now()

	// Create source segment for org1
	sourceSegmentID := int64(100001)
	err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID1,
		Dateint:        20250830,
		IngestDateint:  20250830,
		FrequencyMs:    10000,
		SegmentID:      sourceSegmentID,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli(),
		EndTs:          now.Add(10 * time.Second).UnixMilli(),
		RecordCount:    300,
		FileSize:       15000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{1001},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		SlotCount:      1,
	})
	require.NoError(t, err)

	// Create target segment for org2 (different org)
	targetSegmentID := int64(200001)
	err = db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID2,
		Dateint:        20250830,
		IngestDateint:  20250830,
		FrequencyMs:    60000,
		SegmentID:      targetSegmentID,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli(),
		EndTs:          now.Add(time.Minute).UnixMilli(),
		RecordCount:    100,
		FileSize:       5000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{2001},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		SlotCount:      1,
	})
	require.NoError(t, err)

	// Rollup should work on different organizations independently
	err = db.RollupMetricSegs(ctx, lrdb.RollupMetricSegsParams{
		SourceSegments: struct {
			OrganizationID uuid.UUID
			Dateint        int32
			FrequencyMs    int32
			InstanceNum    int16
			SlotID         int32
			SegmentIDs     []int64
		}{
			OrganizationID: orgID1, // Source from org1
			Dateint:        20250830,
			FrequencyMs:    10000,
			InstanceNum:    1,
			SlotID:         0,
			SegmentIDs:     []int64{sourceSegmentID},
		},
		TargetReplacement: lrdb.ReplaceMetricSegsParams{
			OrganizationID: orgID2, // Target in org2
			Dateint:        20250830,
			InstanceNum:    1,
			SlotID:         0,
			SlotCount:      1,
			IngestDateint:  20250830,
			FrequencyMs:    60000,
			Published:      true,
			Rolledup:       false,
			CreatedBy:      lrdb.CreateByRollup,
			SortVersion:    lrdb.CurrentMetricSortVersion,
			OldRecords: []lrdb.ReplaceMetricSegsOld{
				{SegmentID: targetSegmentID, SlotID: 0},
			},
			NewRecords: []lrdb.ReplaceMetricSegsNew{
				{
					SegmentID:    300001,
					StartTs:      now.UnixMilli(),
					EndTs:        now.Add(time.Minute).UnixMilli(),
					RecordCount:  150,
					FileSize:     7500,
					Fingerprints: []int64{3001},
				},
			},
		},
	})
	require.NoError(t, err)

	// Verify org1 source segment was marked as rolled up
	sourceSegs, err := db.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: orgID1,
		Dateint:        20250830,
		FrequencyMs:    10000,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli() - 1000,
		EndTs:          now.Add(2 * time.Minute).UnixMilli(),
	})
	require.NoError(t, err)
	require.Len(t, sourceSegs, 1)
	assert.True(t, sourceSegs[0].Rolledup, "Org1 source segment should be rolled up")

	// Verify org2 target segment was replaced
	targetSegs, err := db.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: orgID2,
		Dateint:        20250830,
		FrequencyMs:    60000,
		InstanceNum:    1,
		SlotID:         0,
		StartTs:        now.UnixMilli() - 1000,
		EndTs:          now.Add(2 * time.Minute).UnixMilli(),
	})
	require.NoError(t, err)
	require.Len(t, targetSegs, 1)
	assert.Equal(t, int64(300001), targetSegs[0].SegmentID)
	assert.Equal(t, []int64{3001}, targetSegs[0].Fingerprints)
}

func TestRollupMetricSegs_DifferentSlots(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create source segments in slot 0
	sourceSegmentIDs := []int64{110001, 110002}
	for _, segmentID := range sourceSegmentIDs {
		err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
			OrganizationID: orgID,
			Dateint:        20250830,
			IngestDateint:  20250830,
			FrequencyMs:    10000,
			SegmentID:      segmentID,
			InstanceNum:    1,
			SlotID:         0, // Slot 0
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(10 * time.Second).UnixMilli(),
			RecordCount:    200,
			FileSize:       10000,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Fingerprints:   []int64{1100 + segmentID},
			SortVersion:    lrdb.CurrentMetricSortVersion,
			SlotCount:      2, // 2 slots total
		})
		require.NoError(t, err)
	}

	// Create target segments in slot 1
	targetSegmentID := int64(120001)
	err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		IngestDateint:  20250830,
		FrequencyMs:    60000,
		SegmentID:      targetSegmentID,
		InstanceNum:    1,
		SlotID:         1, // Slot 1
		StartTs:        now.UnixMilli(),
		EndTs:          now.Add(time.Minute).UnixMilli(),
		RecordCount:    50,
		FileSize:       2500,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{1201},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		SlotCount:      2, // 2 slots total
	})
	require.NoError(t, err)

	// Rollup from slot 0 source to slot 1 target
	err = db.RollupMetricSegs(ctx, lrdb.RollupMetricSegsParams{
		SourceSegments: struct {
			OrganizationID uuid.UUID
			Dateint        int32
			FrequencyMs    int32
			InstanceNum    int16
			SlotID         int32
			SegmentIDs     []int64
		}{
			OrganizationID: orgID,
			Dateint:        20250830,
			FrequencyMs:    10000,
			InstanceNum:    1,
			SlotID:         0, // Source slot 0
			SegmentIDs:     sourceSegmentIDs,
		},
		TargetReplacement: lrdb.ReplaceMetricSegsParams{
			OrganizationID: orgID,
			Dateint:        20250830,
			InstanceNum:    1,
			SlotID:         1, // Target slot 1
			SlotCount:      2,
			IngestDateint:  20250830,
			FrequencyMs:    60000,
			Published:      true,
			Rolledup:       false,
			CreatedBy:      lrdb.CreateByRollup,
			SortVersion:    lrdb.CurrentMetricSortVersion,
			OldRecords: []lrdb.ReplaceMetricSegsOld{
				{SegmentID: targetSegmentID, SlotID: 1},
			},
			NewRecords: []lrdb.ReplaceMetricSegsNew{
				{
					SegmentID:    130001,
					StartTs:      now.UnixMilli(),
					EndTs:        now.Add(time.Minute).UnixMilli(),
					RecordCount:  80,
					FileSize:     4000,
					Fingerprints: []int64{1301},
				},
			},
		},
	})
	require.NoError(t, err)

	// Verify source segments in slot 0 were marked as rolled up
	sourceSegs, err := db.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    10000,
		InstanceNum:    1,
		SlotID:         0, // Check slot 0
		StartTs:        now.UnixMilli() - 1000,
		EndTs:          now.Add(2 * time.Minute).UnixMilli(),
	})
	require.NoError(t, err)

	sourceSegMap := make(map[int64]lrdb.MetricSeg)
	for _, seg := range sourceSegs {
		sourceSegMap[seg.SegmentID] = seg
	}

	for _, segmentID := range sourceSegmentIDs {
		seg, exists := sourceSegMap[segmentID]
		require.True(t, exists)
		assert.True(t, seg.Rolledup, "Source segment in slot 0 should be rolled up")
		assert.Equal(t, int32(0), seg.SlotID)
		assert.Equal(t, int32(2), seg.SlotCount)
	}

	// Verify target segment in slot 1 was replaced
	targetSegs, err := db.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    60000,
		InstanceNum:    1,
		SlotID:         1, // Check slot 1
		StartTs:        now.UnixMilli() - 1000,
		EndTs:          now.Add(2 * time.Minute).UnixMilli(),
	})
	require.NoError(t, err)
	require.Len(t, targetSegs, 1)

	newSeg := targetSegs[0]
	assert.Equal(t, int64(130001), newSeg.SegmentID)
	assert.Equal(t, int32(1), newSeg.SlotID)
	assert.Equal(t, int32(2), newSeg.SlotCount)
	assert.Equal(t, []int64{1301}, newSeg.Fingerprints)
}
