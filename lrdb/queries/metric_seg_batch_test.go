//go:build integration

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

func TestInsertMetricSegmentsBatch(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	dateint := int32(20250115)
	now := time.Now()

	// Prepare metric segments
	segments := []lrdb.InsertMetricSegmentParams{
		{
			OrganizationID: orgID,
			Dateint:        dateint,
			FrequencyMs:    5000,
			SegmentID:      1001,
			InstanceNum:    1,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(1 * time.Minute).UnixMilli(),
			RecordCount:    100,
			FileSize:       1024,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Compacted:      false,
			Rolledup:       false,
			Fingerprints:   []int64{123, 456},
			SortVersion:    lrdb.CurrentMetricSortVersion,
		},
		{
			OrganizationID: orgID,
			Dateint:        dateint,
			FrequencyMs:    5000,
			SegmentID:      1002,
			InstanceNum:    1,
			StartTs:        now.Add(1 * time.Minute).UnixMilli(),
			EndTs:          now.Add(2 * time.Minute).UnixMilli(),
			RecordCount:    150,
			FileSize:       2048,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Compacted:      false,
			Rolledup:       false,
			Fingerprints:   []int64{789, 1011},
			SortVersion:    lrdb.CurrentMetricSortVersion,
		},
	}

	// Insert batch
	err := db.InsertMetricSegmentsBatch(ctx, segments)
	require.NoError(t, err)

	// Verify metric segments were inserted
	insertedSegs, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        dateint,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     []int64{1001, 1002},
	})
	require.NoError(t, err)
	assert.Len(t, insertedSegs, 2)

	// Verify first segment
	seg1 := insertedSegs[0]
	assert.Equal(t, int64(1001), seg1.SegmentID)
	assert.Equal(t, int64(100), seg1.RecordCount)
	assert.Equal(t, int64(1024), seg1.FileSize)
	assert.True(t, seg1.Published)
	assert.False(t, seg1.Compacted)

	// Verify second segment
	seg2 := insertedSegs[1]
	assert.Equal(t, int64(1002), seg2.SegmentID)
	assert.Equal(t, int64(150), seg2.RecordCount)
	assert.Equal(t, int64(2048), seg2.FileSize)
}

func TestInsertMetricSegmentsBatch_EmptyOffsets(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	dateint := int32(20250115)
	now := time.Now()

	// Prepare a single metric segment
	segments := []lrdb.InsertMetricSegmentParams{
		{
			OrganizationID: orgID,
			Dateint:        dateint,
			FrequencyMs:    5000,
			SegmentID:      2001,
			InstanceNum:    1,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(1 * time.Minute).UnixMilli(),
			RecordCount:    50,
			FileSize:       512,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Compacted:      false,
			Rolledup:       false,
			Fingerprints:   []int64{111},
			SortVersion:    lrdb.CurrentMetricSortVersion,
		},
	}

	// Insert batch
	err := db.InsertMetricSegmentsBatch(ctx, segments)
	require.NoError(t, err)

	// Verify metric segment was inserted
	insertedSegs, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        dateint,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     []int64{2001},
	})
	require.NoError(t, err)
	assert.Len(t, insertedSegs, 1)
	assert.Equal(t, int64(2001), insertedSegs[0].SegmentID)
}

func TestCompactMetricSegments(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	dateint := int32(20250115)
	now := time.Now()

	// First, insert some segments that will be compacted
	oldSegments := []lrdb.InsertMetricSegmentParams{
		{
			OrganizationID: orgID,
			Dateint:        dateint,
			FrequencyMs:    5000,
			SegmentID:      3001,
			InstanceNum:    1,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(1 * time.Minute).UnixMilli(),
			RecordCount:    50,
			FileSize:       512,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Compacted:      false,
			Rolledup:       false,
			Fingerprints:   []int64{111, 222},
			SortVersion:    lrdb.CurrentMetricSortVersion,
		},
		{
			OrganizationID: orgID,
			Dateint:        dateint,
			FrequencyMs:    5000,
			SegmentID:      3002,
			InstanceNum:    1,
			StartTs:        now.Add(1 * time.Minute).UnixMilli(),
			EndTs:          now.Add(2 * time.Minute).UnixMilli(),
			RecordCount:    60,
			FileSize:       612,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Compacted:      false,
			Rolledup:       false,
			Fingerprints:   []int64{333, 444},
			SortVersion:    lrdb.CurrentMetricSortVersion,
		},
	}

	// Insert the old segments first
	err := db.InsertMetricSegmentsBatch(ctx, oldSegments)
	require.NoError(t, err)

	// Prepare compaction parameters
	compactParams := lrdb.CompactMetricSegsParams{
		OrganizationID: orgID,
		Dateint:        dateint,
		InstanceNum:    1,
		IngestDateint:  dateint,
		FrequencyMs:    5000,
		OldRecords: []lrdb.CompactMetricSegsOld{
			{SegmentID: 3001},
			{SegmentID: 3002},
		},
		NewRecords: []lrdb.CompactMetricSegsNew{
			{
				SegmentID:    4001, // New compacted segment
				StartTs:      now.UnixMilli(),
				EndTs:        now.Add(2 * time.Minute).UnixMilli(),
				RecordCount:  110, // Combined count
				FileSize:     1024,
				Fingerprints: []int64{111, 222, 333, 444}, // Combined fingerprints
			},
		},
		CreatedBy: lrdb.CreatedByCompact,
	}

	// Perform compaction
	err = db.CompactMetricSegments(ctx, compactParams)
	require.NoError(t, err)

	// Verify old segments are marked as compacted
	oldSegs, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        dateint,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     []int64{3001, 3002},
	})
	require.NoError(t, err)
	assert.Len(t, oldSegs, 2)
	for _, seg := range oldSegs {
		assert.True(t, seg.Compacted, "Old segment %d should be marked as compacted", seg.SegmentID)
	}

	// Verify new compacted segment was inserted
	newSegs, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        dateint,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     []int64{4001},
	})
	require.NoError(t, err)
	assert.Len(t, newSegs, 1)
	assert.Equal(t, int64(4001), newSegs[0].SegmentID)
	assert.Equal(t, int64(110), newSegs[0].RecordCount)
	assert.True(t, newSegs[0].Compacted)
	assert.True(t, newSegs[0].Published)
}

func TestRollupMetricSegments(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	sourceDateint := int32(20250115)
	targetDateint := int32(20250116) // Rollup to next day
	now := time.Now()

	// First, insert source segments that will be rolled up
	sourceSegments := []lrdb.InsertMetricSegmentParams{
		{
			OrganizationID: orgID,
			Dateint:        sourceDateint,
			FrequencyMs:    5000, // 5 second frequency
			SegmentID:      5001,
			InstanceNum:    1,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(1 * time.Hour).UnixMilli(),
			RecordCount:    720, // 720 data points (1 per 5 seconds for an hour)
			FileSize:       8192,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Compacted:      false,
			Rolledup:       false,
			Fingerprints:   []int64{1111, 2222},
			SortVersion:    lrdb.CurrentMetricSortVersion,
		},
		{
			OrganizationID: orgID,
			Dateint:        sourceDateint,
			FrequencyMs:    5000,
			SegmentID:      5002,
			InstanceNum:    1,
			StartTs:        now.Add(1 * time.Hour).UnixMilli(),
			EndTs:          now.Add(2 * time.Hour).UnixMilli(),
			RecordCount:    720,
			FileSize:       8192,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Compacted:      false,
			Rolledup:       false,
			Fingerprints:   []int64{3333, 4444},
			SortVersion:    lrdb.CurrentMetricSortVersion,
		},
	}

	// Insert the source segments
	err := db.InsertMetricSegmentsBatch(ctx, sourceSegments)
	require.NoError(t, err)

	// Prepare rollup parameters
	sourceParams := lrdb.RollupSourceParams{
		OrganizationID: orgID,
		Dateint:        sourceDateint,
		FrequencyMs:    5000,
		InstanceNum:    1,
	}

	targetParams := lrdb.RollupTargetParams{
		OrganizationID: orgID,
		Dateint:        targetDateint,
		FrequencyMs:    60000, // 1 minute frequency (rollup from 5s to 1m)
		InstanceNum:    1,
		IngestDateint:  sourceDateint,
		SortVersion:    lrdb.CurrentMetricSortVersion,
	}

	sourceSegmentIDs := []int64{5001, 5002}

	newRecords := []lrdb.RollupNewRecord{
		{
			SegmentID:    6001, // New rolled up segment
			StartTs:      now.UnixMilli(),
			EndTs:        now.Add(2 * time.Hour).UnixMilli(),
			RecordCount:  120, // 120 data points (1 per minute for 2 hours)
			FileSize:     4096,
			Fingerprints: []int64{1111, 2222, 3333, 4444}, // Combined fingerprints
		},
	}

	// Perform rollup
	err = db.RollupMetricSegments(ctx, sourceParams, targetParams, sourceSegmentIDs, newRecords)
	require.NoError(t, err)

	// Verify source segments are marked as rolled up
	sourceSegs, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        sourceDateint,
		FrequencyMs:    5000,
		InstanceNum:    1,
		SegmentIds:     sourceSegmentIDs,
	})
	require.NoError(t, err)
	assert.Len(t, sourceSegs, 2)
	for _, seg := range sourceSegs {
		assert.True(t, seg.Rolledup, "Source segment %d should be marked as rolled up", seg.SegmentID)
	}

	// Verify new rollup segment was inserted
	targetSegs, err := db.GetMetricSegsByIds(ctx, lrdb.GetMetricSegsByIdsParams{
		OrganizationID: orgID,
		Dateint:        targetDateint,
		FrequencyMs:    60000,
		InstanceNum:    1,
		SegmentIds:     []int64{6001},
	})
	require.NoError(t, err)
	assert.Len(t, targetSegs, 1)
	assert.Equal(t, int64(6001), targetSegs[0].SegmentID)
	assert.Equal(t, int64(120), targetSegs[0].RecordCount)
	assert.False(t, targetSegs[0].Compacted)
	assert.False(t, targetSegs[0].Rolledup)
	assert.True(t, targetSegs[0].Published)
	assert.Equal(t, lrdb.CreatedByRollup, targetSegs[0].CreatedBy)
}
