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

package compaction

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/testhelpers"
)

func TestReplaceCompactedSegments_WithCompactMetricSegs(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)
	ll := slog.Default()

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

	// Create old rows for the function input with proper IngestDateint
	oldRows := []lrdb.MetricSeg{
		{SegmentID: oldSegmentIDs[0], IngestDateint: 20250830},
		{SegmentID: oldSegmentIDs[1], IngestDateint: 20250830},
	}

	// Create parquet results with metadata
	results := []parquetwriter.Result{
		{
			FileName:    "/tmp/compacted1.parquet",
			RecordCount: 1500,
			FileSize:    40000,
			Metadata:    factories.MetricsFileStats{Fingerprints: []int64{789, 101112}},
		},
	}

	// Create work item
	workItem := lrdb.ClaimMetricCompactionWorkRow{
		OrganizationID: orgID,
		Dateint:        20250830,
		FrequencyMs:    5000,
		InstanceNum:    1,
		TsRange: pgtype.Range[pgtype.Timestamptz]{
			Lower:     pgtype.Timestamptz{Time: now, Valid: true},
			Upper:     pgtype.Timestamptz{Time: now.Add(time.Hour), Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		},
	}

	// Test segment IDs for the new compacted segments
	newSegmentIDs := []int64{99999}

	// Call the function under test
	inputRecords := int64(2000) // Sum of old segments record count (1000 + 1000)
	inputBytes := int64(100000) // Sum of old segments file size (50000 + 50000)
	err := replaceCompactedSegments(ctx, ll, db, results, oldRows, workItem, newSegmentIDs, inputRecords, inputBytes)
	require.NoError(t, err)

	// Verify segments by querying all segments for this org/dateint/frequency
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
	newSeg, exists := segmentMap[newSegmentIDs[0]]
	require.True(t, exists, "New segment should exist")
	assert.False(t, newSeg.Compacted, "New segment should not be compacted")
	assert.True(t, newSeg.Published, "New segment should be published")
	assert.Equal(t, int64(1500), newSeg.RecordCount)
	assert.Equal(t, int64(40000), newSeg.FileSize)
	assert.Equal(t, []int64{789, 101112}, newSeg.Fingerprints)
}
