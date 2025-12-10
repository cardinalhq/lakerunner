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

//go:build integration

package queries

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/testhelpers"
)

// extractStreamValues extracts the stream values from ListLogStreamsRow results.
func extractStreamValues(rows []lrdb.ListLogStreamsRow) []string {
	var values []string
	for _, row := range rows {
		values = append(values, row.StreamValue)
	}
	return values
}

func TestLogSegStreamIDs(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	dateint := int32(20250828)
	now := time.Now()
	streamIDField := "resource_service_name"

	t.Run("insert and query stream_ids", func(t *testing.T) {
		// Insert a log segment with stream_ids
		err := store.InsertLogSegment(ctx, lrdb.InsertLogSegmentParams{
			OrganizationID: orgID,
			Dateint:        dateint,
			SegmentID:      1001,
			InstanceNum:    1,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(time.Hour).UnixMilli(),
			FileSize:       1024,
			RecordCount:    10,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{100},
			Published:      true,
			Compacted:      false,
			StreamIds:      []string{"service-a", "service-b"},
			StreamIDField:  &streamIDField,
		})
		require.NoError(t, err)

		// Query stream_ids
		rows, err := store.ListLogStreams(ctx, lrdb.ListLogStreamsParams{
			OrganizationID: orgID,
			StartDateint:   dateint,
			EndDateint:     dateint,
			StartTs:        now.UnixMilli() - 1,
			EndTs:          now.Add(2 * time.Hour).UnixMilli(),
		})
		require.NoError(t, err)

		streamIDs := extractStreamValues(rows)
		// Sort for consistent comparison
		sort.Strings(streamIDs)

		assert.Contains(t, streamIDs, "service-a")
		assert.Contains(t, streamIDs, "service-b")
		assert.Len(t, streamIDs, 2)
	})

	t.Run("query stream_ids across multiple segments", func(t *testing.T) {
		orgID2 := uuid.New()

		// Insert multiple segments with different stream_ids
		err := store.InsertLogSegment(ctx, lrdb.InsertLogSegmentParams{
			OrganizationID: orgID2,
			Dateint:        dateint,
			SegmentID:      1002,
			InstanceNum:    1,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(time.Hour).UnixMilli(),
			FileSize:       1024,
			RecordCount:    10,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{101},
			Published:      true,
			Compacted:      false,
			StreamIds:      []string{"customer-x.com", "api-service"},
			StreamIDField:  &streamIDField,
		})
		require.NoError(t, err)

		err = store.InsertLogSegment(ctx, lrdb.InsertLogSegmentParams{
			OrganizationID: orgID2,
			Dateint:        dateint,
			SegmentID:      1003,
			InstanceNum:    1,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(time.Hour).UnixMilli(),
			FileSize:       1024,
			RecordCount:    10,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{102},
			Published:      true,
			Compacted:      false,
			StreamIds:      []string{"customer-y.com", "api-service"}, // api-service is duplicate
			StreamIDField:  &streamIDField,
		})
		require.NoError(t, err)

		// Query stream_ids - should be deduplicated
		rows, err := store.ListLogStreams(ctx, lrdb.ListLogStreamsParams{
			OrganizationID: orgID2,
			StartDateint:   dateint,
			EndDateint:     dateint,
			StartTs:        now.UnixMilli() - 1,
			EndTs:          now.Add(2 * time.Hour).UnixMilli(),
		})
		require.NoError(t, err)

		streamIDs := extractStreamValues(rows)
		sort.Strings(streamIDs)

		// Should have 3 unique stream_ids (api-service deduplicated)
		assert.Contains(t, streamIDs, "api-service")
		assert.Contains(t, streamIDs, "customer-x.com")
		assert.Contains(t, streamIDs, "customer-y.com")
		assert.Len(t, streamIDs, 3)
	})

	t.Run("query stream_ids with dateint filtering", func(t *testing.T) {
		orgID3 := uuid.New()
		dateint1 := int32(20250828)
		dateint2 := int32(20250829)

		// Insert segment on dateint1
		err := store.InsertLogSegment(ctx, lrdb.InsertLogSegmentParams{
			OrganizationID: orgID3,
			Dateint:        dateint1,
			SegmentID:      1004,
			InstanceNum:    1,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(time.Hour).UnixMilli(),
			FileSize:       1024,
			RecordCount:    10,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{103},
			Published:      true,
			Compacted:      false,
			StreamIds:      []string{"day1-service"},
			StreamIDField:  &streamIDField,
		})
		require.NoError(t, err)

		// Insert segment on dateint2
		err = store.InsertLogSegment(ctx, lrdb.InsertLogSegmentParams{
			OrganizationID: orgID3,
			Dateint:        dateint2,
			SegmentID:      1005,
			InstanceNum:    1,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(time.Hour).UnixMilli(),
			FileSize:       1024,
			RecordCount:    10,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{104},
			Published:      true,
			Compacted:      false,
			StreamIds:      []string{"day2-service"},
			StreamIDField:  &streamIDField,
		})
		require.NoError(t, err)

		// Query only dateint1
		rows, err := store.ListLogStreams(ctx, lrdb.ListLogStreamsParams{
			OrganizationID: orgID3,
			StartDateint:   dateint1,
			EndDateint:     dateint1,
			StartTs:        now.UnixMilli() - 1,
			EndTs:          now.Add(2 * time.Hour).UnixMilli(),
		})
		require.NoError(t, err)
		streamIDs := extractStreamValues(rows)
		assert.Len(t, streamIDs, 1)
		assert.Contains(t, streamIDs, "day1-service")

		// Query both dateints
		rows, err = store.ListLogStreams(ctx, lrdb.ListLogStreamsParams{
			OrganizationID: orgID3,
			StartDateint:   dateint1,
			EndDateint:     dateint2,
			StartTs:        now.UnixMilli() - 1,
			EndTs:          now.Add(2 * time.Hour).UnixMilli(),
		})
		require.NoError(t, err)
		streamIDs = extractStreamValues(rows)
		assert.Len(t, streamIDs, 2)
		assert.Contains(t, streamIDs, "day1-service")
		assert.Contains(t, streamIDs, "day2-service")
	})

	t.Run("unpublished segments are excluded", func(t *testing.T) {
		orgID4 := uuid.New()

		// Insert unpublished segment
		err := store.InsertLogSegment(ctx, lrdb.InsertLogSegmentParams{
			OrganizationID: orgID4,
			Dateint:        dateint,
			SegmentID:      1006,
			InstanceNum:    1,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(time.Hour).UnixMilli(),
			FileSize:       1024,
			RecordCount:    10,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{105},
			Published:      false, // Unpublished
			Compacted:      false,
			StreamIds:      []string{"unpublished-service"},
			StreamIDField:  &streamIDField,
		})
		require.NoError(t, err)

		// Query stream_ids - should return empty
		rows, err := store.ListLogStreams(ctx, lrdb.ListLogStreamsParams{
			OrganizationID: orgID4,
			StartDateint:   dateint,
			EndDateint:     dateint,
			StartTs:        now.UnixMilli() - 1,
			EndTs:          now.Add(2 * time.Hour).UnixMilli(),
		})
		require.NoError(t, err)
		assert.Len(t, rows, 0)
	})

	t.Run("null stream_ids handled correctly", func(t *testing.T) {
		orgID5 := uuid.New()

		// Insert segment without stream_ids (nil)
		err := store.InsertLogSegment(ctx, lrdb.InsertLogSegmentParams{
			OrganizationID: orgID5,
			Dateint:        dateint,
			SegmentID:      1007,
			InstanceNum:    1,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(time.Hour).UnixMilli(),
			FileSize:       1024,
			RecordCount:    10,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{106},
			Published:      true,
			Compacted:      false,
			StreamIds:      nil, // No stream_ids
			StreamIDField:  nil, // No stream_id_field
		})
		require.NoError(t, err)

		// Query stream_ids - should return empty (segment excluded due to NULL stream_ids)
		rows, err := store.ListLogStreams(ctx, lrdb.ListLogStreamsParams{
			OrganizationID: orgID5,
			StartDateint:   dateint,
			EndDateint:     dateint,
			StartTs:        now.UnixMilli() - 1,
			EndTs:          now.Add(2 * time.Hour).UnixMilli(),
		})
		require.NoError(t, err)
		assert.Len(t, rows, 0)
	})

	t.Run("compaction preserves stream_ids", func(t *testing.T) {
		orgID6 := uuid.New()

		// Insert two source segments with stream_ids
		err := store.InsertLogSegment(ctx, lrdb.InsertLogSegmentParams{
			OrganizationID: orgID6,
			Dateint:        dateint,
			SegmentID:      2001,
			InstanceNum:    1,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(time.Hour).UnixMilli(),
			FileSize:       1024,
			RecordCount:    10,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{201},
			Published:      true,
			Compacted:      false,
			StreamIds:      []string{"source-service-a", "source-service-b"},
			StreamIDField:  &streamIDField,
		})
		require.NoError(t, err)

		err = store.InsertLogSegment(ctx, lrdb.InsertLogSegmentParams{
			OrganizationID: orgID6,
			Dateint:        dateint,
			SegmentID:      2002,
			InstanceNum:    1,
			StartTs:        now.UnixMilli(),
			EndTs:          now.Add(time.Hour).UnixMilli(),
			FileSize:       1024,
			RecordCount:    10,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{202},
			Published:      true,
			Compacted:      false,
			StreamIds:      []string{"source-service-c"},
			StreamIDField:  &streamIDField,
		})
		require.NoError(t, err)

		// Run compaction - this marks old segments and inserts new compacted segment
		err = store.CompactLogSegments(ctx, lrdb.CompactLogSegsParams{
			OrganizationID: orgID6,
			Dateint:        dateint,
			InstanceNum:    1,
			OldRecords: []lrdb.CompactLogSegsOld{
				{SegmentID: 2001},
				{SegmentID: 2002},
			},
			NewRecords: []lrdb.CompactLogSegsNew{
				{
					SegmentID:     3001,
					StartTs:       now.UnixMilli(),
					EndTs:         now.Add(time.Hour).UnixMilli(),
					RecordCount:   20,
					FileSize:      2048,
					Fingerprints:  []int64{201, 202},
					StreamIds:     []string{"source-service-a", "source-service-b", "source-service-c"},
					StreamIdField: &streamIDField,
				},
			},
			CreatedBy: lrdb.CreatedByCompact,
		})
		require.NoError(t, err)

		// Query stream_ids - should return the compacted segment's stream_ids
		rows, err := store.ListLogStreams(ctx, lrdb.ListLogStreamsParams{
			OrganizationID: orgID6,
			StartDateint:   dateint,
			EndDateint:     dateint,
			StartTs:        now.UnixMilli() - 1,
			EndTs:          now.Add(2 * time.Hour).UnixMilli(),
		})
		require.NoError(t, err)

		streamIDs := extractStreamValues(rows)
		sort.Strings(streamIDs)

		// Compacted segment should have all 3 stream_ids
		assert.Contains(t, streamIDs, "source-service-a")
		assert.Contains(t, streamIDs, "source-service-b")
		assert.Contains(t, streamIDs, "source-service-c")
		assert.Len(t, streamIDs, 3)
	})

	t.Run("timestamp range filtering works correctly", func(t *testing.T) {
		orgID7 := uuid.New()

		// Create three segments at different time windows on the same day
		// Segment 1: 00:00 - 01:00
		seg1Start := time.Date(2025, 8, 28, 0, 0, 0, 0, time.UTC)
		seg1End := time.Date(2025, 8, 28, 1, 0, 0, 0, time.UTC)

		// Segment 2: 02:00 - 03:00
		seg2Start := time.Date(2025, 8, 28, 2, 0, 0, 0, time.UTC)
		seg2End := time.Date(2025, 8, 28, 3, 0, 0, 0, time.UTC)

		// Segment 3: 04:00 - 05:00
		seg3Start := time.Date(2025, 8, 28, 4, 0, 0, 0, time.UTC)
		seg3End := time.Date(2025, 8, 28, 5, 0, 0, 0, time.UTC)

		err := store.InsertLogSegment(ctx, lrdb.InsertLogSegmentParams{
			OrganizationID: orgID7,
			Dateint:        dateint,
			SegmentID:      4001,
			InstanceNum:    1,
			StartTs:        seg1Start.UnixMilli(),
			EndTs:          seg1End.UnixMilli(),
			FileSize:       1024,
			RecordCount:    10,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{401},
			Published:      true,
			Compacted:      false,
			StreamIds:      []string{"early-morning-service"},
			StreamIDField:  &streamIDField,
		})
		require.NoError(t, err)

		err = store.InsertLogSegment(ctx, lrdb.InsertLogSegmentParams{
			OrganizationID: orgID7,
			Dateint:        dateint,
			SegmentID:      4002,
			InstanceNum:    1,
			StartTs:        seg2Start.UnixMilli(),
			EndTs:          seg2End.UnixMilli(),
			FileSize:       1024,
			RecordCount:    10,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{402},
			Published:      true,
			Compacted:      false,
			StreamIds:      []string{"mid-morning-service"},
			StreamIDField:  &streamIDField,
		})
		require.NoError(t, err)

		err = store.InsertLogSegment(ctx, lrdb.InsertLogSegmentParams{
			OrganizationID: orgID7,
			Dateint:        dateint,
			SegmentID:      4003,
			InstanceNum:    1,
			StartTs:        seg3Start.UnixMilli(),
			EndTs:          seg3End.UnixMilli(),
			FileSize:       1024,
			RecordCount:    10,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{403},
			Published:      true,
			Compacted:      false,
			StreamIds:      []string{"late-morning-service"},
			StreamIDField:  &streamIDField,
		})
		require.NoError(t, err)

		// Query for 00:30 - 00:45 (only overlaps segment 1)
		rows, err := store.ListLogStreams(ctx, lrdb.ListLogStreamsParams{
			OrganizationID: orgID7,
			StartDateint:   dateint,
			EndDateint:     dateint,
			StartTs:        time.Date(2025, 8, 28, 0, 30, 0, 0, time.UTC).UnixMilli(),
			EndTs:          time.Date(2025, 8, 28, 0, 45, 0, 0, time.UTC).UnixMilli(),
		})
		require.NoError(t, err)
		streamIDs := extractStreamValues(rows)
		assert.Len(t, streamIDs, 1)
		assert.Contains(t, streamIDs, "early-morning-service")

		// Query for 01:30 - 02:30 (only overlaps segment 2)
		rows, err = store.ListLogStreams(ctx, lrdb.ListLogStreamsParams{
			OrganizationID: orgID7,
			StartDateint:   dateint,
			EndDateint:     dateint,
			StartTs:        time.Date(2025, 8, 28, 1, 30, 0, 0, time.UTC).UnixMilli(),
			EndTs:          time.Date(2025, 8, 28, 2, 30, 0, 0, time.UTC).UnixMilli(),
		})
		require.NoError(t, err)
		streamIDs = extractStreamValues(rows)
		assert.Len(t, streamIDs, 1)
		assert.Contains(t, streamIDs, "mid-morning-service")

		// Query for 00:00 - 03:00 (overlaps segments 1 and 2)
		rows, err = store.ListLogStreams(ctx, lrdb.ListLogStreamsParams{
			OrganizationID: orgID7,
			StartDateint:   dateint,
			EndDateint:     dateint,
			StartTs:        time.Date(2025, 8, 28, 0, 0, 0, 0, time.UTC).UnixMilli(),
			EndTs:          time.Date(2025, 8, 28, 3, 0, 0, 0, time.UTC).UnixMilli(),
		})
		require.NoError(t, err)
		streamIDs = extractStreamValues(rows)
		assert.Len(t, streamIDs, 2)
		assert.Contains(t, streamIDs, "early-morning-service")
		assert.Contains(t, streamIDs, "mid-morning-service")

		// Query for 00:00 - 06:00 (overlaps all segments)
		rows, err = store.ListLogStreams(ctx, lrdb.ListLogStreamsParams{
			OrganizationID: orgID7,
			StartDateint:   dateint,
			EndDateint:     dateint,
			StartTs:        time.Date(2025, 8, 28, 0, 0, 0, 0, time.UTC).UnixMilli(),
			EndTs:          time.Date(2025, 8, 28, 6, 0, 0, 0, time.UTC).UnixMilli(),
		})
		require.NoError(t, err)
		streamIDs = extractStreamValues(rows)
		assert.Len(t, streamIDs, 3)
		assert.Contains(t, streamIDs, "early-morning-service")
		assert.Contains(t, streamIDs, "mid-morning-service")
		assert.Contains(t, streamIDs, "late-morning-service")

		// Query for 10:00 - 11:00 (no overlap)
		rows, err = store.ListLogStreams(ctx, lrdb.ListLogStreamsParams{
			OrganizationID: orgID7,
			StartDateint:   dateint,
			EndDateint:     dateint,
			StartTs:        time.Date(2025, 8, 28, 10, 0, 0, 0, time.UTC).UnixMilli(),
			EndTs:          time.Date(2025, 8, 28, 11, 0, 0, 0, time.UTC).UnixMilli(),
		})
		require.NoError(t, err)
		assert.Len(t, rows, 0)
	})
}
