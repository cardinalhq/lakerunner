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

func TestLogSegStreamIDs(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	dateint := int32(20250828)
	now := time.Now()

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
		})
		require.NoError(t, err)

		// Query stream_ids
		streamIDs, err := store.ListLogStreamIDs(ctx, lrdb.ListLogStreamIDsParams{
			OrganizationID: orgID,
			StartDateint:   dateint,
			EndDateint:     dateint,
		})
		require.NoError(t, err)

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
		})
		require.NoError(t, err)

		// Query stream_ids - should be deduplicated
		streamIDs, err := store.ListLogStreamIDs(ctx, lrdb.ListLogStreamIDsParams{
			OrganizationID: orgID2,
			StartDateint:   dateint,
			EndDateint:     dateint,
		})
		require.NoError(t, err)

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
		})
		require.NoError(t, err)

		// Query only dateint1
		streamIDs, err := store.ListLogStreamIDs(ctx, lrdb.ListLogStreamIDsParams{
			OrganizationID: orgID3,
			StartDateint:   dateint1,
			EndDateint:     dateint1,
		})
		require.NoError(t, err)
		assert.Len(t, streamIDs, 1)
		assert.Contains(t, streamIDs, "day1-service")

		// Query both dateints
		streamIDs, err = store.ListLogStreamIDs(ctx, lrdb.ListLogStreamIDsParams{
			OrganizationID: orgID3,
			StartDateint:   dateint1,
			EndDateint:     dateint2,
		})
		require.NoError(t, err)
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
		})
		require.NoError(t, err)

		// Query stream_ids - should return empty
		streamIDs, err := store.ListLogStreamIDs(ctx, lrdb.ListLogStreamIDsParams{
			OrganizationID: orgID4,
			StartDateint:   dateint,
			EndDateint:     dateint,
		})
		require.NoError(t, err)
		assert.Len(t, streamIDs, 0)
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
		})
		require.NoError(t, err)

		// Query stream_ids - should return empty (segment excluded due to NULL stream_ids)
		streamIDs, err := store.ListLogStreamIDs(ctx, lrdb.ListLogStreamIDsParams{
			OrganizationID: orgID5,
			StartDateint:   dateint,
			EndDateint:     dateint,
		})
		require.NoError(t, err)
		assert.Len(t, streamIDs, 0)
	})
}
