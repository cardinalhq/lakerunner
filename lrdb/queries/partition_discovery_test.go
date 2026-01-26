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

//go:build integration

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

func TestPartitionDiscovery(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t)

	// Test organization UUID
	orgID := uuid.New()
	dateint := int32(20250828)

	t.Run("metric partition discovery", func(t *testing.T) {
		// Insert a metric segment to create partitions
		err := store.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
			OrganizationID: orgID,
			Dateint:        dateint,
			FrequencyMs:    60000,
			SegmentID:      1,
			InstanceNum:    1,
			StartTs:        time.Now().UnixMilli(),
			EndTs:          time.Now().Add(time.Hour).UnixMilli(),
			FileSize:       1024,
			RecordCount:    10,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{100},
			SortVersion:    lrdb.CurrentMetricSortVersion,
			Compacted:      false,
			Published:      false,
		})
		require.NoError(t, err)

		// Test partition discovery
		partitions, err := store.ParseMetricPartitions(ctx)
		require.NoError(t, err)

		// Should find our inserted org-dateint combination
		found := false
		for _, p := range partitions {
			if p.OrganizationID == orgID && p.Dateint == dateint {
				found = true
				break
			}
		}
		assert.True(t, found, "Should find inserted org-dateint combination")
	})

	t.Run("log partition discovery", func(t *testing.T) {
		// Insert a log segment to create partitions
		err := store.InsertLogSegment(ctx, lrdb.InsertLogSegmentParams{
			OrganizationID: orgID,
			Dateint:        dateint,
			SegmentID:      2,
			InstanceNum:    1,
			StartTs:        time.Now().UnixMilli(),
			EndTs:          time.Now().Add(time.Hour).UnixMilli(),
			FileSize:       1024,
			RecordCount:    10,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{200},
			Published:      false,
			Compacted:      false,
		})
		require.NoError(t, err)

		// Test partition discovery
		partitions, err := store.ParseLogPartitions(ctx)
		require.NoError(t, err)

		// Should find our inserted org-dateint combination
		found := false
		for _, p := range partitions {
			if p.OrganizationID == orgID && p.Dateint == dateint {
				found = true
				break
			}
		}
		assert.True(t, found, "Should find inserted org-dateint combination")
	})

	t.Run("trace partition discovery", func(t *testing.T) {
		// Insert a trace segment to create partitions
		err := store.InsertTraceSegment(ctx, lrdb.InsertTraceSegmentParams{
			OrganizationID: orgID,
			Dateint:        dateint,
			SegmentID:      3,
			InstanceNum:    1,
			StartTs:        time.Now().UnixMilli(),
			EndTs:          time.Now().Add(time.Hour).UnixMilli(),
			FileSize:       1024,
			RecordCount:    10,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{300},
			Published:      false,
			Compacted:      false,
		})
		require.NoError(t, err)

		// Test partition discovery
		partitions, err := store.ParseTracePartitions(ctx)
		require.NoError(t, err)

		// Should find our inserted org-dateint combination
		found := false
		for _, p := range partitions {
			if p.OrganizationID == orgID && p.Dateint == dateint {
				found = true
				break
			}
		}
		assert.True(t, found, "Should find inserted org-dateint combination")
	})
}
