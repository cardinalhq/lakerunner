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

func TestInsertMetricSegmentBatch(t *testing.T) {
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

	// Prepare kafka offsets for tracking (non-contiguous values)
	kafkaOffsets := []lrdb.KafkaOffsetInfo{
		{
			ConsumerGroup: "metrics-consumer",
			Topic:         "metrics-topic",
			PartitionID:   0,
			BinID:         1,
			Offsets:       []int64{1000, 1003, 1005, 1007, 1012}, // gaps in sequence
		},
		{
			ConsumerGroup: "metrics-consumer",
			Topic:         "metrics-topic",
			PartitionID:   1,
			BinID:         2,
			Offsets:       []int64{2000, 2010, 2015}, // larger gaps
		},
	}

	// Insert batch
	err := db.InsertMetricSegmentBatch(ctx, segments, kafkaOffsets)
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

	// Verify kafka offsets were tracked (with gaps preserved)
	offsets1, err := db.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: "metrics-consumer",
		Topic:         "metrics-topic",
		PartitionID:   0,
		MinOffset:     1004,
	})
	require.NoError(t, err)
	assert.Equal(t, []int64{1005, 1007, 1012}, offsets1) // gaps preserved

	offsets2, err := db.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: "metrics-consumer",
		Topic:         "metrics-topic",
		PartitionID:   1,
		MinOffset:     2001,
	})
	require.NoError(t, err)
	assert.Equal(t, []int64{2010, 2015}, offsets2) // gaps preserved
}

func TestInsertMetricSegmentBatch_EmptyOffsets(t *testing.T) {
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

	// Empty kafka offsets (should be skipped)
	kafkaOffsets := []lrdb.KafkaOffsetInfo{
		{
			ConsumerGroup: "metrics-consumer",
			Topic:         "metrics-topic",
			PartitionID:   0,
			BinID:         1,
			Offsets:       []int64{}, // Empty offsets
		},
	}

	// Should succeed even with empty offsets
	err := db.InsertMetricSegmentBatch(ctx, segments, kafkaOffsets)
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

	// Verify no kafka offsets were tracked
	offsets, err := db.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: "metrics-consumer",
		Topic:         "metrics-topic",
		PartitionID:   0,
		MinOffset:     0,
	})
	require.NoError(t, err)
	assert.Empty(t, offsets)
}