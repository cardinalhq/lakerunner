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
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/testhelpers"
)

func TestPutMetricCompactionWork(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	segmentID := int64(12345)

	now := time.Now()
	tsRange := pgtype.Range[pgtype.Timestamptz]{
		Lower:     pgtype.Timestamptz{Time: now, Valid: true},
		Upper:     pgtype.Timestamptz{Time: now.Add(time.Hour), Valid: true},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Valid:     true,
	}

	err := db.PutMetricCompactionWork(ctx, lrdb.PutMetricCompactionWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    5000,
		SegmentID:      segmentID,
		InstanceNum:    1,
		TsRange:        tsRange,
		RecordCount:    1000,
		Priority:       1,
	})
	require.NoError(t, err)
}

func TestPutMetricCompactionWork_MultipleItems(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	workItems := []lrdb.PutMetricCompactionWorkParams{
		{
			OrganizationID: orgID,
			Dateint:        20250829,
			FrequencyMs:    5000,
			SegmentID:      int64(12346),
			InstanceNum:    1,
			TsRange: pgtype.Range[pgtype.Timestamptz]{
				Lower:     pgtype.Timestamptz{Time: now, Valid: true},
				Upper:     pgtype.Timestamptz{Time: now.Add(time.Hour), Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			RecordCount: 500,
			Priority:    1,
		},
		{
			OrganizationID: orgID,
			Dateint:        20250829,
			FrequencyMs:    7000,
			SegmentID:      int64(12346),
			InstanceNum:    1,
			TsRange: pgtype.Range[pgtype.Timestamptz]{
				Lower:     pgtype.Timestamptz{Time: now.Add(time.Hour), Valid: true},
				Upper:     pgtype.Timestamptz{Time: now.Add(2 * time.Hour), Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			RecordCount: 1500,
			Priority:    2,
		},
	}

	for _, item := range workItems {
		err := db.PutMetricCompactionWork(ctx, item)
		require.NoError(t, err)
	}
}

func TestClaimMetricCompactionWork_BasicClaim(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)
	now := time.Now()

	workItems := []lrdb.PutMetricCompactionWorkParams{
		{
			OrganizationID: orgID,
			Dateint:        20250829,
			FrequencyMs:    5000,
			SegmentID:      int64(12346),
			InstanceNum:    1,
			TsRange: pgtype.Range[pgtype.Timestamptz]{
				Lower:     pgtype.Timestamptz{Time: now, Valid: true},
				Upper:     pgtype.Timestamptz{Time: now.Add(time.Hour), Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			RecordCount: 1000,
			Priority:    1,
		},
		{
			OrganizationID: orgID,
			Dateint:        20250829,
			FrequencyMs:    5000,
			SegmentID:      int64(12346),
			InstanceNum:    1,
			TsRange: pgtype.Range[pgtype.Timestamptz]{
				Lower:     pgtype.Timestamptz{Time: now.Add(time.Hour), Valid: true},
				Upper:     pgtype.Timestamptz{Time: now.Add(2 * time.Hour), Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			RecordCount: 1500,
			Priority:    1,
		},
	}

	for _, item := range workItems {
		err := db.PutMetricCompactionWork(ctx, item)
		require.NoError(t, err)
	}

	claimedBatch, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
		WorkerID:             workerID,
		DefaultTargetRecords: 2500,
		MaxAgeSeconds:        30,
		BatchCount:           5,
	})
	require.NoError(t, err)

	assert.Len(t, claimedBatch, 2)
	for _, item := range claimedBatch {
		assert.Equal(t, orgID, item.OrganizationID)
		assert.Equal(t, workerID, item.ClaimedBy)
		assert.Equal(t, int16(1), item.InstanceNum)
		assert.NotNil(t, item.ClaimedAt)
	}

	totalRecords := int64(0)
	for _, item := range claimedBatch {
		totalRecords += item.RecordCount
	}
	assert.Equal(t, int64(2500), totalRecords)
}

func TestClaimMetricCompactionWork_ExactFill(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)
	now := time.Now()

	workItems := []lrdb.PutMetricCompactionWorkParams{
		{
			OrganizationID: orgID,
			Dateint:        20250829,
			FrequencyMs:    5000,
			SegmentID:      int64(12346),
			InstanceNum:    1,
			TsRange: pgtype.Range[pgtype.Timestamptz]{
				Lower:     pgtype.Timestamptz{Time: now, Valid: true},
				Upper:     pgtype.Timestamptz{Time: now.Add(time.Hour), Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			RecordCount: 1000,
			Priority:    1,
		},
		{
			OrganizationID: orgID,
			Dateint:        20250829,
			FrequencyMs:    5000,
			SegmentID:      int64(12346),
			InstanceNum:    1,
			TsRange: pgtype.Range[pgtype.Timestamptz]{
				Lower:     pgtype.Timestamptz{Time: now.Add(time.Hour), Valid: true},
				Upper:     pgtype.Timestamptz{Time: now.Add(2 * time.Hour), Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			RecordCount: 500,
			Priority:    1,
		},
	}

	for _, item := range workItems {
		err := db.PutMetricCompactionWork(ctx, item)
		require.NoError(t, err)
	}

	claimedBatch, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
		WorkerID:             workerID,
		DefaultTargetRecords: 2000,
		MaxAgeSeconds:        30,
		BatchCount:           5,
	})
	require.NoError(t, err)

	assert.Len(t, claimedBatch, 0, "Should not claim items when total records (1500) < target_records (2000) and items are fresh")
}

func TestClaimMetricCompactionWork_AgeThreshold(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)
	now := time.Now()

	err := db.PutMetricCompactionWork(ctx, lrdb.PutMetricCompactionWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    5000,
		SegmentID:      int64(12347),
		InstanceNum:    1,
		TsRange: pgtype.Range[pgtype.Timestamptz]{
			Lower:     pgtype.Timestamptz{Time: now, Valid: true},
			Upper:     pgtype.Timestamptz{Time: now.Add(time.Hour), Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		},
		RecordCount: 500,
		Priority:    1,
	})
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	claimedBatch, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
		WorkerID:             workerID,
		DefaultTargetRecords: 1000,
		MaxAgeSeconds:        1,
		BatchCount:           5,
	})
	require.NoError(t, err)

	assert.Len(t, claimedBatch, 1, "Should claim old items even when below target_records")
	if len(claimedBatch) > 0 {
		assert.Equal(t, int64(500), claimedBatch[0].RecordCount)
	}
}

func TestClaimMetricCompactionWork_OversizedItem(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)
	now := time.Now()

	workItems := []lrdb.PutMetricCompactionWorkParams{
		{
			OrganizationID: orgID,
			Dateint:        20250829,
			FrequencyMs:    5000,
			SegmentID:      int64(12346),
			InstanceNum:    1,
			TsRange: pgtype.Range[pgtype.Timestamptz]{
				Lower:     pgtype.Timestamptz{Time: now, Valid: true},
				Upper:     pgtype.Timestamptz{Time: now.Add(time.Hour), Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			RecordCount: 5000,
			Priority:    1,
		},
		{
			OrganizationID: orgID,
			Dateint:        20250829,
			FrequencyMs:    5000,
			SegmentID:      int64(12346),
			InstanceNum:    1,
			TsRange: pgtype.Range[pgtype.Timestamptz]{
				Lower:     pgtype.Timestamptz{Time: now.Add(time.Hour), Valid: true},
				Upper:     pgtype.Timestamptz{Time: now.Add(2 * time.Hour), Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			RecordCount: 500,
			Priority:    1,
		},
	}

	for _, item := range workItems {
		err := db.PutMetricCompactionWork(ctx, item)
		require.NoError(t, err)
	}

	claimedBatch, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
		WorkerID:             workerID,
		DefaultTargetRecords: 2000,
		MaxAgeSeconds:        30,
		BatchCount:           5,
	})
	require.NoError(t, err)

	assert.Len(t, claimedBatch, 1, "Should claim only the oversized item")
	assert.Equal(t, int64(5000), claimedBatch[0].RecordCount)
}

func TestClaimMetricCompactionWork_EmptyQueue(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	workerID := int64(12345)

	claimedBatch, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
		WorkerID:             workerID,
		DefaultTargetRecords: 3000,
		MaxAgeSeconds:        30,
		BatchCount:           5,
	})

	require.NoError(t, err)
	assert.Len(t, claimedBatch, 0)
}

func TestClaimMetricCompactionWork_Priority(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)
	now := time.Now()

	tsRange := pgtype.Range[pgtype.Timestamptz]{
		Lower:     pgtype.Timestamptz{Time: now, Valid: true},
		Upper:     pgtype.Timestamptz{Time: now.Add(time.Hour), Valid: true},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Valid:     true,
	}

	err := db.PutMetricCompactionWork(ctx, lrdb.PutMetricCompactionWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    5000,
		SegmentID:      int64(12347),
		InstanceNum:    1,
		TsRange:        tsRange,
		RecordCount:    1000,
		Priority:       1,
	})
	require.NoError(t, err)

	err = db.PutMetricCompactionWork(ctx, lrdb.PutMetricCompactionWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    5000,
		SegmentID:      int64(12347),
		InstanceNum:    1,
		TsRange:        tsRange,
		RecordCount:    1000,
		Priority:       5,
	})
	require.NoError(t, err)

	claimedBatch, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
		WorkerID:             workerID,
		DefaultTargetRecords: 1000,
		MaxAgeSeconds:        30,
		BatchCount:           1,
	})
	require.NoError(t, err)

	assert.Len(t, claimedBatch, 1)
	assert.Equal(t, int32(5), claimedBatch[0].Priority, "Should claim higher priority item first")
}

func TestReleaseMetricCompactionWork(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)
	now := time.Now()

	err := db.PutMetricCompactionWork(ctx, lrdb.PutMetricCompactionWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    5000,
		SegmentID:      int64(12347),
		InstanceNum:    1,
		TsRange: pgtype.Range[pgtype.Timestamptz]{
			Lower:     pgtype.Timestamptz{Time: now, Valid: true},
			Upper:     pgtype.Timestamptz{Time: now.Add(time.Hour), Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		},
		RecordCount: 1000,
		Priority:    1,
	})
	require.NoError(t, err)

	claimedBatch, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
		WorkerID:             workerID,
		DefaultTargetRecords: 1000,
		MaxAgeSeconds:        30,
		BatchCount:           5,
	})
	require.NoError(t, err)
	require.Len(t, claimedBatch, 1)

	claimedItem := claimedBatch[0]
	originalTries := claimedItem.Tries

	err = db.ReleaseMetricCompactionWork(ctx, lrdb.ReleaseMetricCompactionWorkParams{
		ID:        claimedItem.ID,
		ClaimedBy: workerID,
	})
	require.NoError(t, err)

	claimedBatch2, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
		WorkerID:             workerID + 1,
		DefaultTargetRecords: 1000,
		MaxAgeSeconds:        30,
		BatchCount:           5,
	})
	require.NoError(t, err)
	require.Len(t, claimedBatch2, 1)

	assert.Equal(t, originalTries+1, claimedBatch2[0].Tries, "Tries should increment after release")
	assert.Equal(t, workerID+1, claimedBatch2[0].ClaimedBy, "Released and reclaimed item should show new worker ID")
}

func TestReleaseMetricCompactionWork_OnlyReleasesByCorrectWorker(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	workerID := int64(12345)
	wrongWorkerID := int64(54321)
	now := time.Now()

	err := db.PutMetricCompactionWork(ctx, lrdb.PutMetricCompactionWorkParams{
		OrganizationID: orgID,
		Dateint:        20250829,
		FrequencyMs:    5000,
		SegmentID:      int64(12347),
		InstanceNum:    1,
		TsRange: pgtype.Range[pgtype.Timestamptz]{
			Lower:     pgtype.Timestamptz{Time: now, Valid: true},
			Upper:     pgtype.Timestamptz{Time: now.Add(time.Hour), Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		},
		RecordCount: 1000,
		Priority:    1,
	})
	require.NoError(t, err)

	claimedBatch, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
		WorkerID:             workerID,
		DefaultTargetRecords: 1000,
		MaxAgeSeconds:        30,
		BatchCount:           5,
	})
	require.NoError(t, err)
	require.Len(t, claimedBatch, 1)

	claimedItem := claimedBatch[0]

	err = db.ReleaseMetricCompactionWork(ctx, lrdb.ReleaseMetricCompactionWorkParams{
		ID:        claimedItem.ID,
		ClaimedBy: wrongWorkerID,
	})
	require.NoError(t, err)

	claimedBatch2, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
		WorkerID:             workerID + 1,
		DefaultTargetRecords: 1000,
		MaxAgeSeconds:        30,
		BatchCount:           5,
	})
	require.NoError(t, err)

	assert.Len(t, claimedBatch2, 0, "Item should not be released by wrong worker")
}
