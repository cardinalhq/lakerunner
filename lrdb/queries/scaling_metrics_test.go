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

func TestWorkQueueScalingDepth(t *testing.T) {
	ctx := context.Background()
	store := testhelpers.NewTestLRDBStore(t)
	db := store.(*lrdb.Store) // Cast to concrete type to access Pool()

	orgID := uuid.New()
	now := time.Now()

	tests := []struct {
		name          string
		signal        lrdb.SignalEnum
		action        lrdb.ActionEnum
		setupData     []testWorkQueueItem
		expectedCount int64
	}{
		{
			name:          "empty work queue",
			signal:        lrdb.SignalEnumMetrics,
			action:        lrdb.ActionEnumCompact,
			setupData:     []testWorkQueueItem{},
			expectedCount: 0,
		},
		{
			name:   "metrics compact queue with runnable items",
			signal: lrdb.SignalEnumMetrics,
			action: lrdb.ActionEnumCompact,
			setupData: []testWorkQueueItem{
				{
					orgID:      orgID,
					signal:     lrdb.SignalEnumMetrics,
					action:     lrdb.ActionEnumCompact,
					needsRun:   true,
					runnableAt: now.Add(-1 * time.Minute), // Past time, should be runnable
					slotID:     1,
				},
				{
					orgID:      orgID,
					signal:     lrdb.SignalEnumMetrics,
					action:     lrdb.ActionEnumCompact,
					needsRun:   true,
					runnableAt: now.Add(-30 * time.Second), // Past time, should be runnable
					slotID:     2,
				},
			},
			expectedCount: 2,
		},
		{
			name:   "metrics rollup queue with mixed conditions",
			signal: lrdb.SignalEnumMetrics,
			action: lrdb.ActionEnumRollup,
			setupData: []testWorkQueueItem{
				{
					orgID:      orgID,
					signal:     lrdb.SignalEnumMetrics,
					action:     lrdb.ActionEnumRollup,
					needsRun:   true,
					runnableAt: now.Add(-1 * time.Minute), // Runnable
					slotID:     1,
				},
				{
					orgID:      orgID,
					signal:     lrdb.SignalEnumMetrics,
					action:     lrdb.ActionEnumRollup,
					needsRun:   false, // Not needed, should not count
					runnableAt: now.Add(-1 * time.Minute),
					slotID:     2,
				},
				{
					orgID:      orgID,
					signal:     lrdb.SignalEnumMetrics,
					action:     lrdb.ActionEnumRollup,
					needsRun:   true,
					runnableAt: now.Add(1 * time.Hour), // Future time, should not count
					slotID:     3,
				},
				{
					orgID:      orgID,
					signal:     lrdb.SignalEnumLogs, // Different signal, should not count
					action:     lrdb.ActionEnumCompact,
					needsRun:   true,
					runnableAt: now.Add(-1 * time.Minute),
					slotID:     4,
				},
			},
			expectedCount: 1,
		},
		{
			name:   "logs compact queue",
			signal: lrdb.SignalEnumLogs,
			action: lrdb.ActionEnumCompact,
			setupData: []testWorkQueueItem{
				{
					orgID:      orgID,
					signal:     lrdb.SignalEnumLogs,
					action:     lrdb.ActionEnumCompact,
					needsRun:   true,
					runnableAt: now.Add(-5 * time.Minute),
					slotID:     1,
				},
			},
			expectedCount: 1,
		},
		{
			name:   "traces compact queue",
			signal: lrdb.SignalEnumTraces,
			action: lrdb.ActionEnumCompact,
			setupData: []testWorkQueueItem{
				{
					orgID:      orgID,
					signal:     lrdb.SignalEnumTraces,
					action:     lrdb.ActionEnumCompact,
					needsRun:   true,
					runnableAt: now.Add(-2 * time.Minute),
					slotID:     1,
				},
				{
					orgID:      orgID,
					signal:     lrdb.SignalEnumTraces,
					action:     lrdb.ActionEnumCompact,
					needsRun:   true,
					runnableAt: now.Add(-1 * time.Minute),
					slotID:     2,
				},
			},
			expectedCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean up any existing data
			_, err := db.Pool().Exec(ctx, "TRUNCATE TABLE work_queue")
			require.NoError(t, err)

			// Setup test data
			for _, item := range tt.setupData {
				err := addWorkQueueItem(ctx, db, item)
				require.NoError(t, err)
			}

			// Test the query
			result, err := db.WorkQueueScalingDepth(ctx, lrdb.WorkQueueScalingDepthParams{
				Signal: tt.signal,
				Action: tt.action,
			})
			require.NoError(t, err)

			count, ok := result.(int64)
			require.True(t, ok, "Expected int64 result, got %T", result)
			assert.Equal(t, tt.expectedCount, count)
		})
	}
}

// Helper struct for test data
type testWorkQueueItem struct {
	orgID      uuid.UUID
	signal     lrdb.SignalEnum
	action     lrdb.ActionEnum
	needsRun   bool
	runnableAt time.Time
	slotID     int32
}

// Helper function to add work queue items directly
func addWorkQueueItem(ctx context.Context, store lrdb.StoreFull, item testWorkQueueItem) error {
	db := store.(*lrdb.Store) // Cast to concrete type to access Pool()
	// Create a time range for the work item
	start := item.runnableAt
	end := start.Add(1 * time.Hour)
	tsRange := pgtype.Range[pgtype.Timestamptz]{
		Lower:     pgtype.Timestamptz{Time: start, Valid: true},
		Upper:     pgtype.Timestamptz{Time: end, Valid: true},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Valid:     true,
	}

	query := `
		INSERT INTO work_queue (
			organization_id, instance_num, dateint, frequency_ms, signal, action,
			needs_run, tries, ts_range, claimed_by, runnable_at, heartbeated_at,
			priority, slot_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
	`

	_, err := db.Pool().Exec(ctx, query,
		item.orgID,      // organization_id
		int16(1),        // instance_num
		20250101,        // dateint
		int32(60000),    // frequency_ms (1 minute)
		item.signal,     // signal
		item.action,     // action
		item.needsRun,   // needs_run
		int32(0),        // tries
		tsRange,         // ts_range
		int64(-1),       // claimed_by (unclaimed)
		item.runnableAt, // runnable_at
		time.Now(),      // heartbeated_at
		int32(0),        // priority
		item.slotID,     // slot_id
	)

	return err
}
