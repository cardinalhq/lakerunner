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

package workqueue

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/dbopen"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// TestWorkQueueDepthAll_UsesIndex verifies the query plan uses an index scan
func TestWorkQueueDepthAll_UsesIndex(t *testing.T) {
	ctx := context.Background()

	// Connect to test database
	pool, err := lrdb.ConnectTolrdb(ctx, dbopen.SkipMigrationCheck())
	require.NoError(t, err)
	defer pool.Close()

	db := lrdb.New(pool)

	// Add some test data
	orgID := uuid.New()
	_, err = db.WorkQueueAdd(ctx, lrdb.WorkQueueAddParams{
		TaskName:       config.BoxerTaskIngestLogs,
		OrganizationID: orgID,
		InstanceNum:    1,
		Spec:           map[string]any{},
	})
	require.NoError(t, err)

	_, err = db.WorkQueueAdd(ctx, lrdb.WorkQueueAddParams{
		TaskName:       config.BoxerTaskCompactMetrics,
		OrganizationID: orgID,
		InstanceNum:    1,
		Spec:           map[string]any{},
	})
	require.NoError(t, err)

	// Disable sequential scans to force index usage (table is too small for planner to choose index)
	_, err = pool.Exec(ctx, "SET enable_seqscan = off")
	require.NoError(t, err)

	// Get the full query plan
	query := `
		EXPLAIN
		SELECT task_name, COUNT(*) as depth
		  FROM work_queue
		 WHERE claimed_by = -1
		   AND failed = false
		 GROUP BY task_name
	`
	rows, err := pool.Query(ctx, query)
	require.NoError(t, err)
	defer rows.Close()

	var planLines []string
	for rows.Next() {
		var line string
		err = rows.Scan(&line)
		require.NoError(t, err)
		planLines = append(planLines, line)
	}
	require.NoError(t, rows.Err())

	fullPlan := strings.Join(planLines, "\n")

	// Verify the plan uses an index scan
	// The query should use idx_work_queue_monitoring or idx_work_queue_claim
	planLower := strings.ToLower(fullPlan)
	hasIndexScan := strings.Contains(planLower, "index scan") ||
		strings.Contains(planLower, "index only scan") ||
		strings.Contains(planLower, "bitmap index scan")

	assert.True(t, hasIndexScan, "Query plan should use an index scan, got:\n%s", fullPlan)

	// Clean up
	_, err = pool.Exec(ctx, "DELETE FROM work_queue WHERE organization_id = $1", orgID)
	require.NoError(t, err)
}

// TestWorkQueueDepthAll_Integration verifies the query works correctly
func TestWorkQueueDepthAll_Integration(t *testing.T) {
	ctx := context.Background()

	// Connect to test database
	pool, err := lrdb.ConnectTolrdb(ctx, dbopen.SkipMigrationCheck())
	require.NoError(t, err)
	defer pool.Close()

	db := lrdb.New(pool)

	// Add test work items for different tasks
	orgID := uuid.New()
	tasksToAdd := map[string]int{
		config.BoxerTaskIngestLogs:     5,
		config.BoxerTaskCompactMetrics: 3,
		config.BoxerTaskRollupMetrics:  2,
	}

	for taskName, count := range tasksToAdd {
		for i := 0; i < count; i++ {
			_, err = db.WorkQueueAdd(ctx, lrdb.WorkQueueAddParams{
				TaskName:       taskName,
				OrganizationID: orgID,
				InstanceNum:    int16(i),
				Spec:           map[string]any{},
			})
			require.NoError(t, err)
		}
	}

	// Get depths
	depths, err := db.WorkQueueDepthAll(ctx)
	require.NoError(t, err)

	// Convert to map for easier verification
	depthMap := make(map[string]int64)
	for _, d := range depths {
		depthMap[d.TaskName] = d.Depth
	}

	// Verify depths (should at least include our test data)
	assert.GreaterOrEqual(t, depthMap[config.BoxerTaskIngestLogs], int64(5))
	assert.GreaterOrEqual(t, depthMap[config.BoxerTaskCompactMetrics], int64(3))
	assert.GreaterOrEqual(t, depthMap[config.BoxerTaskRollupMetrics], int64(2))

	// Clean up
	_, err = pool.Exec(ctx, "DELETE FROM work_queue WHERE organization_id = $1", orgID)
	require.NoError(t, err)
}
