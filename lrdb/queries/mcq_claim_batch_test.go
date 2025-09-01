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
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/testhelpers"
)

func TestClaimMetricCompactionWork_ImprovedFromTestData(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	// Load test data from file
	err := loadMetricCompactionQueueTestData(ctx, db.(*lrdb.Store))
	require.NoError(t, err, "Failed to load test data")

	// Verify data was loaded
	var rowCount int
	err = db.(*lrdb.Store).Pool().QueryRow(ctx, "SELECT COUNT(*) FROM metric_compaction_queue").Scan(&rowCount)
	require.NoError(t, err)
	fmt.Printf("Loaded %d rows into metric_compaction_queue\n", rowCount)

	// Show a few sample rows to verify the data looks correct
	rows, err := db.(*lrdb.Store).Pool().Query(ctx, `
		SELECT id, queue_ts, priority, organization_id, record_count, claimed_at 
		FROM metric_compaction_queue 
		ORDER BY queue_ts 
		LIMIT 3
	`)
	require.NoError(t, err)
	defer rows.Close()

	fmt.Printf("Sample rows:\n")
	for rows.Next() {
		var id int64
		var queueTs time.Time
		var priority int32
		var orgID uuid.UUID
		var recordCount int64
		var claimedAt *time.Time

		err = rows.Scan(&id, &queueTs, &priority, &orgID, &recordCount, &claimedAt)
		require.NoError(t, err)

		fmt.Printf("  ID=%d, QueueTS=%s, Priority=%d, OrgID=%s, Records=%d, ClaimedAt=%v\n",
			id, queueTs.Format("15:04:05.000"), priority, orgID.String()[:8]+"...", recordCount, claimedAt)
	}

	workerID := int64(99999)

	// Test different time offsets to understand the age threshold behavior
	lastDataTime := time.Date(2025, 9, 1, 15, 12, 55, 809443000, time.UTC)
	testTimes := []struct {
		name   string
		time   time.Time
		maxAge int32
	}{
		{"1 min after, 5min age", lastDataTime.Add(1 * time.Minute), 300},
		{"6 min after, 5min age", lastDataTime.Add(6 * time.Minute), 300},
		{"1 min after, 30sec age", lastDataTime.Add(1 * time.Minute), 30},
		{"Current time, 5min age", time.Now(), 300},
	}

	for _, tt := range testTimes {
		fmt.Printf("\n=== IMPROVED QUERY Test: %s ===\n", tt.name)
		fmt.Printf("Query time: %s\n", tt.time.Format(time.RFC3339Nano))
		fmt.Printf("Max age seconds: %d\n", tt.maxAge)

		// Run several batches to see what gets claimed
		for batchNum := 1; batchNum <= 3; batchNum++ {
			fmt.Printf("\n--- Batch %d ---\n", batchNum)

			claimedBatch, err := db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
				WorkerID:             workerID,
				DefaultTargetRecords: 40000,
				MaxAgeSeconds:        tt.maxAge,
				BatchCount:           50,
				NowTs:                &tt.time,
			})
			require.NoError(t, err)

			if len(claimedBatch) == 0 {
				fmt.Printf("No items claimed in batch %d\n", batchNum)
				break
			}

			fmt.Printf("Claimed %d items in batch %d:\n", len(claimedBatch), batchNum)

			totalRecords := int64(0)
			for i, item := range claimedBatch {
				if i < 10 { // Show first 10 items details
					fmt.Printf("  [%d] ID=%d, OrgID=%s, Priority=%d, Records=%d, QueueTS=%s\n",
						i+1, item.ID, item.OrganizationID.String()[:8]+"...",
						item.Priority, item.RecordCount, item.QueueTs.Format("15:04:05.000"))
				}
				totalRecords += item.RecordCount
			}

			if len(claimedBatch) > 10 {
				fmt.Printf("  ... and %d more items\n", len(claimedBatch)-10)
			}

			fmt.Printf("Total records in batch: %d\n", totalRecords)
			fmt.Printf("Target records used: %d (%.1f%% of target)\n",
				claimedBatch[0].UsedTargetRecords,
				float64(totalRecords)/float64(claimedBatch[0].UsedTargetRecords)*100)
			fmt.Printf("Estimate source: %s\n", claimedBatch[0].EstimateSource)

			// Group by organization and show distribution
			orgCounts := make(map[string]int)
			for _, item := range claimedBatch {
				orgKey := item.OrganizationID.String()[:8]
				orgCounts[orgKey]++
			}

			fmt.Printf("Organization distribution:\n")
			for orgKey, count := range orgCounts {
				fmt.Printf("  %s...: %d items\n", orgKey, count)
			}

			// Check if we're getting efficient batches
			targetRecords := claimedBatch[0].UsedTargetRecords
			efficiency := float64(totalRecords) / float64(targetRecords)
			if efficiency >= 1.0 && efficiency <= 1.2 {
				fmt.Printf("✓ Efficient batch: %.1f%% of target (within 100-120%% range)\n", efficiency*100)
			} else {
				fmt.Printf("⏰ Processed regardless of size (likely old items)\n")
			}
		}
	}
}

func loadMetricCompactionQueueTestData(ctx context.Context, db *lrdb.Store) error {
	file, err := os.Open("testdata/mcq/queue-test.txt")
	if err != nil {
		return fmt.Errorf("failed to open test data file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	rowCount := 0
	// Skip the header lines until we find the data
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Data rows should contain pipe separators and start with a number
		if strings.Contains(line, "|") && len(line) > 0 {
			// Check if the first non-space character is a digit (this is a data row)
			trimmed := strings.TrimLeft(line, " ")
			if len(trimmed) > 0 && trimmed[0] >= '0' && trimmed[0] <= '9' {
				// This looks like a data row, parse it
				if err := parseAndInsertMetricCompactionRow(ctx, db, line); err != nil {
					fmt.Printf("Failed to parse row: %s, error: %v\n", line, err)
					continue
				}
				rowCount++
				if rowCount <= 3 {
					displayLine := line
					if len(line) > 100 {
						displayLine = line[:100] + "..."
					}
					fmt.Printf("Successfully loaded row %d: %s\n", rowCount, displayLine)
				}
			}
		}
	}

	fmt.Printf("Total rows processed: %d\n", rowCount)

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}

	return nil
}

func parseAndInsertMetricCompactionRow(ctx context.Context, db *lrdb.Store, line string) error {
	// Parse the pipe-separated values
	parts := strings.Split(line, "|")
	if len(parts) < 13 {
		return nil // Skip malformed lines
	}

	// Trim spaces from each part
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}

	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil // Skip header or malformed lines
	}

	queueTs, err := time.Parse("2006-01-02 15:04:05.999999+00", parts[1])
	if err != nil {
		return fmt.Errorf("failed to parse queue_ts: %w", err)
	}

	priority, err := strconv.ParseInt(parts[2], 10, 32)
	if err != nil {
		return fmt.Errorf("failed to parse priority: %w", err)
	}

	orgID, err := uuid.Parse(parts[3])
	if err != nil {
		return fmt.Errorf("failed to parse organization_id: %w", err)
	}

	dateint, err := strconv.ParseInt(parts[4], 10, 32)
	if err != nil {
		return fmt.Errorf("failed to parse dateint: %w", err)
	}

	frequencyMs, err := strconv.ParseInt(parts[5], 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse frequency_ms: %w", err)
	}

	segmentID, err := strconv.ParseInt(parts[6], 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse segment_id: %w", err)
	}

	instanceNum, err := strconv.ParseInt(parts[7], 10, 16)
	if err != nil {
		return fmt.Errorf("failed to parse instance_num: %w", err)
	}

	recordCount, err := strconv.ParseInt(parts[8], 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse record_count: %w", err)
	}

	// Insert directly into the database using raw SQL since we need to set specific IDs and timestamps
	query := `
		INSERT INTO metric_compaction_queue 
		(id, queue_ts, priority, organization_id, dateint, frequency_ms, segment_id, instance_num, record_count, tries, claimed_by, claimed_at, heartbeated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 0, -1, NULL, NULL)
	`

	_, err = db.Pool().Exec(ctx, query, id, queueTs, int32(priority), orgID, int32(dateint), frequencyMs, segmentID, int16(instanceNum), recordCount)
	if err != nil {
		return fmt.Errorf("failed to insert row: %w", err)
	}

	return nil
}
