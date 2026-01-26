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
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/testhelpers"
)

func TestMetricSegEstimator(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	now := time.Now().UTC()
	dateint := int32(now.Year()*10000 + int(now.Month())*100 + now.Day())

	// Test case: Overhead-aware calculation verification
	// The estimator now accounts for 15K per-file overhead
	// Formula: bytes_per_record = (total_bytes - (file_count * 15000)) / total_records
	// Then: target_records = CEIL((1,000,000 - 15,000) / bytes_per_record)
	const estimatedOverhead = 15_000
	const targetBytes = config.TargetFileSize
	const effectiveTargetBytes = targetBytes - estimatedOverhead // 985,000

	testCases := []struct {
		name            string
		fileSize        int64
		recordCount     int64
		expectedBPR     float64
		expectedRecords int64
	}{
		{
			name:            "100 bytes per record after overhead",
			fileSize:        100_000 + estimatedOverhead, // 115KB file (100KB + 15KB overhead)
			recordCount:     1_000,                       // 1K records
			expectedBPR:     100.0,                       // (115K - 15K) / 1K = 100 bytes per record
			expectedRecords: 9850,                        // CEIL(985,000 / 100) = 9,850
		},
		{
			name:            "50 bytes per record after overhead",
			fileSize:        50_000 + estimatedOverhead, // 65KB file (50KB + 15KB overhead)
			recordCount:     1_000,                      // 1K records
			expectedBPR:     50.0,                       // (65K - 15K) / 1K = 50 bytes per record
			expectedRecords: 19700,                      // CEIL(985,000 / 50) = 19,700
		},
		{
			name:            "200 bytes per record after overhead",
			fileSize:        200_000 + estimatedOverhead, // 215KB file (200KB + 15KB overhead)
			recordCount:     1_000,                       // 1K records
			expectedBPR:     200.0,                       // (215K - 15K) / 1K = 200 bytes per record
			expectedRecords: 4925,                        // CEIL(985,000 / 200) = 4,925
		},
	}

	for i, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Use unique orgID for each test
			testOrgID := uuid.New()

			// Insert test data
			err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
				OrganizationID: testOrgID,
				Dateint:        dateint,
				FrequencyMs:    10000,           // 10s frequency
				SegmentID:      int64(1000 + i), // unique segment ID
				InstanceNum:    1,
				StartTs:        now.Add(-30 * time.Minute).UnixMilli(),
				EndTs:          now.Add(-29 * time.Minute).UnixMilli(),
				RecordCount:    tc.recordCount,
				FileSize:       tc.fileSize,
				CreatedBy:      lrdb.CreatedByIngest,
				Published:      true,
				Fingerprints:   []int64{123, 456},
				SortVersion:    lrdb.CurrentMetricSortVersion,
				Compacted:      false,
			})
			require.NoError(t, err)

			// Run the estimator
			result, err := db.MetricSegEstimator(ctx, lrdb.MetricSegEstimatorParams{
				TargetBytes: float64(config.TargetFileSize),
				DateintLow:  dateint,
				DateintHigh: dateint,
				MsLow:       now.Add(-1 * time.Hour).UnixMilli(),
				MsHigh:      now.UnixMilli(),
			})
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(result), 1, "Should have at least one result")

			// Find the result for our test organization
			var row *lrdb.MetricSegEstimatorRow
			for _, r := range result {
				if r.OrganizationID == testOrgID && r.FrequencyMs == 10000 {
					row = &r
					break
				}
			}
			require.NotNil(t, row, "Should find result for our test organization")

			// The key test: verify the overhead-aware math
			// bytes_per_record = (file_size - overhead) / record_count
			actualBPRAfterOverhead := float64(tc.fileSize-estimatedOverhead) / float64(tc.recordCount)
			// target_records = CEIL((target_bytes - overhead) / bytes_per_record)
			expectedRecordsFloat := float64(effectiveTargetBytes) / actualBPRAfterOverhead
			expectedRecordsCeil := int64(expectedRecordsFloat)
			if expectedRecordsFloat > float64(expectedRecordsCeil) {
				expectedRecordsCeil++
			}
			// But never less than 1000 (as per the GREATEST clause)
			if expectedRecordsCeil < 1000 {
				expectedRecordsCeil = 1000
			}

			t.Logf("Test case: %s", tc.name)
			t.Logf("File size: %d bytes", tc.fileSize)
			t.Logf("Record count: %d", tc.recordCount)
			t.Logf("BPR after overhead: %.2f", actualBPRAfterOverhead)
			t.Logf("Expected records for 985KB: %.2f", expectedRecordsFloat)
			t.Logf("Expected records (ceil): %d", expectedRecordsCeil)
			t.Logf("Actual result: %d", row.EstimatedRecords)
			t.Logf("Total results returned: %d", len(result))

			assert.Equal(t, expectedRecordsCeil, row.EstimatedRecords,
				"EstimatedRecords should be CEIL((985KB) / overhead_adjusted_bytes_per_record)")
		})
	}
}

func TestMetricSegEstimatorMultipleFiles(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now().UTC()
	dateint := int32(now.Year()*10000 + int(now.Month())*100 + now.Day())

	// Insert multiple files with different BPR to test averaging (with overhead)
	// Files now include overhead - estimator will subtract it out
	const estimatedOverhead = 15_000
	files := []struct {
		fileSize    int64
		recordCount int64
	}{
		{100_000 + estimatedOverhead, 1_000}, // 100 BPR after overhead
		{200_000 + estimatedOverhead, 1_000}, // 200 BPR after overhead
		{150_000 + estimatedOverhead, 1_000}, // 150 BPR after overhead
	}
	// Total bytes = 450,000 + (3 * 15,000) = 495,000
	// Total records = 3,000
	// Overhead-adjusted BPR = (495,000 - 45,000) / 3,000 = 450,000 / 3,000 = 150
	// Expected records for effective target = (985,000) / 150 = 6,566.67, ceil = 6,567

	for i, f := range files {
		err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
			OrganizationID: orgID,
			Dateint:        dateint,
			FrequencyMs:    10000,
			SegmentID:      int64(2000 + i), // unique segment ID
			InstanceNum:    1,
			StartTs:        now.Add(time.Duration(-30+i) * time.Minute).UnixMilli(),
			EndTs:          now.Add(time.Duration(-29+i) * time.Minute).UnixMilli(),
			RecordCount:    f.recordCount,
			FileSize:       f.fileSize,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Fingerprints:   []int64{123, 456},
			SortVersion:    lrdb.CurrentMetricSortVersion,
			Compacted:      false,
		})
		require.NoError(t, err)
	}

	// Run the estimator
	result, err := db.MetricSegEstimator(ctx, lrdb.MetricSegEstimatorParams{
		TargetBytes: float64(config.TargetFileSize),
		DateintLow:  dateint,
		DateintHigh: dateint,
		MsLow:       now.Add(-1 * time.Hour).UnixMilli(),
		MsHigh:      now.UnixMilli(),
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(result), 1, "Should have at least one result")

	// Find the result for our test organization
	var row *lrdb.MetricSegEstimatorRow
	for _, r := range result {
		if r.OrganizationID == orgID && r.FrequencyMs == 10000 {
			row = &r
			break
		}
	}
	require.NotNil(t, row, "Should find result for our test organization")

	// Calculate expected: (sum(file_size) - (file_count * overhead)) / sum(record_count) = avg BPR after overhead
	totalFileSize := int64((100_000 + estimatedOverhead) + (200_000 + estimatedOverhead) + (150_000 + estimatedOverhead)) // 495,000
	totalRecords := int64(1_000 + 1_000 + 1_000)                                                                          // 3,000
	fileCount := int64(3)
	totalOverhead := fileCount * estimatedOverhead                                      // 45,000
	avgBPRAfterOverhead := float64(totalFileSize-totalOverhead) / float64(totalRecords) // (495,000 - 45,000) / 3,000 = 150.0
	effectiveTargetBytes := config.TargetFileSize - estimatedOverhead                   // Target - overhead
	expectedRecordsFloat := float64(effectiveTargetBytes) / avgBPRAfterOverhead         // 985,000 / 150 = 6,566.67
	expectedRecords := int64(math.Ceil(expectedRecordsFloat))                           // ceil(6566.67) = 6567

	t.Logf("Total file size: %d bytes", totalFileSize)
	t.Logf("Total records: %d", totalRecords)
	t.Logf("File count: %d", fileCount)
	t.Logf("Total overhead: %d bytes", totalOverhead)
	t.Logf("Average BPR after overhead: %.2f", avgBPRAfterOverhead)
	t.Logf("Effective target bytes: %d", effectiveTargetBytes)
	t.Logf("Expected records for effective target: %.2f", expectedRecordsFloat)
	t.Logf("Expected records (ceil): %d", expectedRecords)
	t.Logf("Actual result: %d", row.EstimatedRecords)
	t.Logf("Total results returned: %d", len(result))

	assert.Equal(t, expectedRecords, row.EstimatedRecords)
}
