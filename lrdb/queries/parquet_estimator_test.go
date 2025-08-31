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

func TestMetricSegEstimator(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	now := time.Now().UTC()
	dateint := int32(now.Year()*10000 + int(now.Month())*100 + now.Day())

	// Test case: Simple math verification
	// If we have files with known bytes per record, the estimator should calculate
	// how many records we need for a 1MB target file
	testCases := []struct {
		name            string
		fileSize        int64
		recordCount     int64
		expectedBPR     float64
		expectedRecords int64
	}{
		{
			name:            "100 bytes per record",
			fileSize:        100_000, // 100KB file
			recordCount:     1_000,   // 1K records
			expectedBPR:     100.0,   // 100 bytes per record
			expectedRecords: 10_000,  // 1MB / 100 bytes = 10K records
		},
		{
			name:            "50 bytes per record",
			fileSize:        50_000, // 50KB file
			recordCount:     1_000,  // 1K records
			expectedBPR:     50.0,   // 50 bytes per record
			expectedRecords: 20_000, // 1MB / 50 bytes = 20K records
		},
		{
			name:            "200 bytes per record",
			fileSize:        200_000, // 200KB file
			recordCount:     1_000,   // 1K records
			expectedBPR:     200.0,   // 200 bytes per record
			expectedRecords: 5_000,   // 1MB / 200 bytes = 5K records
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
				IngestDateint:  dateint,
				FrequencyMs:    10000,           // 10s frequency
				SegmentID:      int64(1000 + i), // unique segment ID
				InstanceNum:    1,
				SlotID:         0,
				StartTs:        now.Add(-30 * time.Minute).UnixMilli(),
				EndTs:          now.Add(-29 * time.Minute).UnixMilli(),
				RecordCount:    tc.recordCount,
				FileSize:       tc.fileSize,
				CreatedBy:      lrdb.CreatedByIngest,
				Published:      true,
				Fingerprints:   []int64{123, 456},
				SortVersion:    lrdb.CurrentMetricSortVersion,
				SlotCount:      1,
				Compacted:      false,
			})
			require.NoError(t, err)

			// Run the estimator
			result, err := db.MetricSegEstimator(ctx, lrdb.MetricSegEstimatorParams{
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

			// The key test: verify the math
			actualBPR := float64(tc.fileSize) / float64(tc.recordCount)
			expectedRecordsFloat := 1_000_000.0 / actualBPR
			expectedRecordsCeil := int64(expectedRecordsFloat)
			if expectedRecordsFloat > float64(expectedRecordsCeil) {
				expectedRecordsCeil++
			}

			t.Logf("Test case: %s", tc.name)
			t.Logf("File size: %d bytes", tc.fileSize)
			t.Logf("Record count: %d", tc.recordCount)
			t.Logf("Actual BPR: %.2f", actualBPR)
			t.Logf("Expected records for 1MB: %.2f", expectedRecordsFloat)
			t.Logf("Expected records (ceil): %d", expectedRecordsCeil)
			t.Logf("Actual result: %d", row.EstimatedRecords)
			t.Logf("Total results returned: %d", len(result))

			assert.Equal(t, expectedRecordsCeil, row.EstimatedRecords,
				"EstimatedRecords should be CEIL(1MB / bytes_per_record)")
		})
	}
}

func TestMetricSegEstimatorMultipleFiles(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now().UTC()
	dateint := int32(now.Year()*10000 + int(now.Month())*100 + now.Day())

	// Insert multiple files with different BPR to test averaging
	files := []struct {
		fileSize    int64
		recordCount int64
	}{
		{100_000, 1_000}, // 100 BPR
		{200_000, 1_000}, // 200 BPR
		{150_000, 1_000}, // 150 BPR
	}
	// Average BPR should be (100 + 200 + 150) / 3 = 150
	// Expected records for 1MB = 1,000,000 / 150 = 6,666.67, ceil = 6,667

	for i, f := range files {
		err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
			OrganizationID: orgID,
			Dateint:        dateint,
			IngestDateint:  dateint,
			FrequencyMs:    10000,
			SegmentID:      int64(2000 + i), // unique segment ID
			InstanceNum:    1,
			SlotID:         0,
			StartTs:        now.Add(time.Duration(-30+i) * time.Minute).UnixMilli(),
			EndTs:          now.Add(time.Duration(-29+i) * time.Minute).UnixMilli(),
			RecordCount:    f.recordCount,
			FileSize:       f.fileSize,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Fingerprints:   []int64{123, 456},
			SortVersion:    lrdb.CurrentMetricSortVersion,
			SlotCount:      1,
			Compacted:      false,
		})
		require.NoError(t, err)
	}

	// Run the estimator
	result, err := db.MetricSegEstimator(ctx, lrdb.MetricSegEstimatorParams{
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

	// Calculate expected: sum(file_size) / sum(record_count) = avg BPR
	totalFileSize := int64(100_000 + 200_000 + 150_000)      // 450,000
	totalRecords := int64(1_000 + 1_000 + 1_000)             // 3,000
	avgBPR := float64(totalFileSize) / float64(totalRecords) // 150.0
	expectedRecords := int64(1_000_000.0/avgBPR + 0.5)       // ceil(6666.67) = 6667

	t.Logf("Total file size: %d bytes", totalFileSize)
	t.Logf("Total records: %d", totalRecords)
	t.Logf("Average BPR: %.2f", avgBPR)
	t.Logf("Expected records for 1MB: %.2f", 1_000_000.0/avgBPR)
	t.Logf("Expected records (ceil): %d", expectedRecords)
	t.Logf("Actual result: %d", row.EstimatedRecords)
	t.Logf("Total results returned: %d", len(result))

	assert.Equal(t, expectedRecords, row.EstimatedRecords)
}
