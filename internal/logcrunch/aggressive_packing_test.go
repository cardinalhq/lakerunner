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

package logcrunch

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// TestAggressivePackingImprovements tests the effect of changing the file size filter from 90% to 100%
// and increasing the record count threshold by 10% to allow for compression variability
func TestAggressivePackingImprovements(t *testing.T) {
	// Use same production data
	productionSegments := []lrdb.GetLogSegmentsForCompactionRow{
		// Very small segments (1-10 records)
		{SegmentID: 298474784809812455, StartTs: 1755741609646, EndTs: 1755741613411, RecordCount: 4, FileSize: 16457},
		{SegmentID: 298474816099320295, StartTs: 1755741637915, EndTs: 1755741637953, RecordCount: 2, FileSize: 16306},
		{SegmentID: 298474833514070503, StartTs: 1755741643093, EndTs: 1755741643136, RecordCount: 2, FileSize: 16138},
		{SegmentID: 298474885036900839, StartTs: 1755741669645, EndTs: 1755741669646, RecordCount: 1, FileSize: 15990},
		{SegmentID: 298474881480131047, StartTs: 1755741673188, EndTs: 1755741673333, RecordCount: 13, FileSize: 17334},
		{SegmentID: 298474935167222247, StartTs: 1755741697915, EndTs: 1755741703117, RecordCount: 7, FileSize: 17304},
		{SegmentID: 298474985800860135, StartTs: 1755741729645, EndTs: 1755741729646, RecordCount: 1, FileSize: 15990},
		{SegmentID: 298474986891379175, StartTs: 1755741733195, EndTs: 1755741733420, RecordCount: 3, FileSize: 16375},

		// Small to medium segments (100-1000 records)
		{SegmentID: 298474768468804071, StartTs: 1755741600001, EndTs: 1755741609642, RecordCount: 594, FileSize: 71692},
		{SegmentID: 298474819286991335, StartTs: 1755741614640, EndTs: 1755741638941, RecordCount: 638, FileSize: 67062},
		{SegmentID: 298474852891754983, StartTs: 1755741635333, EndTs: 1755741659033, RecordCount: 662, FileSize: 67623},
		{SegmentID: 298474869366981095, StartTs: 1755741644936, EndTs: 1755741669129, RecordCount: 893, FileSize: 75646},
		{SegmentID: 298474882302214631, StartTs: 1755741657039, EndTs: 1755741678151, RecordCount: 556, FileSize: 65548},

		// Medium segments (1000-2000 records)
		{SegmentID: 298474769995530727, StartTs: 1755741600005, EndTs: 1755741608644, RecordCount: 1346, FileSize: 78081},
		{SegmentID: 298474786906964455, StartTs: 1755741600029, EndTs: 1755741619738, RecordCount: 1087, FileSize: 76721},
		{SegmentID: 298474785967440359, StartTs: 1755741601236, EndTs: 1755741619005, RecordCount: 1589, FileSize: 93291},
		{SegmentID: 298474798550352359, StartTs: 1755741603929, EndTs: 1755741629735, RecordCount: 1089, FileSize: 73296},
		{SegmentID: 298474835795771879, StartTs: 1755741624129, EndTs: 1755741647939, RecordCount: 1316, FileSize: 90518},
		{SegmentID: 298474834889802215, StartTs: 1755741624931, EndTs: 1755741648936, RecordCount: 2184, FileSize: 105720},

		// Larger segments (2000+ records)
		{SegmentID: 298474799942861287, StartTs: 1755741604534, EndTs: 1755741629010, RecordCount: 2138, FileSize: 113018},
		{SegmentID: 298474817575715303, StartTs: 1755741614029, EndTs: 1755741639941, RecordCount: 2599, FileSize: 116663},
		{SegmentID: 298474852287775207, StartTs: 1755741634253, EndTs: 1755741658236, RecordCount: 2316, FileSize: 115292},
		{SegmentID: 298474868477788647, StartTs: 1755741642851, EndTs: 1755741669145, RecordCount: 2328, FileSize: 111414},
	}

	// Calculate realistic bytes per record (accounting for 15KB overhead, >100 records only)
	avgBytesPerRecord := calculateAvgBytesPerRecord(productionSegments)

	// Calculate realistic record counts for different scenarios
	targetFileSize := int64(1_100_000) // 1.1MB target
	baseRecordsForTarget := calculateRecordCountForFileSize(targetFileSize, avgBytesPerRecord)

	t.Logf("Average bytes per record: %.2f", avgBytesPerRecord)
	t.Logf("Base records for 1.1MB target: %d", baseRecordsForTarget)

	tests := []struct {
		name            string
		recordThreshold int64
		description     string
		scenario        string
	}{
		{
			name:            "Conservative (90% file size filter)",
			recordThreshold: baseRecordsForTarget * 9 / 10, // Simulates old approach where large files were excluded
			description:     "Previous approach: 90% file size filter + 90% record threshold",
			scenario:        "old",
		},
		{
			name:            "Improved (110% of target)",
			recordThreshold: baseRecordsForTarget * 11 / 10, // New approach
			description:     "New approach: 100% file size filter + 110% record threshold",
			scenario:        "new",
		},
		{
			name:            "Full target (100%)",
			recordThreshold: baseRecordsForTarget,
			description:     "Exactly target capacity",
			scenario:        "baseline",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
		groups, err := PackSegments(ctx, productionSegments, tt.recordThreshold, NoOpMetricRecorder{}, "test-org", "1", "logs", "compact")
			require.NoError(t, err)

			// Calculate metrics
			totalRecords := int64(0)
			maxGroupRecords := int64(0)
			for _, group := range groups {
				groupRecords := int64(0)
				for _, seg := range group {
					groupRecords += seg.RecordCount
				}
				totalRecords += groupRecords
				if groupRecords > maxGroupRecords {
					maxGroupRecords = groupRecords
				}
			}

			utilizationPercent := float64(maxGroupRecords) / float64(tt.recordThreshold) * 100
			compressionRatio := float64(len(productionSegments)) / float64(len(groups))

			t.Logf("%s:", tt.description)
			t.Logf("  Groups created: %d (%.1fx compression)", len(groups), compressionRatio)
			t.Logf("  Largest group: %d records (%.1f%% of threshold)", maxGroupRecords, utilizationPercent)
			t.Logf("  Record threshold: %d", tt.recordThreshold)

			// Verify no group exceeds threshold
			for i, group := range groups {
				groupRecords := int64(0)
				for _, seg := range group {
					groupRecords += seg.RecordCount
				}
				assert.LessOrEqual(t, groupRecords, tt.recordThreshold,
					"Group %d exceeds threshold in %s scenario", i, tt.scenario)
			}

			// Store results for comparison
			if tt.scenario == "old" {
				// Conservative should create more groups (worse compression)
				assert.GreaterOrEqual(t, len(groups), 2, "Conservative approach should create multiple groups")
			} else if tt.scenario == "new" {
				// Aggressive should create fewer groups (better compression)
				// but this depends on the actual threshold values, so we'll log for manual verification
				t.Logf("Aggressive packing created %d groups", len(groups))
			}
		})
	}
}

// TestPackingWithVariousFileSizes tests how the improved packing handles segments
// that would have been excluded by the 90% file size filter
func TestPackingWithVariousFileSizes(t *testing.T) {
	// Create segments with sizes that span the old 90% threshold
	targetFileSize := int64(1_100_000)
	avgBytesPerRecord := 51.4 // From production data
	baseThreshold := calculateRecordCountForFileSize(targetFileSize, avgBytesPerRecord)

	// Create segments around the old 90% boundary
	oldThreshold90Percent := calculateRecordCountForFileSize(targetFileSize*9/10, avgBytesPerRecord)

	segments := []lrdb.GetLogSegmentsForCompactionRow{
		// Small segments that would always be included
		{SegmentID: 1, StartTs: 1755741600000, EndTs: 1755741610000, RecordCount: 100, FileSize: 20000},
		{SegmentID: 2, StartTs: 1755741610000, EndTs: 1755741620000, RecordCount: 200, FileSize: 25000},

		// Medium segments right at the old 90% threshold
		{SegmentID: 3, StartTs: 1755741620000, EndTs: 1755741630000, RecordCount: oldThreshold90Percent - 100, FileSize: int64(float64(oldThreshold90Percent-100) * avgBytesPerRecord)},
		{SegmentID: 4, StartTs: 1755741630000, EndTs: 1755741640000, RecordCount: oldThreshold90Percent + 100, FileSize: int64(float64(oldThreshold90Percent+100) * avgBytesPerRecord)},

		// Larger segments that would have been excluded by 90% filter
		{SegmentID: 5, StartTs: 1755741640000, EndTs: 1755741650000, RecordCount: baseThreshold - 500, FileSize: int64(float64(baseThreshold-500) * avgBytesPerRecord)},
	}

	t.Logf("Base threshold: %d records", baseThreshold)
	t.Logf("Old 90%% threshold: %d records", oldThreshold90Percent)

	// Test both old conservative and new aggressive approaches
	ctx := context.Background()
	conservativeGroups, err := PackSegments(ctx, segments, oldThreshold90Percent, NoOpMetricRecorder{}, "test-org", "1", "logs", "compact")
	require.NoError(t, err)

	aggressiveGroups, err := PackSegments(ctx, segments, baseThreshold*11/10, NoOpMetricRecorder{}, "test-org", "1", "logs", "compact") // 110% for compression tolerance
	require.NoError(t, err)

	t.Logf("Conservative (90%% threshold): %d groups", len(conservativeGroups))
	t.Logf("Aggressive (110%% threshold): %d groups", len(aggressiveGroups))

	// The aggressive approach should achieve better or equal compression
	assert.LessOrEqual(t, len(aggressiveGroups), len(conservativeGroups),
		"Aggressive packing should create fewer or equal groups compared to conservative")

	// Log detailed results
	for i, group := range aggressiveGroups {
		totalRecords := int64(0)
		segmentIDs := []int64{}
		for _, seg := range group {
			totalRecords += seg.RecordCount
			segmentIDs = append(segmentIDs, seg.SegmentID)
		}
		t.Logf("Aggressive Group %d: %d records, segments %v", i, totalRecords, segmentIDs)
	}
}
