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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// calculateAvgBytesPerRecord calculates the average bytes per record from a set of segments
// accounting for file overhead and focusing on larger files for accuracy
func calculateAvgBytesPerRecord(segments []lrdb.GetLogSegmentsForCompactionRow) float64 {
	const fileOverhead = int64(15000) // Base file overhead ~15KB
	const minRecordsForAccuracy = 100 // Only use files with >100 records for calculation

	totalBytes := int64(0)
	totalRecords := int64(0)

	for _, seg := range segments {
		if seg.RecordCount > minRecordsForAccuracy { // Only use larger segments
			// Subtract file overhead to get actual data bytes
			dataBytes := seg.FileSize - fileOverhead
			if dataBytes > 0 { // Ensure we don't get negative values
				totalBytes += dataBytes
				totalRecords += seg.RecordCount
			}
		}
	}

	if totalRecords == 0 {
		return 0
	}

	return float64(totalBytes) / float64(totalRecords)
}

// calculateRecordCountForFileSize calculates how many records would fit in a target file size
// based on the average bytes per record from the sample data, accounting for file overhead
func calculateRecordCountForFileSize(targetFileSize int64, avgBytesPerRecord float64) int64 {
	const fileOverhead = int64(15000) // Base file overhead ~15KB

	if avgBytesPerRecord <= 0 {
		return 40000 // fallback to existing test default
	}

	// Subtract overhead to get available space for actual data
	availableSpace := targetFileSize - fileOverhead
	if availableSpace <= 0 {
		return 0
	}

	return int64(float64(availableSpace) / avgBytesPerRecord)
}

// TestPackSegments_ProductionData tests PackSegments with realistic production data
// from a real log_seg table query showing compaction challenges with many small segments
func TestPackSegments_ProductionData(t *testing.T) {
	// Sample of actual production data - segments from same hour with various sizes
	productionSegments := []lrdb.GetLogSegmentsForCompactionRow{
		// Very small segments (1-10 records) - these should be packed together
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

	// Calculate realistic bytes per record from production data
	avgBytesPerRecord := calculateAvgBytesPerRecord(productionSegments)

	// Count how many segments were used for calculation (>100 records)
	usedSegments := 0
	for _, seg := range productionSegments {
		if seg.RecordCount > 100 {
			usedSegments++
		}
	}

	t.Logf("Calculated average bytes per record: %.2f (using %d segments with >100 records, accounting for 15KB file overhead)",
		avgBytesPerRecord, usedSegments)

	// Calculate realistic record counts for different target file sizes
	targetFileSize := int64(1_100_000) // From cmd/root.go
	recordsForTargetSize := calculateRecordCountForFileSize(targetFileSize, avgBytesPerRecord)
	recordsFor90Percent := calculateRecordCountForFileSize(targetFileSize*9/10, avgBytesPerRecord) // 90% threshold used in compaction

	t.Logf("Records for target file size (1.1MB): %d", recordsForTargetSize)
	t.Logf("Records for 90%% threshold (990KB): %d", recordsFor90Percent)

	tests := []struct {
		name                 string
		segments             []lrdb.GetLogSegmentsForCompactionRow
		estimatedRecordCount int64
		expectedGroups       int
		description          string
	}{
		{
			name:                 "Production data with realistic target size",
			segments:             productionSegments,
			estimatedRecordCount: recordsForTargetSize,
			expectedGroups:       2, // Actually creates 2 groups due to greedy packing
			description:          "Production segments pack into 2 groups under realistic target size",
		},
		{
			name:                 "Production data with 90% threshold (compaction query limit)",
			segments:             productionSegments,
			estimatedRecordCount: recordsFor90Percent,
			expectedGroups:       2, // Same as target size due to greedy algorithm
			description:          "Production segments create 2 groups under 90% file size threshold",
		},
		{
			name:                 "Half target size forces more groups",
			segments:             productionSegments,
			estimatedRecordCount: recordsForTargetSize / 2,
			expectedGroups:       3, // Actually creates 3 groups
			description:          "Smaller threshold forces segments into 3 groups",
		},
		{
			name:                 "Very small threshold",
			segments:             productionSegments[:8], // Just the tiny segments
			estimatedRecordCount: 100,                    // Very small
			expectedGroups:       1,                      // Small segments pack together
			description:          "Tiny segments group together even with very small threshold",
		},
		{
			name:                 "Quarter target size",
			segments:             productionSegments,
			estimatedRecordCount: recordsForTargetSize / 4,
			expectedGroups:       5, // Creates 5 groups with updated calculation
			description:          "Quarter threshold creates multiple groups, larger segments go solo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groups, err := PackSegments(tt.segments, tt.estimatedRecordCount)
			require.NoError(t, err, tt.description)

			// Verify number of groups
			assert.Equal(t, tt.expectedGroups, len(groups),
				"Expected %d groups but got %d. %s",
				tt.expectedGroups, len(groups), tt.description)

			// Verify no group exceeds the record limit
			totalRecords := int64(0)
			for i, group := range groups {
				groupRecords := int64(0)
				for _, seg := range group {
					groupRecords += seg.RecordCount
				}
				assert.LessOrEqual(t, groupRecords, tt.estimatedRecordCount,
					"Group %d has %d records, exceeding limit of %d",
					i, groupRecords, tt.estimatedRecordCount)
				totalRecords += groupRecords
			}

			// Verify all segments are included (no data loss)
			expectedTotal := int64(0)
			for _, seg := range tt.segments {
				if seg.RecordCount > 0 { // PackSegments filters out zero-record segments
					expectedTotal += seg.RecordCount
				}
			}
			assert.Equal(t, expectedTotal, totalRecords,
				"Total records in groups (%d) doesn't match input (%d)",
				totalRecords, expectedTotal)

			// Log group composition for debugging
			t.Logf("Groups created: %d", len(groups))
			for i, group := range groups {
				groupRecords := int64(0)
				for _, seg := range group {
					groupRecords += seg.RecordCount
				}
				t.Logf("Group %d: %d segments, %d total records", i, len(group), groupRecords)
			}
		})
	}
}

// TestPackSegments_CompactionEfficiency tests how well PackSegments handles
// the specific case of many tiny segments that need compaction
func TestPackSegments_CompactionEfficiency(t *testing.T) {
	// Create many tiny segments (similar to production pattern)
	tinySegments := []lrdb.GetLogSegmentsForCompactionRow{}
	baseTime := int64(1755741600000)

	// Use realistic bytes per record based on production data (accounting for file overhead)
	avgBytesPerRecord := 51.4 // From actual production data calculation after overhead adjustment

	// 50 very small segments (1-10 records each)
	for i := 0; i < 50; i++ {
		records := int64(1 + i%10)                                                           // 1-10 records
		fileSize := int64(float64(records) * avgBytesPerRecord * (0.8 + 0.4*float64(i%5)/4)) // Some variance

		tinySegments = append(tinySegments, lrdb.GetLogSegmentsForCompactionRow{
			SegmentID:   int64(i + 1),
			StartTs:     baseTime + int64(i*1000),
			EndTs:       baseTime + int64(i*1000) + 500,
			RecordCount: records,
			FileSize:    fileSize,
		})
	}

	// Calculate realistic threshold based on target file size
	targetFileSize := int64(1_100_000)
	recordThreshold := calculateRecordCountForFileSize(targetFileSize/10, avgBytesPerRecord) // Use 10% of target for testing

	groups, err := PackSegments(tinySegments, recordThreshold)
	require.NoError(t, err)

	// Should pack efficiently - many tiny segments into fewer groups
	assert.Less(t, len(groups), 20, "Should pack 50 tiny segments into fewer than 20 groups")

	// Verify each group uses capacity efficiently
	for i, group := range groups {
		if len(group) == 1 {
			// Single-segment groups should only happen if the segment is large enough
			assert.GreaterOrEqual(t, group[0].RecordCount, int64(500),
				"Single-segment group %d has only %d records - should be packed with others",
				i, group[0].RecordCount)
		}
	}

	// Calculate efficiency: ratio of groups to input segments
	efficiency := float64(len(groups)) / float64(len(tinySegments))
	t.Logf("Compaction efficiency: %d groups from %d segments (%.2f ratio)",
		len(groups), len(tinySegments), efficiency)

	// Should achieve at least 50% reduction in segment count
	assert.Less(t, efficiency, 0.5, "Should achieve at least 50%% compaction efficiency")
}

// TestPackSegments_HourBoundaryFiltering tests that segments crossing hour boundaries
// are properly filtered out, using realistic timestamp data
func TestPackSegments_HourBoundaryFiltering(t *testing.T) {
	// Use hour boundaries: 1755741600000 = hour 487705, next hour = 487706
	// 1 hour = 3,600,000 ms
	hour1Start := int64(1755741600000)
	hour2Start := hour1Start + 3600000

	segments := []lrdb.GetLogSegmentsForCompactionRow{
		// Valid segment within hour 1
		{SegmentID: 1, StartTs: hour1Start, EndTs: hour1Start + 1800000, RecordCount: 100, FileSize: 50000}, // 30 min within hour 1

		// Invalid segment crossing hour boundary
		{SegmentID: 2, StartTs: hour1Start + 1800000, EndTs: hour2Start + 1800000, RecordCount: 200, FileSize: 100000}, // Crosses from hour 1 to hour 2

		// Another valid segment within hour 1
		{SegmentID: 3, StartTs: hour1Start + 900000, EndTs: hour1Start + 2700000, RecordCount: 150, FileSize: 75000}, // Different part of hour 1
	}

	groups, err := PackSegments(segments, 1000)
	require.NoError(t, err)

	// Should only have segments 1 and 3 (segment 2 filtered out)
	totalSegments := 0
	for _, group := range groups {
		totalSegments += len(group)
	}
	assert.Equal(t, 2, totalSegments, "Should only have 2 segments after filtering hour boundary crossers")

	// Verify specific segments are present
	foundSegments := make(map[int64]bool)
	for _, group := range groups {
		for _, seg := range group {
			foundSegments[seg.SegmentID] = true
		}
	}
	assert.True(t, foundSegments[1], "Segment 1 should be present")
	assert.False(t, foundSegments[2], "Segment 2 should be filtered out")
	assert.True(t, foundSegments[3], "Segment 3 should be present")
}
