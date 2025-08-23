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

package metriccompaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
)

func TestPackMetricSegments(t *testing.T) {
	tests := []struct {
		name                 string
		segments             []lrdb.MetricSeg
		estimatedRecordCount int64
		expectedGroups       int
		expectedError        string
		validateGroups       func(t *testing.T, groups [][]lrdb.MetricSeg)
	}{
		{
			name:                 "empty segments",
			segments:             []lrdb.MetricSeg{},
			estimatedRecordCount: 1000,
			expectedGroups:       0,
		},
		{
			name:                 "invalid estimate",
			segments:             []lrdb.MetricSeg{{RecordCount: 100, FileSize: 1000}},
			estimatedRecordCount: 0,
			expectedError:        "estimatedRecordCount must be positive, got 0",
		},
		{
			name: "single segment",
			segments: []lrdb.MetricSeg{
				{RecordCount: 500, FileSize: 1000},
			},
			estimatedRecordCount: 1000,
			expectedGroups:       1,
			validateGroups: func(t *testing.T, groups [][]lrdb.MetricSeg) {
				require.Len(t, groups, 1)
				assert.Len(t, groups[0], 1)
				assert.Equal(t, int64(500), groups[0][0].RecordCount)
			},
		},
		{
			name: "filters zero record segments",
			segments: []lrdb.MetricSeg{
				{RecordCount: 0, FileSize: 1000},   // Should be filtered
				{RecordCount: 500, FileSize: 0},    // Should be filtered
				{RecordCount: 500, FileSize: 1000}, // Should be kept
			},
			estimatedRecordCount: 1000,
			expectedGroups:       1,
			validateGroups: func(t *testing.T, groups [][]lrdb.MetricSeg) {
				require.Len(t, groups, 1)
				assert.Len(t, groups[0], 1)
				assert.Equal(t, int64(500), groups[0][0].RecordCount)
				assert.Equal(t, int64(1000), groups[0][0].FileSize)
			},
		},
		{
			name: "all segments filtered",
			segments: []lrdb.MetricSeg{
				{RecordCount: 0, FileSize: 1000},
				{RecordCount: 500, FileSize: 0},
			},
			estimatedRecordCount: 1000,
			expectedGroups:       0,
		},
		{
			name: "segments fit in one group",
			segments: []lrdb.MetricSeg{
				{RecordCount: 300, FileSize: 1000},
				{RecordCount: 400, FileSize: 1200},
				{RecordCount: 200, FileSize: 800},
			},
			estimatedRecordCount: 1000,
			expectedGroups:       1,
			validateGroups: func(t *testing.T, groups [][]lrdb.MetricSeg) {
				require.Len(t, groups, 1)
				assert.Len(t, groups[0], 3)
				totalRecords := int64(0)
				for _, seg := range groups[0] {
					totalRecords += seg.RecordCount
				}
				assert.Equal(t, int64(900), totalRecords)
			},
		},
		{
			name: "segments require multiple groups",
			segments: []lrdb.MetricSeg{
				{RecordCount: 600, FileSize: 1000},
				{RecordCount: 500, FileSize: 1200}, // Should start new group
				{RecordCount: 300, FileSize: 800},  // Should fit in second group
			},
			estimatedRecordCount: 1000,
			expectedGroups:       2,
			validateGroups: func(t *testing.T, groups [][]lrdb.MetricSeg) {
				require.Len(t, groups, 2)
				
				// First group: just the 600 record segment
				assert.Len(t, groups[0], 1)
				assert.Equal(t, int64(600), groups[0][0].RecordCount)
				
				// Second group: 500 + 300 = 800 records
				assert.Len(t, groups[1], 2)
				totalRecords := int64(0)
				for _, seg := range groups[1] {
					totalRecords += seg.RecordCount
				}
				assert.Equal(t, int64(800), totalRecords)
			},
		},
		{
			name: "greedy packing - fits exactly at threshold",
			segments: []lrdb.MetricSeg{
				{RecordCount: 1000, FileSize: 1000}, // Exactly at threshold
				{RecordCount: 500, FileSize: 500},   // Should start new group
			},
			estimatedRecordCount: 1000,
			expectedGroups:       2,
			validateGroups: func(t *testing.T, groups [][]lrdb.MetricSeg) {
				require.Len(t, groups, 2)
				assert.Len(t, groups[0], 1)
				assert.Equal(t, int64(1000), groups[0][0].RecordCount)
				assert.Len(t, groups[1], 1)
				assert.Equal(t, int64(500), groups[1][0].RecordCount)
			},
		},
		{
			name: "large single segment exceeds threshold",
			segments: []lrdb.MetricSeg{
				{RecordCount: 1500, FileSize: 2000}, // Exceeds threshold but still gets own group
			},
			estimatedRecordCount: 1000,
			expectedGroups:       1,
			validateGroups: func(t *testing.T, groups [][]lrdb.MetricSeg) {
				require.Len(t, groups, 1)
				assert.Len(t, groups[0], 1)
				assert.Equal(t, int64(1500), groups[0][0].RecordCount)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groups, err := PackMetricSegments(tt.segments, tt.estimatedRecordCount)
			
			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
				return
			}
			
			require.NoError(t, err)
			assert.Len(t, groups, tt.expectedGroups)
			
			if tt.validateGroups != nil {
				tt.validateGroups(t, groups)
			}
		})
	}
}

func TestExtractTidPartition(t *testing.T) {
	tests := []struct {
		name     string
		tid      int64
		expected int16
	}{
		{
			name:     "zero TID",
			tid:      0,
			expected: 0,
		},
		{
			name:     "small TID - no partition bits set",
			tid:      12345,
			expected: 0,
		},
		{
			name:     "TID with partition 1",
			tid:      int64(1) << 56, // Set bit 56
			expected: 1,
		},
		{
			name:     "TID with partition 127 (max positive 8-bit value in signed int16)",
			tid:      int64(127) << 56,
			expected: 127,
		},
		{
			name:     "TID with partition and lower bits set",
			tid:      (int64(5) << 56) | 0xFFFFFFFFFFFFFF, // Partition 5 with all lower bits set
			expected: 5,
		},
		{
			name:     "negative TID",
			tid:      -1, // All bits set
			expected: -1,
		},
		{
			name:     "max positive int64",
			tid:      9223372036854775807, // 0x7FFFFFFFFFFFFFFF
			expected: 127,                 // Upper bit clear, so 0x7F = 127
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractTidPartition(tt.tid)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestShouldCompactMetricGroupAdditionalCases(t *testing.T) {
	tests := []struct {
		name     string
		segments []lrdb.MetricSeg
		expected bool
	}{
		{
			name:     "empty segments",
			segments: []lrdb.MetricSeg{},
			expected: false,
		},
		{
			name: "mixed small and large files",
			segments: []lrdb.MetricSeg{
				{FileSize: targetFileSize / 10},     // Small file
				{FileSize: targetFileSize * 3},      // Large file
			},
			expected: true,
		},
		{
			name: "many small files that would benefit from compaction",
			segments: []lrdb.MetricSeg{
				{FileSize: targetFileSize / 10},
				{FileSize: targetFileSize / 10},
				{FileSize: targetFileSize / 10},
				{FileSize: targetFileSize / 10},
				{FileSize: targetFileSize / 10},
			},
			expected: true,
		},
		{
			name: "files at optimal size - no compaction needed",
			segments: []lrdb.MetricSeg{
				{FileSize: targetFileSize},
				{FileSize: targetFileSize},
			},
			expected: false,
		},
		{
			name: "borderline case - just above small threshold",
			segments: []lrdb.MetricSeg{
				{FileSize: targetFileSize * 4 / 10}, // Just above 30% threshold
				{FileSize: targetFileSize * 4 / 10},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShouldCompactMetricGroup(tt.segments)
			assert.Equal(t, tt.expected, result)
		})
	}
}