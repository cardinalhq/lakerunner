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

// helper to extract SegmentIDs for comparison
func SegmentIDsOf(groups [][]lrdb.GetLogSegmentsForCompactionRow) [][]int64 {
	out := make([][]int64, len(groups))
	for i, g := range groups {
		ids := make([]int64, len(g))
		for j, s := range g {
			ids[j] = s.SegmentID
		}
		out[i] = ids
	}
	return out
}

const (
	targetSize        = 1_000_000 // 1 MB
	targetRecordCount = 40_000    // 40k records
)

func TestPackSegments_NoSplit(t *testing.T) {
	// record counts: 3k + 4k + 2k = 9k <= 10k
	segments := []lrdb.GetLogSegmentsForCompactionRow{
		{SegmentID: 1, StartTs: 0, EndTs: 10, RecordCount: 3000},
		{SegmentID: 2, StartTs: 11, EndTs: 20, RecordCount: 4000},
		{SegmentID: 3, StartTs: 21, EndTs: 30, RecordCount: 2000},
	}

	groups, err := PackSegments(segments, 40_000)
	require.NoError(t, err)

	expected := [][]int64{{1, 2, 3}}
	assert.Equal(t, expected, SegmentIDsOf(groups))
}

func TestPackSegments_SplitByRecords(t *testing.T) {
	// record counts: 6k, 5k, 3k
	// 6k <=10k → first group [1]
	// 5k+3k=8k <=10k → second group [2,3]
	segments := []lrdb.GetLogSegmentsForCompactionRow{
		{SegmentID: 1, StartTs: 0, EndTs: 10, RecordCount: 6000},
		{SegmentID: 2, StartTs: 11, EndTs: 20, RecordCount: 5000},
		{SegmentID: 3, StartTs: 21, EndTs: 30, RecordCount: 3000},
	}

	groups, err := PackSegments(segments, 10_000)
	require.NoError(t, err)

	expected := [][]int64{{1}, {2, 3}}
	assert.Equal(t, expected, SegmentIDsOf(groups))
}

func TestPackSegments_MultiGroup(t *testing.T) {
	// record counts: 3k,3k,3k,3k
	// 3k+3k+3k=9k <=10k → first group [1,2,3]
	// remaining 3k → second group [4]
	segments := []lrdb.GetLogSegmentsForCompactionRow{
		{SegmentID: 1, StartTs: 0, EndTs: 10, RecordCount: 3000},
		{SegmentID: 2, StartTs: 11, EndTs: 20, RecordCount: 3000},
		{SegmentID: 3, StartTs: 21, EndTs: 30, RecordCount: 3000},
		{SegmentID: 4, StartTs: 31, EndTs: 40, RecordCount: 3000},
	}

	groups, err := PackSegments(segments, 10_000)
	require.NoError(t, err)

	expected := [][]int64{{1, 2, 3}, {4}}
	assert.Equal(t, expected, SegmentIDsOf(groups))
}

func TestPackSegments_ExactThreshold(t *testing.T) {
	// exactly 10k records per group
	segments := []lrdb.GetLogSegmentsForCompactionRow{
		{SegmentID: 1, StartTs: 0, EndTs: 10, RecordCount: targetRecordCount / 2},
		{SegmentID: 2, StartTs: 11, EndTs: 20, RecordCount: targetRecordCount / 2},
		{SegmentID: 3, StartTs: 21, EndTs: 30, RecordCount: targetRecordCount},
	}

	// first two sum to 5k+5k=10k, then 10k on its own
	groups, err := PackSegments(segments, targetRecordCount)
	require.NoError(t, err)

	expected := [][]int64{
		{1, 2},
		{3},
	}
	assert.Equal(t, expected, SegmentIDsOf(groups))
}

func TestPackSegments_HourBoundaryViolations(t *testing.T) {
	tests := []struct {
		name          string
		segments      []lrdb.GetLogSegmentsForCompactionRow
		expectError   bool
		errorContains string
	}{
		{
			name: "Segments from different hours should error",
			segments: []lrdb.GetLogSegmentsForCompactionRow{
				{SegmentID: 1, StartTs: 1672531200000, EndTs: 1672532400000, RecordCount: 100}, // Hour 0: 2023-01-01 00:00:00 - 00:20:00
				{SegmentID: 2, StartTs: 1672534800000, EndTs: 1672536000000, RecordCount: 200}, // Hour 1: 2023-01-01 01:00:00 - 01:20:00
			},
			expectError:   true,
			errorContains: "segments must be from the same UTC hour",
		},
		{
			name: "Single segment crossing hour boundary should be filtered out",
			segments: []lrdb.GetLogSegmentsForCompactionRow{
				{SegmentID: 1, StartTs: 1672533600000, EndTs: 1672537200000, RecordCount: 100}, // Crosses from hour 0 to hour 1
			},
			expectError: false, // No error because segment is filtered out, leaving empty list
		},
		{
			name: "Segments within same hour should succeed",
			segments: []lrdb.GetLogSegmentsForCompactionRow{
				{SegmentID: 1, StartTs: 1672531200000, EndTs: 1672532400000, RecordCount: 100}, // 2023-01-01 00:00:00 - 00:20:00
				{SegmentID: 2, StartTs: 1672532400000, EndTs: 1672534800000, RecordCount: 200}, // 2023-01-01 00:20:00 - 01:00:00 (exclusive)
			},
			expectError: false,
		},
		{
			name: "Segment exactly at hour boundary (end exclusive) should succeed",
			segments: []lrdb.GetLogSegmentsForCompactionRow{
				{SegmentID: 1, StartTs: 1672531200000, EndTs: 1672534800000, RecordCount: 100}, // 2023-01-01 00:00:00 - 01:00:00 (end exclusive)
			},
			expectError: false,
		},
		{
			name: "Invalid time range should error",
			segments: []lrdb.GetLogSegmentsForCompactionRow{
				{SegmentID: 1, StartTs: 1672532400000, EndTs: 1672531200000, RecordCount: 100}, // End before start (both in same hour to pass filtering)
			},
			expectError:   true,
			errorContains: "invalid segment time range",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := PackSegments(tt.segments, 1000)

			if tt.expectError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPackSegments_WithHourFiltering(t *testing.T) {
	// Test that segments crossing hour boundaries are filtered out before validation
	segments := []lrdb.GetLogSegmentsForCompactionRow{
		{SegmentID: 1, StartTs: 1672531200000, EndTs: 1672532400000, RecordCount: 100}, // Within hour 0
		{SegmentID: 2, StartTs: 1672533600000, EndTs: 1672537200000, RecordCount: 200}, // Crosses hour boundary - should be filtered
		{SegmentID: 3, StartTs: 1672532400000, EndTs: 1672534800000, RecordCount: 150}, // Within hour 0
	}

	groups, err := PackSegments(segments, 1000)
	assert.NoError(t, err)

	// Should only have segments 1 and 3 (segment 2 filtered out for crossing hour boundary)
	expected := [][]int64{{1, 3}}
	assert.Equal(t, expected, SegmentIDsOf(groups))
}

// Existing helper tests for hourFromMillis and filterSegments remain valid

func TestHourFromMillis(t *testing.T) {
	tests := []struct {
		name     string
		millis   int64
		expected int64
	}{
		{"Epoch", 0, 0},
		{"Start of 2023 UTC", 1672531200000, 464592}, // 2023-01-01 00:00:00 UTC
		{"Hour 1", 1672534800000, 464593},            // 2023-01-01 01:00:00 UTC
		{"Middle of hour", 1672533000000, 464592},    // 2023-01-01 00:30:00 UTC
		{"End of hour", 1672534799999, 464592},       // 2023-01-01 00:59:59.999 UTC
		{"Next hour", 1672534800000, 464593},         // 2023-01-01 01:00:00 UTC
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hourFromMillis(tt.millis)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestFilterHourConformingSegments(t *testing.T) {
	tests := []struct {
		name     string
		input    []lrdb.GetLogSegmentsForCompactionRow
		expected []lrdb.GetLogSegmentsForCompactionRow
	}{
		{
			name: "All segments within same hour",
			input: []lrdb.GetLogSegmentsForCompactionRow{
				{SegmentID: 1, StartTs: 1672531200000, EndTs: 1672532400000, RecordCount: 100}, // 2023-01-01 00:00:00 - 00:20:00
				{SegmentID: 2, StartTs: 1672532400000, EndTs: 1672534800000, RecordCount: 200}, // 2023-01-01 00:20:00 - 01:00:00
			},
			expected: []lrdb.GetLogSegmentsForCompactionRow{
				{SegmentID: 1, StartTs: 1672531200000, EndTs: 1672532400000, RecordCount: 100},
				{SegmentID: 2, StartTs: 1672532400000, EndTs: 1672534800000, RecordCount: 200},
			},
		},
		{
			name: "Some segments cross hour boundaries",
			input: []lrdb.GetLogSegmentsForCompactionRow{
				{SegmentID: 1, StartTs: 1672531200000, EndTs: 1672532400000, RecordCount: 100}, // Within hour 0
				{SegmentID: 2, StartTs: 1672533600000, EndTs: 1672537200000, RecordCount: 200}, // Crosses from hour 0 to hour 1
				{SegmentID: 3, StartTs: 1672534800000, EndTs: 1672536000000, RecordCount: 150}, // Within hour 1
			},
			expected: []lrdb.GetLogSegmentsForCompactionRow{
				{SegmentID: 1, StartTs: 1672531200000, EndTs: 1672532400000, RecordCount: 100}, // Keep: within hour
				{SegmentID: 3, StartTs: 1672534800000, EndTs: 1672536000000, RecordCount: 150}, // Keep: within hour
			},
		},
		{
			name: "All segments cross hour boundaries",
			input: []lrdb.GetLogSegmentsForCompactionRow{
				{SegmentID: 1, StartTs: 1672533600000, EndTs: 1672537200000, RecordCount: 100}, // Crosses hour boundary
				{SegmentID: 2, StartTs: 1672536000000, EndTs: 1672539600000, RecordCount: 200}, // Crosses hour boundary
			},
			expected: []lrdb.GetLogSegmentsForCompactionRow{}, // All filtered out
		},
		{
			name: "Segment exactly at hour boundary (end exclusive)",
			input: []lrdb.GetLogSegmentsForCompactionRow{
				{SegmentID: 1, StartTs: 1672531200000, EndTs: 1672534800000, RecordCount: 100}, // 00:00:00 - 01:00:00 (end exclusive, so last ms is 00:59:59.999)
			},
			expected: []lrdb.GetLogSegmentsForCompactionRow{
				{SegmentID: 1, StartTs: 1672531200000, EndTs: 1672534800000, RecordCount: 100}, // Keep: end is exclusive
			},
		},
		{
			name:     "Empty input",
			input:    []lrdb.GetLogSegmentsForCompactionRow{},
			expected: []lrdb.GetLogSegmentsForCompactionRow{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterHourConformingSegments(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFilterSegments(t *testing.T) {
	cases := []struct {
		name     string
		input    []lrdb.GetLogSegmentsForCompactionRow
		expected []int64
	}{
		{"All positive", []lrdb.GetLogSegmentsForCompactionRow{
			{SegmentID: 1, FileSize: 10, RecordCount: 1},
			{SegmentID: 2, FileSize: 20, RecordCount: 1},
		}, []int64{1, 2}},
		{"Zero and negative", []lrdb.GetLogSegmentsForCompactionRow{
			{SegmentID: 1, FileSize: 0, RecordCount: 0},
			{SegmentID: 2, FileSize: 15, RecordCount: 1},
			{SegmentID: 3, FileSize: -5, RecordCount: 0},
		}, []int64{2}},
		{"Empty input", []lrdb.GetLogSegmentsForCompactionRow{}, []int64{}},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			out := filterSegments(tt.input)
			ids := make([]int64, len(out))
			for i, s := range out {
				ids[i] = s.SegmentID
			}
			assert.Equal(t, tt.expected, ids)
		})
	}
}
