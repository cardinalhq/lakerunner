// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logcrunch

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pkg/lrdb"
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
	bytesPerRecord    = 100.0     // estimate
	targetRecordCount = int64(targetSize / int64(bytesPerRecord))
)

func TestPackSegments_NoSplit(t *testing.T) {
	// record counts: 3k + 4k + 2k = 9k <= 10k
	segments := []lrdb.GetLogSegmentsForCompactionRow{
		{SegmentID: 1, StartTs: 0, EndTs: 10, RecordCount: 3000},
		{SegmentID: 2, StartTs: 11, EndTs: 20, RecordCount: 4000},
		{SegmentID: 3, StartTs: 21, EndTs: 30, RecordCount: 2000},
	}

	groups, err := PackSegments(segments, targetSize, bytesPerRecord)
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

	groups, err := PackSegments(segments, targetSize, bytesPerRecord)
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

	groups, err := PackSegments(segments, targetSize, bytesPerRecord)
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
	groups, err := PackSegments(segments, targetSize, bytesPerRecord)
	require.NoError(t, err)

	expected := [][]int64{
		{1, 2},
		{3},
	}
	assert.Equal(t, expected, SegmentIDsOf(groups))
}

// Existing helper tests for dayFromMillis and filterSegments remain valid

func TestDayFromMillis(t *testing.T) {
	tests := []struct {
		name     string
		millis   int64
		expected time.Time
	}{
		{"Epoch", 0, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"Start of 2023 UTC", 1672531200000, time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"Middle of day", 1672574400123, time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"End of day", 1672617599999, time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"Next day", 1672617600000, time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := dayFromMillis(tt.millis)
			assert.Equal(t, tt.expected, got)
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
