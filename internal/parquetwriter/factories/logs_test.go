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

package factories

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func TestLogsStatsAccumulator_StreamIDTracking(t *testing.T) {
	tests := []struct {
		name              string
		rows              []pipeline.Row
		expectedStreamIDs []string
	}{
		{
			name: "tracks single stream_id",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890123), wkk.RowKeyCStreamID: "service-a"},
			},
			expectedStreamIDs: []string{"service-a"},
		},
		{
			name: "tracks multiple unique stream_ids",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890123), wkk.RowKeyCStreamID: "service-a"},
				{wkk.RowKeyCTimestamp: int64(1234567890124), wkk.RowKeyCStreamID: "service-b"},
				{wkk.RowKeyCTimestamp: int64(1234567890125), wkk.RowKeyCStreamID: "service-c"},
			},
			expectedStreamIDs: []string{"service-a", "service-b", "service-c"},
		},
		{
			name: "deduplicates stream_ids",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890123), wkk.RowKeyCStreamID: "service-a"},
				{wkk.RowKeyCTimestamp: int64(1234567890124), wkk.RowKeyCStreamID: "service-a"},
				{wkk.RowKeyCTimestamp: int64(1234567890125), wkk.RowKeyCStreamID: "service-b"},
				{wkk.RowKeyCTimestamp: int64(1234567890126), wkk.RowKeyCStreamID: "service-b"},
			},
			expectedStreamIDs: []string{"service-a", "service-b"},
		},
		{
			name: "ignores rows without stream_id",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890123), wkk.RowKeyCStreamID: "service-a"},
				{wkk.RowKeyCTimestamp: int64(1234567890124)}, // No stream_id
				{wkk.RowKeyCTimestamp: int64(1234567890125), wkk.RowKeyCStreamID: "service-b"},
			},
			expectedStreamIDs: []string{"service-a", "service-b"},
		},
		{
			name: "ignores empty stream_id",
			rows: []pipeline.Row{
				{wkk.RowKeyCTimestamp: int64(1234567890123), wkk.RowKeyCStreamID: "service-a"},
				{wkk.RowKeyCTimestamp: int64(1234567890124), wkk.RowKeyCStreamID: ""}, // Empty string
				{wkk.RowKeyCTimestamp: int64(1234567890125), wkk.RowKeyCStreamID: "service-b"},
			},
			expectedStreamIDs: []string{"service-a", "service-b"},
		},
		{
			name:              "handles no rows with stream_id",
			rows:              []pipeline.Row{{wkk.RowKeyCTimestamp: int64(1234567890123)}},
			expectedStreamIDs: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &LogsStatsProvider{}
			acc := provider.NewAccumulator()

			for _, row := range tt.rows {
				acc.Add(row)
			}

			result := acc.Finalize()
			stats, ok := result.(LogsFileStats)
			require.True(t, ok, "Finalize should return LogsFileStats")

			// Sort both slices for comparison
			actualIDs := stats.StreamIDs
			sort.Strings(actualIDs)
			sort.Strings(tt.expectedStreamIDs)

			assert.Equal(t, tt.expectedStreamIDs, actualIDs, "stream IDs mismatch")
		})
	}
}

func TestLogsStatsAccumulator_FingerprintsAndTimestamps(t *testing.T) {
	provider := &LogsStatsProvider{}
	acc := provider.NewAccumulator()

	// Add rows with timestamps
	rows := []pipeline.Row{
		{wkk.RowKeyCTimestamp: int64(1234567890100), wkk.RowKeyCStreamID: "service-a"},
		{wkk.RowKeyCTimestamp: int64(1234567890200), wkk.RowKeyCStreamID: "service-b"},
		{wkk.RowKeyCTimestamp: int64(1234567890050), wkk.RowKeyCStreamID: "service-c"}, // Earlier timestamp
	}

	for _, row := range rows {
		acc.Add(row)
	}

	result := acc.Finalize()
	stats, ok := result.(LogsFileStats)
	require.True(t, ok, "Finalize should return LogsFileStats")

	// Verify timestamp tracking
	assert.Equal(t, int64(1234567890050), stats.FirstTS, "FirstTS should be minimum timestamp")
	assert.Equal(t, int64(1234567890200), stats.LastTS, "LastTS should be maximum timestamp")
}

func TestLogsFileStats_Structure(t *testing.T) {
	stats := LogsFileStats{
		FirstTS:      1234567890000,
		LastTS:       1234567890999,
		Fingerprints: []int64{100, 200, 300},
		StreamIDs:    []string{"service-a", "service-b"},
	}

	assert.Equal(t, int64(1234567890000), stats.FirstTS)
	assert.Equal(t, int64(1234567890999), stats.LastTS)
	assert.Len(t, stats.Fingerprints, 3)
	assert.Len(t, stats.StreamIDs, 2)
	assert.Contains(t, stats.StreamIDs, "service-a")
	assert.Contains(t, stats.StreamIDs, "service-b")
}
