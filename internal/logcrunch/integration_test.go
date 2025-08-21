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
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func TestHourBasedProcessing_EndToEnd(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test data spanning multiple hours
	testRecords := []map[string]any{
		// Hour 0: 2023-01-01 00:XX:XX
		{"_cardinalhq.timestamp": int64(1672531200000), "msg": "hour0_msg1", "level": "INFO"}, // 00:00:00
		{"_cardinalhq.timestamp": int64(1672533000000), "msg": "hour0_msg2", "level": "WARN"}, // 00:30:00

		// Hour 1: 2023-01-01 01:XX:XX
		{"_cardinalhq.timestamp": int64(1672534800000), "msg": "hour1_msg1", "level": "ERROR"}, // 01:00:00
		{"_cardinalhq.timestamp": int64(1672536600000), "msg": "hour1_msg2", "level": "INFO"},  // 01:30:00

		// Hour 2: 2023-01-01 02:XX:XX
		{"_cardinalhq.timestamp": int64(1672538400000), "msg": "hour2_msg1", "level": "DEBUG"}, // 02:00:00
	}

	// Step 1: Test ProcessAndSplit creates hour-based splits
	nodes := map[string]parquet.Node{
		"_cardinalhq.timestamp": parquet.Int(64),
		"msg":                   parquet.String(),
		"level":                 parquet.String(),
	}
	schema := filecrunch.SchemaFromNodes(nodes)

	inputFile, err := os.CreateTemp(tmpDir, "input-*.parquet")
	require.NoError(t, err)
	defer os.Remove(inputFile.Name())

	pw := parquet.NewWriter(inputFile, schema)
	for _, record := range testRecords {
		require.NoError(t, pw.Write(record))
	}
	require.NoError(t, pw.Close())
	require.NoError(t, inputFile.Close())

	fh, err := filecrunch.LoadSchemaForFile(inputFile.Name())
	require.NoError(t, err)
	defer fh.Close()

	ll := slog.New(slog.NewTextHandler(io.Discard, nil))
	results, err := ProcessAndSplit(ll, fh, tmpDir, 20230101, 1000)
	require.NoError(t, err)

	// Should have 3 splits (3 different hours)
	assert.Len(t, results, 3)

	// Step 2: Verify each split contains only records from correct hour
	var splitKeys []SplitKey
	for key := range results {
		splitKeys = append(splitKeys, key)
	}
	sort.Slice(splitKeys, func(i, j int) bool {
		return splitKeys[i].Hour < splitKeys[j].Hour
	})

	expectedHours := []int16{0, 1, 2}
	expectedCounts := []int64{2, 2, 1}

	for i, key := range splitKeys {
		assert.Equal(t, expectedHours[i], key.Hour)
		assert.Equal(t, expectedCounts[i], results[key].RecordCount)
	}

	// Step 3: Test that PackSegments works with hour-conforming segments
	// Create mock segments that represent the splits we just created
	segments := []lrdb.GetLogSegmentsForCompactionRow{
		{SegmentID: 1, StartTs: 1672531200000, EndTs: 1672534800000, RecordCount: 2, FileSize: 1000}, // Hour 0
		{SegmentID: 2, StartTs: 1672534800000, EndTs: 1672538400000, RecordCount: 2, FileSize: 1000}, // Hour 1
		{SegmentID: 3, StartTs: 1672538400000, EndTs: 1672542000000, RecordCount: 1, FileSize: 500},  // Hour 2
	}

	// Each hour should be processed separately
	for i, seg := range segments {
		t.Run(fmt.Sprintf("Hour_%d", i), func(t *testing.T) {
			ctx := context.Background()
		groups, err := PackSegments(ctx, []lrdb.GetLogSegmentsForCompactionRow{seg}, 1000, NoOpMetricRecorder{}, "test-org", "1", "logs", "compact")
			require.NoError(t, err)
			assert.Len(t, groups, 1)
			assert.Len(t, groups[0], 1)
			assert.Equal(t, seg.SegmentID, groups[0][0].SegmentID)
		})
	}

	// Step 4: Test that segments from different hours are filtered to keep only the first hour
	mixedHourSegments := []lrdb.GetLogSegmentsForCompactionRow{
		{SegmentID: 1, StartTs: 1672531200000, EndTs: 1672534800000, RecordCount: 2, FileSize: 1000}, // Hour 0
		{SegmentID: 2, StartTs: 1672534800000, EndTs: 1672538400000, RecordCount: 2, FileSize: 1000}, // Hour 1
	}

	ctx := context.Background()
	groups, err := PackSegments(ctx, mixedHourSegments, 1000, NoOpMetricRecorder{}, "test-org", "1", "logs", "compact")
	assert.NoError(t, err)
	// Should only include the first segment (hour 0), second segment (hour 1) should be filtered out
	assert.Len(t, groups, 1)
	assert.Len(t, groups[0], 1)
	assert.Equal(t, int64(1), groups[0][0].SegmentID)
}

func TestHourBasedProcessing_TransitionPeriod(t *testing.T) {
	// Test the transition period where some segments cross hour boundaries
	// and should be filtered out

	segments := []lrdb.GetLogSegmentsForCompactionRow{
		// Good segments (within hour boundaries)
		{SegmentID: 1, StartTs: 1672531200000, EndTs: 1672532400000, RecordCount: 100, FileSize: 1000}, // Within hour 0
		{SegmentID: 2, StartTs: 1672534800000, EndTs: 1672536000000, RecordCount: 150, FileSize: 1500}, // Within hour 1

		// Bad segments (cross hour boundaries - legacy from day-based system)
		{SegmentID: 3, StartTs: 1672533600000, EndTs: 1672537200000, RecordCount: 200, FileSize: 2000}, // Crosses hour 0->1
		{SegmentID: 4, StartTs: 1672536000000, EndTs: 1672539600000, RecordCount: 250, FileSize: 2500}, // Crosses hour 1->2
	}

	// Since segments 1 and 2 are from different hours, they can't be packed together
	// Each should be processed separately

	// Test segment 1 (hour 0) separately
	ctx := context.Background()
	groups1, err := PackSegments(ctx, []lrdb.GetLogSegmentsForCompactionRow{segments[0]}, 1000, NoOpMetricRecorder{}, "test-org", "1", "logs", "compact")
	require.NoError(t, err)
	assert.Len(t, groups1, 1)

	// Test segment 2 (hour 1) separately
	ctx = context.Background()
	groups2, err := PackSegments(ctx, []lrdb.GetLogSegmentsForCompactionRow{segments[1]}, 1000, NoOpMetricRecorder{}, "test-org", "1", "logs", "compact")
	require.NoError(t, err)
	assert.Len(t, groups2, 1)

	// Test that mixing segments from different hours filters to keep only the first hour
	ctx = context.Background()
	mixedGroups, err := PackSegments(ctx, []lrdb.GetLogSegmentsForCompactionRow{segments[0], segments[1]}, 1000, NoOpMetricRecorder{}, "test-org", "1", "logs", "compact")
	assert.NoError(t, err)
	assert.Len(t, mixedGroups, 1)                          // Should only keep segment from first hour
	assert.Equal(t, int64(1), mixedGroups[0][0].SegmentID) // Only segment 1 (hour 0) should remain

	// Test that the transition segments get filtered out in mixed scenarios
	mixedSegments := []lrdb.GetLogSegmentsForCompactionRow{segments[0], segments[2]} // hour 0 + cross-boundary
	ctx = context.Background()
	groups, err := PackSegments(ctx, mixedSegments, 1000, NoOpMetricRecorder{}, "test-org", "1", "logs", "compact")
	require.NoError(t, err)
	assert.Len(t, groups, 1) // Only the good segment should remain

	// Extract segment ID that made it through
	assert.Equal(t, int64(1), groups[0][0].SegmentID) // Only segment 1 should remain
}

func TestHourBasedProcessing_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		segments []lrdb.GetLogSegmentsForCompactionRow
		wantErr  bool
	}{
		{
			name: "Segment exactly at hour boundary (end exclusive)",
			segments: []lrdb.GetLogSegmentsForCompactionRow{
				{SegmentID: 1, StartTs: 1672531200000, EndTs: 1672534800000, RecordCount: 100, FileSize: 1000}, // 00:00:00 - 01:00:00
			},
			wantErr: false, // Should be valid since end is exclusive
		},
		{
			name: "Segment spanning exactly one hour",
			segments: []lrdb.GetLogSegmentsForCompactionRow{
				{SegmentID: 1, StartTs: 1672534800000, EndTs: 1672538400000, RecordCount: 100, FileSize: 1000}, // 01:00:00 - 02:00:00
			},
			wantErr: false, // Should be valid since end is exclusive
		},
		{
			name: "Zero-duration segment",
			segments: []lrdb.GetLogSegmentsForCompactionRow{
				{SegmentID: 1, StartTs: 1672531200000, EndTs: 1672531200000, RecordCount: 100, FileSize: 1000}, // Same start/end but has records
			},
			wantErr: false, // Invalid time range is filtered out, returns empty result
		},
		{
			name: "Segment with end before start",
			segments: []lrdb.GetLogSegmentsForCompactionRow{
				{SegmentID: 1, StartTs: 1672534800000, EndTs: 1672531200000, RecordCount: 100, FileSize: 1000}, // End before start
			},
			wantErr: false, // Invalid time range is filtered out, returns empty result
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
		_, err := PackSegments(ctx, tt.segments, 1000, NoOpMetricRecorder{}, "test-org", "1", "logs", "compact")
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
