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

package cmd

import (
	"testing"
	"time"

	"github.com/cardinalhq/lakerunner/internal/buffet"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/internal/helpers"
)

func TestWorkQueueDeduplication(t *testing.T) {
	t.Run("Single hour with multiple files creates one work queue item", func(t *testing.T) {
		// Simulate multiple files for the same dateint/hour
		splitResults := map[buffet.SplitKey]buffet.HourlyResult{
			{DateInt: 20230101, Hour: 14, IngestDateint: 20230101, FileIndex: 0}: {
				FirstTS: 1672581600000, // 2023-01-01 14:00:00 UTC
				LastTS:  1672582499999, // 2023-01-01 14:14:59.999 UTC
			},
			{DateInt: 20230101, Hour: 14, IngestDateint: 20230101, FileIndex: 1}: {
				FirstTS: 1672582500000, // 2023-01-01 14:15:00 UTC
				LastTS:  1672584999999, // 2023-01-01 14:56:39.999 UTC
			},
			{DateInt: 20230101, Hour: 14, IngestDateint: 20230101, FileIndex: 2}: {
				FirstTS: 1672582000000, // 2023-01-01 14:06:40 UTC
				LastTS:  1672584199999, // 2023-01-01 14:49:59.999 UTC
			},
		}

		// Collect unique hour boundaries as the ingestion code does
		hourlyTriggers := make(map[helpers.HourBoundary]int64)

		for key, split := range splitResults {
			// Validate boundary compliance (as the ingestion code does)
			splitTimeRange := helpers.TimeRange{
				Start: helpers.UnixMillisToTime(split.FirstTS),
				End:   helpers.UnixMillisToTime(split.LastTS + 1),
			}
			assert.True(t, helpers.IsSameDateintHour(splitTimeRange),
				"Split should not cross hour boundaries for key %v", key)

			// Verify key matches time range
			expectedDateint, expectedHour := helpers.MSToDateintHour(split.FirstTS)
			assert.Equal(t, key.DateInt, expectedDateint)
			assert.Equal(t, key.Hour, expectedHour)

			// Collect triggers
			hourBoundary := helpers.HourBoundary{DateInt: key.DateInt, Hour: key.Hour}
			if existingTS, exists := hourlyTriggers[hourBoundary]; !exists || split.FirstTS < existingTS {
				hourlyTriggers[hourBoundary] = split.FirstTS
			}
		}

		// Should have exactly one unique hour boundary
		assert.Len(t, hourlyTriggers, 1, "Should have exactly one work queue trigger for the hour")

		// Should use the earliest timestamp as trigger
		expectedBoundary := helpers.HourBoundary{DateInt: 20230101, Hour: 14}
		assert.Contains(t, hourlyTriggers, expectedBoundary)
		assert.Equal(t, int64(1672581600000), hourlyTriggers[expectedBoundary], "Should use earliest FirstTS as trigger")
	})

	t.Run("Multiple hours create multiple work queue items", func(t *testing.T) {
		// Simulate files across different hours
		splitResults := map[buffet.SplitKey]buffet.HourlyResult{
			// Hour 14 - 2 files
			{DateInt: 20230101, Hour: 14, IngestDateint: 20230101, FileIndex: 0}: {
				FirstTS: 1672581600000, // 2023-01-01 14:00:00 UTC
				LastTS:  1672585199999, // 2023-01-01 14:59:59.999 UTC
			},
			{DateInt: 20230101, Hour: 14, IngestDateint: 20230101, FileIndex: 1}: {
				FirstTS: 1672582500000, // 2023-01-01 14:15:00 UTC
				LastTS:  1672584999999, // 2023-01-01 14:56:39.999 UTC
			},
			// Hour 15 - 3 files
			{DateInt: 20230101, Hour: 15, IngestDateint: 20230101, FileIndex: 0}: {
				FirstTS: 1672585200000, // 2023-01-01 15:00:00 UTC
				LastTS:  1672588199999, // 2023-01-01 15:49:59.999 UTC
			},
			{DateInt: 20230101, Hour: 15, IngestDateint: 20230101, FileIndex: 1}: {
				FirstTS: 1672585800000, // 2023-01-01 15:10:00 UTC
				LastTS:  1672588799999, // 2023-01-01 15:59:59.999 UTC
			},
			{DateInt: 20230101, Hour: 15, IngestDateint: 20230101, FileIndex: 2}: {
				FirstTS: 1672585500000, // 2023-01-01 15:05:00 UTC
				LastTS:  1672587899999, // 2023-01-01 15:44:59.999 UTC
			},
		}

		// Collect unique hour boundaries
		hourlyTriggers := make(map[helpers.HourBoundary]int64)

		for key, split := range splitResults {
			hourBoundary := helpers.HourBoundary{DateInt: key.DateInt, Hour: key.Hour}
			if existingTS, exists := hourlyTriggers[hourBoundary]; !exists || split.FirstTS < existingTS {
				hourlyTriggers[hourBoundary] = split.FirstTS
			}
		}

		// Should have exactly two unique hour boundaries
		assert.Len(t, hourlyTriggers, 2, "Should have exactly two work queue triggers")

		// Verify the two hours and their earliest timestamps
		hour14 := helpers.HourBoundary{DateInt: 20230101, Hour: 14}
		hour15 := helpers.HourBoundary{DateInt: 20230101, Hour: 15}

		assert.Contains(t, hourlyTriggers, hour14)
		assert.Contains(t, hourlyTriggers, hour15)

		assert.Equal(t, int64(1672581600000), hourlyTriggers[hour14], "Hour 14 should use earliest FirstTS")
		assert.Equal(t, int64(1672585200000), hourlyTriggers[hour15], "Hour 15 should use earliest FirstTS")
	})

	t.Run("Boundary violation detection", func(t *testing.T) {
		// Test case where a split crosses hour boundaries (this should be caught as a bug)
		splitTimeRange := helpers.TimeRange{
			Start: time.Date(2023, 1, 1, 14, 30, 0, 0, time.UTC), // 14:30
			End:   time.Date(2023, 1, 1, 15, 30, 0, 0, time.UTC), // 15:30 (crosses into next hour)
		}

		// This should fail the boundary check
		assert.False(t, helpers.IsSameDateintHour(splitTimeRange),
			"Split crossing hour boundaries should fail validation")
	})

	t.Run("Key validation", func(t *testing.T) {
		// Test that split key matches the actual time range
		firstTS := int64(1672581600000) // 2023-01-01 14:00:00 UTC
		expectedDateint, expectedHour := helpers.MSToDateintHour(firstTS)

		assert.Equal(t, int32(20230101), expectedDateint)
		assert.Equal(t, int16(14), expectedHour)

		// A valid key
		validKey := buffet.SplitKey{DateInt: 20230101, Hour: 14, IngestDateint: 20230101, FileIndex: 0}
		assert.Equal(t, expectedDateint, validKey.DateInt)
		assert.Equal(t, expectedHour, validKey.Hour)

		// An invalid key (wrong hour)
		invalidKey := buffet.SplitKey{DateInt: 20230101, Hour: 13, IngestDateint: 20230101, FileIndex: 0}
		assert.NotEqual(t, expectedHour, invalidKey.Hour, "Invalid key should not match expected hour")
	})
}

func TestHourBoundaryAsMapKey(t *testing.T) {
	// Test that HourBoundary works correctly as a map key for deduplication
	triggers := make(map[helpers.HourBoundary]int64)

	boundary1 := helpers.HourBoundary{DateInt: 20230101, Hour: 14}
	boundary2 := helpers.HourBoundary{DateInt: 20230101, Hour: 14} // Same as boundary1
	boundary3 := helpers.HourBoundary{DateInt: 20230101, Hour: 15} // Different hour

	triggers[boundary1] = 1000
	triggers[boundary2] = 2000 // Should overwrite boundary1
	triggers[boundary3] = 3000

	assert.Len(t, triggers, 2, "Should have 2 unique boundaries")
	assert.Equal(t, int64(2000), triggers[boundary1], "boundary2 should overwrite boundary1")
	assert.Equal(t, int64(2000), triggers[boundary2], "boundary2 should have same value as boundary1")
	assert.Equal(t, int64(3000), triggers[boundary3], "boundary3 should be separate")
}
