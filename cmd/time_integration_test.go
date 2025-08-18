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

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/internal/helpers"
)

// TestWorkQueueTimeRangeCompatibility tests that work queue time ranges
// are compatible with compaction validation.
func TestWorkQueueTimeRangeCompatibility(t *testing.T) {
	tests := []struct {
		name        string
		triggerTime time.Time
		expectValid bool
		description string
	}{
		{
			name:        "perfect hour boundary",
			triggerTime: time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
			expectValid: true,
			description: "trigger at exact hour boundary should create valid range",
		},
		{
			name:        "middle of hour",
			triggerTime: time.Date(2023, 1, 1, 14, 30, 45, 123000000, time.UTC),
			expectValid: true,
			description: "trigger in middle of hour should create valid range",
		},
		{
			name:        "end of hour",
			triggerTime: time.Date(2023, 1, 1, 14, 59, 59, 999000000, time.UTC),
			expectValid: true,
			description: "trigger near end of hour should create valid range",
		},
		{
			name:        "cross day boundary",
			triggerTime: time.Date(2023, 1, 1, 23, 45, 0, 0, time.UTC),
			expectValid: true,
			description: "trigger near day boundary should create valid range",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the work queue creation process
			triggerRange := helpers.TimeRange{
				Start: tt.triggerTime,
				End:   tt.triggerTime.Add(time.Minute), // Small trigger range
			}

			// Create the hour range as the work queue would
			hourRange := helpers.CreateHourRange(triggerRange.Start)

			// Verify the hour range is properly aligned
			assert.True(t, helpers.IsHourAligned(hourRange), "hour range should be properly aligned")

			// Most importantly: verify it passes compaction validation
			valid := helpers.IsSameDateintHour(hourRange)
			assert.Equal(t, tt.expectValid, valid, tt.description)

			if valid {
				// Extra validation: ensure boundaries match
				startBoundary, endBoundary := helpers.TimeRangeToHourBoundaries(hourRange)
				assert.Equal(t, startBoundary, endBoundary, "start and end boundaries should be identical")
			}
		})
	}
}

// TestCompactionTimeRangeEdgeCases tests edge cases that previously caused
// the "Range bounds are not the same dateint-hour" error.
func TestCompactionTimeRangeEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		pgRange     pgtype.Range[pgtype.Timestamptz]
		expectValid bool
		description string
	}{
		{
			name: "perfect hour range",
			pgRange: pgtype.Range[pgtype.Timestamptz]{
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Lower:     pgtype.Timestamptz{Time: time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC), Valid: true},
				Upper:     pgtype.Timestamptz{Time: time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC), Valid: true},
				Valid:     true,
			},
			expectValid: true,
			description: "perfect hour range should be valid",
		},
		{
			name: "partial hour within same hour",
			pgRange: pgtype.Range[pgtype.Timestamptz]{
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Lower:     pgtype.Timestamptz{Time: time.Date(2023, 1, 1, 14, 15, 0, 0, time.UTC), Valid: true},
				Upper:     pgtype.Timestamptz{Time: time.Date(2023, 1, 1, 14, 45, 0, 0, time.UTC), Valid: true},
				Valid:     true,
			},
			expectValid: true,
			description: "partial hour range should be valid",
		},
		{
			name: "range crossing hour boundary",
			pgRange: pgtype.Range[pgtype.Timestamptz]{
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Lower:     pgtype.Timestamptz{Time: time.Date(2023, 1, 1, 14, 30, 0, 0, time.UTC), Valid: true},
				Upper:     pgtype.Timestamptz{Time: time.Date(2023, 1, 1, 15, 30, 0, 0, time.UTC), Valid: true},
				Valid:     true,
			},
			expectValid: false,
			description: "range crossing hour boundary should be invalid",
		},
		{
			name: "range crossing day boundary",
			pgRange: pgtype.Range[pgtype.Timestamptz]{
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Lower:     pgtype.Timestamptz{Time: time.Date(2023, 1, 1, 23, 30, 0, 0, time.UTC), Valid: true},
				Upper:     pgtype.Timestamptz{Time: time.Date(2023, 1, 2, 0, 30, 0, 0, time.UTC), Valid: true},
				Valid:     true,
			},
			expectValid: false,
			description: "range crossing day boundary should be invalid",
		},
		{
			name: "millisecond precision at hour boundary",
			pgRange: pgtype.Range[pgtype.Timestamptz]{
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Lower:     pgtype.Timestamptz{Time: time.Date(2023, 1, 1, 14, 59, 59, 999000000, time.UTC), Valid: true},
				Upper:     pgtype.Timestamptz{Time: time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC), Valid: true},
				Valid:     true,
			},
			expectValid: true,
			description: "range ending exactly at hour boundary should be valid (exclusive end)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Extract time range using our helper
			timeRange, ok := helpers.NewTimeRangeFromPgRange(tt.pgRange)
			assert.True(t, ok, "should be able to extract time range")

			// Test the validation logic used by compaction
			valid := helpers.IsSameDateintHour(timeRange)
			assert.Equal(t, tt.expectValid, valid, tt.description)
		})
	}
}

// TestTimeRangeConsistency ensures all time range operations use consistent semantics.
func TestTimeRangeConsistency(t *testing.T) {
	t.Run("roundtrip through work queue and compaction", func(t *testing.T) {
		// Start with a trigger time
		triggerTime := time.Date(2023, 1, 1, 14, 30, 45, 123000000, time.UTC)

		// Simulate work queue process
		triggerRange := helpers.TimeRange{
			Start: triggerTime,
			End:   triggerTime.Add(time.Minute),
		}

		// Create hour range as work queue would
		hourRange := helpers.CreateHourRange(triggerRange.Start)
		pgRange := hourRange.ToPgRange()

		// Simulate compaction process
		extractedRange, ok := helpers.NewTimeRangeFromPgRange(pgRange)
		assert.True(t, ok, "should extract range successfully")

		// Verify compaction validation passes
		assert.True(t, helpers.IsSameDateintHour(extractedRange), "compaction validation should pass")

		// Verify ranges are equivalent
		assert.Equal(t, hourRange.Start, extractedRange.Start, "start times should match")
		assert.Equal(t, hourRange.End, extractedRange.End, "end times should match")
	})

	t.Run("consistency across different boundary times", func(t *testing.T) {
		// Test various times within the same hour
		baseTimes := []time.Time{
			time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),           // start of hour
			time.Date(2023, 1, 1, 14, 15, 30, 0, time.UTC),         // early in hour
			time.Date(2023, 1, 1, 14, 30, 45, 123000000, time.UTC), // middle of hour
			time.Date(2023, 1, 1, 14, 45, 0, 0, time.UTC),          // late in hour
			time.Date(2023, 1, 1, 14, 59, 59, 999000000, time.UTC), // end of hour
		}

		expectedHourRange := helpers.TimeRange{
			Start: time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
			End:   time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC),
		}

		for i, baseTime := range baseTimes {
			t.Run(t.Name()+"_time_"+string(rune('0'+i)), func(t *testing.T) {
				hourRange := helpers.CreateHourRange(baseTime)
				assert.Equal(t, expectedHourRange, hourRange, "all times in same hour should create same hour range")
				assert.True(t, helpers.IsSameDateintHour(hourRange), "all hour ranges should pass validation")
			})
		}
	})
}
