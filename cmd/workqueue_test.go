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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func TestQmcFromInqueue(t *testing.T) {
	orgID := uuid.New()
	instanceNum := int16(1)
	startTS := time.Date(2023, 1, 1, 14, 30, 45, 123000000, time.UTC).UnixMilli()

	mockInqueue := lrdb.Inqueue{
		OrganizationID: orgID,
		InstanceNum:    instanceNum,
	}

	tests := []struct {
		name        string
		frequency   int32
		expectValid bool
		description string
	}{
		{
			name:        "log compaction frequency (1 hour)",
			frequency:   3600000,
			expectValid: true,
			description: "frequency 3600000 should create valid 1-hour range",
		},
		{
			name:        "metrics 10s frequency",
			frequency:   10_000,
			expectValid: true,
			description: "positive frequency should create valid range",
		},
		{
			name:        "metrics 1min frequency",
			frequency:   60_000,
			expectValid: true,
			description: "1-minute frequency should create valid range",
		},
		{
			name:        "metrics 5min frequency",
			frequency:   300_000,
			expectValid: true,
			description: "5-minute frequency should create valid range",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := qmcFromInqueue(mockInqueue, tt.frequency, startTS)

			// Verify basic fields are set correctly
			assert.Equal(t, orgID, result.OrganizationID)
			assert.Equal(t, instanceNum, result.InstanceNum)
			assert.Equal(t, tt.frequency, result.FrequencyMs)

			// Extract and validate the time range
			timeRange, ok := helpers.NewTimeRangeFromPgRange(result.TsRange)
			assert.True(t, ok, "should be able to extract time range")
			assert.True(t, timeRange.IsValid(), "time range should be valid (start < end)")

			// Verify the start time matches the input
			expectedStart := time.UnixMilli(startTS).UTC()
			assert.Equal(t, expectedStart, timeRange.Start, "start time should match input")

			// Verify expected duration based on frequency
			// All frequencies now create ranges based on frequency duration
			expectedDuration := time.Duration(tt.frequency) * time.Millisecond
			assert.Equal(t, expectedDuration, timeRange.Duration(), "range should use frequency duration")

			// Verify the range is valid for the downstream work queue processing
			assert.True(t, tt.expectValid == timeRange.IsValid(), tt.description)
		})
	}
}

// TestQmcFromInqueue_LogCompactionIntegration tests the full integration
// from log ingestion trigger range to final work queue compaction range.
func TestQmcFromInqueue_LogCompactionIntegration(t *testing.T) {
	orgID := uuid.New()
	instanceNum := int16(1)

	// Simulate various log ingestion trigger times
	triggerTimes := []time.Time{
		time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),           // Hour boundary
		time.Date(2023, 1, 1, 14, 30, 45, 123000000, time.UTC), // Mid-hour
		time.Date(2023, 1, 1, 14, 59, 59, 999000000, time.UTC), // Near hour end
	}

	mockInqueue := lrdb.Inqueue{
		OrganizationID: orgID,
		InstanceNum:    instanceNum,
	}

	for i, triggerTime := range triggerTimes {
		t.Run(t.Name()+"_time_"+string(rune('0'+i)), func(t *testing.T) {
			// Step 1: Create hour-aligned timestamp as log ingestion would
			hourAlignedTS := helpers.TruncateToHour(triggerTime).UnixMilli()

			// Step 2: Create hour range using the new approach (1 hour frequency)
			hourRangeQmc := qmcFromInqueue(mockInqueue, 3600000, hourAlignedTS)

			// Verify the range is valid and hour-aligned
			extractedRange, ok := helpers.NewTimeRangeFromPgRange(hourRangeQmc.TsRange)
			assert.True(t, ok, "should extract hour range")
			assert.True(t, extractedRange.IsValid(), "hour range should be valid")
			assert.True(t, helpers.IsHourAligned(extractedRange), "range should be hour-aligned")
			assert.True(t, helpers.IsSameDateintHour(extractedRange), "range should pass compaction validation")

			// Verify the hour range encompasses the original trigger time
			assert.True(t, extractedRange.Contains(triggerTime), "hour range should contain original trigger time")

			// Verify the range covers exactly one hour
			expectedHourRange := helpers.CreateHourRange(triggerTime)
			assert.Equal(t, expectedHourRange, extractedRange, "should create proper hour range")
		})
	}
}

// TestQmcFromInqueue_EdgeCases tests edge cases and potential issues.
func TestQmcFromInqueue_EdgeCases(t *testing.T) {
	orgID := uuid.New()
	instanceNum := int16(1)

	mockInqueue := lrdb.Inqueue{
		OrganizationID: orgID,
		InstanceNum:    instanceNum,
	}

	t.Run("zero timestamp with hour frequency", func(t *testing.T) {
		result := qmcFromInqueue(mockInqueue, 3600000, 0)
		timeRange, ok := helpers.NewTimeRangeFromPgRange(result.TsRange)
		assert.True(t, ok)
		assert.True(t, timeRange.IsValid(), "should handle zero timestamp")
		assert.True(t, helpers.IsHourAligned(timeRange), "should create hour-aligned range")
	})

	t.Run("very large timestamp with hour frequency", func(t *testing.T) {
		// Year 2050 - hour-aligned
		largeTime := time.Date(2050, 12, 31, 23, 0, 0, 0, time.UTC)
		result := qmcFromInqueue(mockInqueue, 3600000, largeTime.UnixMilli())
		timeRange, ok := helpers.NewTimeRangeFromPgRange(result.TsRange)
		assert.True(t, ok)
		assert.True(t, timeRange.IsValid(), "should handle large timestamps")
		assert.True(t, helpers.IsHourAligned(timeRange), "should create hour-aligned range")
	})

	t.Run("day boundary trigger with hour frequency", func(t *testing.T) {
		// Trigger at day boundary - hour 23
		dayBoundary := time.Date(2023, 1, 1, 23, 0, 0, 0, time.UTC)
		result := qmcFromInqueue(mockInqueue, 3600000, dayBoundary.UnixMilli())
		timeRange, ok := helpers.NewTimeRangeFromPgRange(result.TsRange)
		assert.True(t, ok)
		assert.True(t, timeRange.IsValid(), "should handle day boundary triggers")
		assert.True(t, helpers.IsHourAligned(timeRange), "should create hour-aligned range")

		// Verify it creates exactly the hour 23:00-24:00
		expectedEnd := time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC)
		assert.Equal(t, dayBoundary, timeRange.Start)
		assert.Equal(t, expectedEnd, timeRange.End)
	})
}
