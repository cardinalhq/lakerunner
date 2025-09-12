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

package sweeper

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestMakeOrgDateintKey(t *testing.T) {
	orgID := uuid.New()
	dateInt := int32(20250101)

	key := makeOrgDateintKey(orgID, dateInt)

	expected := orgID.String() + "-20250101"
	assert.Equal(t, expected, key)
}

func TestGetHourFromTimestamp(t *testing.T) {
	tests := []struct {
		name        string
		timestampMs int64
		expectedHr  int16
	}{
		{
			name:        "midnight",
			timestampMs: 1640995200000, // 2022-01-01 00:00:00 UTC
			expectedHr:  0,
		},
		{
			name:        "noon",
			timestampMs: 1641038400000, // 2022-01-01 12:00:00 UTC
			expectedHr:  12,
		},
		{
			name:        "11pm",
			timestampMs: 1641078000000, // 2022-01-01 23:00:00 UTC
			expectedHr:  23,
		},
		{
			name:        "6am",
			timestampMs: 1641016800000, // 2022-01-01 06:00:00 UTC
			expectedHr:  6,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hour := getHourFromTimestamp(tt.timestampMs)
			assert.Equal(t, tt.expectedHr, hour)
		})
	}
}

func TestUpdateBackoffTiming(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name             string
		consecutiveEmpty int
		expectedMin      time.Duration
		expectedMax      time.Duration
	}{
		{
			name:             "0 empty - 1 minute",
			consecutiveEmpty: 0,
			expectedMin:      59 * time.Second,
			expectedMax:      61 * time.Second,
		},
		{
			name:             "2 empty - 1 minute",
			consecutiveEmpty: 2,
			expectedMin:      59 * time.Second,
			expectedMax:      61 * time.Second,
		},
		{
			name:             "3 empty - 5 minutes",
			consecutiveEmpty: 3,
			expectedMin:      4*time.Minute + 59*time.Second,
			expectedMax:      5*time.Minute + 1*time.Second,
		},
		{
			name:             "5 empty - 5 minutes",
			consecutiveEmpty: 5,
			expectedMin:      4*time.Minute + 59*time.Second,
			expectedMax:      5*time.Minute + 1*time.Second,
		},
		{
			name:             "6 empty - 15 minutes",
			consecutiveEmpty: 6,
			expectedMin:      14*time.Minute + 59*time.Second,
			expectedMax:      15*time.Minute + 1*time.Second,
		},
		{
			name:             "10 empty - 15 minutes",
			consecutiveEmpty: 10,
			expectedMin:      14*time.Minute + 59*time.Second,
			expectedMax:      15*time.Minute + 1*time.Second,
		},
		{
			name:             "11 empty - 30 minutes",
			consecutiveEmpty: 11,
			expectedMin:      29*time.Minute + 59*time.Second,
			expectedMax:      30*time.Minute + 1*time.Second,
		},
		{
			name:             "20 empty - 30 minutes",
			consecutiveEmpty: 20,
			expectedMin:      29*time.Minute + 59*time.Second,
			expectedMax:      30*time.Minute + 1*time.Second,
		},
		{
			name:             "21 empty - 1 hour",
			consecutiveEmpty: 21,
			expectedMin:      59*time.Minute + 59*time.Second,
			expectedMax:      60*time.Minute + 1*time.Second,
		},
		{
			name:             "100 empty - 1 hour",
			consecutiveEmpty: 100,
			expectedMin:      59*time.Minute + 59*time.Second,
			expectedMax:      60*time.Minute + 1*time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := updateBackoffTiming(tt.consecutiveEmpty)

			duration := result.Sub(now)
			assert.GreaterOrEqual(t, duration, tt.expectedMin, "Duration too short")
			assert.LessOrEqual(t, duration, tt.expectedMax, "Duration too long")
		})
	}
}
