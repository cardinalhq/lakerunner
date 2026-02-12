// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package queryapi

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDateIntHoursRange(t *testing.T) {
	tests := []struct {
		name          string
		start         time.Time
		end           time.Time
		expectedHours int
	}{
		{
			name:          "1 hour query spanning two hours",
			start:         time.Date(2026, 2, 11, 15, 45, 0, 0, time.UTC),
			end:           time.Date(2026, 2, 11, 16, 45, 0, 0, time.UTC),
			expectedHours: 2, // hours 15 and 16
		},
		{
			name:          "exact hour boundaries",
			start:         time.Date(2026, 2, 11, 15, 0, 0, 0, time.UTC),
			end:           time.Date(2026, 2, 11, 16, 0, 0, 0, time.UTC),
			expectedHours: 2, // hours 15 and 16
		},
		{
			name:          "3 hour query",
			start:         time.Date(2026, 2, 11, 14, 30, 0, 0, time.UTC),
			end:           time.Date(2026, 2, 11, 17, 30, 0, 0, time.UTC),
			expectedHours: 4, // hours 14, 15, 16, 17
		},
		{
			name:          "same hour",
			start:         time.Date(2026, 2, 11, 15, 10, 0, 0, time.UTC),
			end:           time.Date(2026, 2, 11, 15, 50, 0, 0, time.UTC),
			expectedHours: 1, // hour 15 only
		},
		{
			name:          "cross day boundary",
			start:         time.Date(2026, 2, 11, 23, 30, 0, 0, time.UTC),
			end:           time.Date(2026, 2, 12, 1, 30, 0, 0, time.UTC),
			expectedHours: 3, // hours 23, 00, 01
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := dateIntHoursRange(tt.start.UnixMilli(), tt.end.UnixMilli(), time.UTC, false)
			totalHours := 0
			for _, dih := range result {
				totalHours += len(dih.Hours)
			}
			assert.Equal(t, tt.expectedHours, totalHours, "unexpected number of hours")
		})
	}
}

func TestDateIntHoursRange_NoExtraHour(t *testing.T) {
	// Regression test: ensure we don't query an extra hour beyond the end time
	start := time.Date(2026, 2, 11, 15, 0, 0, 0, time.UTC)
	end := time.Date(2026, 2, 11, 16, 0, 0, 0, time.UTC)

	result := dateIntHoursRange(start.UnixMilli(), end.UnixMilli(), time.UTC, false)

	// Should only have hours 15 and 16, not 17
	totalHours := 0
	var allHours []string
	for _, dih := range result {
		totalHours += len(dih.Hours)
		allHours = append(allHours, dih.Hours...)
	}

	assert.Equal(t, 2, totalHours, "should have exactly 2 hours")
	assert.NotContains(t, allHours, "17", "should not include hour 17")
}
