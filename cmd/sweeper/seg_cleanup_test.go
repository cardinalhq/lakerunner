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

package sweeper

import (
	"testing"

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
