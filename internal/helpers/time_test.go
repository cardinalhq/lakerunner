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

package helpers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMSToDateintHour(t *testing.T) {
	tests := []struct {
		name        string
		ms          int64
		wantDateint int32
		wantHour    int16
	}{
		{
			name:        "2023-01-01 14:30:45.123 UTC",
			ms:          1672583445123,
			wantDateint: 20230101,
			wantHour:    14,
		},
		{
			name:        "2023-12-31 23:59:59.999 UTC",
			ms:          1704067199999,
			wantDateint: 20231231,
			wantHour:    23,
		},
		{
			name:        "2023-01-01 00:00:00.000 UTC",
			ms:          1672531200000,
			wantDateint: 20230101,
			wantHour:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dateint, hour := MSToDateintHour(tt.ms)
			assert.Equal(t, tt.wantDateint, dateint)
			assert.Equal(t, tt.wantHour, hour)
		})
	}
}

func TestTruncateToHour(t *testing.T) {
	tests := []struct {
		name string
		t    time.Time
		want time.Time
	}{
		{
			name: "already at hour boundary",
			t:    time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
			want: time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
		},
		{
			name: "middle of hour",
			t:    time.Date(2023, 1, 1, 14, 30, 45, 123000000, time.UTC),
			want: time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
		},
		{
			name: "end of hour",
			t:    time.Date(2023, 1, 1, 14, 59, 59, 999000000, time.UTC),
			want: time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, TruncateToHour(tt.t))
		})
	}
}
