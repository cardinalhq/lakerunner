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

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		want     string
	}{
		{
			name:     "zero duration",
			duration: 0,
			want:     "0s",
		},
		{
			name:     "30 seconds",
			duration: 30 * time.Second,
			want:     "30s",
		},
		{
			name:     "50 seconds",
			duration: 50 * time.Second,
			want:     "50s",
		},
		{
			name:     "59 seconds",
			duration: 59 * time.Second,
			want:     "59s",
		},
		{
			name:     "exactly 1 minute",
			duration: time.Minute,
			want:     "1m",
		},
		{
			name:     "1 minute 30 seconds",
			duration: time.Minute + 30*time.Second,
			want:     "1m30s",
		},
		{
			name:     "2 minutes",
			duration: 2 * time.Minute,
			want:     "2m",
		},
		{
			name:     "5 minutes 45 seconds",
			duration: 5*time.Minute + 45*time.Second,
			want:     "5m45s",
		},
		{
			name:     "exactly 1 hour",
			duration: time.Hour,
			want:     "1h",
		},
		{
			name:     "1 hour 30 minutes",
			duration: time.Hour + 30*time.Minute,
			want:     "1h30m",
		},
		{
			name:     "1 hour 1 minute",
			duration: time.Hour + time.Minute,
			want:     "1h1m",
		},
		{
			name:     "2 hours",
			duration: 2 * time.Hour,
			want:     "2h",
		},
		{
			name:     "2 hours 15 minutes",
			duration: 2*time.Hour + 15*time.Minute,
			want:     "2h15m",
		},
		{
			name:     "24 hours",
			duration: 24 * time.Hour,
			want:     "24h",
		},
		{
			name:     "25 hours 30 minutes",
			duration: 25*time.Hour + 30*time.Minute,
			want:     "25h30m",
		},
		{
			name:     "fractional seconds rounds to nearest",
			duration: time.Duration(30.7 * float64(time.Second)),
			want:     "31s",
		},
		{
			name:     "fractional seconds rounds up",
			duration: time.Duration(30.9 * float64(time.Second)),
			want:     "31s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatDuration(tt.duration)
			assert.Equal(t, tt.want, got, "FormatDuration(%v)", tt.duration)
		})
	}
}
