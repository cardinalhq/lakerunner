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

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeRange_IsValid(t *testing.T) {
	tests := []struct {
		name string
		tr   TimeRange
		want bool
	}{
		{
			name: "valid range",
			tr:   TimeRange{Start: time.Unix(1000, 0), End: time.Unix(2000, 0)},
			want: true,
		},
		{
			name: "invalid range - same time",
			tr:   TimeRange{Start: time.Unix(1000, 0), End: time.Unix(1000, 0)},
			want: false,
		},
		{
			name: "invalid range - end before start",
			tr:   TimeRange{Start: time.Unix(2000, 0), End: time.Unix(1000, 0)},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.tr.IsValid())
		})
	}
}

func TestTimeRange_Contains(t *testing.T) {
	start := time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC)
	end := time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC)
	tr := TimeRange{Start: start, End: end}

	tests := []struct {
		name string
		t    time.Time
		want bool
	}{
		{
			name: "before range",
			t:    start.Add(-time.Minute),
			want: false,
		},
		{
			name: "at start (inclusive)",
			t:    start,
			want: true,
		},
		{
			name: "inside range",
			t:    start.Add(30 * time.Minute),
			want: true,
		},
		{
			name: "at end (exclusive)",
			t:    end,
			want: false,
		},
		{
			name: "after range",
			t:    end.Add(time.Minute),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tr.Contains(tt.t))
		})
	}
}

func TestTimeRange_Overlaps(t *testing.T) {
	base := TimeRange{
		Start: time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
		End:   time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC),
	}

	tests := []struct {
		name  string
		other TimeRange
		want  bool
	}{
		{
			name: "no overlap - before",
			other: TimeRange{
				Start: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "no overlap - after",
			other: TimeRange{
				Start: time.Date(2023, 1, 1, 16, 0, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 17, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "no overlap - adjacent before",
			other: TimeRange{
				Start: time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "no overlap - adjacent after",
			other: TimeRange{
				Start: time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 16, 0, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "overlap - partial before",
			other: TimeRange{
				Start: time.Date(2023, 1, 1, 13, 30, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 14, 30, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "overlap - partial after",
			other: TimeRange{
				Start: time.Date(2023, 1, 1, 14, 30, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 15, 30, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "overlap - completely inside",
			other: TimeRange{
				Start: time.Date(2023, 1, 1, 14, 15, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 14, 45, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "overlap - completely outside",
			other: TimeRange{
				Start: time.Date(2023, 1, 1, 13, 0, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 16, 0, 0, 0, time.UTC),
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, base.Overlaps(tt.other))
		})
	}
}

func TestTimeRange_ToMillis(t *testing.T) {
	start := time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC)
	end := time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC)
	tr := TimeRange{Start: start, End: end}

	startMs, endMs := tr.ToMillis()
	assert.Equal(t, start.UnixMilli(), startMs)
	assert.Equal(t, end.UnixMilli(), endMs)
}

func TestNewTimeRangeFromMillis(t *testing.T) {
	startMs := int64(1672574400000) // 2023-01-01 14:00:00 UTC
	endMs := int64(1672578000000)   // 2023-01-01 15:00:00 UTC

	tr := NewTimeRangeFromMillis(startMs, endMs)

	assert.Equal(t, startMs, tr.Start.UnixMilli())
	assert.Equal(t, endMs, tr.End.UnixMilli())
	assert.True(t, tr.IsValid())
}

func TestNewTimeRangeFromPgRange(t *testing.T) {
	start := time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC)
	end := time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC)

	pgRange := pgtype.Range[pgtype.Timestamptz]{
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Lower:     pgtype.Timestamptz{Time: start, Valid: true},
		Upper:     pgtype.Timestamptz{Time: end, Valid: true},
		Valid:     true,
	}

	tr, ok := NewTimeRangeFromPgRange(pgRange)
	require.True(t, ok)
	assert.Equal(t, start, tr.Start)
	assert.Equal(t, end, tr.End)
}

func TestTimeRange_ToPgRange(t *testing.T) {
	start := time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC)
	end := time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC)
	tr := TimeRange{Start: start, End: end}

	pgRange := tr.ToPgRange()

	assert.Equal(t, pgtype.Inclusive, pgRange.LowerType)
	assert.Equal(t, pgtype.Exclusive, pgRange.UpperType)
	assert.Equal(t, start, pgRange.Lower.Time)
	assert.Equal(t, end, pgRange.Upper.Time)
	assert.True(t, pgRange.Valid)
}

func TestHourBoundary_ToTime(t *testing.T) {
	tests := []struct {
		name     string
		boundary HourBoundary
		want     time.Time
	}{
		{
			name:     "basic hour",
			boundary: HourBoundary{DateInt: 20230101, Hour: 14},
			want:     time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
		},
		{
			name:     "midnight",
			boundary: HourBoundary{DateInt: 20230101, Hour: 0},
			want:     time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "end of day",
			boundary: HourBoundary{DateInt: 20230101, Hour: 23},
			want:     time.Date(2023, 1, 1, 23, 0, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.boundary.ToTime())
		})
	}
}

func TestHourBoundary_ToTimeRange(t *testing.T) {
	boundary := HourBoundary{DateInt: 20230101, Hour: 14}
	tr := boundary.ToTimeRange()

	expectedStart := time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC)
	expectedEnd := time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC)

	assert.Equal(t, expectedStart, tr.Start)
	assert.Equal(t, expectedEnd, tr.End)
	assert.Equal(t, time.Hour, tr.Duration())
}

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

func TestTimeToHourBoundary(t *testing.T) {
	tests := []struct {
		name string
		t    time.Time
		want HourBoundary
	}{
		{
			name: "exact hour",
			t:    time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
			want: HourBoundary{DateInt: 20230101, Hour: 14},
		},
		{
			name: "middle of hour",
			t:    time.Date(2023, 1, 1, 14, 30, 45, 123000000, time.UTC),
			want: HourBoundary{DateInt: 20230101, Hour: 14},
		},
		{
			name: "midnight",
			t:    time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
			want: HourBoundary{DateInt: 20230101, Hour: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, TimeToHourBoundary(tt.t))
		})
	}
}

func TestTimeRangeToHourBoundaries(t *testing.T) {
	tests := []struct {
		name      string
		tr        TimeRange
		wantStart HourBoundary
		wantEnd   HourBoundary
	}{
		{
			name: "exact hour range",
			tr: TimeRange{
				Start: time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC),
			},
			wantStart: HourBoundary{DateInt: 20230101, Hour: 14},
			wantEnd:   HourBoundary{DateInt: 20230101, Hour: 14},
		},
		{
			name: "partial hour range within same hour",
			tr: TimeRange{
				Start: time.Date(2023, 1, 1, 14, 15, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 14, 45, 0, 0, time.UTC),
			},
			wantStart: HourBoundary{DateInt: 20230101, Hour: 14},
			wantEnd:   HourBoundary{DateInt: 20230101, Hour: 14},
		},
		{
			name: "range crossing hour boundary",
			tr: TimeRange{
				Start: time.Date(2023, 1, 1, 14, 30, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 15, 30, 0, 0, time.UTC),
			},
			wantStart: HourBoundary{DateInt: 20230101, Hour: 14},
			wantEnd:   HourBoundary{DateInt: 20230101, Hour: 15},
		},
		{
			name: "range crossing day boundary",
			tr: TimeRange{
				Start: time.Date(2023, 1, 1, 23, 30, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 2, 0, 30, 0, 0, time.UTC),
			},
			wantStart: HourBoundary{DateInt: 20230101, Hour: 23},
			wantEnd:   HourBoundary{DateInt: 20230102, Hour: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end := TimeRangeToHourBoundaries(tt.tr)
			assert.Equal(t, tt.wantStart, start)
			assert.Equal(t, tt.wantEnd, end)
		})
	}
}

func TestIsHourAligned(t *testing.T) {
	tests := []struct {
		name string
		tr   TimeRange
		want bool
	}{
		{
			name: "perfect hour alignment",
			tr: TimeRange{
				Start: time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "start not at hour boundary",
			tr: TimeRange{
				Start: time.Date(2023, 1, 1, 14, 1, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 15, 1, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "wrong duration",
			tr: TimeRange{
				Start: time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 14, 30, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "invalid range",
			tr: TimeRange{
				Start: time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsHourAligned(tt.tr))
		})
	}
}

func TestIsSameDateintHour(t *testing.T) {
	tests := []struct {
		name string
		tr   TimeRange
		want bool
	}{
		{
			name: "same hour - exact boundaries",
			tr: TimeRange{
				Start: time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "same hour - partial range",
			tr: TimeRange{
				Start: time.Date(2023, 1, 1, 14, 15, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 14, 45, 0, 0, time.UTC),
			},
			want: true,
		},
		{
			name: "different hours",
			tr: TimeRange{
				Start: time.Date(2023, 1, 1, 14, 30, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 15, 30, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "different days",
			tr: TimeRange{
				Start: time.Date(2023, 1, 1, 23, 30, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 2, 0, 30, 0, 0, time.UTC),
			},
			want: false,
		},
		{
			name: "invalid range",
			tr: TimeRange{
				Start: time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsSameDateintHour(tt.tr))
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

func TestCreateHourRange(t *testing.T) {
	tests := []struct {
		name string
		t    time.Time
		want TimeRange
	}{
		{
			name: "at hour boundary",
			t:    time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
			want: TimeRange{
				Start: time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "middle of hour",
			t:    time.Date(2023, 1, 1, 14, 30, 45, 123000000, time.UTC),
			want: TimeRange{
				Start: time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
				End:   time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CreateHourRange(tt.t)
			assert.Equal(t, tt.want, result)
			assert.True(t, IsHourAligned(result))
			assert.True(t, IsSameDateintHour(result))
		})
	}
}

func TestCreateHourRangeFromBoundary(t *testing.T) {
	boundary := HourBoundary{DateInt: 20230101, Hour: 14}
	tr := CreateHourRangeFromBoundary(boundary)

	expected := TimeRange{
		Start: time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
		End:   time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC),
	}

	assert.Equal(t, expected, tr)
	assert.True(t, IsHourAligned(tr))
	assert.True(t, IsSameDateintHour(tr))
}

// Test edge cases around millisecond precision and boundary conditions
func TestTimeRangeBoundaryEdgeCases(t *testing.T) {
	t.Run("millisecond before hour boundary", func(t *testing.T) {
		// Range that ends exactly at hour boundary (exclusive)
		tr := TimeRange{
			Start: time.Date(2023, 1, 1, 14, 0, 0, 0, time.UTC),
			End:   time.Date(2023, 1, 1, 15, 0, 0, 0, time.UTC),
		}

		// Should be same dateint-hour because end is exclusive
		assert.True(t, IsSameDateintHour(tr))

		// The last included moment should be 14:59:59.999
		startBoundary, endBoundary := TimeRangeToHourBoundaries(tr)
		assert.Equal(t, startBoundary, endBoundary)
	})

	t.Run("millisecond precision edge case", func(t *testing.T) {
		// Range that would cross boundary if we incorrectly handle exclusivity
		tr := TimeRange{
			Start: time.Date(2023, 1, 1, 14, 59, 59, 999000000, time.UTC),
			End:   time.Date(2023, 1, 1, 15, 0, 0, 1000000, time.UTC), // 1ms into next hour
		}

		// Should NOT be same dateint-hour because it crosses boundary
		assert.False(t, IsSameDateintHour(tr))
	})

	t.Run("cross day boundary", func(t *testing.T) {
		tr := TimeRange{
			Start: time.Date(2023, 1, 1, 23, 30, 0, 0, time.UTC),
			End:   time.Date(2023, 1, 2, 0, 30, 0, 0, time.UTC),
		}

		assert.False(t, IsSameDateintHour(tr))

		startBoundary, endBoundary := TimeRangeToHourBoundaries(tr)
		assert.Equal(t, int32(20230101), startBoundary.DateInt)
		assert.Equal(t, int16(23), startBoundary.Hour)
		assert.Equal(t, int32(20230102), endBoundary.DateInt)
		assert.Equal(t, int16(0), endBoundary.Hour)
	})
}

// Test roundtrip conversions to ensure no data loss
func TestTimeRangeRoundtripConversions(t *testing.T) {
	t.Run("TimeRange -> PgRange -> TimeRange", func(t *testing.T) {
		original := TimeRange{
			Start: time.Date(2023, 1, 1, 14, 30, 45, 123000000, time.UTC),
			End:   time.Date(2023, 1, 1, 15, 30, 45, 123000000, time.UTC),
		}

		pgRange := original.ToPgRange()
		converted, ok := NewTimeRangeFromPgRange(pgRange)

		require.True(t, ok)
		assert.Equal(t, original.Start, converted.Start)
		assert.Equal(t, original.End, converted.End)
	})

	t.Run("Millis -> TimeRange -> Millis", func(t *testing.T) {
		originalStartMs := int64(1672574445123) // 2023-01-01 14:30:45.123 UTC
		originalEndMs := int64(1672578045123)   // 2023-01-01 15:30:45.123 UTC

		tr := NewTimeRangeFromMillis(originalStartMs, originalEndMs)
		convertedStartMs, convertedEndMs := tr.ToMillis()

		assert.Equal(t, originalStartMs, convertedStartMs)
		assert.Equal(t, originalEndMs, convertedEndMs)
	})

	t.Run("HourBoundary -> TimeRange -> HourBoundary", func(t *testing.T) {
		original := HourBoundary{DateInt: 20230101, Hour: 14}

		tr := original.ToTimeRange()

		// Convert back - should get same boundary for both start and end
		startBoundary, endBoundary := TimeRangeToHourBoundaries(tr)

		assert.Equal(t, original, startBoundary)
		assert.Equal(t, original, endBoundary) // Since it's a perfect hour range
	})
}
