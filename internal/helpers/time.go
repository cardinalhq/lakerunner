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
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// TimeRange represents a half-open interval [start, end) with millisecond precision.
// This matches PostgreSQL's int8range('[start, end)') semantics used throughout the codebase.
type TimeRange struct {
	Start time.Time // Inclusive
	End   time.Time // Exclusive
}

// IsValid returns true if the time range is valid (start < end).
func (tr TimeRange) IsValid() bool {
	return tr.Start.Before(tr.End)
}

// Contains checks if a timestamp falls within the range [start, end).
func (tr TimeRange) Contains(t time.Time) bool {
	return !t.Before(tr.Start) && t.Before(tr.End)
}

// Overlaps checks if this range overlaps with another range.
func (tr TimeRange) Overlaps(other TimeRange) bool {
	return tr.Start.Before(other.End) && other.Start.Before(tr.End)
}

// Duration returns the duration of the time range.
func (tr TimeRange) Duration() time.Duration {
	if !tr.IsValid() {
		return 0
	}
	return tr.End.Sub(tr.Start)
}

// ToMillis returns the range as [startMs, endMs) in milliseconds since epoch.
func (tr TimeRange) ToMillis() (startMs, endMs int64) {
	return tr.Start.UnixMilli(), tr.End.UnixMilli()
}

// NewTimeRangeFromMillis creates a TimeRange from millisecond timestamps.
// Follows [start, end) semantics (start inclusive, end exclusive).
func NewTimeRangeFromMillis(startMs, endMs int64) TimeRange {
	return TimeRange{
		Start: UnixMillisToTime(startMs),
		End:   UnixMillisToTime(endMs),
	}
}

// NewTimeRangeFromPgRange extracts a TimeRange from a PostgreSQL range type.
// Returns the range in normalized [start, end) format.
func NewTimeRangeFromPgRange(r pgtype.Range[pgtype.Timestamptz]) (TimeRange, bool) {
	start, end, ok := RangeBounds(r)
	if !ok {
		return TimeRange{}, false
	}
	return TimeRange{
		Start: start.Time,
		End:   end.Time,
	}, true
}

// ToPgRange converts a TimeRange to a PostgreSQL range type.
func (tr TimeRange) ToPgRange() pgtype.Range[pgtype.Timestamptz] {
	return pgtype.Range[pgtype.Timestamptz]{
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Lower:     pgtype.Timestamptz{Time: tr.Start, Valid: true},
		Upper:     pgtype.Timestamptz{Time: tr.End, Valid: true},
		Valid:     true,
	}
}

// HourBoundary represents a specific hour boundary for data partitioning.
type HourBoundary struct {
	DateInt int32 // YYYYMMDD format
	Hour    int16 // 0-23
}

// ToTime converts the hour boundary to a time.Time at the start of that hour.
func (hb HourBoundary) ToTime() time.Time {
	year := int(hb.DateInt / 10000)
	month := int((hb.DateInt / 100) % 100)
	day := int(hb.DateInt % 100)
	return time.Date(year, time.Month(month), day, int(hb.Hour), 0, 0, 0, time.UTC)
}

// ToTimeRange returns the full hour range [hour_start, hour_start+1h) for this boundary.
func (hb HourBoundary) ToTimeRange() TimeRange {
	start := hb.ToTime()
	return TimeRange{
		Start: start,
		End:   start.Add(time.Hour),
	}
}

// Converts milliseconds since epoch to (dateint, hour)
func MSToDateintHour(ms int64) (int32, int16) {
	t := UnixMillisToTime(ms).UTC()
	dateint := int32(t.Year()*10000 + int(t.Month())*100 + t.Day())
	hour := int16(t.Hour())
	return dateint, hour
}

// TimeToHourBoundary extracts the hour boundary from a timestamp.
func TimeToHourBoundary(t time.Time) HourBoundary {
	dateint, hour := MSToDateintHour(t.UnixMilli())
	return HourBoundary{DateInt: dateint, Hour: hour}
}

// TimeRangeToHourBoundaries extracts start and end hour boundaries from a time range.
// For a valid hour-aligned range, both boundaries should be the same.
func TimeRangeToHourBoundaries(tr TimeRange) (start, end HourBoundary) {
	start = TimeToHourBoundary(tr.Start)
	// For exclusive end time, check the last included moment
	lastIncluded := tr.End.Add(-time.Millisecond)
	end = TimeToHourBoundary(lastIncluded)
	return start, end
}

// IsHourAligned checks if a time range is properly aligned to hour boundaries.
// Returns true if the range represents exactly one hour: [hour_start, hour_start+1h).
func IsHourAligned(tr TimeRange) bool {
	if !tr.IsValid() {
		return false
	}

	// Check if start is at the beginning of an hour
	if tr.Start.Minute() != 0 || tr.Start.Second() != 0 || tr.Start.Nanosecond() != 0 {
		return false
	}

	// Check if duration is exactly one hour
	if tr.Duration() != time.Hour {
		return false
	}

	// Verify boundaries are the same
	startBoundary, endBoundary := TimeRangeToHourBoundaries(tr)
	return startBoundary == endBoundary
}

// IsSameDateintHour checks if a time range falls entirely within the same dateint-hour.
// This is the key validation used by compaction logic.
func IsSameDateintHour(tr TimeRange) bool {
	if !tr.IsValid() {
		return false
	}

	startBoundary, endBoundary := TimeRangeToHourBoundaries(tr)
	return startBoundary == endBoundary
}

// TruncateToHour truncates a timestamp to the beginning of its hour.
func TruncateToHour(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.UTC)
}

// CreateHourRange creates a time range for a complete hour starting at the given time.
// The input time is truncated to the hour boundary.
func CreateHourRange(t time.Time) TimeRange {
	start := TruncateToHour(t)
	return TimeRange{
		Start: start,
		End:   start.Add(time.Hour),
	}
}

// CreateHourRangeFromBoundary creates a time range for a specific hour boundary.
func CreateHourRangeFromBoundary(hb HourBoundary) TimeRange {
	return hb.ToTimeRange()
}

// Helper to convert ms since epoch to time.Time
func UnixMillisToTime(ms int64) time.Time {
	sec := ms / 1000
	nsec := (ms % 1000) * 1e6
	return time.Unix(sec, nsec).UTC()
}
