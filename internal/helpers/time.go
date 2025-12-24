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
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

// Converts milliseconds since epoch to (dateint, hour)
func MSToDateintHour(ms int64) (int32, int16) {
	t := UnixMillisToTime(ms).UTC()
	dateint := int32(t.Year()*10000 + int(t.Month())*100 + t.Day())
	hour := int16(t.Hour())
	return dateint, hour
}

// TruncateToHour truncates a timestamp to the beginning of its hour.
func TruncateToHour(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.UTC)
}

// Helper to convert ms since epoch to time.Time
func UnixMillisToTime(ms int64) time.Time {
	sec := ms / 1000
	nsec := (ms % 1000) * 1e6
	return time.Unix(sec, nsec).UTC()
}

// HourFromMillis extracts the hour component from a Unix timestamp in milliseconds
func HourFromMillis(ms int64) int16 {
	t := UnixMillisToTime(ms)
	return int16(t.Hour())
}

// FormatDuration formats a duration in a compact, human-readable way.
// Examples: "50s", "1m30s", "1h30m", "2h"
func FormatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}

	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if hours > 0 {
		if minutes > 0 {
			return fmt.Sprintf("%dh%dm", hours, minutes)
		}
		return fmt.Sprintf("%dh", hours)
	}

	if seconds > 0 {
		return fmt.Sprintf("%dm%ds", minutes, seconds)
	}

	return fmt.Sprintf("%dm", minutes)
}

// RangeBounds returns the bounds of the given range. The returned start time is inclusive
// and the end time is exclusive. If the range is unbounded, or cannot be converted
// to concrete bounds, the function returns ok=false and the bounds are invalid.
func RangeBounds[T int | int16 | int32 | int64 | pgtype.Int8 | time.Time | pgtype.Timestamptz](r pgtype.Range[T]) (lower, upper T, ok bool) {
	if r.LowerType != pgtype.Inclusive && r.LowerType != pgtype.Exclusive {
		return lower, upper, false
	}
	if r.UpperType != pgtype.Inclusive && r.UpperType != pgtype.Exclusive {
		return lower, upper, false
	}

	lower = r.Lower
	upper = r.Upper

	// Always return inclusive lower and exclusive upper.
	// Only accept inclusive or exclusive lower, and inclusive or exclusive upper.
	if r.LowerType != pgtype.Inclusive && r.LowerType != pgtype.Exclusive {
		return lower, upper, false
	}
	if r.UpperType != pgtype.Inclusive && r.UpperType != pgtype.Exclusive {
		return lower, upper, false
	}

	// If the input lower is exclusive, increment lower by 1ns or 1 (for ints).
	if r.LowerType == pgtype.Exclusive {
		switch v := any(lower).(type) {
		case time.Time:
			lower = any(v.Add(time.Nanosecond)).(T)
		case pgtype.Timestamptz:
			shifted := pgtype.Timestamptz{
				Time:  v.Time.Add(time.Nanosecond),
				Valid: true,
			}
			lower = any(shifted).(T)
		case pgtype.Int8:
			shifted := pgtype.Int8{
				Int64: v.Int64 + 1,
				Valid: true,
			}
			lower = any(shifted).(T)
		case int:
			lower = any(v + 1).(T)
		case int16:
			lower = any(v + 1).(T)
		case int32:
			lower = any(v + 1).(T)
		case int64:
			lower = any(v + 1).(T)
		default:
			return lower, upper, false
		}
	}

	// If the input upper is inclusive, increment upper by 1ns or 1 (for ints) to make it exclusive.
	if r.UpperType == pgtype.Inclusive {
		switch v := any(upper).(type) {
		case time.Time:
			upper = any(v.Add(time.Nanosecond)).(T)
		case pgtype.Timestamptz:
			shifted := pgtype.Timestamptz{
				Time:  v.Time.Add(time.Nanosecond),
				Valid: true,
			}
			upper = any(shifted).(T)
		case pgtype.Int8:
			shifted := pgtype.Int8{
				Int64: v.Int64 + 1,
				Valid: true,
			}
			upper = any(shifted).(T)
		case int:
			upper = any(v + 1).(T)
		case int16:
			upper = any(v + 1).(T)
		case int32:
			upper = any(v + 1).(T)
		case int64:
			upper = any(v + 1).(T)
		default:
			return lower, upper, false
		}
	}

	return lower, upper, true
}

// CurrentDateInt returns the current UTC date in YYYYMMDD format.
// This is used throughout the codebase for IngestDateint fields.
func CurrentDateInt() int32 {
	now := time.Now().UTC()
	return int32(now.Year()*10000 + int(now.Month())*100 + now.Day())
}
