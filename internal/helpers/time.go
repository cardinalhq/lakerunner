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

import "time"

// Converts milliseconds since epoch to (dateint, hour)
func MSToDateintHour(ms int64) (int32, int16) {
	t := UnixMillisToTime(ms).UTC()
	dateint := int32(t.Year()*10000 + int(t.Month())*100 + t.Day())
	hour := int16(t.Hour())
	return dateint, hour
}

// Helper to convert ms since epoch to time.Time
func UnixMillisToTime(ms int64) time.Time {
	sec := ms / 1000
	nsec := (ms % 1000) * 1e6
	return time.Unix(sec, nsec).UTC()
}
