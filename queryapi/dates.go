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

package queryapi

import (
	"fmt"
	"sort"
	"time"
)

func StepForQueryDuration(startMs, endMs int64) time.Duration {
	oneHourish := int64(1 * 65 * 60 * 1000)
	twelveHours := int64(12 * 60 * 60 * 1000)
	oneDay := int64(24 * 60 * 60 * 1000)
	threeDays := int64(3 * 24 * 60 * 60 * 1000)

	span := endMs - startMs
	switch {
	case span <= oneHourish:
		return 10 * time.Second
	case span <= twelveHours:
		return time.Minute
	case span <= oneDay:
		return 5 * time.Minute
	case span <= threeDays:
		return 20 * time.Minute
	default:
		return time.Hour
	}
}

type DateIntHours struct {
	DateInt int      // e.g. 20250814
	Hours   []string // "00".."23"
}

// zeroFilledHour returns "00".."23".
func zeroFilledHour(h int) string {
	return fmt.Sprintf("%02d", h)
}

// toDateInt converts a time to YYYYMMDD (UTC unless you pass a different loc).
func toDateInt(t time.Time) int {
	y, m, d := t.Date()
	return y*10000 + int(m)*100 + d
}

// dateIntHoursRange: given a time range produces the date int hours, in reverse order of date ints, and hours are reverse as well.
func dateIntHoursRange(startMs, endMs int64, loc *time.Location) []DateIntHours {
	if loc == nil {
		loc = time.UTC
	}
	start := time.UnixMilli(startMs).In(loc).Truncate(time.Hour)
	end := time.UnixMilli(endMs).In(loc).Truncate(time.Hour)

	var out []DateIntHours

	var curDateInt int
	hoursSet := make(map[string]struct{})
	flush := func() {
		if curDateInt == 0 || len(hoursSet) == 0 {
			return
		}
		hh := make([]string, 0, len(hoursSet))
		for h := range hoursSet {
			hh = append(hh, h)
		}
		// reverse hour order "23".."00"
		sort.Sort(sort.Reverse(sort.StringSlice(hh)))
		out = append(out, DateIntHours{DateInt: curDateInt, Hours: hh})
		hoursSet = make(map[string]struct{})
	}

	for t := start; !t.After(end.Add(time.Hour)); t = t.Add(time.Hour) {
		di := toDateInt(t)
		if curDateInt != 0 && di != curDateInt {
			flush()
		}
		curDateInt = di
		hoursSet[zeroFilledHour(t.Hour())] = struct{}{}
	}
	flush()

	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}

	return out
}
