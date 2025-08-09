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

package logcrunch

import (
	"fmt"
	"time"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// PackSegments groups segments into packs such that the sum of RecordCount in each
// pack is <= estimatedRecordCount (greedy, one-or-more per pack).
// NOTE: segments must all lie within the same UTC day and must be sorted by StartTs ascending.
func PackSegments(segments []lrdb.GetLogSegmentsForCompactionRow, estimatedRecordCount int64) ([][]lrdb.GetLogSegmentsForCompactionRow, error) {
	if estimatedRecordCount <= 0 {
		return nil, fmt.Errorf("estimatedRecordCount must be positive, got %d", estimatedRecordCount)
	}
	if len(segments) == 0 {
		return [][]lrdb.GetLogSegmentsForCompactionRow{}, nil
	}

	// 1) Drop zero-record segments up front.
	segments = filterSegments(segments)
	if len(segments) == 0 {
		return [][]lrdb.GetLogSegmentsForCompactionRow{}, nil
	}

	// 2) Validate same-day and basic time sanity.
	day := dayFromMillis(segments[0].StartTs)
	for _, seg := range segments {
		if seg.StartTs >= seg.EndTs {
			return nil, fmt.Errorf("invalid segment time range: [%d,%d)", seg.StartTs, seg.EndTs)
		}
		// Use end-1 to keep [start,end) semantics in the same day
		endMinusOne := seg.EndTs - 1
		// Guard (very defensive)
		if seg.EndTs == -9223372036854775808 { // math.MinInt64, inline to avoid import
			return nil, fmt.Errorf("invalid EndTs (MinInt64) for segment starting at %d", seg.StartTs)
		}
		if dayFromMillis(seg.StartTs) != day || dayFromMillis(endMinusOne) != day {
			return nil, fmt.Errorf("segments must be from the same UTC day; offending start=%d end=%d", seg.StartTs, seg.EndTs)
		}
	}

	// 3) Greedy packing by record count threshold.
	groups := make([][]lrdb.GetLogSegmentsForCompactionRow, 0, len(segments)/2+1)
	current := make([]lrdb.GetLogSegmentsForCompactionRow, 0, 8)
	var sumRecords int64

	for _, seg := range segments {
		rc := seg.RecordCount
		if len(current) == 0 || sumRecords+rc <= estimatedRecordCount {
			current = append(current, seg)
			sumRecords += rc
			continue
		}
		groups = append(groups, current)
		current = []lrdb.GetLogSegmentsForCompactionRow{seg}
		sumRecords = rc
	}
	if len(current) > 0 {
		groups = append(groups, current)
	}
	return groups, nil
}

func filterSegments(segs []lrdb.GetLogSegmentsForCompactionRow) []lrdb.GetLogSegmentsForCompactionRow {
	out := make([]lrdb.GetLogSegmentsForCompactionRow, 0, len(segs))
	for _, s := range segs {
		if s.RecordCount > 0 {
			out = append(out, s)
		}
	}
	return out
}

func dayFromMillis(millis int64) time.Time {
	// Normalize to midnight UTC of that day
	t := time.UnixMilli(millis).UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}
