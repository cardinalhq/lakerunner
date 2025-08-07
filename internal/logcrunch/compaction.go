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

// PackSegments groups segments into packs whose total estimated size
// (based on estBytesPerRecord × record count) is close to targetSize.
//
// Parameters:
//   - segments: input slice of Segment, already sorted by StartTs ascending.
//   - targetSize: desired pack size in bytes.
//   - estBytesPerRecord: estimated average bytes per record.
//
// Returns a slice of packs; each pack is a slice of Segment.
func PackSegments(
	segments []lrdb.GetLogSegmentsForCompactionRow,
	targetSize int64,
	estBytesPerRecord float64,
) ([][]lrdb.GetLogSegmentsForCompactionRow, error) {
	if len(segments) == 0 {
		return nil, nil
	}

	// all segments must fall on the same day
	day := dayFromMillis(segments[0].StartTs)
	for _, seg := range segments {
		if dayFromMillis(seg.StartTs) != day || dayFromMillis(seg.EndTs-1) != day {
			return nil, fmt.Errorf("segments must be from the same day: %v", seg.StartTs)
		}
	}

	segments = filterSegments(segments)
	if len(segments) == 0 {
		return nil, nil
	}

	// compute how many records we want per group to approximate targetSize bytes
	targetRecords := float64(targetSize) / estBytesPerRecord

	var groups [][]lrdb.GetLogSegmentsForCompactionRow
	var current []lrdb.GetLogSegmentsForCompactionRow
	var sumRecords float64

	for _, seg := range segments {
		recCount := float64(seg.RecordCount)

		// start a new group if adding this segment would exceed targetRecords,
		// unless current group is empty (always include at least one)
		if len(current) == 0 || sumRecords+recCount <= targetRecords {
			current = append(current, seg)
			sumRecords += recCount
		} else {
			groups = append(groups, current)
			current = []lrdb.GetLogSegmentsForCompactionRow{seg}
			sumRecords = recCount
		}
	}

	// flush the last group
	if len(current) > 0 {
		groups = append(groups, current)
	}
	return groups, nil
}

// filterSegments drops any segments with zero records.
func filterSegments(segments []lrdb.GetLogSegmentsForCompactionRow) []lrdb.GetLogSegmentsForCompactionRow {
	filtered := make([]lrdb.GetLogSegmentsForCompactionRow, 0, len(segments))
	for _, seg := range segments {
		if seg.RecordCount > 0 {
			filtered = append(filtered, seg)
		}
	}
	return filtered
}

// dayFromMillis converts milliseconds since epoch to a UTC date at 00:00:00.
func dayFromMillis(millis int64) time.Time {
	return time.Unix(millis/1000, (millis%1000)*int64(time.Millisecond)).
		Truncate(24 * time.Hour).UTC()
}
