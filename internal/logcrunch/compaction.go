// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logcrunch

import (
	"fmt"
	"time"

	"github.com/cardinalhq/lakerunner/pkg/lrdb"
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
