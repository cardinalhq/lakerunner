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

// Copyright (C) 2025 CardinalHQ, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the GNU Affero General Public License, version 3.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR ANY PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package tracecompaction

import (
	"fmt"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// PackTraceSegments groups segments into packs such that the sum of FileSize in each
// pack is <= targetFileSize (greedy, one-or-more per pack).
// NOTE: segments must all be from the same slot_id
func PackTraceSegments(segments []lrdb.GetTraceSegmentsForCompactionRow, targetFileSize int64) ([][]lrdb.GetTraceSegmentsForCompactionRow, error) {
	if targetFileSize <= 0 {
		return nil, fmt.Errorf("targetFileSize must be positive, got %d", targetFileSize)
	}
	if len(segments) == 0 {
		return [][]lrdb.GetTraceSegmentsForCompactionRow{}, nil
	}

	// 1) Drop zero-record segments up front.
	segments = filterTraceSegments(segments)
	if len(segments) == 0 {
		return [][]lrdb.GetTraceSegmentsForCompactionRow{}, nil
	}

	// 3) Greedy packing by file size threshold.
	groups := make([][]lrdb.GetTraceSegmentsForCompactionRow, 0, len(segments)/2+1)
	current := make([]lrdb.GetTraceSegmentsForCompactionRow, 0, 8)
	var sumFileSize int64

	for _, seg := range segments {
		fs := seg.FileSize
		if len(current) == 0 || sumFileSize+fs <= targetFileSize {
			current = append(current, seg)
			sumFileSize += fs
			continue
		}
		groups = append(groups, current)
		current = []lrdb.GetTraceSegmentsForCompactionRow{seg}
		sumFileSize = fs
	}
	if len(current) > 0 {
		groups = append(groups, current)
	}
	return groups, nil
}

// filterTraceSegments removes segments with zero record count
func filterTraceSegments(segs []lrdb.GetTraceSegmentsForCompactionRow) []lrdb.GetTraceSegmentsForCompactionRow {
	j := 0
	for _, s := range segs {
		if s.RecordCount > 0 {
			segs[j] = s
			j++
		}
	}
	return segs[:j]
}
