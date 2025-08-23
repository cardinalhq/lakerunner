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

package metriccompaction

import (
	"fmt"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// PackMetricSegments groups metric segments into packs such that the estimated
// total size in each pack is suitable for compaction. This is similar to logs
// packing but adapted for metrics compaction requirements.
func PackMetricSegments(segments []lrdb.MetricSeg, estimatedRecordCount int64) ([][]lrdb.MetricSeg, error) {
	if estimatedRecordCount <= 0 {
		return nil, fmt.Errorf("estimatedRecordCount must be positive, got %d", estimatedRecordCount)
	}
	if len(segments) == 0 {
		return [][]lrdb.MetricSeg{}, nil
	}

	// Filter out zero-record segments
	filteredSegments := make([]lrdb.MetricSeg, 0, len(segments))
	for _, seg := range segments {
		if seg.RecordCount > 0 && seg.FileSize > 0 {
			filteredSegments = append(filteredSegments, seg)
		}
	}

	if len(filteredSegments) == 0 {
		return [][]lrdb.MetricSeg{}, nil
	}

	// Greedy packing by record count threshold
	groups := make([][]lrdb.MetricSeg, 0, len(filteredSegments)/2+1)
	current := make([]lrdb.MetricSeg, 0, 8)
	var sumRecords int64

	for _, seg := range filteredSegments {
		rc := seg.RecordCount
		if len(current) == 0 || sumRecords+rc <= estimatedRecordCount {
			current = append(current, seg)
			sumRecords += rc
			continue
		}
		groups = append(groups, current)
		current = []lrdb.MetricSeg{seg}
		sumRecords = rc
	}
	if len(current) > 0 {
		groups = append(groups, current)
	}

	return groups, nil
}

// ShouldCompactMetricGroup determines if a group of segments should be compacted.
// This is a simpler version that focuses on whether compaction would be beneficial.
func ShouldCompactMetricGroup(segments []lrdb.MetricSeg) bool {
	if len(segments) < 2 {
		return false
	}

	const smallThreshold = int64(targetFileSize) * 3 / 10

	var totalSize int64
	hasSmallFiles := false
	hasLargeFiles := false

	for _, seg := range segments {
		totalSize += seg.FileSize
		if seg.FileSize < smallThreshold {
			hasSmallFiles = true
		}
		if seg.FileSize > targetFileSize*2 {
			hasLargeFiles = true
		}
	}

	// Compact if we have small files that can be combined, or large files that should be split
	if hasSmallFiles || hasLargeFiles {
		return true
	}

	// Also compact if we can significantly reduce the number of files
	estimatedFileCount := (totalSize + targetFileSize - 1) / targetFileSize
	return estimatedFileCount < int64(len(segments))-2
}

// ExtractTidPartition extracts the tid_partition from a TID value.
// TID partition is the upper 8 bits of the TID (TID >> 56).
func ExtractTidPartition(tid int64) int16 {
	return int16(tid >> 56)
}
