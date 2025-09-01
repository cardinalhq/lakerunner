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
	"context"
	"fmt"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// MetricRecorder is an interface for recording compaction metrics
type MetricRecorder interface {
	RecordFilteredSegments(ctx context.Context, count int64, organizationID, instanceNum, signal, action, reason string)
}

// NoOpMetricRecorder is a no-op implementation for testing
type NoOpMetricRecorder struct{}

func (NoOpMetricRecorder) RecordFilteredSegments(context.Context, int64, string, string, string, string, string) {
}

// PackSegments groups segments into packs such that the sum of RecordCount in each
// pack is <= estimatedRecordCount (greedy, one-or-more per pack).
// NOTE: segments must all lie within the same UTC hour and must be sorted by StartTs ascending.
func PackSegments(ctx context.Context, segments []lrdb.GetLogSegmentsForCompactionRow, estimatedRecordCount int64, recorder MetricRecorder, organizationID, instanceNum, signal, action string) ([][]lrdb.GetLogSegmentsForCompactionRow, error) {
	if estimatedRecordCount <= 0 {
		return nil, fmt.Errorf("estimatedRecordCount must be positive, got %d", estimatedRecordCount)
	}
	if len(segments) == 0 {
		return [][]lrdb.GetLogSegmentsForCompactionRow{}, nil
	}

	// 1) Drop zero-record segments up front.
	originalCount := len(segments)
	segments = filterSegments(segments)
	if filteredCount := int64(originalCount - len(segments)); filteredCount > 0 {
		recorder.RecordFilteredSegments(ctx, filteredCount, organizationID, instanceNum, signal, action, "zero_records")
	}
	if len(segments) == 0 {
		return [][]lrdb.GetLogSegmentsForCompactionRow{}, nil
	}

	// 2) Filter out segments that cross hour boundaries during transition period.
	beforeHourFilter := len(segments)
	segments = filterHourConformingSegments(segments)
	if filteredCount := int64(beforeHourFilter - len(segments)); filteredCount > 0 {
		recorder.RecordFilteredSegments(ctx, filteredCount, organizationID, instanceNum, signal, action, "hour_boundary")
	}
	if len(segments) == 0 {
		return [][]lrdb.GetLogSegmentsForCompactionRow{}, nil
	}

	// 3) Filter out segments with invalid time ranges and ensure hour conformity.
	validSegments := make([]lrdb.GetLogSegmentsForCompactionRow, 0, len(segments))
	var hour int64 = -1
	var invalidTimeRangeCount, minInt64Count, crossHourCount, differentHourCount int64

	for _, seg := range segments {
		// Skip segments with invalid time ranges
		if seg.StartTs >= seg.EndTs {
			invalidTimeRangeCount++
			continue
		}
		// Guard against MinInt64
		if seg.EndTs == -9223372036854775808 { // math.MinInt64, inline to avoid import
			minInt64Count++
			continue
		}

		// Use end-1 to keep [start,end) semantics in the same hour
		endMinusOne := seg.EndTs - 1
		segStartHour := hourFromMillis(seg.StartTs)
		segEndHour := hourFromMillis(endMinusOne)

		// Skip segments that cross hour boundaries
		if segStartHour != segEndHour {
			crossHourCount++
			continue
		}

		// Set hour from first valid segment, then ensure all subsequent segments are in same hour
		if hour == -1 {
			hour = segStartHour
		} else if segStartHour != hour {
			// Skip segments from different hours
			differentHourCount++
			continue
		}

		validSegments = append(validSegments, seg)
	}

	// Record metrics for different filtering reasons
	if invalidTimeRangeCount > 0 {
		recorder.RecordFilteredSegments(ctx, invalidTimeRangeCount, organizationID, instanceNum, signal, action, "invalid_time_range")
	}
	if minInt64Count > 0 {
		recorder.RecordFilteredSegments(ctx, minInt64Count, organizationID, instanceNum, signal, action, "min_int64")
	}
	if crossHourCount > 0 {
		recorder.RecordFilteredSegments(ctx, crossHourCount, organizationID, instanceNum, signal, action, "cross_hour")
	}
	if differentHourCount > 0 {
		recorder.RecordFilteredSegments(ctx, differentHourCount, organizationID, instanceNum, signal, action, "different_hour")
	}

	// Update segments to only include valid ones
	segments = validSegments
	if len(segments) == 0 {
		return [][]lrdb.GetLogSegmentsForCompactionRow{}, nil
	}

	// 4) Greedy packing by record count threshold.
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
	j := 0
	for _, s := range segs {
		if s.RecordCount > 0 {
			segs[j] = s
			j++
		}
	}
	return segs[:j]
}

const msPerHour int64 = 3_600_000

func hourFromMillis(millis int64) int64 {
	return millis / msPerHour
}

// filterHourConformingSegments removes segments that cross hour boundaries.
// This is used during the transition period to avoid compacting segments
// that don't conform to the new hour-boundary rule.
func filterHourConformingSegments(segs []lrdb.GetLogSegmentsForCompactionRow) []lrdb.GetLogSegmentsForCompactionRow {
	j := 0
	for _, s := range segs {
		// Skip segments with invalid time ranges - let validation handle them later
		if s.StartTs >= s.EndTs {
			segs[j] = s
			j++
			continue
		}

		// Check if segment stays within the same hour
		startHour := hourFromMillis(s.StartTs)
		endHour := hourFromMillis(s.EndTs - 1) // end is exclusive, so subtract 1ms
		if startHour == endHour {
			segs[j] = s
			j++
		}
		// If startHour != endHour, segment crosses hour boundary and we skip it
	}
	return segs[:j]
}
