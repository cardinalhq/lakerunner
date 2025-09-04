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
	"context"
	"fmt"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// MetricRecorder mirrors the logs-side interface so counters stay consistent.
type MetricRecorder interface {
	RecordFilteredSegments(ctx context.Context, count int64, organizationID, instanceNum, signal, action, reason string)
}

// PackTraceSegmentsWithEstimate mirrors logs PackSegments:
// 1) filters zeros/invalids, 2) greedy packs by estimated record count,
// 3) emits filter/processed metrics via recorder.
func PackTraceSegmentsWithEstimate(
	ctx context.Context,
	segments []lrdb.GetTraceSegmentsForCompactionRow,
	estimatedRecordCount int64,
	recorder MetricRecorder,
	organizationID, instanceNum, signal, action string,
) ([][]lrdb.GetTraceSegmentsForCompactionRow, error) {
	if estimatedRecordCount <= 0 {
		return nil, fmt.Errorf("estimatedRecordCount must be positive, got %d", estimatedRecordCount)
	}
	if len(segments) == 0 {
		return [][]lrdb.GetTraceSegmentsForCompactionRow{}, nil
	}

	// (1) Drop zero-record segments up front.
	orig := len(segments)
	segments = filterZeroRecord(segments)
	if dropped := int64(orig - len(segments)); dropped > 0 {
		recorder.RecordFilteredSegments(ctx, dropped, organizationID, instanceNum, signal, action, "zero_records")
	}
	if len(segments) == 0 {
		return [][]lrdb.GetTraceSegmentsForCompactionRow{}, nil
	}

	// (2) Filter invalid time ranges + MinInt64 guard (parity with logs spirit).
	var (
		valid              = make([]lrdb.GetTraceSegmentsForCompactionRow, 0, len(segments))
		invalidRangeCount  int64
		minInt64EndTsCount int64
	)
	for _, seg := range segments {
		if seg.StartTs >= seg.EndTs {
			invalidRangeCount++
			continue
		}
		if seg.EndTs == -9223372036854775808 { // math.MinInt64 inline to avoid import
			minInt64EndTsCount++
			continue
		}
		valid = append(valid, seg)
	}
	if invalidRangeCount > 0 {
		recorder.RecordFilteredSegments(ctx, invalidRangeCount, organizationID, instanceNum, signal, action, "invalid_time_range")
	}
	if minInt64EndTsCount > 0 {
		recorder.RecordFilteredSegments(ctx, minInt64EndTsCount, organizationID, instanceNum, signal, action, "min_int64")
	}
	segments = valid
	if len(segments) == 0 {
		return [][]lrdb.GetTraceSegmentsForCompactionRow{}, nil
	}

	// (3) Greedy packing by record-count threshold (same as logs).
	groups := make([][]lrdb.GetTraceSegmentsForCompactionRow, 0, len(segments)/2+1)
	current := make([]lrdb.GetTraceSegmentsForCompactionRow, 0, 8)
	var sum int64

	for _, seg := range segments {
		rc := countOf(seg)
		if len(current) == 0 || sum+rc <= estimatedRecordCount {
			current = append(current, seg)
			sum += rc
			continue
		}
		groups = append(groups, current)
		current = []lrdb.GetTraceSegmentsForCompactionRow{seg}
		sum = rc
	}
	if len(current) > 0 {
		groups = append(groups, current)
	}

	return groups, nil
}

// ---- helpers ----

// filterZeroRecord keeps only segments with positive record/span count.
func filterZeroRecord(in []lrdb.GetTraceSegmentsForCompactionRow) []lrdb.GetTraceSegmentsForCompactionRow {
	out := make([]lrdb.GetTraceSegmentsForCompactionRow, 0, len(in))
	for _, seg := range in {
		if countOf(seg) > 0 {
			out = append(out, seg)
		}
	}
	return out
}

// countOf returns the unit used for packing.
// If your schema uses SpanCount or EventsCount, adjust this.
func countOf(seg lrdb.GetTraceSegmentsForCompactionRow) int64 {
	return seg.RecordCount
}
