// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package lrdb

import (
	"context"

	"github.com/google/uuid"
)

// Sort version constants for metric segments
const (
	// MetricSortVersionUnknown indicates the file's sort order is unknown or unsorted (legacy files)
	MetricSortVersionUnknown = 0
	// MetricSortVersionNameTidTimestamp indicates the file is sorted by [metric_name, tid, timestamp] (old TID calculation)
	MetricSortVersionNameTidTimestamp = 1
	// MetricSortVersionNameTidTimestampV2 indicates the file is sorted by [metric_name, tid, timestamp] (new TID calculation)
	MetricSortVersionNameTidTimestampV2 = 2
	// due to a bug, we will move everyone to 3, same key though...
	MetricSortVersionNameTidTimestampV3 = 3
)

// MetricType constants matching OTEL metric data types
const (
	MetricTypeUnknown              int16 = 0
	MetricTypeGauge                int16 = 1
	MetricTypeSum                  int16 = 2
	MetricTypeHistogram            int16 = 3
	MetricTypeExponentialHistogram int16 = 4
	MetricTypeSummary              int16 = 5
)

// MetricTypeFromString converts a string metric type to its int16 constant
func MetricTypeFromString(s string) int16 {
	switch s {
	case "gauge":
		return MetricTypeGauge
	case "sum", "counter", "count":
		return MetricTypeSum
	case "histogram":
		return MetricTypeHistogram
	case "exponential_histogram":
		return MetricTypeExponentialHistogram
	case "summary":
		return MetricTypeSummary
	default:
		return MetricTypeUnknown
	}
}

// MetricTypeToString converts an int16 metric type constant to its string representation
func MetricTypeToString(t int16) string {
	switch t {
	case MetricTypeGauge:
		return "gauge"
	case MetricTypeSum:
		return "sum"
	case MetricTypeHistogram:
		return "histogram"
	case MetricTypeExponentialHistogram:
		return "exponential_histogram"
	case MetricTypeSummary:
		return "summary"
	default:
		return "unknown"
	}
}

// Current metric sort configuration - single source of truth for all metric sorting
const (
	// CurrentMetricSortVersion is the sort version used for all newly created metric segments
	CurrentMetricSortVersion = MetricSortVersionNameTidTimestampV3
)

func (q *Store) InsertMetricSegment(ctx context.Context, params InsertMetricSegmentParams) error {
	if err := q.ensureMetricSegmentPartition(ctx, params.OrganizationID, params.Dateint); err != nil {
		return err
	}
	return q.insertMetricSegDirect(ctx, params)
}

type CompactMetricSegsOld struct {
	SegmentID int64
}

type CompactMetricSegsNew struct {
	SegmentID    int64
	StartTs      int64
	EndTs        int64
	RecordCount  int64
	FileSize     int64
	Fingerprints []int64
	MetricNames  []string
	MetricTypes  []int16
}

type RollupSourceParams struct {
	OrganizationID uuid.UUID
	Dateint        int32
	FrequencyMs    int32
	InstanceNum    int16
}

type RollupTargetParams struct {
	OrganizationID uuid.UUID
	Dateint        int32
	FrequencyMs    int32
	InstanceNum    int16
	IngestDateint  int32
	SortVersion    int16
}

type RollupNewRecord struct {
	SegmentID    int64
	StartTs      int64
	EndTs        int64
	RecordCount  int64
	FileSize     int64
	Fingerprints []int64
	MetricNames  []string
	MetricTypes  []int16
	LabelNameMap []byte
}

type CompactMetricSegsParams struct {
	// OrganizationID is the ID of the organization to which the metric segments belong.
	OrganizationID uuid.UUID
	// Dateint is the date in YYYYMMDD format for which the metric segments are being replaced.
	Dateint int32
	// InstanceNum is the instance number for which the segments are being replaced.
	InstanceNum int16
	// IngestDateint is the date in YYYYMMDD format when the segments were ingested.
	IngestDateint int32
	// FrequencyMs is the frequency in milliseconds at which the metrics are collected.
	FrequencyMs int32
	// OldRecords contains the segments to be deleted.
	OldRecords []CompactMetricSegsOld
	// NewRecords contains the segments to be inserted.
	NewRecords []CompactMetricSegsNew
	CreatedBy  CreatedBy
}
