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

package metricsprocessing

import (
	"context"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// MetricIngestStore defines database operations needed for metric ingestion
type MetricIngestStore interface {
	OffsetTrackerStore
	InsertMetricSegmentsBatch(ctx context.Context, segments []lrdb.InsertMetricSegmentParams, kafkaOffsets []lrdb.KafkaOffsetInfo) error
	GetMetricEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64
	BatchUpsertExemplarMetrics(ctx context.Context, batch []lrdb.BatchUpsertExemplarMetricsParams) *lrdb.BatchUpsertExemplarMetricsBatchResults
	UpsertServiceIdentifier(ctx context.Context, arg lrdb.UpsertServiceIdentifierParams) (lrdb.UpsertServiceIdentifierRow, error)
}

// LogIngestStore defines database operations needed for log ingestion
type LogIngestStore interface {
	OffsetTrackerStore
	InsertLogSegmentsBatch(ctx context.Context, segments []lrdb.InsertLogSegmentParams, kafkaOffsets []lrdb.KafkaOffsetInfo) error
	GetLogEstimate(ctx context.Context, orgID uuid.UUID) int64
	BatchUpsertExemplarLogs(ctx context.Context, batch []lrdb.BatchUpsertExemplarLogsParams) *lrdb.BatchUpsertExemplarLogsBatchResults
	UpsertServiceIdentifier(ctx context.Context, arg lrdb.UpsertServiceIdentifierParams) (lrdb.UpsertServiceIdentifierRow, error)
}

// TraceIngestStore defines database operations needed for trace ingestion
type TraceIngestStore interface {
	OffsetTrackerStore
	InsertTraceSegmentsBatch(ctx context.Context, segments []lrdb.InsertTraceSegmentParams, kafkaOffsets []lrdb.KafkaOffsetInfo) error
	GetTraceEstimate(ctx context.Context, orgID uuid.UUID) int64
}

// LogCompactionStore defines database operations needed for log compaction
type LogCompactionStore interface {
	OffsetTrackerStore
	GetLogSeg(ctx context.Context, params lrdb.GetLogSegParams) (lrdb.LogSeg, error)
	CompactLogSegments(ctx context.Context, params lrdb.CompactLogSegsParams, kafkaOffsets []lrdb.KafkaOffsetInfo) error
	MarkLogSegsCompactedByKeys(ctx context.Context, params lrdb.MarkLogSegsCompactedByKeysParams) error
	GetLogEstimate(ctx context.Context, orgID uuid.UUID) int64
}

// MetricCompactionStore defines database operations needed for metric compaction
type MetricCompactionStore interface {
	OffsetTrackerStore
	GetMetricSeg(ctx context.Context, params lrdb.GetMetricSegParams) (lrdb.MetricSeg, error)
	CompactMetricSegments(ctx context.Context, params lrdb.CompactMetricSegsParams, kafkaOffsets []lrdb.KafkaOffsetInfo) error
	MarkMetricSegsCompactedByKeys(ctx context.Context, params lrdb.MarkMetricSegsCompactedByKeysParams) error
	GetMetricEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64
}

// TraceCompactionStore defines database operations needed for trace compaction
type TraceCompactionStore interface {
	OffsetTrackerStore
	GetTraceSeg(ctx context.Context, params lrdb.GetTraceSegParams) (lrdb.TraceSeg, error)
	CompactTraceSegments(ctx context.Context, params lrdb.CompactTraceSegsParams, kafkaOffsets []lrdb.KafkaOffsetInfo) error
	MarkTraceSegsCompactedByKeys(ctx context.Context, params lrdb.MarkTraceSegsCompactedByKeysParams) error
	GetTraceEstimate(ctx context.Context, orgID uuid.UUID) int64
}
