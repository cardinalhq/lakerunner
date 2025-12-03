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

	"github.com/cardinalhq/lakerunner/internal/workqueue"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// MetricIngestStore defines database operations needed for metric ingestion
type MetricIngestStore interface {
	OffsetTrackerStore
	workqueue.DB
	InsertMetricSegmentsBatch(ctx context.Context, segments []lrdb.InsertMetricSegmentParams) error
	GetMetricEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64
	BatchUpsertExemplarMetrics(ctx context.Context, batch []lrdb.BatchUpsertExemplarMetricsParams) *lrdb.BatchUpsertExemplarMetricsBatchResults
	UpsertServiceIdentifier(ctx context.Context, arg lrdb.UpsertServiceIdentifierParams) (lrdb.UpsertServiceIdentifierRow, error)
}

// LogIngestStore defines database operations needed for log ingestion
type LogIngestStore interface {
	OffsetTrackerStore
	workqueue.DB
	InsertLogSegmentsBatch(ctx context.Context, segments []lrdb.InsertLogSegmentParams) error
	GetLogEstimate(ctx context.Context, orgID uuid.UUID) int64
	BatchUpsertExemplarLogs(ctx context.Context, batch []lrdb.BatchUpsertExemplarLogsParams) *lrdb.BatchUpsertExemplarLogsBatchResults
	UpsertServiceIdentifier(ctx context.Context, arg lrdb.UpsertServiceIdentifierParams) (lrdb.UpsertServiceIdentifierRow, error)
}

// TraceIngestStore defines database operations needed for trace ingestion
type TraceIngestStore interface {
	OffsetTrackerStore
	workqueue.DB
	InsertTraceSegmentsBatch(ctx context.Context, segments []lrdb.InsertTraceSegmentParams) error
	GetTraceEstimate(ctx context.Context, orgID uuid.UUID) int64
	BatchUpsertExemplarTraces(ctx context.Context, batch []lrdb.BatchUpsertExemplarTracesParams) *lrdb.BatchUpsertExemplarTracesBatchResults
	UpsertServiceIdentifier(ctx context.Context, arg lrdb.UpsertServiceIdentifierParams) (lrdb.UpsertServiceIdentifierRow, error)
}

// LogCompactionStore defines database operations needed for log compaction
type LogCompactionStore interface {
	OffsetTrackerStore
	workqueue.DB
	GetLogSeg(ctx context.Context, params lrdb.GetLogSegParams) (lrdb.LogSeg, error)
	CompactLogSegments(ctx context.Context, params lrdb.CompactLogSegsParams) error
	MarkLogSegsCompactedByKeys(ctx context.Context, params lrdb.MarkLogSegsCompactedByKeysParams) error
	GetLogEstimate(ctx context.Context, orgID uuid.UUID) int64
}

// MetricCompactionStore defines database operations needed for metric compaction
type MetricCompactionStore interface {
	OffsetTrackerStore
	workqueue.DB
	GetMetricSeg(ctx context.Context, params lrdb.GetMetricSegParams) (lrdb.MetricSeg, error)
	CompactMetricSegments(ctx context.Context, params lrdb.CompactMetricSegsParams) error
	MarkMetricSegsCompactedByKeys(ctx context.Context, params lrdb.MarkMetricSegsCompactedByKeysParams) error
	GetMetricEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64
}

// TraceCompactionStore defines database operations needed for trace compaction
type TraceCompactionStore interface {
	OffsetTrackerStore
	workqueue.DB
	GetTraceSeg(ctx context.Context, params lrdb.GetTraceSegParams) (lrdb.TraceSeg, error)
	CompactTraceSegments(ctx context.Context, params lrdb.CompactTraceSegsParams) error
	MarkTraceSegsCompactedByKeys(ctx context.Context, params lrdb.MarkTraceSegsCompactedByKeysParams) error
	GetTraceEstimate(ctx context.Context, orgID uuid.UUID) int64
}
