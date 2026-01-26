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

package metricsprocessing

import (
	"context"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/workqueue"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// MetricIngestStore defines database operations needed for metric ingestion
type MetricIngestStore interface {
	workqueue.DB
	InsertMetricSegmentsBatch(ctx context.Context, segments []lrdb.InsertMetricSegmentParams) error
	GetMetricEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64
}

// LogIngestStore defines database operations needed for log ingestion
type LogIngestStore interface {
	workqueue.DB
	InsertLogSegmentsBatch(ctx context.Context, segments []lrdb.InsertLogSegmentParams) error
	GetLogEstimate(ctx context.Context, orgID uuid.UUID) int64
}

// TraceIngestStore defines database operations needed for trace ingestion
type TraceIngestStore interface {
	workqueue.DB
	InsertTraceSegmentsBatch(ctx context.Context, segments []lrdb.InsertTraceSegmentParams) error
	GetTraceEstimate(ctx context.Context, orgID uuid.UUID) int64
}

// LogCompactionStore defines database operations needed for log compaction
type LogCompactionStore interface {
	workqueue.DB
	GetLogSeg(ctx context.Context, params lrdb.GetLogSegParams) (lrdb.LogSeg, error)
	CompactLogSegments(ctx context.Context, params lrdb.CompactLogSegsParams) error
	MarkLogSegsCompactedByKeys(ctx context.Context, params lrdb.MarkLogSegsCompactedByKeysParams) error
	GetLogEstimate(ctx context.Context, orgID uuid.UUID) int64
}

// MetricCompactionStore defines database operations needed for metric compaction
type MetricCompactionStore interface {
	workqueue.DB
	GetMetricSeg(ctx context.Context, params lrdb.GetMetricSegParams) (lrdb.MetricSeg, error)
	CompactMetricSegments(ctx context.Context, params lrdb.CompactMetricSegsParams) error
	MarkMetricSegsCompactedByKeys(ctx context.Context, params lrdb.MarkMetricSegsCompactedByKeysParams) error
	GetMetricEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64
}

// TraceCompactionStore defines database operations needed for trace compaction
type TraceCompactionStore interface {
	workqueue.DB
	GetTraceSeg(ctx context.Context, params lrdb.GetTraceSegParams) (lrdb.TraceSeg, error)
	CompactTraceSegments(ctx context.Context, params lrdb.CompactTraceSegsParams) error
	MarkTraceSegsCompactedByKeys(ctx context.Context, params lrdb.MarkTraceSegsCompactedByKeysParams) error
	GetTraceEstimate(ctx context.Context, orgID uuid.UUID) int64
}
