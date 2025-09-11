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

// IngestStore defines common database operations needed for ingestion (Kafka journaling)
type IngestStore interface {
	KafkaGetLastProcessed(ctx context.Context, params lrdb.KafkaGetLastProcessedParams) (int64, error)
}

// MetricIngestStore defines database operations needed for metric ingestion
type MetricIngestStore interface {
	IngestStore
	InsertMetricSegmentBatchWithKafkaOffsets(ctx context.Context, batch lrdb.MetricSegmentBatch) error
	GetMetricEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64
	BatchUpsertExemplarMetrics(ctx context.Context, batch []lrdb.BatchUpsertExemplarMetricsParams) *lrdb.BatchUpsertExemplarMetricsBatchResults
	UpsertServiceIdentifier(ctx context.Context, arg lrdb.UpsertServiceIdentifierParams) (lrdb.UpsertServiceIdentifierRow, error)
}

// LogIngestStore defines database operations needed for log ingestion
type LogIngestStore interface {
	IngestStore
	InsertLogSegmentBatchWithKafkaOffsets(ctx context.Context, batch lrdb.LogSegmentBatch) error
	GetLogEstimate(ctx context.Context, orgID uuid.UUID) int64
	BatchUpsertExemplarLogs(ctx context.Context, batch []lrdb.BatchUpsertExemplarLogsParams) *lrdb.BatchUpsertExemplarLogsBatchResults
	UpsertServiceIdentifier(ctx context.Context, arg lrdb.UpsertServiceIdentifierParams) (lrdb.UpsertServiceIdentifierRow, error)
}

// TraceIngestStore defines database operations needed for trace ingestion
type TraceIngestStore interface {
	IngestStore
	InsertTraceSegmentBatchWithKafkaOffsets(ctx context.Context, batch lrdb.TraceSegmentBatch) error
	GetTraceEstimate(ctx context.Context, orgID uuid.UUID) int64
}
