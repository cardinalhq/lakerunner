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

package lrdb

import (
	"context"
	"time"
)

type LogSegmentUpserter interface {
	InsertLogSegment(ctx context.Context, params InsertLogSegmentParams) error
}

type MetricSegmentInserter interface {
	InsertMetricSegment(ctx context.Context, params InsertMetricSegmentParams) error
	CompactMetricSegs(ctx context.Context, args CompactMetricSegsParams) error
	CompactMetricSegsWithKafkaOffsets(ctx context.Context, params CompactMetricSegsParams, kafkaOffsets []KafkaOffsetUpdate) error
	RollupMetricSegsWithKafkaOffsets(ctx context.Context, sourceParams RollupSourceParams, targetParams RollupTargetParams, sourceSegmentIDs []int64, newRecords []RollupNewRecord, kafkaOffsets []KafkaOffsetUpdate) error
}

type TraceSegmentInserter interface {
	InsertTraceSegment(ctx context.Context, params InsertTraceSegmentDirectParams) error
}

type QuerierFull interface {
	Querier
}

type WorkQueueQuerier interface {
	WorkQueueAdd(ctx context.Context, params WorkQueueAddParams) error
	WorkQueueFail(ctx context.Context, params WorkQueueFailParams) error
	WorkQueueComplete(ctx context.Context, params WorkQueueCompleteParams) error
	WorkQueueDelete(ctx context.Context, params WorkQueueDeleteParams) error
	WorkQueueHeartbeat(ctx context.Context, params WorkQueueHeartbeatParams) error
	WorkQueueCleanup(ctx context.Context, lockTtlDead time.Duration) ([]WorkQueueCleanupRow, error)
	WorkQueueClaim(ctx context.Context, params WorkQueueClaimParams) (WorkQueueClaimRow, error)
}

type StoreFull interface {
	QuerierFull
	LogSegmentUpserter
	MetricSegmentInserter
	TraceSegmentInserter
	WorkQueueQuerier
	SegmentBatcher
}
