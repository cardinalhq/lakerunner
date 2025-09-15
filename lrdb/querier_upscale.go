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
)

type LogSegmentUpserter interface {
	InsertLogSegment(ctx context.Context, params InsertLogSegmentParams) error
	CompactLogSegsWithKafkaOffsets(ctx context.Context, params CompactLogSegsParams, kafkaOffsets []KafkaOffsetUpdate) error
}

type MetricSegmentInserter interface {
	InsertMetricSegment(ctx context.Context, params InsertMetricSegmentParams) error
	InsertMetricSegmentBatch(ctx context.Context, segments []InsertMetricSegmentParams, kafkaOffsets []KafkaOffsetInfo) error
	CompactMetricSegs(ctx context.Context, args CompactMetricSegsParams) error
	CompactMetricSegsWithKafkaOffsets(ctx context.Context, params CompactMetricSegsParams, kafkaOffsets []KafkaOffsetUpdate) error
	RollupMetricSegsWithKafkaOffsets(ctx context.Context, sourceParams RollupSourceParams, targetParams RollupTargetParams, sourceSegmentIDs []int64, newRecords []RollupNewRecord, kafkaOffsets []KafkaOffsetUpdate) error
}

type TraceSegmentInserter interface {
	InsertTraceSegment(ctx context.Context, params InsertTraceSegmentParams) error
	CompactTraceSegsWithKafkaOffsets(ctx context.Context, params CompactTraceSegsParams, kafkaOffsets []KafkaOffsetUpdate) error
}

type QuerierFull interface {
	Querier
	ParseMetricPartitions(ctx context.Context) ([]OrgDateintInfo, error)
	ParseLogPartitions(ctx context.Context) ([]OrgDateintInfo, error)
	ParseTracePartitions(ctx context.Context) ([]OrgDateintInfo, error)
}

type StoreFull interface {
	QuerierFull
	LogSegmentUpserter
	MetricSegmentInserter
	TraceSegmentInserter
	SegmentBatcher
}
