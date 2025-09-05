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

// KafkaOffsetUpdate contains the information needed to update the Kafka offset journal
type KafkaOffsetUpdate struct {
	ConsumerGroup string
	Topic         string
	Partition     int32
	Offset        int64
}

// LogSegmentBatch collects log segments to be inserted with a Kafka offset update
type LogSegmentBatch struct {
	Segments    []InsertLogSegmentParams
	KafkaOffset KafkaOffsetUpdate
}

// MetricSegmentBatch collects metric segments to be inserted with a Kafka offset update
type MetricSegmentBatch struct {
	Segments    []InsertMetricSegmentParams
	KafkaOffset KafkaOffsetUpdate
}

// TraceSegmentBatch collects trace segments to be inserted with a Kafka offset update
type TraceSegmentBatch struct {
	Segments    []InsertTraceSegmentDirectParams
	KafkaOffset KafkaOffsetUpdate
}

// SegmentBatcher interface for database operations that support transactional batch insertion with Kafka offset updates
type SegmentBatcher interface {
	InsertLogSegmentBatchWithKafkaOffset(ctx context.Context, batch LogSegmentBatch) error
	InsertMetricSegmentBatchWithKafkaOffset(ctx context.Context, batch MetricSegmentBatch) error
	InsertTraceSegmentBatchWithKafkaOffset(ctx context.Context, batch TraceSegmentBatch) error
}
