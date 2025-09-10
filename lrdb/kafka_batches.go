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
	"sort"

	"github.com/google/uuid"
)

// KafkaOffsetUpdate contains the information needed to update the Kafka offset journal
type KafkaOffsetUpdate struct {
	ConsumerGroup  string
	Topic          string
	Partition      int32
	Offset         int64
	OrganizationID uuid.UUID
	InstanceNum    int16
}

// LogSegmentBatch collects log segments to be inserted with Kafka offset updates
type LogSegmentBatch struct {
	Segments     []InsertLogSegmentParams
	KafkaOffsets []KafkaOffsetUpdate
}

// MetricSegmentBatch collects metric segments to be inserted with Kafka offset updates
type MetricSegmentBatch struct {
	Segments     []InsertMetricSegmentParams
	KafkaOffsets []KafkaOffsetUpdate
}

// TraceSegmentBatch collects trace segments to be inserted with Kafka offset updates
type TraceSegmentBatch struct {
	Segments     []InsertTraceSegmentParams
	KafkaOffsets []KafkaOffsetUpdate
}

// SegmentBatcher interface for database operations that support transactional batch insertion with Kafka offset updates
type SegmentBatcher interface {
	InsertLogSegmentBatchWithKafkaOffsets(ctx context.Context, batch LogSegmentBatch) error
	InsertMetricSegmentBatchWithKafkaOffsets(ctx context.Context, batch MetricSegmentBatch) error
	InsertTraceSegmentBatchWithKafkaOffsets(ctx context.Context, batch TraceSegmentBatch) error
}

// SortKafkaOffsets sorts Kafka offsets to prevent deadlocks in database operations.
// The sort order is: consumer group, then topic, then partition.
// This ensures consistent lock acquisition order across all database operations.
func SortKafkaOffsets(offsets []KafkaOffsetUpdate) {
	sort.Slice(offsets, func(i, j int) bool {
		a, b := offsets[i], offsets[j]
		if a.ConsumerGroup != b.ConsumerGroup {
			return a.ConsumerGroup < b.ConsumerGroup
		}
		if a.Topic != b.Topic {
			return a.Topic < b.Topic
		}
		return a.Partition < b.Partition
	})
}
