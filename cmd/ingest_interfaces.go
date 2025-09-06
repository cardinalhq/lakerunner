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

package cmd

import (
	"context"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// KafkaJournalDB interface for Kafka offset tracking operations
type KafkaJournalDB interface {
	KafkaJournalGetLastProcessed(ctx context.Context, params lrdb.KafkaJournalGetLastProcessedParams) (int64, error)
	KafkaJournalUpsert(ctx context.Context, params lrdb.KafkaJournalUpsertParams) error
}

// LogSegmentDB interface for log segment operations
type LogSegmentDB interface {
	InsertLogSegment(ctx context.Context, params lrdb.InsertLogSegmentParams) error
}

// MetricSegmentDB interface for metric segment operations
type MetricSegmentDB interface {
	InsertMetricSegment(ctx context.Context, params lrdb.InsertMetricSegmentParams) error
}

// TraceSegmentDB interface for trace segment operations
type TraceSegmentDB interface {
	InsertTraceSegment(ctx context.Context, params lrdb.InsertTraceSegmentDirectParams) error
}

// Verify that lrdb.StoreFull implements all our focused interfaces
var _ KafkaJournalDB = (lrdb.StoreFull)(nil)
var _ LogSegmentDB = (lrdb.StoreFull)(nil)
var _ MetricSegmentDB = (lrdb.StoreFull)(nil)
var _ TraceSegmentDB = (lrdb.StoreFull)(nil)
