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

package accumulation

import (
	"context"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// AccumulationKey uniquely identifies an accumulation group
type AccumulationKey struct {
	OrganizationID uuid.UUID
	Dateint        int32
	FrequencyMs    int32
	InstanceNum    int16
	SlotID         int32 // Used by rollup
	SlotCount      int32 // Used by rollup
}

// AccumulationWork represents work to be processed
type AccumulationWork struct {
	Segments []lrdb.MetricSeg
	Key      AccumulationKey
	Profile  storageprofile.StorageProfile
}

// ProcessedParams contains parameters for marking segments as processed
type ProcessedParams struct {
	Key        AccumulationKey
	OldRecords []lrdb.MetricSeg
	NewRecords []lrdb.MetricSeg
	Profile    storageprofile.StorageProfile
}

// Strategy defines the interface for different accumulation strategies (compaction, rollup, etc.)
type Strategy interface {
	// Key management
	GetAccumulationKey(notification *messages.MetricSegmentNotificationMessage) AccumulationKey

	// Processing decisions
	ShouldProcess(notification *messages.MetricSegmentNotificationMessage) bool
	IsAlreadyOptimized(notification *messages.MetricSegmentNotificationMessage, segment lrdb.MetricSeg, rpfEstimate int64, targetFileSize int64) bool

	// Target frequency for aggregation (same as source for compaction, different for rollup)
	GetTargetFrequency(key AccumulationKey) int32

	// Database operations
	MarkSegmentsProcessed(ctx context.Context, db Store, params ProcessedParams) error
	GetSourceSegments(ctx context.Context, db Store, notification *messages.MetricSegmentNotificationMessage) ([]lrdb.MetricSeg, error)

	// Kafka routing
	GetOutputTopics(key AccumulationKey) []string
	GetConsumerGroup() string
	GetSourceTopic() string

	// Processing configuration
	GetMaxAccumulationTime() string
	GetTargetFileSizeBytes() int64
}

// Store defines the database operations needed by the accumulation framework
type Store interface {
	// Common operations
	GetMetricEstimate(ctx context.Context, organizationID uuid.UUID, frequencyMs int32) int64
	KafkaJournalUpsert(ctx context.Context, params lrdb.KafkaJournalUpsertParams) error
	KafkaJournalGetLastProcessed(ctx context.Context, params lrdb.KafkaJournalGetLastProcessedParams) (int64, error)

	// Strategy-specific operations to be called by the strategies
	GetMetricSegByPrimaryKey(ctx context.Context, params lrdb.GetMetricSegByPrimaryKeyParams) (lrdb.MetricSeg, error)
	SetSingleMetricSegCompacted(ctx context.Context, params lrdb.SetSingleMetricSegCompactedParams) error
	CompactMetricSegs(ctx context.Context, params lrdb.CompactMetricSegsParams) error
	RollupMetricSegs(ctx context.Context, sourceParams lrdb.RollupSourceParams, targetParams lrdb.RollupTargetParams, sourceSegmentIDs []int64, newRecords []lrdb.RollupNewRecord) error
}
