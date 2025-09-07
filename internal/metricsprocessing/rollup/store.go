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

package rollup

import (
	"context"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// rollupStore defines the database operations needed for rollup processing
type rollupStore interface {
	cloudstorage.ObjectCleanupStore
	// Kafka offset tracking
	KafkaJournalGetLastProcessed(ctx context.Context, params lrdb.KafkaJournalGetLastProcessedParams) (int64, error)
	KafkaJournalUpsert(ctx context.Context, params lrdb.KafkaJournalUpsertParams) error
	// Segment operations
	GetMetricSegsByIds(ctx context.Context, params lrdb.GetMetricSegsByIdsParams) ([]lrdb.MetricSeg, error)
	GetMetricSegByPrimaryKey(ctx context.Context, params lrdb.GetMetricSegByPrimaryKeyParams) (lrdb.MetricSeg, error)
	RollupMetricSegs(ctx context.Context, sourceParams lrdb.RollupSourceParams, targetParams lrdb.RollupTargetParams, sourceSegmentIDs []int64, newRecords []lrdb.RollupNewRecord) error
	// Metric estimates
	GetMetricEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64
}
