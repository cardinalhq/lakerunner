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

	"github.com/jackc/pgx/v5/pgtype"
)

type LogSegmentUpserter interface {
	//InsertLogFingerprints(ctx context.Context, params InsertLogFingerprintsParams) error
	InsertLogSegment(ctx context.Context, params InsertLogSegmentParams) error
}

type MetricSegmentInserter interface {
	InsertMetricSegment(ctx context.Context, params InsertMetricSegmentParams) error
}

type QuerierFull interface {
	Querier
	ReplaceMetricSegs(ctx context.Context, args ReplaceMetricSegsParams) error
}

type WorkQueueQuerier interface {
	WorkQueueAdd(ctx context.Context, params WorkQueueAddParams) error
	WorkQueueFail(ctx context.Context, params WorkQueueFailParams) error
	WorkQueueComplete(ctx context.Context, params WorkQueueCompleteParams) error
	WorkQueueDelete(ctx context.Context, params WorkQueueDeleteParams) error
	WorkQueueHeartbeat(ctx context.Context, params WorkQueueHeartbeatParams) error
	WorkQueueCleanup(ctx context.Context, lockTtlDead pgtype.Interval) ([]WorkQueueCleanupRow, error)
	WorkQueueClaim(ctx context.Context, params WorkQueueClaimParams) (WorkQueueClaimRow, error)
}

type StoreFull interface {
	QuerierFull
	LogSegmentUpserter
	MetricSegmentInserter
	WorkQueueQuerier
}
