// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lrdb

import (
	"context"
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
	WorkQueueHeartbeat(ctx context.Context, params WorkQueueHeartbeatParams) error
	WorkQueueCleanup(ctx context.Context) ([]WorkQueueCleanupRow, error)
	WorkQueueClaim(ctx context.Context, params WorkQueueClaimParams) (WorkQueueClaimRow, error)
}

type StoreFull interface {
	QuerierFull
	LogSegmentUpserter
	MetricSegmentInserter
	WorkQueueQuerier
}
