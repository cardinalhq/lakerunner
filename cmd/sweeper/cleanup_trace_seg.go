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

package sweeper

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// TraceCleanupWorkItem implements WorkItem for trace segment cleanup
type TraceCleanupWorkItem struct {
	OrganizationID   uuid.UUID
	DateInt          int32
	NextRunTime      time.Time
	ConsecutiveEmpty int
	sp               storageprofile.StorageProfileProvider
	mdb              lrdb.StoreFull
	cmgr             cloudstorage.ClientProvider
}

func (w *TraceCleanupWorkItem) Perform(ctx context.Context) WorkResult {
	ll := logctx.FromContext(ctx).With(
		slog.String("org_id", w.OrganizationID.String()),
		slog.Int("dateint", int(w.DateInt)),
		slog.String("signal_type", "trace"))

	ageThreshold := time.Now().Add(-time.Hour)

	segments, err := w.mdb.TraceSegmentCleanupGet(ctx, lrdb.TraceSegmentCleanupGetParams{
		OrganizationID: w.OrganizationID,
		Dateint:        w.DateInt,
		AgeThreshold:   ageThreshold,
		MaxRows:        1000,
	})
	if err != nil {
		ll.Error("Failed to get trace segments for cleanup", slog.Any("error", err))
		return WorkResultNoWork
	}

	if len(segments) == 0 {
		return WorkResultNoWork
	}

	// Process segments
	processed := 0
	for _, segment := range segments {
		// Get storage profile and delete from storage/database
		profile, err := w.sp.GetStorageProfileForOrganizationAndInstance(ctx, segment.OrganizationID, segment.InstanceNum)
		if err != nil {
			ll.Error("Failed to get storage profile", slog.Any("error", err))
			continue
		}

		objectKey := helpers.MakeDBObjectID(
			segment.OrganizationID,
			profile.CollectorName,
			segment.Dateint,
			getHourFromTimestamp(segment.TsRangeLower),
			segment.SegmentID,
			"traces",
		)

		if err := deleteSegmentObject(ctx, w.sp, w.cmgr, segment.OrganizationID, segment.InstanceNum, objectKey); err != nil {
			continue
		}

		// Delete from database
		if err := w.mdb.TraceSegmentCleanupDelete(ctx, lrdb.TraceSegmentCleanupDeleteParams{
			OrganizationID: segment.OrganizationID,
			Dateint:        segment.Dateint,
			SegmentID:      segment.SegmentID,
			InstanceNum:    segment.InstanceNum,
		}); err != nil {
			ll.Error("Failed to delete segment from database", slog.Any("error", err))
			continue
		}

		processed++

		// Update metrics
		traceCleanupCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("organization_id", segment.OrganizationID.String()),
		))
		traceCleanupBytes.Add(ctx, segment.FileSize, metric.WithAttributes(
			attribute.String("organization_id", segment.OrganizationID.String()),
		))
	}

	if processed > 0 {
		return WorkResultMaxWork
	}
	return WorkResultNoWork
}

func (w *TraceCleanupWorkItem) GetNextRunTime() time.Time  { return w.NextRunTime }
func (w *TraceCleanupWorkItem) SetNextRunTime(t time.Time) { w.NextRunTime = t }
func (w *TraceCleanupWorkItem) GetKey() string             { return makeOrgDateintKey(w.OrganizationID, w.DateInt) }

func (w *TraceCleanupWorkItem) UpdateBackoff(result WorkResult) {
	switch result {
	case WorkResultNoWork:
		w.ConsecutiveEmpty++
	case WorkResultLittle, WorkResultMaxWork:
		w.ConsecutiveEmpty = 0
	}

	w.NextRunTime = updateBackoffTiming(w.ConsecutiveEmpty)
}
